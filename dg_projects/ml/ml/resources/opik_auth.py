"""ol-llm's process configuration, LLM tracing, and Prompt Library access.

configure_ol_llm hands Opik's Keycloak authentication to ol-llm, which registers
it on the Opik SDK. The @opik.track spans below then share its token cache with
the root traces of ol-llm's runs.
"""

from __future__ import annotations

import dataclasses
import logging
import os
from typing import TYPE_CHECKING, Any, TypeVar

import opik
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from ml.resources.llm import COGNITIVE_SERVICES_SCOPE
from ol_llm import (
    KeycloakClient,
    Settings,
    TracingConfig,
    configure,
    configure_tracing,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.opik import opik_keycloak_vault_location
from opik import opik_context

if TYPE_CHECKING:
    from collections.abc import Callable

    from ol_orchestrate.resources.secrets.vault import Vault

F = TypeVar("F", bound="Callable[..., Any]")

log = logging.getLogger(__name__)

OPIK_PROJECT_NAME = os.environ.get("OPIK_PROJECT_NAME", "dagster-ml")
LOCAL_PROMPT_VERSION = "local"

_configured = False
_opik_client: opik.Opik | None = None
_client_init_attempted = False
# template -> the Prompt Library version publish_prompt_version recorded for it
_published_prompts: dict[str, opik.Prompt] = {}
# Every deployment shares the Production Opik, so each one reads and publishes
# prompts through its own Opik environment: a version takes effect only in the
# environments that point at it, and editing a prompt for QA leaves Production
# on its version until Production is pointed at the new one.
PROMPT_ENVIRONMENT: str = DAGSTER_ENV
# Metadata key naming the environment a version was created for. Versions
# without it predate environment scoping and are Production's.
CREATED_FOR_METADATA_KEY = "created_for_environment"
_prompt_environment_registered = False


class PromptLabelMissingError(LookupError):
    """No version of a prompt carries this deployment's environment label.

    A configuration error, not an outage (llm-opik-lifecycle-spec.md §3), so
    render_prompt raises it instead of falling back to the code default.
    """


def is_opik_configured() -> bool:
    """Whether this deployment has opted into Opik.

    That takes OPIK_URL_OVERRIDE and the Vault location of the Keycloak client
    that instance accepts (OPIK_KEYCLOAK_VAULT_MOUNT/PATH), which ol-infrastructure
    sets together. Checked before decorating a function with @opik.track and
    before building a client to fetch Prompt Library entries -- local dev leaves
    them unset and gets the plain, untraced/hardcoded-prompt path.
    """
    return (
        bool(os.environ.get("OPIK_URL_OVERRIDE"))
        and opik_keycloak_vault_location() is not None
    )


def configure_ol_llm(vault: Vault) -> bool:
    """Configure ol-llm's settings and Opik tracing for this process.

    Must run before anything builds an Opik client or an ol-llm model: the SDK
    reads its client hooks when it builds its HTTP client.

    The environment is DAGSTER_ENV, which is also the Opik label prompts are
    fetched by. Gateway routes come from OL_LLM_GATEWAY_* as ol-llm reads them.
    Azure OpenAI, when LLM_AZURE_ENDPOINT is set, authenticates with an Entra
    token from DefaultAzureCredential, as LLMClientFactory does.

    Tracing is on only when is_opik_configured() and OPIK_TRACK_DISABLE is unset.
    The Keycloak client is read from Vault (KV v1, at
    OPIK_KEYCLOAK_VAULT_MOUNT/OPIK_KEYCLOAK_VAULT_PATH).
    A read failure is logged and leaves tracing off rather than failing the code
    location: tracing is observability, not a pipeline dependency.

    :param vault: the authenticated Vault resource.
    :returns: whether tracing is on.
    :rtype: bool
    """
    global _configured  # noqa: PLW0603

    settings = Settings.from_env({**os.environ, "OL_LLM_ENVIRONMENT": DAGSTER_ENV})
    if azure_endpoint := os.environ.get("LLM_AZURE_ENDPOINT"):
        # The Azure provider reads its endpoint from the SDK's own variable.
        os.environ.setdefault("AZURE_OPENAI_ENDPOINT", azure_endpoint)
        settings = dataclasses.replace(
            settings,
            azure_ad_token_provider=get_bearer_token_provider(
                DefaultAzureCredential(), COGNITIVE_SERVICES_SCOPE
            ),
        )
    configure(settings)

    # from_env keeps OPIK_TRACK_DISABLE and OL_LLM_TRACE_CONTENT in effect.
    tracing = TracingConfig.from_env()
    keycloak = (
        _opik_keycloak_client(vault)
        if tracing.enabled and is_opik_configured()
        else None
    )
    configure_tracing(
        dataclasses.replace(tracing, enabled=keycloak is not None, keycloak=keycloak)
    )
    _configured = keycloak is not None
    return _configured


def _opik_keycloak_client(vault: Vault) -> KeycloakClient | None:
    location = opik_keycloak_vault_location()
    if location is None:
        return None
    mount, path = location
    try:
        secret = vault.client.secrets.kv.v1.read_secret(mount_point=mount, path=path)[
            "data"
        ]
        return KeycloakClient(
            token_url=f"{secret['url']}/protocol/openid-connect/token",
            client_id=secret["client_id"],
            client_secret=secret["client_secret"],
        )
    except Exception:
        log.warning(
            "Failed to read %s/%s from Vault; "
            "Opik tracing/prompt-library stays disabled",
            mount,
            path,
            exc_info=True,
        )
        return None


def get_opik_client() -> opik.Opik | None:
    """Return a shared Opik client for Prompt Library reads, or None if unavailable.

    None whenever is_opik_configured() is False, the Keycloak auth hook never
    registered (see configure_ol_llm), or building the client
    itself fails -- every caller must treat this as "fall back to the local
    hardcoded prompt", never as a hard dependency.
    """
    global _opik_client, _client_init_attempted  # noqa: PLW0603

    if _client_init_attempted:
        return _opik_client
    _client_init_attempted = True

    if not (is_opik_configured() and _configured):
        return None
    try:
        _opik_client = opik.Opik(project_name=OPIK_PROJECT_NAME)
    except Exception:
        log.warning("Failed to initialize Opik client", exc_info=True)
        _opik_client = None
    return _opik_client


def traced(
    name: str, tags: list[str] | None = None, ignore_arguments: list[str] | None = None
) -> Callable[[F], F]:
    """@opik.track, gated on is_opik_configured() -- a no-op decorator otherwise.

    Checked at import/decoration time (an env var, stable for the process's
    life), not at call time -- unlike get_opik_client/render_prompt, which
    also require the Keycloak auth hook to have registered successfully.
    @opik.track itself only queues spans for a background worker, so a hook
    registration failure surfaces as failed/absent traces, not a raised error.

    OPIK_PROJECT_NAME ("dagster-ml") is shared across every Dagster/LLM
    integration in this repo, not just feedback -- tags is how a caller marks
    which one a given call site belongs to (e.g. tags=["feedback"]).

    ignore_arguments excludes named parameters from the auto-captured input --
    pass the same name a caller also hands to attach_span_metadata (e.g.
    "trace_metadata") so identifying info lands once, as metadata, instead of
    also duplicating into the span's input.
    """
    if not is_opik_configured():
        return lambda func: func
    return opik.track(
        name=name,
        type="llm",
        project_name=OPIK_PROJECT_NAME,
        environment=DAGSTER_ENV,
        tags=tags,
        ignore_arguments=ignore_arguments,
    )


def infer_llm_provider(model_version: str, default: str) -> str:
    """Best-effort Opik pricing provider for model_version, else default.

    A client_class="openai_compatible" gateway (e.g. Parley) can proxy Claude
    models under an OpenAI-shaped API -- tagging those "openai" looks up the
    wrong Opik price table and shows no cost.
    """
    return "anthropic" if model_version.startswith("claude") else default


def attach_llm_usage(*, usage: dict[str, int], model: str, provider: str) -> None:
    """Attach token usage to the current @traced span for pricing.

    usage takes OpenAI-shaped keys, or the provider's own keys for a provider
    Opik parses (e.g. Anthropic's cache_read_input_tokens), which it converts.

    No-op if Opik isn't configured (traced() never opened a span to attach to).
    """
    if not is_opik_configured():
        return
    opik_context.update_current_span(usage=usage, model=model, provider=provider)


def attach_span_metadata(metadata: dict[str, Any]) -> None:
    """Attach arbitrary metadata (e.g. which record a call was for) to the
    current @traced span, so a trace can be traced back to its source record
    without changing what's shown as the call's actual input/output.

    No-op if Opik isn't configured (traced() never opened a span to attach to).
    Thread-safe under concurrent calls: opik_context tracks the current span
    per-thread, so each concurrently-running @traced call attaches its own
    metadata to its own span, not a shared/leaked one.
    """
    if not is_opik_configured():
        return
    opik_context.update_current_span(metadata=metadata)


def _register_prompt_environment(client: opik.Opik) -> None:
    """Register PROMPT_ENVIRONMENT in the Opik workspace if it isn't yet.

    Opik only knows development/staging/production out of the box, and
    set_prompt_environments rejects an unregistered environment.
    """
    global _prompt_environment_registered  # noqa: PLW0603

    if _prompt_environment_registered:
        return
    if not any(env.name == PROMPT_ENVIRONMENT for env in client.get_environments()):
        client.create_environment(PROMPT_ENVIRONMENT)
    _prompt_environment_registered = True


def _is_own_version(prompt: opik.Prompt) -> bool:
    """Whether prompt's version was created for PROMPT_ENVIRONMENT."""
    created_for = (prompt.metadata or {}).get(CREATED_FOR_METADATA_KEY)
    if created_for is None:
        return PROMPT_ENVIRONMENT == "production"
    return created_for == PROMPT_ENVIRONMENT


def _point_environment_at(client: opik.Opik, prompt: opik.Prompt) -> bool:
    """Point PROMPT_ENVIRONMENT at prompt's version if no other environment owns it.

    set_prompt_environments replaces a version's whole environment set and Opik
    has no call that adds one environment, so writing another environment's
    ownership back from an earlier read could move that environment back onto
    this version. Every write here names PROMPT_ENVIRONMENT alone, on a version
    no other environment owns (re-read past the SDK's prompt cache), so it can
    never move another environment's pointer.

    Returns False, writing nothing, when another environment owns the version.
    """
    current = client.get_prompt(name=prompt.name, version=prompt.version, no_cache=True)
    owners = set((current.environments if current is not None else None) or [])
    if owners - {PROMPT_ENVIRONMENT}:
        return False
    client.set_prompt_environments(
        prompt.name, [PROMPT_ENVIRONMENT], version=prompt.version
    )
    return True


def _own_version(client: opik.Opik, name: str, template: str) -> opik.Prompt:
    """Return a version of name with template's text for PROMPT_ENVIRONMENT to own.

    Reuses an earlier version created for this environment that no other
    environment owns, and otherwise adds one, marked with the environment, so
    environments never share a version.
    """
    if client.get_prompt(name=name) is not None:
        for prompt in client.get_prompt_history(name=name):
            others = set(prompt.environments or []) - {PROMPT_ENVIRONMENT}
            if prompt.prompt == template and _is_own_version(prompt) and not others:
                return prompt
    return client.create_prompt(
        name=name,
        prompt=template,
        metadata={CREATED_FOR_METADATA_KEY: PROMPT_ENVIRONMENT},
    )


def _seed_environment(client: opik.Opik, name: str, template: str) -> opik.Prompt:
    """Point PROMPT_ENVIRONMENT at its own version of template.

    Never in Production: its label moves only by hand or from the promotion job
    that reads the repository's pointer file, so deployed code never writes it.

    :raises PromptLabelMissingError: in Production.
    """
    if PROMPT_ENVIRONMENT == "production":
        msg = f"No version of Opik prompt {name!r} is labeled production"
        raise PromptLabelMissingError(msg)
    _register_prompt_environment(client)
    prompt = _own_version(client, name, template)
    _point_environment_at(client, prompt)
    return prompt


def seed_prompt_environment(name: str, template: str) -> None:
    """Point CI or QA at its own version of template, for a prompt with no label yet.

    For prompts fetched through ol_llm.get_prompt, which raises LookupError when
    the environment has no version, so CI and QA start from the code default as
    render_prompt's prompts do.

    :param name: the prompt's name.
    :param template: the code default.
    :raises PromptLabelMissingError: in Production, or when there is no Opik
        client to write with.
    """
    client = get_opik_client()
    if client is None:
        msg = f"No Opik client to label prompt {name!r} for {PROMPT_ENVIRONMENT}"
        raise PromptLabelMissingError(msg)
    _seed_environment(client, name, template)


def _environment_prompt(
    client: opik.Opik, name: str, default_template: str
) -> opik.Prompt:
    """Return the version of name that PROMPT_ENVIRONMENT points at.

    CI and QA without a version yet are pointed at their own version of
    default_template, never at whatever version is latest, which may be another
    environment's.

    :raises PromptLabelMissingError: in Production, when no version is labeled
        production.
    """
    _register_prompt_environment(client)
    prompt = client.get_prompt(name=name, environment=PROMPT_ENVIRONMENT)
    if prompt is not None:
        return prompt
    return _seed_environment(client, name, default_template)


def publish_prompt_version(name: str, template: str) -> str:
    """Record template as name's newest Prompt Library version and return it.

    For prompts that stay authoritative in code, where rendering an Opik edit
    would change what is sent without changing what the caller records as the
    prompt version (the courseware extractor keys every row on a hash of its code
    prompt). Opik versions what ran instead, in PROMPT_ENVIRONMENT's own version of
    that text (see _own_version), reusing an earlier one rather than adding a
    version each run. CI and QA are pointed at it; Production's label is left
    where it is, since deployed code never moves it. Returns LOCAL_PROMPT_VERSION
    when Opik is unavailable or the publish fails in any way: this is provenance,
    never a reason to fail a run.
    """
    client = get_opik_client()
    if client is None:
        return LOCAL_PROMPT_VERSION
    try:
        _register_prompt_environment(client)
        prompt = client.get_prompt(name=name, environment=PROMPT_ENVIRONMENT)
        if prompt is None or prompt.prompt != template:
            prompt = _own_version(client, name, template)
            if PROMPT_ENVIRONMENT != "production":
                _point_environment_at(client, prompt)
        version = prompt.version or prompt.commit
    except Exception:
        log.warning("Opik prompt publish failed for %s", name, exc_info=True)
        return LOCAL_PROMPT_VERSION
    if not version:
        return LOCAL_PROMPT_VERSION
    _published_prompts[template] = prompt
    return version


def attach_span_prompt(template: str) -> None:
    """Link the current @traced span to the version published for template.

    Looks up what publish_prompt_version recorded for this exact text in this
    process. No-op if Opik isn't configured or template was not published here
    (the local runner, or a publish that failed): the span is still recorded,
    without the link.
    """
    prompt = _published_prompts.get(template)
    if prompt is None or not is_opik_configured():
        return
    opik_context.update_current_span(prompts=[prompt])


def render_prompt(name: str, default_template: str, **variables: Any) -> str:
    """Render a Prompt Library entry (mustache {{var}} placeholders).

    In CI and QA, seeds Opik with default_template on first use so the prompt
    shows up in the Prompt Library ready to edit. Renders the version
    PROMPT_ENVIRONMENT points at, so an edit takes effect, with no redeploy, in
    the environments pointed at the new version. Falls back to rendering
    default_template locally whenever Opik is unreachable or not configured for
    this environment.

    :raises PromptLabelMissingError: in Production, when no version is labeled
        production.
    """
    template = default_template
    client = get_opik_client()
    if client is not None:
        try:
            template = _environment_prompt(client, name, default_template).prompt
        except PromptLabelMissingError:
            raise
        except Exception:
            log.warning(
                "Opik prompt fetch/create failed for %s; using local default",
                name,
                exc_info=True,
            )
    for key, value in variables.items():
        template = template.replace("{{" + key + "}}", str(value))
    return template
