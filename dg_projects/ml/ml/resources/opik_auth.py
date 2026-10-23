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


def is_opik_configured() -> bool:
    """Whether this deployment has opted into Opik (set via OPIK_URL_OVERRIDE).

    Checked before decorating a function with @opik.track and before building
    a client to fetch Prompt Library entries -- most environments (local dev,
    CI) leave this unset and get the plain, untraced/hardcoded-prompt path.
    """
    return bool(os.environ.get("OPIK_URL_OVERRIDE"))


def configure_ol_llm(vault: Vault) -> bool:
    """Configure ol-llm's settings and Opik tracing for this process.

    Must run before anything builds an Opik client or an ol-llm model: the SDK
    reads its client hooks when it builds its HTTP client.

    The environment is DAGSTER_ENV, which is also the Opik label prompts are
    fetched by. Gateway routes come from OL_LLM_GATEWAY_* as ol-llm reads them.
    Azure OpenAI, when LLM_AZURE_ENDPOINT is set, authenticates with an Entra
    token from DefaultAzureCredential, as LLMClientFactory does.

    Tracing is on only with OPIK_URL_OVERRIDE set and OPIK_TRACK_DISABLE unset.
    The Keycloak client is read from Vault (secret-operations/sso/opik, KV v1).
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
    try:
        secret = vault.client.secrets.kv.v1.read_secret(
            mount_point="secret-operations", path="sso/opik"
        )["data"]
        return KeycloakClient(
            token_url=f"{secret['url']}/protocol/openid-connect/token",
            client_id=secret["client_id"],
            client_secret=secret["client_secret"],
        )
    except Exception:
        log.warning(
            "Failed to read secret-operations/sso/opik from Vault; "
            "Opik tracing/prompt-library stays disabled",
            exc_info=True,
        )
        return None


def get_opik_client() -> opik.Opik | None:
    """Return a shared Opik client for Prompt Library reads, or None if unavailable.

    None whenever OPIK_URL_OVERRIDE is unset, the Keycloak auth hook never
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
    """@opik.track, gated on OPIK_URL_OVERRIDE -- a plain no-op decorator otherwise.

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


def publish_prompt_version(name: str, template: str) -> str:
    """Record template as name's newest Prompt Library version and return it.

    For prompts that stay authoritative in code, where rendering an Opik edit
    would change what is sent without changing what the caller records as the
    prompt version (the courseware extractor keys every row on a hash of its code
    prompt). Opik versions what ran instead: create_prompt adds a version only
    when the template differs from the latest, so calling this every run is
    idempotent. Returns LOCAL_PROMPT_VERSION when Opik is unavailable or the
    publish fails in any way: this is provenance, never a reason to fail a run.
    """
    client = get_opik_client()
    if client is None:
        return LOCAL_PROMPT_VERSION
    try:
        prompt = client.create_prompt(name=name, prompt=template)
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

    Seeds Opik with default_template on first use (via create_prompt) so the
    prompt shows up in the Prompt Library ready to edit -- editing it there
    then takes effect on the next call with no redeploy. Falls back to
    rendering default_template locally whenever Opik is unreachable or not
    configured for this environment.
    """
    template = default_template
    client = get_opik_client()
    if client is not None:
        try:
            prompt = client.get_prompt(name=name) or client.create_prompt(
                name=name, prompt=default_template
            )
            template = prompt.prompt
        except Exception:
            log.warning(
                "Opik prompt fetch/create failed for %s; using local default",
                name,
                exc_info=True,
            )
    for key, value in variables.items():
        template = template.replace("{{" + key + "}}", str(value))
    return template
