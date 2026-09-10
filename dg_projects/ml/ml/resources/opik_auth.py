"""Keycloak-fronted Opik auth, LLM tracing, and Prompt Library access."""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import TYPE_CHECKING, Any, TypeVar

import httpx
import opik
from ol_orchestrate.lib.constants import DAGSTER_ENV
from opik import opik_context
from opik.hooks import HttpxClientHook, add_httpx_client_hook

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from ol_orchestrate.resources.secrets.vault import Vault

F = TypeVar("F", bound="Callable[..., Any]")

log = logging.getLogger(__name__)

OPIK_PROJECT_NAME = os.environ.get("OPIK_PROJECT_NAME", "dagster-ml")

_REFRESH_SKEW_SECONDS = 30
_TOKEN_REQUEST_TIMEOUT_SECONDS = 10

_configured = False
_opik_client: opik.Opik | None = None
_client_init_attempted = False


class KeycloakClientCredentialsAuth(httpx.Auth):
    """httpx auth flow that injects a Keycloak access token via client-credentials.

    A single instance is shared across every request the SDK makes. The cached
    token is refreshed proactively before expiry and reactively on a 401.
    """

    def __init__(self, token_url: str, client_id: str, client_secret: str) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        # Dedicated client for the token endpoint so we never recurse through
        # the SDK's own (authed) client.
        self._token_client = httpx.Client(timeout=_TOKEN_REQUEST_TIMEOUT_SECONDS)

        self._lock = threading.Lock()
        self._access_token: str | None = None
        self._expires_at: float = 0.0  # monotonic-clock deadline

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        """Attach a bearer token; on a 401 refresh it and retry exactly once."""
        request.headers["Authorization"] = f"Bearer {self._get_token()}"
        response = yield request

        if response.status_code == httpx.codes.UNAUTHORIZED:
            response.close()
            request.headers["Authorization"] = (
                f"Bearer {self._get_token(force_refresh=True)}"
            )
            yield request

    def _get_token(self, *, force_refresh: bool = False) -> str:
        with self._lock:
            now = time.monotonic()
            if force_refresh or self._access_token is None or now >= self._expires_at:
                self._refresh_locked()
            assert self._access_token is not None  # noqa: S101 -- set by _refresh_locked above
            return self._access_token

    def _refresh_locked(self) -> None:
        resp = self._token_client.post(
            self._token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        resp.raise_for_status()
        payload = resp.json()

        self._access_token = payload["access_token"]
        expires_in = float(payload.get("expires_in", 60))
        self._expires_at = time.monotonic() + max(
            0.0, expires_in - _REFRESH_SKEW_SECONDS
        )


def is_opik_configured() -> bool:
    """Whether this deployment has opted into Opik (set via OPIK_URL_OVERRIDE).

    Checked before decorating a function with @opik.track and before building
    a client to fetch Prompt Library entries -- most environments (local dev,
    CI) leave this unset and get the plain, untraced/hardcoded-prompt path.
    """
    return bool(os.environ.get("OPIK_URL_OVERRIDE"))


def configure_opik_keycloak_auth(vault: Vault) -> bool:
    """Register the Keycloak auth flow on the Opik SDK's httpx client.

    Reads the client_id/client_secret/realm url from Vault (secret-operations/
    sso/opik, KV v1 -- same mount/version convention as LLMClientFactory and
    tika.py's secret-operations reads). A Vault read failure is logged and
    treated as "not configured" rather than raised -- this is an optional
    observability integration, not a pipeline dependency.

    Returns True if the hook was registered (or already was), False otherwise.
    """
    global _configured  # noqa: PLW0603

    if not is_opik_configured():
        log.debug("OPIK_URL_OVERRIDE not set, skipping Keycloak auth hook")
        return False
    if _configured:
        return True

    try:
        secret = vault.client.secrets.kv.v1.read_secret(
            mount_point="secret-operations", path="sso/opik"
        )["data"]
        auth = KeycloakClientCredentialsAuth(
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
        return False

    add_httpx_client_hook(
        HttpxClientHook(client_modifier=None, client_init_arguments={"auth": auth})
    )
    _configured = True
    log.info("Opik Keycloak auth hook registered")
    return True


def get_opik_client() -> opik.Opik | None:
    """Return a shared Opik client for Prompt Library reads, or None if unavailable.

    None whenever OPIK_URL_OVERRIDE is unset, the Keycloak auth hook never
    registered (see configure_opik_keycloak_auth), or building the client
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


def traced(name: str, tags: list[str] | None = None) -> Callable[[F], F]:
    """@opik.track, gated on OPIK_URL_OVERRIDE -- a plain no-op decorator otherwise.

    Checked at import/decoration time (an env var, stable for the process's
    life), not at call time -- unlike get_opik_client/render_prompt, which
    also require the Keycloak auth hook to have registered successfully.
    @opik.track itself only queues spans for a background worker, so a hook
    registration failure surfaces as failed/absent traces, not a raised error.

    OPIK_PROJECT_NAME ("dagster-ml") is shared across every Dagster/LLM
    integration in this repo, not just feedback -- tags is how a caller marks
    which one a given call site belongs to (e.g. tags=["feedback"]).
    """
    if not is_opik_configured():
        return lambda func: func
    return opik.track(
        name=name,
        type="llm",
        project_name=OPIK_PROJECT_NAME,
        environment=DAGSTER_ENV,
        tags=tags,
    )


def infer_llm_provider(model_version: str, default: str) -> str:
    """Best-effort Opik pricing provider for model_version, else default.

    A client_class="openai_compatible" gateway (e.g. Parley) can proxy Claude
    models under an OpenAI-shaped API -- tagging those "openai" looks up the
    wrong Opik price table and shows no cost.
    """
    return "anthropic" if model_version.startswith("claude") else default


def attach_llm_usage(*, usage: dict[str, int], model: str, provider: str) -> None:
    """Attach token usage (OpenAI-shaped keys) to the current @traced span for pricing.

    No-op if Opik isn't configured (traced() never opened a span to attach to).
    """
    if not is_opik_configured():
        return
    opik_context.update_current_span(usage=usage, model=model, provider=provider)


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
