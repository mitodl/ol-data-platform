"""Tests for ml.resources.opik_auth."""

from typing import Any

import opik
import pytest
from ml.resources import opik_auth
from ol_orchestrate.resources.secrets.vault import Vault


class _FakeKvV1:
    def __init__(self, secrets: dict[str, dict[str, str]]) -> None:
        self._secrets = secrets
        self.reads = 0

    def read_secret(self, mount_point: str, path: str) -> dict[str, dict[str, str]]:
        self.reads += 1
        return {"data": self._secrets[f"{mount_point}/{path}"]}


class _FakeHvacClient:
    """Stands in for the authenticated hvac.Client Vault.client returns."""

    def __init__(self, kv_v1: _FakeKvV1) -> None:
        kv = type("_Kv", (), {"v1": kv_v1})()
        self.secrets = type("_Secrets", (), {"kv": kv})()

    def is_authenticated(self) -> bool:
        return True


def _build_vault(kv_v1: _FakeKvV1) -> Vault:
    vault = Vault(
        vault_addr="https://vault.example.com", vault_auth_type="token", vault_token="x"
    )
    vault._client = _FakeHvacClient(kv_v1)
    return vault


_OPIK_SECRET = {
    "secret-operations/sso/opik": {
        "url": "https://sso-qa.ol.mit.edu/realms/ol-platform-engineering",
        "client_id": "ol-opik-client",
        "client_secret": "test-secret",  # pragma: allowlist secret
    }
}


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test starts from the same "never configured" process state.

    configure_opik_keycloak_auth/get_opik_client cache themselves in module
    globals (by design -- see their docstrings), which would otherwise leak
    True/a client across tests depending on run order.
    """
    monkeypatch.setattr(opik_auth, "_configured", False)
    monkeypatch.setattr(opik_auth, "_opik_client", None)
    monkeypatch.setattr(opik_auth, "_client_init_attempted", False)
    monkeypatch.delenv("OPIK_URL_OVERRIDE", raising=False)


def test_is_opik_configured_false_without_url_override() -> None:
    assert opik_auth.is_opik_configured() is False


def test_is_opik_configured_true_with_url_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik-ci.ol.mit.edu/api/")

    assert opik_auth.is_opik_configured() is True


def test_configure_skips_vault_when_not_configured() -> None:
    """No OPIK_URL_OVERRIDE means no Vault read -- a bad secret shouldn't matter."""
    kv_v1 = _FakeKvV1({})  # a read would KeyError

    result = opik_auth.configure_opik_keycloak_auth(_build_vault(kv_v1))

    assert result is False
    assert kv_v1.reads == 0


def test_configure_reads_vault_and_registers_hook_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik-ci.ol.mit.edu/api/")
    kv_v1 = _FakeKvV1(_OPIK_SECRET)
    vault = _build_vault(kv_v1)
    registered: list[Any] = []
    monkeypatch.setattr(opik_auth, "add_httpx_client_hook", registered.append)

    first = opik_auth.configure_opik_keycloak_auth(vault)
    second = opik_auth.configure_opik_keycloak_auth(vault)

    assert first is True
    assert second is True
    assert len(registered) == 1  # second call short-circuits on _configured
    assert kv_v1.reads == 1
    auth = registered[0].update_init_arguments({})["auth"]
    assert isinstance(auth, opik_auth.KeycloakClientCredentialsAuth)
    expected_endpoint = (
        "https://sso-qa.ol.mit.edu/realms/ol-platform-engineering"
        "/protocol/openid-connect/token"
    )
    assert auth._token_url == expected_endpoint
    assert auth._client_id == "ol-opik-client"


def test_configure_handles_vault_read_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik-ci.ol.mit.edu/api/")
    kv_v1 = _FakeKvV1({})  # secret-operations/sso/opik missing -> KeyError inside

    result = opik_auth.configure_opik_keycloak_auth(_build_vault(kv_v1))

    assert result is False
    assert opik_auth._configured is False


def test_get_opik_client_none_when_not_configured() -> None:
    assert opik_auth.get_opik_client() is None


def test_get_opik_client_none_when_auth_hook_never_registered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OPIK_URL_OVERRIDE set but configure_opik_keycloak_auth never ran (or failed)."""
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik-ci.ol.mit.edu/api/")

    assert opik_auth.get_opik_client() is None


def test_get_opik_client_returns_none_when_construction_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik-ci.ol.mit.edu/api/")
    monkeypatch.setattr(opik_auth, "_configured", True)

    def _raise(**kwargs: Any) -> None:  # noqa: ARG001
        msg = "boom"
        raise RuntimeError(msg)

    monkeypatch.setattr(opik, "Opik", _raise)

    assert opik_auth.get_opik_client() is None


def test_get_opik_client_caches_after_first_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik-ci.ol.mit.edu/api/")
    monkeypatch.setattr(opik_auth, "_configured", True)
    build_calls: list[Any] = []

    def fake_opik(**kwargs: Any) -> str:
        build_calls.append(kwargs)
        return "a-fake-client"

    monkeypatch.setattr(opik, "Opik", fake_opik)

    first = opik_auth.get_opik_client()
    second = opik_auth.get_opik_client()

    assert first == "a-fake-client"
    assert second == "a-fake-client"
    assert len(build_calls) == 1  # second call hit the _client_init_attempted cache


class _FakePrompt:
    def __init__(self, template: str) -> None:
        self.prompt = template


class _FakeOpikClient:
    def __init__(self, existing: dict[str, _FakePrompt] | None = None) -> None:
        self._existing = existing or {}
        self.created: dict[str, str] = {}

    def get_prompt(self, name: str) -> _FakePrompt | None:
        return self._existing.get(name)

    def create_prompt(self, name: str, prompt: str) -> _FakePrompt:
        self.created[name] = prompt
        return _FakePrompt(prompt)


def test_render_prompt_falls_back_locally_when_opik_unavailable() -> None:
    """No Opik client at all -- render straight from default_template."""
    result = opik_auth.render_prompt(
        "feedback-summary", "Summarize: {{conversation_text}}", conversation_text="hi"
    )

    assert result == "Summarize: hi"


def test_render_prompt_uses_existing_opik_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeOpikClient(
        existing={"feedback-summary": _FakePrompt("Custom: {{conversation_text}}")}
    )
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)

    result = opik_auth.render_prompt(
        "feedback-summary", "Summarize: {{conversation_text}}", conversation_text="hi"
    )

    assert result == "Custom: hi"


def test_render_prompt_seeds_opik_when_prompt_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeOpikClient()
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)

    result = opik_auth.render_prompt(
        "feedback-summary", "Summarize: {{conversation_text}}", conversation_text="hi"
    )

    assert result == "Summarize: hi"
    assert fake_client.created == {
        "feedback-summary": "Summarize: {{conversation_text}}"
    }


def test_render_prompt_falls_back_when_opik_call_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailingClient:
        def get_prompt(self, name: str) -> None:  # noqa: ARG002
            msg = "boom"
            raise RuntimeError(msg)

    def _build_failing_client() -> _FailingClient:
        return _FailingClient()

    monkeypatch.setattr(opik_auth, "get_opik_client", _build_failing_client)

    result = opik_auth.render_prompt(
        "feedback-summary", "Summarize: {{conversation_text}}", conversation_text="hi"
    )

    assert result == "Summarize: hi"


def test_traced_is_a_noop_decorator_when_not_configured() -> None:
    @opik_auth.traced("some-span")
    def add_one(x: int) -> int:
        return x + 1

    assert add_one(1) == 2


def test_traced_wraps_with_opik_track_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik-ci.ol.mit.edu/api/")
    track_calls: list[dict[str, Any]] = []

    def fake_track(**kwargs: Any):
        track_calls.append(kwargs)
        return lambda func: func

    monkeypatch.setattr(opik, "track", fake_track)

    @opik_auth.traced("some-span", tags=["feedback"])
    def add_one(x: int) -> int:
        return x + 1

    assert add_one(1) == 2
    assert track_calls == [
        {
            "name": "some-span",
            "type": "llm",
            "project_name": opik_auth.OPIK_PROJECT_NAME,
            "environment": opik_auth.DAGSTER_ENV,
            "tags": ["feedback"],
        }
    ]


class _FakeTokenResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeTokenClient:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = list(responses)
        self.calls = 0

    def post(self, url: str, data: dict[str, Any]) -> _FakeTokenResponse:  # noqa: ARG002
        self.calls += 1
        return _FakeTokenResponse(self._responses.pop(0))


def test_keycloak_auth_fetches_token_once_and_reuses_it() -> None:
    auth = opik_auth.KeycloakClientCredentialsAuth(
        token_url="https://sso.example.com/protocol/openid-connect/token",
        client_id="ol-opik-client",
        client_secret="secret",  # pragma: allowlist secret
    )
    fake_token_client = _FakeTokenClient(
        [{"access_token": "tok-1", "expires_in": 3600}]
    )
    auth._token_client = fake_token_client

    first = auth._get_token()
    second = auth._get_token()

    assert first == "tok-1"
    assert second == "tok-1"
    assert fake_token_client.calls == 1


def test_keycloak_auth_force_refresh_gets_a_new_token() -> None:
    auth = opik_auth.KeycloakClientCredentialsAuth(
        token_url="https://sso.example.com/protocol/openid-connect/token",
        client_id="ol-opik-client",
        client_secret="secret",  # pragma: allowlist secret
    )
    fake_token_client = _FakeTokenClient(
        [
            {"access_token": "tok-1", "expires_in": 3600},
            {"access_token": "tok-2", "expires_in": 3600},
        ]
    )
    auth._token_client = fake_token_client

    auth._get_token()
    refreshed = auth._get_token(force_refresh=True)

    assert refreshed == "tok-2"
    assert fake_token_client.calls == 2
