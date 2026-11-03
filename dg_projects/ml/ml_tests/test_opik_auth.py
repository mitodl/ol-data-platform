"""Tests for ml.resources.opik_auth."""

from types import SimpleNamespace
from typing import Any

import opik
import pytest
from ml.resources import opik_auth
from ol_llm import KeycloakClient, TracingConfig
from ol_orchestrate.lib.constants import DAGSTER_ENV
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
    "secret-opik/sdk-client": {
        "url": "https://sso.ol.mit.edu/realms/ol-platform-engineering",
        "client_id": "ol-opik-sdk",
        "client_secret": "test-secret",  # pragma: allowlist secret
    }
}


def _configure_opik_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set what ol-infrastructure sets on a deployment that traces to Opik."""
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik.ol.mit.edu/api/")
    monkeypatch.setenv("OPIK_KEYCLOAK_VAULT_MOUNT", "secret-opik")
    monkeypatch.setenv("OPIK_KEYCLOAK_VAULT_PATH", "sdk-client")


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test starts from the same "never configured" process state.

    configure_ol_llm/get_opik_client cache themselves in module
    globals (by design -- see their docstrings), which would otherwise leak
    True/a client across tests depending on run order.
    """
    monkeypatch.setattr(opik_auth, "_configured", False)
    monkeypatch.setattr(opik_auth, "_opik_client", None)
    monkeypatch.setattr(opik_auth, "_client_init_attempted", False)
    monkeypatch.setattr(opik_auth, "_published_prompts", {})
    monkeypatch.setattr(opik_auth, "_prompt_environment_registered", False)
    monkeypatch.setattr(opik_auth, "PROMPT_ENVIRONMENT", "qa")
    monkeypatch.delenv("OPIK_URL_OVERRIDE", raising=False)
    monkeypatch.delenv("OPIK_KEYCLOAK_VAULT_MOUNT", raising=False)
    monkeypatch.delenv("OPIK_KEYCLOAK_VAULT_PATH", raising=False)


def test_is_opik_configured_false_without_url_override() -> None:
    assert opik_auth.is_opik_configured() is False


def test_is_opik_configured_true_with_url_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_opik_env(monkeypatch)

    assert opik_auth.is_opik_configured() is True


def test_is_opik_configured_false_without_the_client_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A URL alone, without where its Keycloak client is, doesn't turn Opik on"""
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik.ol.mit.edu/api/")

    assert opik_auth.is_opik_configured() is False


@pytest.fixture
def ol_llm_calls(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[Any]]:
    """Record what configure_ol_llm hands ol-llm instead of changing its state."""
    calls: dict[str, list[Any]] = {"settings": [], "tracing": []}
    monkeypatch.setattr(opik_auth, "configure", calls["settings"].append)
    monkeypatch.setattr(opik_auth, "configure_tracing", calls["tracing"].append)
    return calls


def test_configure_skips_vault_when_not_configured(
    ol_llm_calls: dict[str, list[Any]],
) -> None:
    """No OPIK_URL_OVERRIDE means no Vault read -- a bad secret shouldn't matter."""
    kv_v1 = _FakeKvV1({})  # a read would KeyError

    result = opik_auth.configure_ol_llm(_build_vault(kv_v1))

    assert result is False
    assert kv_v1.reads == 0
    assert ol_llm_calls["tracing"] == [TracingConfig(enabled=False)]
    assert ol_llm_calls["settings"][0].environment == DAGSTER_ENV


def test_configure_reads_the_keycloak_client_from_vault(
    monkeypatch: pytest.MonkeyPatch, ol_llm_calls: dict[str, list[Any]]
) -> None:
    _configure_opik_env(monkeypatch)
    kv_v1 = _FakeKvV1(_OPIK_SECRET)

    result = opik_auth.configure_ol_llm(_build_vault(kv_v1))

    assert result is True
    assert opik_auth._configured is True
    assert ol_llm_calls["tracing"] == [
        TracingConfig(
            keycloak=KeycloakClient(
                token_url="https://sso.ol.mit.edu/realms/ol-platform-engineering"
                "/protocol/openid-connect/token",
                client_id="ol-opik-sdk",
                client_secret="test-secret",  # pragma: allowlist secret
            )
        )
    ]


def test_configure_handles_vault_read_failure(
    monkeypatch: pytest.MonkeyPatch, ol_llm_calls: dict[str, list[Any]]
) -> None:
    _configure_opik_env(monkeypatch)
    kv_v1 = _FakeKvV1({})  # secret-opik/sdk-client missing -> KeyError inside

    result = opik_auth.configure_ol_llm(_build_vault(kv_v1))

    assert result is False
    assert opik_auth._configured is False
    assert ol_llm_calls["tracing"] == [TracingConfig(enabled=False)]


def test_configure_gives_azure_an_entra_token_provider(
    monkeypatch: pytest.MonkeyPatch, ol_llm_calls: dict[str, list[Any]]
) -> None:
    monkeypatch.setenv("LLM_AZURE_ENDPOINT", "https://example.openai.azure.com/")
    monkeypatch.delenv("AZURE_OPENAI_ENDPOINT", raising=False)
    monkeypatch.setattr(opik_auth, "DefaultAzureCredential", lambda: "fake-cred")
    monkeypatch.setattr(
        opik_auth,
        "get_bearer_token_provider",
        lambda _credential, _scope: lambda: "entra-test-token",
    )

    opik_auth.configure_ol_llm(_build_vault(_FakeKvV1({})))

    provider = ol_llm_calls["settings"][0].azure_ad_token_provider
    assert provider is not None
    assert provider() == "entra-test-token"
    assert opik_auth.os.environ["AZURE_OPENAI_ENDPOINT"] == (
        "https://example.openai.azure.com/"
    )


def test_get_opik_client_none_when_not_configured() -> None:
    assert opik_auth.get_opik_client() is None


def test_get_opik_client_none_when_auth_hook_never_registered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OPIK_URL_OVERRIDE set but configure_ol_llm never ran (or failed)."""
    _configure_opik_env(monkeypatch)

    assert opik_auth.get_opik_client() is None


def test_get_opik_client_returns_none_when_construction_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_opik_env(monkeypatch)
    monkeypatch.setattr(opik_auth, "_configured", True)

    def _raise(**kwargs: Any) -> None:  # noqa: ARG001
        msg = "boom"
        raise RuntimeError(msg)

    monkeypatch.setattr(opik, "Opik", _raise)

    assert opik_auth.get_opik_client() is None


def test_get_opik_client_caches_after_first_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_opik_env(monkeypatch)
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
    def __init__(
        self,
        name: str,
        template: str,
        version: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.prompt = template
        self.version: str | None = version
        self.commit = None
        self.metadata = metadata
        self.environments: list[str] | None = None


def _created_for(environment: str) -> dict[str, str]:
    return {opik_auth.CREATED_FOR_METADATA_KEY: environment}


class _FakeOpikClient:
    """A Prompt Library with versions and environment pointers, as Opik keeps them.

    existing maps a prompt name to its versions, oldest first, each a template or
    a (template, environment it was created for) pair; pointers maps an
    environment to the version it points at.
    """

    def __init__(
        self,
        existing: dict[str, list[str | tuple[str, str]]] | None = None,
        pointers: dict[str, str] | None = None,
    ) -> None:
        self.versions: dict[str, list[_FakePrompt]] = {}
        for name, entries in (existing or {}).items():
            self.versions[name] = []
            for number, entry in enumerate(entries, start=1):
                template, created_for = (
                    entry if isinstance(entry, tuple) else (entry, None)
                )
                metadata = _created_for(created_for) if created_for else None
                self.versions[name].append(
                    _FakePrompt(name, template, f"v{number}", metadata)
                )
        self.registered = {"development", "staging", "production"}
        self.writes: list[tuple[str, list[str]]] = []
        for environment, version in (pointers or {}).items():
            self.registered.add(environment)
            for prompts in self.versions.values():
                for prompt in prompts:
                    if prompt.version == version:
                        prompt.environments = [
                            *(prompt.environments or []),
                            environment,
                        ]

    def get_environments(self) -> list[SimpleNamespace]:
        return [SimpleNamespace(name=name) for name in self.registered]

    def create_environment(self, name: str) -> None:
        self.registered.add(name)

    def get_prompt(
        self,
        name: str,
        environment: str | None = None,
        version: str | None = None,
        no_cache: bool = False,  # noqa: ARG002, FBT001, FBT002
    ) -> _FakePrompt | None:
        prompts = self.versions.get(name, [])
        if version is not None:
            return next((p for p in prompts if p.version == version), None)
        if environment is None:
            return prompts[-1] if prompts else None
        # the server answers 404 for an environment it doesn't know, which the
        # SDK returns as None
        return next((p for p in prompts if environment in (p.environments or [])), None)

    def get_prompt_history(self, name: str) -> list[_FakePrompt]:
        return list(reversed(self.versions[name]))

    def create_prompt(
        self, name: str, prompt: str, metadata: dict[str, Any] | None = None
    ) -> _FakePrompt:
        prompts = self.versions.setdefault(name, [])
        # Opik adds a version only when the text or metadata differs from the latest
        if prompts and (prompts[-1].prompt, prompts[-1].metadata) == (prompt, metadata):
            return prompts[-1]
        created = _FakePrompt(name, prompt, f"v{len(prompts) + 1}", metadata)
        prompts.append(created)
        return created

    def set_prompt_environments(
        self, prompt_name: str, environments: list[str], *, version: str | None
    ) -> None:
        """Replace the version's environments, moving each one off its old version"""
        assert set(environments) <= self.registered
        self.writes.append((prompt_name, list(environments)))
        for prompt in self.versions[prompt_name]:
            if prompt.version == version:
                prompt.environments = list(environments)
            elif prompt.environments:
                kept = [e for e in prompt.environments if e not in environments]
                prompt.environments = kept or None

    def pointers(self, name: str) -> dict[str, str | None]:
        return {
            environment: prompt.version
            for prompt in self.versions[name]
            for environment in prompt.environments or []
        }


def test_render_prompt_falls_back_locally_when_opik_unavailable() -> None:
    """No Opik client at all -- render straight from default_template."""
    result = opik_auth.render_prompt(
        "feedback-summary", "Summarize: {{conversation_text}}", conversation_text="hi"
    )

    assert result == "Summarize: hi"


def test_render_prompt_uses_the_version_its_environment_points_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An edit made for QA (v2, latest) leaves Production rendering v1"""
    fake_client = _FakeOpikClient(
        existing={
            "feedback-summary": [
                "Prod: {{conversation_text}}",
                ("QA: {{conversation_text}}", "qa"),
            ]
        },
        pointers={"production": "v1", "qa": "v2"},
    )
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)
    monkeypatch.setattr(opik_auth, "PROMPT_ENVIRONMENT", "production")

    result = opik_auth.render_prompt(
        "feedback-summary", "Summarize: {{conversation_text}}", conversation_text="hi"
    )

    assert result == "Prod: hi"


def test_render_prompt_new_environment_uses_its_own_default_not_latest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An environment's first read adds its own version of its default"""
    fake_client = _FakeOpikClient(
        existing={
            "feedback-summary": [
                "Prod: {{conversation_text}}",
                "Production edit: {{conversation_text}}",
            ]
        },
        pointers={"production": "v2"},
    )
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)

    result = opik_auth.render_prompt(
        "feedback-summary", "Summarize: {{conversation_text}}", conversation_text="hi"
    )

    assert result == "Summarize: hi"
    assert "qa" in fake_client.registered
    assert fake_client.pointers("feedback-summary") == {"production": "v2", "qa": "v3"}
    assert fake_client.versions["feedback-summary"][2].metadata == _created_for("qa")


def test_render_prompt_raises_when_production_has_no_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production never labels a version itself, however old or new it is"""
    fake_client = _FakeOpikClient(
        existing={"feedback-summary": ["Old: {{x}}", ("QA: {{x}}", "qa")]},
        pointers={"qa": "v2"},
    )
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)
    monkeypatch.setattr(opik_auth, "PROMPT_ENVIRONMENT", "production")

    with pytest.raises(opik_auth.PromptLabelMissingError, match="labeled production"):
        opik_auth.render_prompt("feedback-summary", "Default: {{x}}", x="hi")

    assert fake_client.writes == []
    assert len(fake_client.versions["feedback-summary"]) == 2


def test_seed_prompt_environment_points_qa_at_its_own_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeOpikClient(
        existing={"feedback-summary": ["Prod: {{x}}"]}, pointers={"production": "v1"}
    )
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)
    monkeypatch.setattr(opik_auth, "PROMPT_ENVIRONMENT", "qa")

    opik_auth.seed_prompt_environment("feedback-summary", "Default: {{x}}")

    assert fake_client.pointers("feedback-summary") == {"production": "v1", "qa": "v2"}
    assert fake_client.versions["feedback-summary"][1].metadata == _created_for("qa")


def test_seed_prompt_environment_refuses_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeOpikClient(existing={"feedback-summary": ["Prod: {{x}}"]})
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)
    monkeypatch.setattr(opik_auth, "PROMPT_ENVIRONMENT", "production")

    with pytest.raises(opik_auth.PromptLabelMissingError):
        opik_auth.seed_prompt_environment("feedback-summary", "Default: {{x}}")

    assert fake_client.writes == []
    assert len(fake_client.versions["feedback-summary"]) == 1


def test_seed_prompt_environment_raises_without_an_opik_client() -> None:
    with pytest.raises(opik_auth.PromptLabelMissingError, match="No Opik client"):
        opik_auth.seed_prompt_environment("feedback-summary", "Default: {{x}}")


def test_render_prompt_seeds_opik_when_prompt_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeOpikClient()
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)

    result = opik_auth.render_prompt(
        "feedback-summary", "Summarize: {{conversation_text}}", conversation_text="hi"
    )

    assert result == "Summarize: hi"
    assert [p.prompt for p in fake_client.versions["feedback-summary"]] == [
        "Summarize: {{conversation_text}}"
    ]
    assert fake_client.pointers("feedback-summary") == {"qa": "v1"}


def test_publish_prompt_version_adds_its_own_version_for_new_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeOpikClient(
        existing={"extract": ["prod", ("old", "qa")]},
        pointers={"production": "v1", "qa": "v2"},
    )
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)

    assert opik_auth.publish_prompt_version("extract", "new") == "v3"
    assert fake_client.pointers("extract") == {"production": "v1", "qa": "v3"}


def test_publish_prompt_version_leaves_the_production_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production records the version that ran without moving its label"""
    fake_client = _FakeOpikClient(
        existing={"extract": ["prod"]}, pointers={"production": "v1"}
    )
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)
    monkeypatch.setattr(opik_auth, "PROMPT_ENVIRONMENT", "production")

    assert opik_auth.publish_prompt_version("extract", "new") == "v2"
    assert fake_client.pointers("extract") == {"production": "v1"}
    assert fake_client.writes == []
    assert opik_auth.publish_prompt_version("extract", "new") == "v2"
    assert len(fake_client.versions["extract"]) == 2


def test_publish_prompt_version_reuses_its_own_earlier_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """QA going back to text it published before reuses that version"""
    fake_client = _FakeOpikClient(
        existing={"extract": [("first", "qa"), ("second", "qa")]}, pointers={"qa": "v2"}
    )
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)

    assert opik_auth.publish_prompt_version("extract", "first") == "v1"
    assert len(fake_client.versions["extract"]) == 2
    assert fake_client.pointers("extract") == {"qa": "v1"}


def test_publish_prompt_version_does_not_share_another_environments_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    QA publishing Production's text gets its own copy, so no write ever names
    production and a later move of Production's pointer can't be undone by it
    """
    fake_client = _FakeOpikClient(
        existing={"extract": ["prod", ("qa branch", "qa")]},
        pointers={"production": "v1", "qa": "v2"},
    )
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)

    assert opik_auth.publish_prompt_version("extract", "prod") == "v3"
    assert fake_client.pointers("extract") == {"production": "v1", "qa": "v3"}
    assert all(environments == ["qa"] for _, environments in fake_client.writes)


def test_point_environment_at_leaves_a_version_another_environment_owns() -> None:
    fake_client = _FakeOpikClient(
        existing={"extract": [("prod", "qa")]}, pointers={"production": "v1"}
    )

    assert not opik_auth._point_environment_at(
        fake_client, fake_client.versions["extract"][0]
    )
    assert fake_client.writes == []


def test_publish_prompt_version_keeps_an_unchanged_pointer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeOpikClient(
        existing={"extract": [("same", "qa"), "other"]}, pointers={"qa": "v1"}
    )
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: fake_client)

    assert opik_auth.publish_prompt_version("extract", "same") == "v1"
    assert fake_client.pointers("extract") == {"qa": "v1"}
    assert fake_client.writes == []


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
    _configure_opik_env(monkeypatch)
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
            "ignore_arguments": None,
        }
    ]


def test_traced_forwards_ignore_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_opik_env(monkeypatch)
    track_calls: list[dict[str, Any]] = []

    def fake_track(**kwargs: Any):
        track_calls.append(kwargs)
        return lambda func: func

    monkeypatch.setattr(opik, "track", fake_track)

    @opik_auth.traced("some-span", ignore_arguments=["trace_metadata"])
    def add_one(x: int, *, trace_metadata: dict[str, Any]) -> int:  # noqa: ARG001
        return x + 1

    assert add_one(1, trace_metadata={}) == 2
    assert track_calls[0]["ignore_arguments"] == ["trace_metadata"]


def _record_span_updates(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    updates: list[dict[str, Any]] = []
    monkeypatch.setattr(
        opik_auth.opik_context,
        "update_current_span",
        lambda **kwargs: updates.append(kwargs),
    )
    return updates


def test_attach_span_prompt_links_the_published_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_opik_env(monkeypatch)
    client = _FakeOpikClient()
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: client)
    updates = _record_span_updates(monkeypatch)

    assert opik_auth.publish_prompt_version("name", "template") == "v1"
    [prompt] = client.versions["name"]
    opik_auth.attach_span_prompt("template")
    opik_auth.attach_span_prompt("a template never published")

    assert updates == [{"prompts": [prompt]}]


def test_attach_span_prompt_skips_a_failed_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_opik_env(monkeypatch)
    # a Prompt with no version recorded is not linked either
    client = _FakeOpikClient(existing={"name": ["template"]}, pointers={"qa": "v1"})
    client.versions["name"][0].version = None
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: client)
    updates = _record_span_updates(monkeypatch)

    assert opik_auth.publish_prompt_version("name", "template") == (
        opik_auth.LOCAL_PROMPT_VERSION
    )
    opik_auth.attach_span_prompt("template")

    assert updates == []
