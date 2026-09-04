"""Tests for ml.resources.llm.LLMClientFactory."""

import pytest
from anthropic import Anthropic, AnthropicBedrock
from ml.resources.llm import LLMClientFactory
from ol_orchestrate.resources.secrets.vault import Vault
from openai import OpenAI


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


def test_get_client_reads_the_documented_vault_path_and_caches() -> None:
    """pipelines/feedback-llm/api_key is the ops path; one Vault read per client."""
    fake_api_key = "sk-ant-test"  # pragma: allowlist secret
    kv_v1 = _FakeKvV1({"secret-data/pipelines/feedback-llm": {"api_key": fake_api_key}})
    factory = LLMClientFactory(vault=_build_vault(kv_v1))

    first = factory.get_client()
    second = factory.get_client()

    assert isinstance(first, Anthropic)
    assert first.api_key == fake_api_key
    assert first is second
    assert kv_v1.reads == 1


def test_get_client_honors_the_client_class_field() -> None:
    """client_class="openai" returns an OpenAI client, not Anthropic."""
    fake_api_key = "sk-openai-test"  # pragma: allowlist secret
    kv_v1 = _FakeKvV1({"secret-data/pipelines/feedback-llm": {"api_key": fake_api_key}})
    factory = LLMClientFactory(vault=_build_vault(kv_v1), client_class="openai")

    client = factory.get_client()

    assert isinstance(client, OpenAI)
    assert client.api_key == fake_api_key


def test_get_client_openai_compatible_skips_vault() -> None:
    """client_class="openai_compatible" hits base_url, not Vault."""
    kv_v1 = _FakeKvV1({})  # no secrets configured -- a Vault read would KeyError
    factory = LLMClientFactory(
        vault=_build_vault(kv_v1),
        client_class="openai_compatible",
        base_url="http://gpu-node.internal:8000/v1",
    )

    client = factory.get_client()

    assert isinstance(client, OpenAI)
    assert str(client.base_url) == "http://gpu-node.internal:8000/v1/"
    assert kv_v1.reads == 0


def test_get_client_openai_compatible_requires_base_url() -> None:
    kv_v1 = _FakeKvV1({})
    factory = LLMClientFactory(
        vault=_build_vault(kv_v1), client_class="openai_compatible"
    )

    with pytest.raises(ValueError, match="base_url"):
        factory.get_client()


def test_get_client_bedrock_skips_vault_and_uses_iam_auth() -> None:
    """client_class="bedrock" hits AWS IAM auth, not Vault -- no API key at all."""
    kv_v1 = _FakeKvV1({})  # no secrets configured -- a Vault read would KeyError
    factory = LLMClientFactory(
        vault=_build_vault(kv_v1), client_class="bedrock", aws_region="us-west-2"
    )

    client = factory.get_client()

    assert isinstance(client, AnthropicBedrock)
    assert client.aws_region == "us-west-2"
    assert kv_v1.reads == 0


def test_get_client_bedrock_defaults_region_and_caches() -> None:
    kv_v1 = _FakeKvV1({})
    factory = LLMClientFactory(vault=_build_vault(kv_v1), client_class="bedrock")

    first = factory.get_client()
    second = factory.get_client()

    assert first.aws_region == "us-east-1"
    assert first is second


def test_get_client_azure_openai_requires_endpoint() -> None:
    kv_v1 = _FakeKvV1({})
    factory = LLMClientFactory(vault=_build_vault(kv_v1), client_class="azure_openai")

    with pytest.raises(ValueError, match="azure_endpoint"):
        factory.get_client()


def test_get_client_azure_openai_reads_vault_and_caches() -> None:
    fake_api_key = "azure-test-key"  # pragma: allowlist secret
    kv_v1 = _FakeKvV1({"secret-data/pipelines/feedback-llm": {"api_key": fake_api_key}})
    factory = LLMClientFactory(
        vault=_build_vault(kv_v1),
        client_class="azure_openai",
        azure_endpoint="https://example-resource.openai.azure.com",
    )

    first = factory.get_client()
    second = factory.get_client()

    assert isinstance(first, OpenAI)
    assert first.api_key == fake_api_key
    assert str(first.base_url) == "https://example-resource.openai.azure.com/openai/v1/"
    assert first is second
    assert kv_v1.reads == 1


def test_model_version_for_client_defaults_to_anthropic_model_version() -> None:
    kv_v1 = _FakeKvV1({})
    factory = LLMClientFactory(vault=_build_vault(kv_v1))

    assert factory.model_version_for_client() == "claude-haiku-4-5"


def test_model_version_for_client_uses_bedrock_model_version_for_bedrock() -> None:
    """model_version is a plain Anthropic API id, never valid on Bedrock -- the
    bedrock client_class must select bedrock_model_version instead.
    """
    kv_v1 = _FakeKvV1({})
    factory = LLMClientFactory(vault=_build_vault(kv_v1), client_class="bedrock")

    assert (
        factory.model_version_for_client()
        == "global.anthropic.claude-haiku-4-5-20251001-v1:0"
    )


def test_model_version_for_client_is_overridable_per_run() -> None:
    """Both fields are ordinary resource config -- overridable via Dagster run
    config (e.g. the launchpad) to experiment with a different model, with no
    code change or redeploy needed.
    """
    kv_v1 = _FakeKvV1({})
    factory = LLMClientFactory(
        vault=_build_vault(kv_v1),
        client_class="bedrock",
        bedrock_model_version="us.anthropic.claude-sonnet-4-5-20250929-v1:0",
    )

    assert (
        factory.model_version_for_client()
        == "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
    )


def test_get_client_azure_openai_honors_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AZURE_OPENAI_API_KEY skips Vault, matching the other client classes."""
    fake_api_key = "azure-env-test-key"  # pragma: allowlist secret
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", fake_api_key)
    kv_v1 = _FakeKvV1({})  # no secrets configured -- a Vault read would KeyError
    factory = LLMClientFactory(
        vault=_build_vault(kv_v1),
        client_class="azure_openai",
        azure_endpoint="https://example-resource.openai.azure.com",
    )

    client = factory.get_client()

    assert isinstance(client, OpenAI)
    assert client.api_key == fake_api_key
    assert kv_v1.reads == 0
