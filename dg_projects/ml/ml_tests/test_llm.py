"""Tests for ml.resources.llm.LLMClientFactory."""

import pytest
from anthropic import Anthropic, AnthropicBedrock
from ml.resources.llm import LLMClientFactory
from openai import OpenAI


def test_get_client_reads_anthropic_api_key_env_var_and_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_api_key = "sk-ant-test"  # pragma: allowlist secret
    monkeypatch.setenv("ANTHROPIC_API_KEY", fake_api_key)
    factory = LLMClientFactory()

    first = factory.get_client()
    second = factory.get_client()

    assert isinstance(first, Anthropic)
    assert first.api_key == fake_api_key
    assert first is second


def test_get_client_requires_anthropic_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    factory = LLMClientFactory()

    with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
        factory.get_client()


def test_get_client_honors_the_client_class_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """client_class="openai" returns an OpenAI client, not Anthropic."""
    fake_api_key = "sk-openai-test"  # pragma: allowlist secret
    monkeypatch.setenv("OPENAI_API_KEY", fake_api_key)
    factory = LLMClientFactory(client_class="openai")

    client = factory.get_client()

    assert isinstance(client, OpenAI)
    assert client.api_key == fake_api_key


def test_get_client_openai_compatible_uses_unused_api_key_by_default() -> None:
    factory = LLMClientFactory(
        client_class="openai_compatible",
        base_url="http://gpu-node.internal:8000/v1",
    )

    client = factory.get_client()

    assert isinstance(client, OpenAI)
    assert str(client.base_url) == "http://gpu-node.internal:8000/v1/"
    assert client.api_key == "unused"  # pragma: allowlist secret


def test_get_client_openai_compatible_honors_api_key_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An authenticated gateway (e.g. an internal LLM proxy) needs a real bearer
    token instead of the unauthenticated-server default.
    """
    monkeypatch.setenv("OPENAI_COMPATIBLE_API_KEY", "sk-gateway-test")
    factory = LLMClientFactory(
        client_class="openai_compatible",
        base_url="https://gateway.internal/v1",
    )

    client = factory.get_client()

    assert client.api_key == "sk-gateway-test"  # pragma: allowlist secret


def test_get_client_openai_compatible_requires_base_url() -> None:
    factory = LLMClientFactory(client_class="openai_compatible")

    with pytest.raises(ValueError, match="base_url"):
        factory.get_client()


def test_get_client_bedrock_uses_iam_auth_no_api_key_needed() -> None:
    """client_class="bedrock" needs no API key at all -- IAM metadata auth."""
    factory = LLMClientFactory(client_class="bedrock", aws_region="us-west-2")

    client = factory.get_client()

    assert isinstance(client, AnthropicBedrock)
    assert client.aws_region == "us-west-2"


def test_get_client_bedrock_defaults_region_and_caches() -> None:
    factory = LLMClientFactory(client_class="bedrock")

    first = factory.get_client()
    second = factory.get_client()

    assert first.aws_region == "us-east-1"
    assert first is second


def test_get_client_azure_openai_requires_endpoint() -> None:
    factory = LLMClientFactory(client_class="azure_openai")

    with pytest.raises(ValueError, match="azure_endpoint"):
        factory.get_client()


def test_get_client_azure_openai_requires_api_key_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AZURE_OPENAI_API_KEY", raising=False)
    factory = LLMClientFactory(
        client_class="azure_openai",
        azure_endpoint="https://example-resource.openai.azure.com",
    )

    with pytest.raises(ValueError, match="AZURE_OPENAI_API_KEY"):
        factory.get_client()


def test_get_client_azure_openai_reads_env_var_and_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_api_key = "azure-env-test-key"  # pragma: allowlist secret
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", fake_api_key)
    factory = LLMClientFactory(
        client_class="azure_openai",
        azure_endpoint="https://example-resource.openai.azure.com",
    )

    first = factory.get_client()
    second = factory.get_client()

    assert isinstance(first, OpenAI)
    assert first.api_key == fake_api_key
    assert str(first.base_url) == "https://example-resource.openai.azure.com/openai/v1/"
    assert first is second
