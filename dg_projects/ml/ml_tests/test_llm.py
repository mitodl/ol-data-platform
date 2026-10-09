"""Tests for ml.resources.llm.LLMClientFactory."""

from collections.abc import Callable

import httpx2
import pytest
from anthropic import Anthropic, AnthropicBedrock
from google import genai
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


@pytest.fixture
def entra_token_provider(monkeypatch: pytest.MonkeyPatch) -> list[tuple[object, ...]]:
    """Replace the Entra token provider so no Azure credential is resolved.

    :returns: the (credential, scopes) each get_bearer_token_provider call got.
    """
    calls: list[tuple[object, ...]] = []

    def fake_get_bearer_token_provider(
        credential: object, *scopes: str
    ) -> Callable[[], str]:
        calls.append((credential, *scopes))
        return lambda: "entra-test-token"  # pragma: allowlist secret

    monkeypatch.setattr(
        "ml.resources.llm.get_bearer_token_provider", fake_get_bearer_token_provider
    )
    monkeypatch.setattr("ml.resources.llm.DefaultAzureCredential", lambda: "fake-cred")
    return calls


def test_get_client_azure_openai_uses_entra_token_and_caches(
    monkeypatch: pytest.MonkeyPatch,
    entra_token_provider: list[tuple[object, ...]],
) -> None:
    """The accounts disable key auth, so an API key in the env is never read."""
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "ignored")  # pragma: allowlist secret
    factory = LLMClientFactory(
        client_class="azure_openai",
        azure_endpoint="https://example-resource.openai.azure.com/",
    )

    first = factory.get_client()
    second = factory.get_client()

    assert isinstance(first, OpenAI)
    assert str(first.base_url) == "https://example-resource.openai.azure.com/openai/v1/"
    assert first is second
    assert entra_token_provider == [
        ("fake-cred", "https://cognitiveservices.azure.com/.default")
    ]


@pytest.mark.usefixtures("entra_token_provider")
def test_get_client_azure_openai_sends_entra_token_as_bearer() -> None:
    seen_auth: list[str] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen_auth.append(request.headers["Authorization"])
        return httpx2.Response(
            200,
            json={
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "created": 0,
                "model": "gpt-5-mini",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "ok"},
                        "finish_reason": "stop",
                    }
                ],
            },
        )

    client = LLMClientFactory(
        client_class="azure_openai",
        azure_endpoint="https://example-resource.openai.azure.com",
    ).get_client()
    assert isinstance(client, OpenAI)

    client.with_options(
        http_client=httpx2.Client(transport=httpx2.MockTransport(handler))
    ).chat.completions.create(
        model="gpt-5-mini", messages=[{"role": "user", "content": "hi"}]
    )

    assert seen_auth == ["Bearer entra-test-token"]


def test_get_client_gemini_uses_vertex_ai_without_an_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deployed pods authenticate with workload identity, so a key is ignored."""
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-test")
    monkeypatch.setenv("GOOGLE_GENAI_USE_ENTERPRISE", "false")
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "true")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("GOOGLE_CLOUD_LOCATION", "global")

    client = LLMClientFactory(client_class="gemini").get_client()

    assert isinstance(client, genai.Client)
    assert client.vertexai is True
    assert client._api_client.project == "test-project"
    assert client._api_client.location == "global"
    assert client._api_client.api_key is None


def test_get_client_gemini_on_vertex_ai_requires_a_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "1")
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    monkeypatch.setenv("GOOGLE_CLOUD_LOCATION", "global")

    with pytest.raises(ValueError, match="GOOGLE_CLOUD_PROJECT"):
        LLMClientFactory(client_class="gemini").get_client()


def test_get_client_gemini_reads_the_api_key_outside_vertex_ai(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GOOGLE_GENAI_USE_VERTEXAI", raising=False)
    monkeypatch.setenv("GOOGLE_GENAI_USE_ENTERPRISE", "true")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-test")

    client = LLMClientFactory(client_class="gemini").get_client()

    assert isinstance(client, genai.Client)
    assert client.vertexai is False


def test_get_client_gemini_requires_an_api_key_outside_vertex_ai(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GOOGLE_GENAI_USE_VERTEXAI", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)

    with pytest.raises(ValueError, match="GEMINI_API_KEY"):
        LLMClientFactory(client_class="gemini").get_client()
