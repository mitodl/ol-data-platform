"""Tests for the Qdrant and ol-llm embedding resources, with no network calls."""

import os
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from dagster import build_init_resource_context
from ol_llm import KeycloakClient, Settings, TracingConfig
from ol_orchestrate.lib.embeddings.learn import LEARN_V1_CONTENT_FILES
from ol_orchestrate.resources import embeddings
from ol_orchestrate.resources.embeddings import (
    EmbeddingsResource,
    ProviderKeySecret,
    QdrantResource,
)
from ol_orchestrate.resources.secrets.vault import Vault

QDRANT_HOST = "https://qdrant.example.com"
QDRANT_KEY = "qk"  # pragma: allowlist secret
OPIK_CLIENT_SECRET = "cs"  # noqa: S105  # pragma: allowlist secret
OPENAI_KEY = "ok"  # pragma: allowlist secret
SECRETS = {
    ("secret-data", "pipelines/openai"): {"api_key": OPENAI_KEY},
    ("secret-data", "pipelines/qdrant"): {"host": QDRANT_HOST, "api_key": QDRANT_KEY},
    ("secret-opik", "sdk-client"): {
        "url": "https://sso.example.com/realms/ol",
        "client_id": "opik-client",
        "client_secret": OPIK_CLIENT_SECRET,
    },
}


def _read_secret(mount_point: str, path: str) -> dict[str, Any]:
    return {"data": SECRETS[mount_point, path]}


@pytest.fixture
def vault_client() -> Iterator[MagicMock]:
    client = MagicMock()
    client.secrets.kv.v1.read_secret.side_effect = _read_secret
    with patch.object(Vault, "client", new_callable=PropertyMock, return_value=client):
        yield client


def _vault() -> Vault:
    return Vault(vault_addr="https://vault.example.com", vault_auth_type="token")


def _qdrant() -> QdrantResource:
    return QdrantResource(
        vault=_vault(),
        vault_path="pipelines/qdrant",
        base_collection_name="mitlearn-production",
    )


def test_qdrant_client_has_cloud_inference(vault_client: MagicMock) -> None:
    resource = _qdrant()
    with patch.object(embeddings, "QdrantClient") as qdrant_client:
        assert resource.get_client() is resource.get_client()

    qdrant_client.assert_called_once_with(
        url=QDRANT_HOST,
        api_key=QDRANT_KEY,
        grpc_port=6334,
        prefer_grpc=True,
        cloud_inference=True,
        timeout=10,
    )
    vault_client.secrets.kv.v1.read_secret.assert_called_once_with(
        mount_point="secret-data", path="pipelines/qdrant"
    )


def test_collection_names_follow_learn() -> None:
    assert LEARN_V1_CONTENT_FILES.qdrant is not None
    assert (
        _qdrant().collection_name(LEARN_V1_CONTENT_FILES.qdrant)
        == "mitlearn-production.platform_content_files"
    )


@pytest.fixture
def ol_llm_setup() -> Iterator[tuple[MagicMock, MagicMock]]:
    with (
        patch.object(embeddings, "configure") as configure,
        patch.object(embeddings, "configure_tracing") as configure_tracing,
    ):
        yield configure, configure_tracing


def _set_up(resource: EmbeddingsResource) -> None:
    resource.setup_for_execution(build_init_resource_context())


def test_tracing_is_off_without_an_opik_url(
    ol_llm_setup: tuple[MagicMock, MagicMock],
    vault_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPIK_URL_OVERRIDE", raising=False)
    monkeypatch.setenv("OL_LLM_ENVIRONMENT", "qa")
    configure, configure_tracing = ol_llm_setup

    _set_up(EmbeddingsResource(vault=_vault(), environment="ci"))

    configure.assert_called_once_with(Settings(environment="ci"))
    configure_tracing.assert_called_once_with(
        TracingConfig(enabled=False, include_content=False)
    )
    vault_client.secrets.kv.v1.read_secret.assert_not_called()


@pytest.mark.usefixtures("ol_llm_setup")
def test_provider_keys_are_copied_from_vault(
    vault_client: MagicMock,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPIK_URL_OVERRIDE", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    resource = EmbeddingsResource(
        vault=_vault(),
        environment="ci",
        provider_keys=[
            ProviderKeySecret(env_var="OPENAI_API_KEY", vault_path="pipelines/openai")
        ],
    )

    _set_up(resource)

    assert os.environ["OPENAI_API_KEY"] == OPENAI_KEY


def test_azure_ad_auth_gives_ol_llm_a_token_provider(
    ol_llm_setup: tuple[MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPIK_URL_OVERRIDE", raising=False)
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://example.openai.azure.com")
    monkeypatch.setenv("OPENAI_API_VERSION", "2024-10-21")
    configure, _ = ol_llm_setup
    embeddings._azure_ad_token_provider.cache_clear()

    with (
        patch.object(embeddings, "DefaultAzureCredential") as credential,
        patch.object(
            embeddings, "get_bearer_token_provider", return_value=lambda: "token"
        ) as token_provider,
    ):
        _set_up(
            EmbeddingsResource(vault=_vault(), environment="ci", azure_ad_auth=True)
        )
        _set_up(
            EmbeddingsResource(vault=_vault(), environment="ci", azure_ad_auth=True)
        )
    embeddings._azure_ad_token_provider.cache_clear()

    credential.assert_called_once_with()
    token_provider.assert_called_once_with(
        credential.return_value, "https://cognitiveservices.azure.com/.default"
    )
    settings = configure.call_args.args[0]
    assert settings.environment == "ci"
    assert settings.azure_ad_token_provider() == "token"


@pytest.mark.usefixtures("ol_llm_setup")
def test_azure_ad_auth_needs_the_endpoint_and_api_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPIK_URL_OVERRIDE", raising=False)
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://example.openai.azure.com")
    monkeypatch.delenv("OPENAI_API_VERSION", raising=False)

    with pytest.raises(ValueError, match="OPENAI_API_VERSION"):
        _set_up(
            EmbeddingsResource(vault=_vault(), environment="ci", azure_ad_auth=True)
        )


def test_tracing_is_off_without_the_keycloak_client_location(
    ol_llm_setup: tuple[MagicMock, MagicMock],
    vault_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An Opik URL without the client's Vault location doesn't trace (or fail)"""
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik.example.com/api")
    monkeypatch.delenv("OPIK_KEYCLOAK_VAULT_MOUNT", raising=False)
    monkeypatch.delenv("OPIK_KEYCLOAK_VAULT_PATH", raising=False)
    _, configure_tracing = ol_llm_setup

    _set_up(EmbeddingsResource(vault=_vault(), environment="qa"))

    configure_tracing.assert_called_once_with(
        TracingConfig(enabled=False, include_content=False)
    )
    vault_client.secrets.kv.v1.read_secret.assert_not_called()


def test_tracing_authenticates_with_the_vault_keycloak_client(
    ol_llm_setup: tuple[MagicMock, MagicMock],
    vault_client: MagicMock,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik.example.com/api")
    monkeypatch.setenv("OPIK_KEYCLOAK_VAULT_MOUNT", "secret-opik")
    monkeypatch.setenv("OPIK_KEYCLOAK_VAULT_PATH", "sdk-client")
    _, configure_tracing = ol_llm_setup

    _set_up(EmbeddingsResource(vault=_vault(), environment="production"))

    configure_tracing.assert_called_once_with(
        TracingConfig(
            keycloak=KeycloakClient(
                token_url="https://sso.example.com/realms/ol/protocol/openid-connect/token",
                client_id="opik-client",
                client_secret=OPIK_CLIENT_SECRET,
            ),
            include_content=False,
        )
    )


@pytest.mark.usefixtures("ol_llm_setup")
def test_each_strategy_gets_its_own_use_case(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPIK_URL_OVERRIDE", raising=False)
    resource = EmbeddingsResource(vault=_vault(), environment="ci")
    _set_up(resource)

    embedder = resource.embedder(LEARN_V1_CONTENT_FILES)

    assert embedder.spec == LEARN_V1_CONTENT_FILES.model
    assert embedder._use_case.name == "embed-learn-v1-content-files"
    assert embedder._use_case.app == "ol-data-platform"
    assert embedder._use_case.profile == "batch"
