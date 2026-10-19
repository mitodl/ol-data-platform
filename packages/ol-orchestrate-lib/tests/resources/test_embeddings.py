"""Tests for the Qdrant and LiteLLM embedding resources, with no network calls."""

from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from ol_orchestrate.resources import embeddings
from ol_orchestrate.resources.embeddings import LiteLLMEmbedderResource, QdrantResource
from ol_orchestrate.resources.secrets.vault import Vault

QDRANT_HOST = "https://qdrant.example.com"
QDRANT_KEY = "qk"  # pragma: allowlist secret
OPENAI_KEY = "ok"  # pragma: allowlist secret
SECRETS = {
    "pipelines/qdrant": {"host": QDRANT_HOST, "api_key": QDRANT_KEY},
    "pipelines/openai": {"api_key": OPENAI_KEY},
}


def _read_secret(mount_point: str, path: str) -> dict[str, Any]:
    assert mount_point == "secret-data"
    return {"data": SECRETS[path]}


@pytest.fixture
def vault_client() -> Iterator[MagicMock]:
    client = MagicMock()
    client.secrets.kv.v1.read_secret.side_effect = _read_secret
    with patch.object(Vault, "client", new_callable=PropertyMock, return_value=client):
        yield client


def _vault() -> Vault:
    return Vault(vault_addr="https://vault.example.com", vault_auth_type="token")


def _embedding_response(texts: list[str], dim: int) -> MagicMock:
    response = MagicMock()
    response.to_dict.return_value = {
        "data": [
            {"index": i, "embedding": [float(len(text))] * dim}
            for i, text in enumerate(texts)
        ]
    }
    return response


def test_qdrant_client_has_cloud_inference(vault_client: MagicMock) -> None:
    resource = QdrantResource(
        vault=_vault(),
        vault_path="pipelines/qdrant",
        resources_collection="r",
        content_files_collection="c",
        topics_collection="t",
    )
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


def _embedder(**config: Any) -> LiteLLMEmbedderResource:
    return LiteLLMEmbedderResource(
        vault=_vault(), vault_path="pipelines/openai", **config
    )


@pytest.mark.usefixtures("vault_client")
def test_embedder_batches_and_calls_litellm_as_learn_does() -> None:
    texts = [f"text {i}" for i in range(250)]
    embedder = _embedder(embedding_dim=2)
    with patch(
        "litellm.embedding",
        side_effect=lambda **kwargs: _embedding_response(kwargs["input"], 2),
    ) as embedding:
        vectors = embedder.embed_documents(texts)

    assert vectors == [[float(len(text))] * 2 for text in texts]
    assert [len(call.kwargs["input"]) for call in embedding.call_args_list] == [
        100,
        100,
        50,
    ]
    assert embedding.call_args_list[0].kwargs == {
        "model": "text-embedding-3-large",
        "input": texts[:100],
        "custom_llm_provider": "openai",
        "api_base": None,
        "api_key": OPENAI_KEY,
        "max_retries": 3,
        "timeout": 600,
    }
    assert embedder.vector_name == "text-embedding-3-large"


@pytest.mark.usefixtures("vault_client")
def test_embedder_rejects_vectors_of_the_wrong_length() -> None:
    embedder = _embedder()
    with (
        patch(
            "litellm.embedding",
            side_effect=lambda **kwargs: _embedding_response(kwargs["input"], 1536),
        ),
        pytest.raises(ValueError, match="not 3072 long"),
    ):
        embedder.embed_documents(["a"])


@pytest.mark.usefixtures("vault_client")
def test_embedder_rejects_a_short_response() -> None:
    embedder = _embedder(embedding_dim=1)
    with (
        patch(
            "litellm.embedding",
            side_effect=lambda **kwargs: _embedding_response(kwargs["input"][:1], 1),
        ),
        pytest.raises(ValueError, match="2 texts returned 1 vectors"),
    ):
        embedder.embed_documents(["a", "b"])


def test_embedder_sends_nothing_for_no_texts() -> None:
    with patch("litellm.embedding") as embedding:
        assert _embedder().embed_documents([]) == []
    embedding.assert_not_called()
