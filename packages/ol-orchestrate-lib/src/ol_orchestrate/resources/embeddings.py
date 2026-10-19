"""Qdrant and LiteLLM resources for writing MIT Learn's vector collections.

Requires the ``embeddings`` extra.
"""

from collections.abc import Sequence
from itertools import batched
from typing import Any, cast

import litellm
from dagster import ConfigurableResource, ResourceDependency
from pydantic import Field, PrivateAttr
from qdrant_client import QdrantClient

from ol_orchestrate.lib.learn_embeddings import DENSE_MODEL_NAME, vector_name
from ol_orchestrate.resources.secrets.vault import Vault

QDRANT_GRPC_PORT = 6334
QDRANT_TIMEOUT_SECONDS = 10
EMBEDDING_REQUEST_TIMEOUT_SECONDS = 600
TEXT_EMBEDDING_3_LARGE_DIMENSIONS = 3072


def _read_kv1_secret(vault: Vault, mount_point: str, path: str) -> dict[str, Any]:
    return vault.client.secrets.kv.v1.read_secret(mount_point=mount_point, path=path)[
        "data"
    ]


class QdrantResource(ConfigurableResource[None]):
    """A Qdrant Cloud client with cloud inference on.

    Cloud inference is what turns the ``models.Document`` sparse inputs from
    :func:`ol_orchestrate.lib.learn_embeddings.build_point` into BM25 vectors.
    The Vault secret holds ``host`` (the cluster URL) and ``api_key``.
    """

    vault: ResourceDependency[Vault]
    vault_mount_point: str = "secret-data"
    vault_path: str = Field(description="KV v1 path of the secret with host, api_key")
    resources_collection: str = Field(description="one point per learning resource")
    content_files_collection: str = Field(
        description="content file chunks and resource metadata chunks"
    )
    topics_collection: str = Field(description="one point per topic")
    grpc_port: int = QDRANT_GRPC_PORT
    timeout_seconds: int = QDRANT_TIMEOUT_SECONDS

    _client: QdrantClient | None = PrivateAttr(default=None)

    def get_client(self) -> QdrantClient:
        """Return the client, reading the credentials from Vault on first use.

        :rtype: QdrantClient
        """
        if self._client is None:
            secret = _read_kv1_secret(
                self.vault, self.vault_mount_point, self.vault_path
            )
            self._client = QdrantClient(
                url=secret["host"],
                api_key=secret["api_key"],
                grpc_port=self.grpc_port,
                prefer_grpc=True,
                cloud_inference=True,
                timeout=self.timeout_seconds,
            )
        return self._client


class LiteLLMEmbedderResource(ConfigurableResource[None]):
    """Dense embeddings through ``litellm.embedding``, called as MIT Learn calls it.

    The Vault secret holds ``api_key`` for the provider. Texts are sent
    ``batch_size`` at a time, and the provider client retries rate limits,
    timeouts and server errors ``max_retries`` times with backoff.
    """

    vault: ResourceDependency[Vault]
    vault_mount_point: str = "secret-data"
    vault_path: str = Field(description="KV v1 path of the secret with api_key")
    model_name: str = DENSE_MODEL_NAME
    embedding_dim: int = Field(
        default=TEXT_EMBEDDING_3_LARGE_DIMENSIONS,
        description="length every returned vector must have",
    )
    custom_llm_provider: str = "openai"
    api_base: str | None = None
    batch_size: int = 100
    max_retries: int = 3
    request_timeout_seconds: int = EMBEDDING_REQUEST_TIMEOUT_SECONDS

    _api_key: str | None = PrivateAttr(default=None)

    @property
    def vector_name(self) -> str:
        """The name of the dense vector in Learn's collections.

        :rtype: str
        """
        return vector_name(self.model_name)

    def _get_api_key(self) -> str:
        if self._api_key is None:
            self._api_key = _read_kv1_secret(
                self.vault, self.vault_mount_point, self.vault_path
            )["api_key"]
        return self._api_key

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        """Embed texts, ``batch_size`` per request.

        :param texts: the texts to embed
        :returns: one vector per text, in order
        :rtype: list[list[float]]
        :raises ValueError: when the provider returns the wrong number of
            vectors or a vector whose length is not ``embedding_dim``
        """
        vectors: list[list[float]] = []
        for batch in batched(texts, self.batch_size):
            response = litellm.embedding(
                model=self.model_name,
                input=list(batch),
                custom_llm_provider=self.custom_llm_provider,
                api_base=self.api_base,
                api_key=self._get_api_key(),
                max_retries=self.max_retries,
                timeout=self.request_timeout_seconds,
            )
            data = cast(list[dict[str, Any]], response.to_dict()["data"])
            batch_vectors = [item["embedding"] for item in data]
            if len(batch_vectors) != len(batch):
                msg = f"{len(batch)} texts returned {len(batch_vectors)} vectors"
                raise ValueError(msg)
            if any(len(vector) != self.embedding_dim for vector in batch_vectors):
                msg = (
                    f"{self.model_name} returned a vector not {self.embedding_dim} long"
                )
                raise ValueError(msg)
            vectors.extend(batch_vectors)
        return vectors
