"""Resources for computing embeddings through ol-llm and writing them to Qdrant.

Requires the ``embeddings`` extra.
"""

import dataclasses
import os
from collections.abc import Callable
from functools import cache
from typing import Any

from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from dagster import (
    Config,
    ConfigurableResource,
    InitResourceContext,
    ResourceDependency,
)
from ol_llm import (
    Attribution,
    KeycloakClient,
    Settings,
    TracingConfig,
    UseCase,
    configure,
    configure_tracing,
)
from pydantic import Field, PrivateAttr
from qdrant_client import QdrantClient

from ol_orchestrate.lib.embeddings.models import OlLlmEmbedder
from ol_orchestrate.lib.embeddings.strategy import EmbeddingStrategy, QdrantTarget
from ol_orchestrate.resources.secrets.vault import Vault

QDRANT_GRPC_PORT = 6334
QDRANT_TIMEOUT_SECONDS = 10
AZURE_COGNITIVE_SERVICES_SCOPE = "https://cognitiveservices.azure.com/.default"
# ol-llm's Azure OpenAI client reads these from the environment itself.
AZURE_OPENAI_ENV_VARS = ("AZURE_OPENAI_ENDPOINT", "OPENAI_API_VERSION")


def _read_kv1_secret(vault: Vault, mount_point: str, path: str) -> dict[str, Any]:
    return vault.client.secrets.kv.v1.read_secret(mount_point=mount_point, path=path)[
        "data"
    ]


@cache
def _azure_ad_token_provider() -> Callable[[], str]:
    # One credential per process: it caches its token, and every resource setup
    # and event loop can share it because the provider is synchronous.
    return get_bearer_token_provider(
        DefaultAzureCredential(), AZURE_COGNITIVE_SERVICES_SCOPE
    )


class QdrantResource(ConfigurableResource[None]):
    """A Qdrant Cloud client with cloud inference on.

    Cloud inference is what turns the ``models.Document`` sparse inputs from
    :func:`ol_orchestrate.lib.embeddings.qdrant.build_points` into BM25 vectors.
    The Vault secret holds ``host`` (the cluster URL) and ``api_key``.
    """

    vault: ResourceDependency[Vault]
    vault_mount_point: str = "secret-data"
    vault_path: str = Field(description="KV v1 path of the secret with host, api_key")
    base_collection_name: str = Field(
        description="the collections' common prefix, e.g. mitlearn-production"
    )
    grpc_port: int = QDRANT_GRPC_PORT
    timeout_seconds: int = QDRANT_TIMEOUT_SECONDS

    _client: QdrantClient | None = PrivateAttr(default=None)

    def collection_name(self, target: QdrantTarget) -> str:
        """Return a target's collection name, ``{base}.{collection}`` as Learn's are.

        :param target: a strategy's Qdrant target
        :rtype: str
        """
        return f"{self.base_collection_name}.{target.collection}"

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


class ProviderKeySecret(Config):
    """A provider API key in Vault, and the variable its SDK reads it from.

    ol-llm builds provider clients that read their key from the environment, so
    the resource copies each key from Vault into its variable.
    """

    env_var: str = Field(description="e.g. OPENAI_API_KEY")
    vault_mount_point: str = "secret-data"
    vault_path: str = Field(description="KV v1 path of the secret")
    key: str = "api_key"


class EmbeddingsResource(ConfigurableResource[None]):
    """Embedders for strategies, calling models through ol-llm.

    Each strategy gets its own ol-llm use case, ``embed-{strategy id}``, so its
    requests and their cost can be told apart in Opik. Provider keys come from
    Vault (``provider_keys``); Bedrock needs none, as it authenticates with the
    pod's IAM role, and neither does Azure OpenAI with ``azure_ad_auth`` set.

    Tracing to Opik is on when ``OPIK_URL_OVERRIDE`` is set, as for the ml code
    location. The Keycloak client that authenticates to it is read from Vault.
    Traces leave out the embedded texts unless ``trace_content`` is set, since
    a backfill would otherwise copy the corpus into Opik.
    """

    vault: ResourceDependency[Vault]
    environment: str = Field(description="put on every trace, e.g. production")
    app: str = Field(
        default="ol-data-platform",
        description="the ol-llm app, which is the Opik project",
    )
    provider_keys: list[ProviderKeySecret] = Field(default_factory=list)
    azure_ad_auth: bool = Field(
        default=False,
        description="Authenticate to Azure OpenAI with an Entra ID token from "
        "DefaultAzureCredential (e.g. workload identity), not an API key. "
        "AZURE_OPENAI_ENDPOINT and OPENAI_API_VERSION must be set.",
    )
    trace_content: bool = False
    opik_vault_mount_point: str = "secret-opik"
    opik_vault_path: str = Field(
        default="sdk-client",
        description="KV v1 path of the Keycloak client with url, client_id, "
        "client_secret",
    )

    _run_id: str | None = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Configure ol-llm and its tracing for this process.

        :param context: the resource context, for the run id
        """
        for secret in self.provider_keys:
            os.environ[secret.env_var] = _read_kv1_secret(
                self.vault, secret.vault_mount_point, secret.vault_path
            )[secret.key]
        # Read the rest of the settings (gateway routes) from OL_LLM_* as usual.
        settings = Settings.from_env(
            {**os.environ, "OL_LLM_ENVIRONMENT": self.environment}
        )
        if self.azure_ad_auth:
            # Checked here so a missing one fails the setup, not every request.
            if missing := [
                name for name in AZURE_OPENAI_ENV_VARS if not os.environ.get(name)
            ]:
                msg = f"azure_ad_auth needs {', '.join(missing)} set"
                raise ValueError(msg)
            settings = dataclasses.replace(
                settings, azure_ad_token_provider=_azure_ad_token_provider()
            )
        configure(settings)
        configure_tracing(self._tracing_config())
        self._run_id = context.run_id

    def _tracing_config(self) -> TracingConfig:
        if not os.environ.get("OPIK_URL_OVERRIDE"):
            return TracingConfig(enabled=False, include_content=self.trace_content)
        secret = _read_kv1_secret(
            self.vault, self.opik_vault_mount_point, self.opik_vault_path
        )
        return TracingConfig(
            keycloak=KeycloakClient(
                token_url=f"{secret['url']}/protocol/openid-connect/token",
                client_id=secret["client_id"],
                client_secret=secret["client_secret"],
            ),
            include_content=self.trace_content,
        )

    def embedder(self, strategy: EmbeddingStrategy) -> OlLlmEmbedder:
        """Return an embedder for a strategy's model.

        :param strategy: the strategy
        :rtype: OlLlmEmbedder
        """
        use_case = UseCase(
            app=self.app,
            name=f"embed-{strategy.strategy_id.replace('_', '-')}",
            profile="batch",
            model=strategy.model.model,
        )
        return OlLlmEmbedder(strategy.model, use_case, Attribution(run_id=self._run_id))
