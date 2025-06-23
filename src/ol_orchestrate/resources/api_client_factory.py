from collections.abc import Generator
from contextlib import contextmanager
from typing import ClassVar, Optional, Self

from dagster import ConfigurableResource, InitResourceContext, ResourceDependency
from pydantic import Field, PrivateAttr

from ol_orchestrate.resources.api_client import BaseApiClient
from ol_orchestrate.resources.canvas_api import CanvasApiClient
from ol_orchestrate.resources.learn_api import MITLearnApiClient
from ol_orchestrate.resources.secrets.vault import Vault


class ApiClientFactory(ConfigurableResource):
    deployment: str = Field(
        description="The deployment name this resource is used in. e.g. canvas"
    )

    vault: ResourceDependency[Vault]

    client_class: str = Field(
        description="Type of client to instantiate. e.g. 'CanvasApiClient'"
    )
    config_path: str = Field(
        description="Vault secret path where API credentials are stored. "
        "e.g. 'pipelines/canvas'"
    )
    mount_point: str = Field(
        description="Vault mount point for secrets. e.g. 'secret-data'"
    )
    kv_version: str = Field(
        default="1",
        description="The version of the KV secrets engine to use. "
        "Either '1' for KV v1 or '2' for KV v2.",
    )
    _client: Optional[BaseApiClient] = PrivateAttr(default=None)

    supported_client_class: ClassVar[dict[str, type[BaseApiClient]]] = {
        "CanvasApiClient": CanvasApiClient,
        "MITLearnApiClient": MITLearnApiClient,
    }

    def _initialize_client(self) -> BaseApiClient:
        client_class = self.supported_client_class[self.client_class]
        client_secrets = self._read_vault_secret(
            mount_point=self.mount_point,
            path=self.config_path,
            version=self.kv_version,  # e.g., "1" or "2"
        )

        return client_class.from_secret(client_secrets)

    def _read_vault_secret(
        self, mount_point: str, path: str, version: str = "1"
    ) -> dict[str, str]:
        """
        Read a Vault secret from KV v1 or v2, based on the given version.
        """
        if version == "2":
            return self.vault.client.secrets.kv.v2.read_secret_version(
                mount_point=mount_point,
                path=path,
            )["data"]["data"]
        if version == "1":
            return self.vault.client.secrets.kv.v1.read_secret(
                mount_point=mount_point,
                path=path,
            )["data"]
        msg = f"Unsupported KV version: {version}"
        raise ValueError(msg)

    @property
    def client(self) -> BaseApiClient:
        if not self._client:
            self._client = self._initialize_client()
        return self._client

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self]:  # noqa: ARG002
        yield self
