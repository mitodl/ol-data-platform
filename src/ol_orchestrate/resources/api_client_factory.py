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
    _client: Optional[BaseApiClient] = PrivateAttr(default=None)

    supported_client_class: ClassVar[dict[str, type[BaseApiClient]]] = {
        "CanvasApiClient": CanvasApiClient,
        "MITLearnApiClient": MITLearnApiClient,
    }

    def _initialize_client(self) -> BaseApiClient:
        client_class = self.supported_client_class[self.client_class]
        client_secrets = self.vault.client.secrets.kv.v1.read_secret(
            mount_point=self.mount_point,
            path=self.config_path,
        )["data"]

        return client_class(**client_secrets)

    @property
    def client(self) -> BaseApiClient:
        if not self._client:
            self._client = self._initialize_client()
        return self._client

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self]:  # noqa: ARG002
        yield self
