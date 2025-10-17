"""OpenMetadata API client resource for metadata ingestion."""

from typing import Any

from dagster import ConfigurableResource, ResourceDependency
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (  # noqa: E501
    OpenMetadataConnection,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic import Field, PrivateAttr


class OpenMetadataClient(ConfigurableResource):
    """Resource for interacting with OpenMetadata API."""

    vault: ResourceDependency[Vault]
    base_url: str = Field(
        description="Base URL for the OpenMetadata API server",
    )
    vault_secret_path: str = Field(
        default="dagster/openmetadata",
        description="Path in Vault to the OpenMetadata credentials",
    )
    _client: OpenMetadata | None = PrivateAttr(default=None)

    @property
    def client(self) -> OpenMetadata:
        """Get or create the OpenMetadata client."""
        if self._client is None:
            # Fetch credentials from Vault
            secret_data = self.vault.client.secrets.kv.v1.read_secret(
                path=self.vault_secret_path,
                mount_point="secret-data",
            )["data"]

            # Create OpenMetadata connection
            server_config = OpenMetadataConnection(
                hostPort=self.base_url,
                authProvider="openmetadata",
                securityConfig={
                    "jwtToken": secret_data.get("jwt_token"),
                },
            )

            # Initialize OpenMetadata client
            self._client = OpenMetadata(server_config)

        return self._client


# Configuration for different environments
OPENMETADATA_CONFIGS = {
    "dev": {
        "base_url": "http://localhost:8585/api",
    },
    "qa": {
        "base_url": "https://openmetadata-qa.odl.mit.edu/api",
    },
    "production": {
        "base_url": "https://openmetadata.odl.mit.edu/api",
    },
}


def get_openmetadata_config() -> dict[str, Any]:
    """Get OpenMetadata configuration for the current environment."""
    return OPENMETADATA_CONFIGS.get(DAGSTER_ENV, OPENMETADATA_CONFIGS["dev"])
