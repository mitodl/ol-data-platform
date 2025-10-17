"""YouTube API client resource for Dagster."""

from dagster import ConfigurableResource, ResourceDependency
from googleapiclient.discovery import Resource, build
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic import PrivateAttr


class YouTubeClientFactory(ConfigurableResource):
    """Factory resource for creating YouTube API clients with credentials from Vault."""

    vault: ResourceDependency[Vault]
    _client: Resource = PrivateAttr(default=None)

    def _initialize_client(self) -> Resource:
        """Initialize the YouTube API client with API key from Vault."""
        secrets = self.vault.client.secrets.kv.v2.read_secret(
            mount_point="secret-mitlearn",
            path="secrets",
        )["data"]["data"]
        api_key = secrets["youtube_developer_key"]
        return build(
            "youtube",
            "v3",
            developerKey=api_key,
            cache_discovery=False,
        )

    @property
    def client(self) -> Resource:
        """Get or create the YouTube API client."""
        if self._client is None:
            self._client = self._initialize_client()
        return self._client
