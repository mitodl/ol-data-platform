"""GitHub API client resource for Dagster pipelines."""

from dagster import ConfigurableResource
from github import Github
from pydantic import Field, PrivateAttr

from ol_orchestrate.resources.secrets.vault import Vault


class GithubApiClientFactory(ConfigurableResource):
    """Factory for creating authenticated GitHub API clients.

    This resource fetches GitHub credentials from Vault and creates an authenticated
    PyGithub client instance for interacting with the GitHub API.
    """

    vault: Vault = Field(description="Vault resource for retrieving GitHub API token")
    vault_mount_point: str = Field(
        default="secret-data", description="Vault mount point for secrets"
    )
    vault_secret_path: str = Field(
        default="pipelines/github/api", description="Path to GitHub API token in Vault"
    )

    _client: Github | None = PrivateAttr(default=None)

    def get_client(self) -> Github:
        """Create and return an authenticated GitHub client.

        Returns:
            Github: Authenticated PyGithub client instance for interacting with
                GitHub API.
        """
        if self._client is None:
            secret_data = self.vault.client.secrets.kv.v1.read_secret(
                mount_point=self.vault_mount_point, path=self.vault_secret_path
            )
            token = secret_data["data"]["token"]
            self._client = Github(token)

        return self._client
