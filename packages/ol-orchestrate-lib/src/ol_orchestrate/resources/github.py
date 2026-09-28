"""GitHub API client resource for Dagster pipelines."""

from dagster import ConfigurableResource
from github import Auth, Github, GithubIntegration
from pydantic import Field

from ol_orchestrate.resources.secrets.vault import Vault


class GithubApiClientFactory(ConfigurableResource):
    """Factory for GitHub API clients authenticated as the data platform's GitHub App.

    The App's id and private key come from Vault. Each client is scoped to the
    App's installation on ``organization``, so what it can touch is bounded by
    which repositories that installation selects, and optionally narrowed further
    per client by ``token_permissions``.
    """

    vault: Vault = Field(description="Vault resource for retrieving the App's key")
    vault_mount_point: str = Field(
        default="secret-data", description="Vault mount point for secrets"
    )
    vault_secret_path: str = Field(
        default="pipelines/github-app",
        description="KV v1 path holding the App's `app_id` and `private_key`",
    )
    organization: str = Field(
        default="mitodl", description="Organization the App is installed on"
    )

    def get_client(self, token_permissions: dict[str, str] | None = None) -> Github:
        """Create a client authenticated as the App's installation.

        PyGithub refreshes the installation token as it nears expiry, so one
        client lasts a whole step.

        :param token_permissions: Subset of the App's permissions to request for
            this client's token, e.g. ``{"contents": "write"}``. None requests
            everything the installation grants.
        :returns: Authenticated PyGithub client
        :rtype: Github
        """
        secret = self.vault.client.secrets.kv.v1.read_secret(
            mount_point=self.vault_mount_point, path=self.vault_secret_path
        )["data"]
        app_auth = Auth.AppAuth(secret["app_id"], secret["private_key"])
        with GithubIntegration(auth=app_auth) as integration:
            installation_id = integration.get_org_installation(self.organization).id
        return Github(
            auth=app_auth.get_installation_auth(
                installation_id, token_permissions=token_permissions
            )
        )
