"""Anthropic LLM client resource for the feedback clustering pipeline."""

from anthropic import Anthropic
from dagster import ConfigurableResource
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic import Field, PrivateAttr


class LLMClientFactory(ConfigurableResource):
    """Factory for creating an authenticated Anthropic client.

    Backs conversation summarization (feedback_summaries) and LLM cluster
    labeling (feedback_category_proposals); feedback_redacted does not use it.
    """

    vault: Vault = Field(description="Vault resource for retrieving the LLM API key")
    vault_mount_point: str = Field(
        default="secret-data", description="Vault mount point for secrets"
    )
    vault_secret_path: str = Field(
        default="pipelines/feedback-llm",
        description="Path to the LLM secret in Vault (without key name)",
    )
    vault_secret_key: str = Field(
        default="api_key", description="Key name within the Vault secret"
    )

    _client: Anthropic | None = PrivateAttr(default=None)

    def get_client(self) -> Anthropic:
        """Create and return an authenticated Anthropic client.

        Returns:
            Anthropic: Authenticated client for summarization and labeling calls.
        """
        if self._client is None:
            # KV v1: secret_data["data"] contains the keys directly
            secret_data = self.vault.client.secrets.kv.v1.read_secret(
                mount_point=self.vault_mount_point, path=self.vault_secret_path
            )
            api_key = secret_data["data"][self.vault_secret_key]
            self._client = Anthropic(api_key=api_key)

        return self._client
