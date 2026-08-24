"""LLM client resource for the feedback clustering pipeline."""

from typing import ClassVar

from anthropic import Anthropic
from dagster import ConfigurableResource
from ol_orchestrate.resources.secrets.vault import Vault
from openai import OpenAI
from pydantic import Field, PrivateAttr


class LLMClientFactory(ConfigurableResource):
    """Factory for creating an authenticated LLM client.

    Backs conversation summarization (feedback_summaries) and LLM cluster
    labeling (feedback_category_proposals); feedback_redacted does not use it.
    """

    vault: Vault = Field(description="Vault resource for retrieving the LLM API key")
    client_class: str = Field(
        default="anthropic", description="Which LLM client to instantiate"
    )
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
    base_url: str | None = Field(
        default=None,
        description=(
            "Base URL of a self-hosted OpenAI-compatible server (vLLM/Ollama/etc."
            " on a GPU node); required when client_class='openai_compatible'"
        ),
    )

    _client: Anthropic | OpenAI | None = PrivateAttr(default=None)

    supported_client_class: ClassVar[dict[str, type]] = {
        "anthropic": Anthropic,
        "openai": OpenAI,
        "openai_compatible": OpenAI,
    }

    def get_client(self) -> Anthropic | OpenAI:
        """Create and return an authenticated LLM client."""
        if self._client is not None:
            return self._client

        client_class = self.supported_client_class[self.client_class]

        if self.client_class == "openai_compatible":
            if not self.base_url:
                msg = "base_url is required for client_class='openai_compatible'"
                raise ValueError(msg)
            self._client = client_class(
                base_url=self.base_url,
                api_key="unused",  # pragma: allowlist secret
            )
            return self._client

        # KV v1: secret_data["data"] contains the keys directly
        secret_data = self.vault.client.secrets.kv.v1.read_secret(
            mount_point=self.vault_mount_point, path=self.vault_secret_path
        )
        api_key = secret_data["data"][self.vault_secret_key]
        self._client = client_class(api_key=api_key)

        return self._client
