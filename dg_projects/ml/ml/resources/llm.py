"""LLM client resource for the feedback clustering pipeline."""

import os
from typing import ClassVar

from anthropic import Anthropic, AnthropicBedrock
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
    aws_region: str = Field(
        default="us-east-1",
        description=(
            "AWS region for the Bedrock endpoint; used when client_class='bedrock'"
        ),
    )
    azure_endpoint: str | None = Field(
        default=None,
        description=(
            "Resource endpoint, e.g. 'https://<resource>.openai.azure.com'; "
            "required when client_class='azure_openai'"
        ),
    )

    _client: Anthropic | OpenAI | AnthropicBedrock | None = PrivateAttr(default=None)

    supported_client_class: ClassVar[dict[str, type]] = {
        "anthropic": Anthropic,
        "openai": OpenAI,
        "openai_compatible": OpenAI,
        "azure_openai": OpenAI,
        "bedrock": AnthropicBedrock,
    }

    def get_client(self) -> Anthropic | OpenAI | AnthropicBedrock:
        """Create and return an authenticated LLM client."""
        if self._client is not None:
            return self._client

        sdk_client_class = self.supported_client_class[self.client_class]

        if self.client_class == "openai_compatible":
            self._client = sdk_client_class(
                base_url=self._require(self.base_url, "base_url"),
                api_key="unused",  # pragma: allowlist secret
            )
            return self._client

        if self.client_class == "bedrock":
            # Deployed environments: no API key at all, auth is the same IAM
            # metadata credentials used for S3 access - AnthropicBedrock
            # picks these up from the standard AWS credential chain.
            self._client = sdk_client_class(aws_region=self.aws_region)
            return self._client

        if self.client_class == "azure_openai":
            endpoint = self._require(self.azure_endpoint, "azure_endpoint")
            self._client = sdk_client_class(
                base_url=f"{endpoint.rstrip('/')}/openai/v1/",
                api_key=self._resolve_api_key("AZURE_OPENAI_API_KEY"),
            )
            return self._client

        # Local dev convenience: ANTHROPIC_API_KEY/OPENAI_API_KEY let a developer
        # run `dagster dev` with their own key instead of Vault.
        env_key_var = {"anthropic": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY"}[
            self.client_class
        ]
        self._client = sdk_client_class(api_key=self._resolve_api_key(env_key_var))

        return self._client

    def _require(self, value: str | None, field_name: str) -> str:
        """Return value, or raise naming the field and client_class that need it."""
        if not value:
            msg = f"{field_name} is required for client_class={self.client_class!r}"
            raise ValueError(msg)
        return value

    def _resolve_api_key(self, env_var: str) -> str:
        """Return env_var's value if set, else the key from Vault.

        Shared by every client_class branch below the openai_compatible/bedrock
        special cases (those need no API key at all), so a Vault outage only
        matters for a developer who hasn't set their own key locally.
        """
        env_value = os.environ.get(env_var)
        if env_value:
            return env_value
        # KV v1: secret_data["data"] contains the keys directly
        secret_data = self.vault.client.secrets.kv.v1.read_secret(
            mount_point=self.vault_mount_point, path=self.vault_secret_path
        )
        return secret_data["data"][self.vault_secret_key]
