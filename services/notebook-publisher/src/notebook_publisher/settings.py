"""Deployment configuration, read from NOTEBOOK_PUBLISHER_* environment variables."""

import json
from functools import cached_property
from pathlib import Path
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ONE_KIBIBYTE = 1024


class Settings(BaseSettings):
    """Where published apps run and how they are rendered.

    The platform stack (ol-infrastructure ``applications/marimo_data``) sets
    every field without a default.
    """

    model_config = SettingsConfigDict(env_prefix="NOTEBOOK_PUBLISHER_")

    namespace: str = "marimo"
    apps_host: str = Field(description="Host every published app is served on.")
    runtime_image: str = Field(description="Image the published marimo apps run in.")
    oidc_plugin_file: Path = Field(
        description=(
            "JSON of the openid-connect plugin entry for published routes, as "
            "OLApisixOIDCResources.get_full_oidc_plugin_config renders it."
        )
    )
    shared_plugin_config_name: str = Field(
        description="ApisixPluginConfig every published route references."
    )
    service_account: str = "marimo-published"
    ingress_class: str = "apache-apisix"
    # Where the hub proxies this service: /services/<service name>/.
    service_prefix: str = "/services/notebook-publisher"
    marimo_port: int = 2718
    rollout_timeout_seconds: int = 300
    rollout_poll_seconds: float = 2.0
    cpu_request: str = "100m"
    memory_request: str = "512Mi"
    memory_limit: str = "1Gi"
    # marimo's sandbox installs what the notebook's header asks for into the
    # pod's emptyDirs, which count against this.
    ephemeral_storage_request: str = "1Gi"
    ephemeral_storage_limit: str = "8Gi"
    max_notebooks_per_user: int = 10
    # The operator stores inline content in a ConfigMap, and the 1 MiB object
    # limit covers its key and metadata as well.
    max_content_bytes: int = 900 * ONE_KIBIBYTE
    marimo_check_timeout_seconds: int = 60

    @cached_property
    def oidc_plugin(self) -> dict[str, Any]:
        """The openid-connect plugin entry that published routes start from."""
        plugin: dict[str, Any] = json.loads(self.oidc_plugin_file.read_text())
        if plugin["name"] != "openid-connect" or not plugin["secretRef"]:
            msg = f"{self.oidc_plugin_file} is not an openid-connect plugin entry"
            raise ValueError(msg)
        return plugin
