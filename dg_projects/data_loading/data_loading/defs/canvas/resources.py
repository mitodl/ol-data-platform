"""Resources for the Canvas export: Vault, the Canvas API and the course ID sheet."""

import warnings

from dagster import Definitions
from ol_orchestrate.io_managers.filepath import S3FileObjectIOManager
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.utils import (
    authenticate_vault,
    s3_uploads_bucket,
    unauthenticated_vault,
)
from ol_orchestrate.resources.api_client_factory import ApiClientFactory

from data_loading.defs.canvas.google_sheet import GoogleSheetConfig

# Resilient loading, as in the canvas location: a code server that cannot reach
# Vault still loads, and only runs that need a secret fail.
try:
    vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)
except Exception as e:  # noqa: BLE001
    warnings.warn(
        f"Failed to authenticate with Vault: {e}. Using mock configuration.",
        stacklevel=2,
    )
    vault = unauthenticated_vault(VAULT_ADDRESS)

defs = Definitions(
    resources={
        # S3 in every environment, dev included, as in delivery: delivery's
        # webhook reads these exports from its own container, so a dev export
        # written to this container's filesystem would be unreadable there.
        "s3file_io_manager": S3FileObjectIOManager(
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
        "vault": vault,
        "canvas_api": ApiClientFactory(
            deployment="canvas",
            client_class="CanvasApiClient",
            mount_point="secret-data",
            config_path="pipelines/canvas",
            kv_version="1",
            vault=vault,
        ),
        "google_sheet_config": GoogleSheetConfig(vault=vault),
    },
)
