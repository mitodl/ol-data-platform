"""Learning resources API data extraction.

Extracts course and program metadata from various learning platforms:
- MIT Sloan Executive Education API
- Video Shorts video processing
- Open Learning Library (future)
"""

import os
from typing import Any

from dagster import (
    AssetSelection,
    ConfigurableResource,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_aws.s3 import S3Resource
from ol_orchestrate.io_managers.filepath import S3FileObjectIOManager
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import (
    default_file_object_io_manager,
    default_io_manager,
)
from ol_orchestrate.lib.utils import authenticate_vault, s3_uploads_bucket
from ol_orchestrate.resources.api_client_factory import ApiClientFactory
from ol_orchestrate.resources.oauth import OAuthApiClientFactory

from learning_resources.assets.sloan_api import sloan_course_metadata
from learning_resources.assets.video_shorts import (
    google_sheets_api,
    video_short_content,
    video_short_metadata,
    video_short_thumbnail_large,
    video_short_thumbnail_small,
    video_short_webhook,
)
from learning_resources.sensors.video_shorts import (
    video_shorts_discovery_sensor,
)

MIT_LEARN_BUCKET_SUFFIXES = {
    "dev": "ci",
    "ci": "ci",
    "qa": "rc",
    "production": "production",
}

# Initialize vault with resilient loading
try:
    vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)
    vault_authenticated = True
except Exception as e:  # noqa: BLE001 (resilient loading)
    import warnings

    from ol_orchestrate.resources.secrets.vault import Vault

    warnings.warn(
        f"Failed to authenticate with Vault: {e}. Using mock configuration.",
        stacklevel=2,
    )
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault_authenticated = False

# Get Google Sheets credentials for Video Shorts
try:
    if vault_authenticated:
        gs_secrets = vault.client.secrets.kv.v1.read_secret(
            mount_point="secret-data",
            path="pipelines/google-service-account",
        )["data"]
    else:
        gs_secrets = {}
except Exception:  # noqa: BLE001
    gs_secrets = {}


class VideoShortsSheetConfig(ConfigurableResource):
    """Configuration for Video Shorts Google Sheet."""

    service_account_json: dict[str, Any]  # Service account JSON credentials
    sheet_id: str = (
        os.environ.get("VIDEO_SHORTS_GOOGLE_SHEETS_ID")
        or "16RpyKIWqqAs2vlu1BZtfldje5Ft030A6pnMG76ysBWc"  # pragma: allowlist secret
    )


# Daily schedule for learning resource API extraction
extract_api_daily_schedule = ScheduleDefinition(
    name="learning_resource_api_schedule",
    target=AssetSelection.assets(sloan_course_metadata),
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)

# Video Shorts jobs for manual triggering
video_shorts_api_job = define_asset_job(
    name="video_shorts_api_job",
    description="Materialize Google Sheets API data to discover new videos",
    selection=AssetSelection.keys(
        ["video_shorts", "sheets_api"],
    ),
)

video_shorts_video_job = define_asset_job(
    name="video_shorts_video_job",
    description="Materialize Video Shorts video assets for specific partitions",
    selection=AssetSelection.keys(
        ["video_shorts", "video_metadata"],
        ["video_shorts", "video_content"],
        ["video_shorts", "video_thumbnail_small"],
        ["video_shorts", "video_thumbnail_large"],
        ["video_shorts", "video_webhook"],
    ),
)


# Video Shorts schedule for periodic discovery
video_shorts_api_schedule = ScheduleDefinition(
    name="video_shorts_api_schedule",
    target=video_shorts_api_job,
    cron_schedule="0 */1 * * *",  # Every hour
    execution_timezone="Etc/UTC",
)

# Create unified definitions
defs = Definitions(
    resources={
        "io_manager": default_io_manager(DAGSTER_ENV),
        "s3file_io_manager": S3FileObjectIOManager(
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
        "yt_s3file_io_manager": default_file_object_io_manager(
            dagster_env=DAGSTER_ENV,
            bucket=(
                os.environ.get("VIDEO_SHORTS_BUCKET")
                or f"ol-mitlearn-app-storage-{MIT_LEARN_BUCKET_SUFFIXES[DAGSTER_ENV]}"
            ),
            path_prefix=os.environ.get("LEARN_SHORTS_PREFIX", "shorts/"),
        ),
        "vault": vault,
        "s3": S3Resource(),
        "sloan_api": OAuthApiClientFactory(deployment="sloan", vault=vault),
        "video_shorts_sheet_config": VideoShortsSheetConfig(
            service_account_json=gs_secrets
        ),
        "learn_api": ApiClientFactory(
            deployment="mit-learn",
            client_class="MITLearnApiClient",
            mount_point="secret-global",
            config_path="shared_hmac",
            kv_version="2",
            vault=vault,
        ),
    },
    assets=[
        sloan_course_metadata,
        google_sheets_api,
        video_short_metadata,
        video_short_content,
        video_short_thumbnail_small,
        video_short_thumbnail_large,
        video_short_webhook,
    ],
    schedules=[extract_api_daily_schedule, video_shorts_api_schedule],
    sensors=[
        video_shorts_discovery_sensor,
    ],
    jobs=[
        video_shorts_api_job,
        video_shorts_video_job,
    ],
)
