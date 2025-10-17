"""Learning resources API data extraction.

Extracts course and program metadata from various learning platforms:
- MIT Sloan Executive Education API
- Open Learning Library (future)
- YouTube Video Shorts
"""

import os

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_aws.s3 import S3Resource
from ol_orchestrate.io_managers.filepath import S3FileObjectIOManager
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import default_io_manager
from ol_orchestrate.lib.utils import authenticate_vault, s3_uploads_bucket
from ol_orchestrate.resources.oauth import OAuthApiClientFactory

from learning_resources.assets.sloan_api import sloan_course_metadata
from learning_resources.assets.youtube_shorts import (
    youtube_video_content,
    youtube_video_metadata,
    youtube_video_thumbnail,
)
from learning_resources.resources.api_client_factory import ApiClientFactory
from learning_resources.resources.youtube_client import YouTubeClientFactory
from learning_resources.resources.youtube_config import YouTubeConfigProvider
from learning_resources.sensors.youtube_shorts import youtube_shorts_sensor

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


# Daily schedule for learning resource API extraction
extract_api_daily_schedule = ScheduleDefinition(
    name="learning_resource_api_schedule",
    target=AssetSelection.assets(sloan_course_metadata),
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)

# YouTube Shorts assets selection
youtube_shorts_assets = AssetSelection.assets(
    youtube_video_metadata,
    youtube_video_content,
    youtube_video_thumbnail,
)

# Create YouTube Shorts job for manual execution
youtube_shorts_job = define_asset_job(
    name="youtube_shorts_job",
    selection=youtube_shorts_assets,
    config={
        "resources": {
            "youtube_shorts_config": {
                "config": {
                    "bucket_name": os.getenv("LR_VIDEO_SHORTS_BUCKET"),
                }
            }
        }
    },
)


# Create unified definitions
defs = Definitions(
    resources={
        "io_manager": default_io_manager(DAGSTER_ENV),
        "s3file_io_manager": S3FileObjectIOManager(
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
        "vault": vault,
        "s3": S3Resource(),
        "sloan_api": OAuthApiClientFactory(deployment="sloan", vault=vault),
        "youtube_client": YouTubeClientFactory(vault=vault),
        "youtube_config_provider": YouTubeConfigProvider(),
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
        youtube_video_metadata,
        youtube_video_content,
        youtube_video_thumbnail,
    ],
    jobs=[youtube_shorts_job],
    schedules=[extract_api_daily_schedule],
    sensors=[youtube_shorts_sensor],
)
