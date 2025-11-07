"""Learning resources API data extraction.

Extracts course and program metadata from various learning platforms:
- MIT Sloan Executive Education API
- YouTube Shorts video processing
- Open Learning Library (future)
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
from ol_orchestrate.lib.dagster_helpers import (
    default_file_object_io_manager,
    default_io_manager,
)
from ol_orchestrate.lib.utils import authenticate_vault, s3_uploads_bucket
from ol_orchestrate.resources.api_client_factory import ApiClientFactory
from ol_orchestrate.resources.oauth import OAuthApiClientFactory

from learning_resources.assets.sloan_api import sloan_course_metadata
from learning_resources.assets.youtube_shorts import (
    youtube_playlist_api,
    youtube_playlist_config,
    youtube_video_content,
    youtube_video_metadata,
    youtube_video_thumbnail,
    youtube_video_webhook,
)
from learning_resources.resources.youtube_api import YouTubeApiClientFactory
from learning_resources.sensors.youtube_shorts import (
    youtube_shorts_discovery_sensor,
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

# Daily schedule for learning resource API extraction
extract_api_daily_schedule = ScheduleDefinition(
    name="learning_resource_api_schedule",
    target=AssetSelection.assets(sloan_course_metadata),
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)

# YouTube Shorts jobs for manual triggering
youtube_shorts_api_job = define_asset_job(
    name="youtube_shorts_api_job",
    description="Materialize playlist config and API data to discover new videos",
    selection=AssetSelection.keys(
        ["youtube_shorts", "playlist_config"],
        ["youtube_shorts", "playlist_api"],
    ),
)

youtube_shorts_video_job = define_asset_job(
    name="youtube_shorts_video_job",
    description="Materialize YouTube video assets for specific partitions",
    selection=AssetSelection.keys(
        ["youtube_shorts", "video_metadata"],
        ["youtube_shorts", "video_content"],
        ["youtube_shorts", "video_thumbnail"],
        ["youtube_shorts", "video_webhook"],
    ),
)


# YouTube Shorts schedule for periodic discovery
youtube_shorts_api_schedule = ScheduleDefinition(
    name="youtube_shorts_api_schedule",
    target=youtube_shorts_api_job,
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
                os.environ.get("YOUTUBE_SHORTS_BUCKET")
                or f"ol-mitlearn-app-storage-{MIT_LEARN_BUCKET_SUFFIXES[DAGSTER_ENV]}"
            ),
            path_prefix=os.environ.get("LEARN_SHORTS_PREFIX", "shorts/"),
        ),
        "vault": vault,
        "s3": S3Resource(),
        "sloan_api": OAuthApiClientFactory(deployment="sloan", vault=vault),
        "youtube_api": YouTubeApiClientFactory(vault=vault),
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
        youtube_playlist_config,
        youtube_playlist_api,
        youtube_video_metadata,
        youtube_video_content,
        youtube_video_thumbnail,
        youtube_video_webhook,
    ],
    schedules=[extract_api_daily_schedule, youtube_shorts_api_schedule],
    sensors=[
        youtube_shorts_discovery_sensor,
    ],
    jobs=[
        youtube_shorts_api_job,
        youtube_shorts_video_job,
    ],
)
