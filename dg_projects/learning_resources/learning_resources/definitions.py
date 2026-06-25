"""Learning resources API data extraction.

Extracts course and program metadata from various learning platforms:
- MIT Sloan Executive Education API
- OVS (Open Video Studio) public videos
- MIT Climate Portal articles
- MIT Professional Education courses and programs
- Open Learning Library courses
- MIT edX programs
"""

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
from ol_orchestrate.resources.api_client_factory import ApiClientFactory
from ol_orchestrate.resources.oauth import OAuthApiClientFactory

from learning_resources.assets.mit_climate import mit_climate_webhook
from learning_resources.assets.mit_edx_programs import mit_edx_programs_webhook
from learning_resources.assets.mitpe import mitpe_webhook
from learning_resources.assets.open_learning_library import oll_webhook
from learning_resources.assets.ovs_videos import (
    video_api,
    video_delete_webhook,
    video_metadata,
    video_webhook,
)
from learning_resources.assets.sloan_api import sloan_course_metadata
from learning_resources.sensors.ovs_videos import (
    ovs_videos_delete_job,
    ovs_videos_delete_partition_cleanup_sensor,
    ovs_videos_discovery_sensor,
    ovs_videos_stale_cleanup_sensor,
)

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


# Daily schedules for REST API webhook delivery sources.
# All run after 06:00 UTC to allow upstream APIs to settle overnight.
mit_climate_schedule = ScheduleDefinition(
    name="mit_climate_schedule",
    target=AssetSelection.assets(mit_climate_webhook),
    cron_schedule="0 6 * * *",
    execution_timezone="Etc/UTC",
)

mitpe_schedule = ScheduleDefinition(
    name="mitpe_schedule",
    target=AssetSelection.assets(mitpe_webhook),
    cron_schedule="15 6 * * *",
    execution_timezone="Etc/UTC",
)

oll_schedule = ScheduleDefinition(
    name="oll_schedule",
    target=AssetSelection.assets(oll_webhook),
    cron_schedule="30 6 * * *",
    execution_timezone="Etc/UTC",
)

mit_edx_programs_schedule = ScheduleDefinition(
    name="mit_edx_programs_schedule",
    target=AssetSelection.assets(mit_edx_programs_webhook),
    cron_schedule="45 6 * * *",
    execution_timezone="Etc/UTC",
)

# Daily schedule for learning resource API extraction
extract_api_daily_schedule = ScheduleDefinition(
    name="learning_resource_api_schedule",
    target=AssetSelection.assets(sloan_course_metadata),
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)

# OVS videos jobs for manual triggering
ovs_videos_api_job = define_asset_job(
    name="ovs_videos_api_job",
    description="Materialize OVS public videos API data to discover new videos",
    selection=AssetSelection.keys(
        ["ovs_videos", "video_api"],
    ),
)

ovs_videos_webhook_job = define_asset_job(
    name="ovs_videos_webhook_job",
    description="Materialize OVS video metadata + webhook for one partition",
    selection=AssetSelection.keys(
        ["ovs_videos", "video_metadata"],
        ["ovs_videos", "video_webhook"],
    ),
)

# OVS videos schedule for periodic discovery
ovs_videos_api_schedule = ScheduleDefinition(
    name="ovs_videos_api_schedule",
    target=ovs_videos_api_job,
    cron_schedule="*/10 * * * *",  # Every 10 minutes
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
        "vault": vault,
        "s3": S3Resource(),
        "sloan_api": OAuthApiClientFactory(deployment="sloan", vault=vault),
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
        video_api,
        video_metadata,
        video_webhook,
        video_delete_webhook,
        # REST API webhook delivery
        mit_climate_webhook,
        mitpe_webhook,
        oll_webhook,
        mit_edx_programs_webhook,
    ],
    schedules=[
        extract_api_daily_schedule,
        ovs_videos_api_schedule,
        mit_climate_schedule,
        mitpe_schedule,
        oll_schedule,
        mit_edx_programs_schedule,
    ],
    sensors=[
        ovs_videos_discovery_sensor,
        ovs_videos_stale_cleanup_sensor,
        ovs_videos_delete_partition_cleanup_sensor,
    ],
    jobs=[
        ovs_videos_api_job,
        ovs_videos_webhook_job,
        ovs_videos_delete_job,
    ],
)
