from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
)
from dagster_aws.s3 import S3Resource

from ol_orchestrate.assets.sloan_api import sloan_course_metadata
from ol_orchestrate.io_managers.filepath import S3FileObjectIOManager
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import default_io_manager
from ol_orchestrate.lib.utils import authenticate_vault, s3_uploads_bucket
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory

vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)

extract_api_daily_schedule = ScheduleDefinition(
    name="learning_resource_api_schedule",
    target=AssetSelection.assets(sloan_course_metadata),
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)

extract_api_data = Definitions(
    resources={
        "io_manager": default_io_manager(DAGSTER_ENV),
        "s3file_io_manager": S3FileObjectIOManager(
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
        "vault": vault,
        "s3": S3Resource(),
        "sloan_api": OpenEdxApiClientFactory(
            deployment="sloan-executive-education", vault=vault
        ),
    },
    assets=[sloan_course_metadata],
    schedules=[extract_api_daily_schedule],
)
