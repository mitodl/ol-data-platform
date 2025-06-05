from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
)
from dagster_aws.s3 import S3Resource

from ol_orchestrate.assets.canvas import export_courses
from ol_orchestrate.io_managers.filepath import S3FileObjectIOManager
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import default_io_manager
from ol_orchestrate.lib.utils import authenticate_vault, s3_uploads_bucket
from ol_orchestrate.resources.canvas_api import CanvasApiClientFactory

vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)

canvas_course_export_schedule = ScheduleDefinition(
    name="canvas_course_export_schedule",
    target=AssetSelection.assets(export_courses),
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)

canvas_course_export = Definitions(
    resources={
        "io_manager": default_io_manager(DAGSTER_ENV),
        "s3file_io_manager": S3FileObjectIOManager(
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
        "vault": vault,
        "s3": S3Resource(),
        "canvas_api": CanvasApiClientFactory(vault=vault),
    },
    assets=[export_courses],
    schedules=[canvas_course_export_schedule],
)
