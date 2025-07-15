from dagster import (
    Definitions,
    build_schedule_from_partitioned_job,
    define_asset_job,
)
from dagster_aws.s3 import S3Resource

from ol_orchestrate.assets.canvas import (
    canvas_course_ids,
    course_content_metadata,
    export_course_content,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import (
    default_file_object_io_manager,
    default_io_manager,
)
from ol_orchestrate.lib.utils import authenticate_vault, s3_uploads_bucket
from ol_orchestrate.resources.api_client_factory import ApiClientFactory

vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)


canvas_course_content_job = define_asset_job(
    name="canvas_course_export_job",
    selection=[export_course_content, course_content_metadata],
    partitions_def=canvas_course_ids,
)

canvas_course_export_schedule = build_schedule_from_partitioned_job(
    name="canvas_course_export_schedule",
    job=canvas_course_content_job,
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)

canvas_course_export = Definitions(
    resources={
        "io_manager": default_io_manager(DAGSTER_ENV),
        "s3file_io_manager": default_file_object_io_manager(
            dagster_env=DAGSTER_ENV,
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
        "vault": vault,
        "s3": S3Resource(),
        "canvas_api": ApiClientFactory(
            deployment="canvas",
            client_class="CanvasApiClient",
            mount_point="secret-data",
            config_path="pipelines/canvas",
            kv_version="1",
            vault=vault,
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
    assets=[export_course_content, course_content_metadata],
    schedules=[canvas_course_export_schedule],
)
