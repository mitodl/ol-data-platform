import os  # noqa: INP001
from typing import Literal

from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    ScheduleDefinition,
    SensorDefinition,
)
from dagster_aws.s3 import s3_resource
from ol_orchestrate.jobs.retrieve_edx_exports import retrieve_edx_exports
from ol_orchestrate.lib.yaml_config_helper import load_yaml_config
from ol_orchestrate.resources.gcp_gcs import GCSConnection
from ol_orchestrate.resources.outputs import DailyResultsDir
from ol_orchestrate.sensors.sync_gcs_to_s3 import check_edxorg_data_dumps_sensor

dagster_env: Literal["dev", "qa", "production"] = os.environ.get(  # type: ignore  # noqa: E501, PGH003
    "DAGSTER_ENVIRONMENT", "dev"
)


def weekly_edx_exports_config(
    irx_edxorg_gcs_bucket,
    ol_edxorg_raw_data_bucket,
    ol_edxorg_tracking_log_prefix,
    ol_edxorg_course_exports_prefix,
):
    return {
        "ops": {
            "download_edx_data_exports": {
                "config": {"irx_edxorg_gcs_bucket": irx_edxorg_gcs_bucket}
            },
            "upload_edx_data_exports": {
                "config": {
                    "edx_irx_exports_bucket": ol_edxorg_raw_data_bucket,
                    "tracking_log_bucket": f"{ol_edxorg_raw_data_bucket}/{ol_edxorg_tracking_log_prefix}",  # noqa: E501
                    "course_exports_bucket": f"{ol_edxorg_raw_data_bucket}/{ol_edxorg_course_exports_prefix}",  # noqa: E501
                }
            },
        }
    }


s3_job_def = retrieve_edx_exports.to_job(
    name="retrieve_edx_exports",
    config=weekly_edx_exports_config(
        "simeon-mitx-pipeline-main",
        "ol-devops-sandbox/pipeline-storage/",
        "ol-devops-sandbox/pipeline-storage/",
        "ol-devops-sandbox/pipeline-storage/",
    ),
)

irx_export_schedule = ScheduleDefinition(
    name="weekly_edx_sync",
    cron_schedule="0 4 * * 6",
    job=s3_job_def,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)


retrieve_edx_exports = Definitions(
    resources={
        "gcp_gcs": GCSConnection(
            **load_yaml_config(
                "/Users/qhoque/GIT/ol-data-platform/etc/edxorg_gcp.yaml"
            )["resources"]["gcp_gcs"]["config"]
        ),
        "s3": s3_resource,
        "exports_dir": DailyResultsDir.configure_at_launch(),
    },
    sensors=[
        SensorDefinition(
            evaluation_fn=check_edxorg_data_dumps_sensor,
            minimum_interval_seconds=86400,
            job=s3_job_def,
            default_status=DefaultSensorStatus.RUNNING,
        )
    ],
    jobs=[s3_job_def],
    schedules=[irx_export_schedule],
)
