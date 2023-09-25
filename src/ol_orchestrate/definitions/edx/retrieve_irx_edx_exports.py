import os  # noqa: INP001
import re
from functools import partial
from typing import Literal

from dagster import (
    DefaultSensorStatus,
    Definitions,
    SensorDefinition,
)
from dagster_aws.s3 import s3_resource
from ol_orchestrate.jobs.retrieve_edx_exports import (
    retrieve_edx_course_exports,
    retrieve_edx_tracking_logs,
)
from ol_orchestrate.lib.yaml_config_helper import load_yaml_config
from ol_orchestrate.resources.gcp_gcs import GCSConnection
from ol_orchestrate.resources.outputs import DailyResultsDir
from ol_orchestrate.sensors.sync_gcs_to_s3 import check_edxorg_data_dumps_sensor

dagster_env: Literal["dev", "qa", "production"] = os.environ.get(  # type: ignore  # noqa: E501, PGH003
    "DAGSTER_ENVIRONMENT", "dev"
)


def weekly_edx_exports_config(
    irx_edxorg_gcs_bucket, ol_edxorg_raw_data_bucket, new_files_to_sync
):
    return {
        "ops": {
            "download_edx_data_exports": {
                "config": {
                    "irx_edxorg_gcs_bucket": irx_edxorg_gcs_bucket,
                    "file_type": "courses",
                    "files_to_sync": list(new_files_to_sync),
                }
            },
            "upload_edx_data_exports": {
                "config": {
                    "edx_irx_exports_bucket": ol_edxorg_raw_data_bucket,
                    "tracking_log_bucket": f"{ol_edxorg_raw_data_bucket}/logs",  # noqa: E501
                    "course_exports_bucket": f"{ol_edxorg_raw_data_bucket}/course_exports",  # noqa: E501
                }
            },
        }
    }


def weekly_edx_logs_config(
    irx_edxorg_gcs_bucket, ol_edxorg_raw_data_bucket, new_files_to_sync
):
    return {
        "ops": {
            "download_edx_data_exports": {
                "config": {
                    "irx_edxorg_gcs_bucket": irx_edxorg_gcs_bucket,
                    "file_type": "logs",
                    "files_to_sync": list(new_files_to_sync),
                }
            },
            "upload_edx_data_exports": {
                "config": {
                    "edx_irx_exports_bucket": ol_edxorg_raw_data_bucket,
                    "tracking_log_bucket": f"{ol_edxorg_raw_data_bucket}/logs",  # noqa: E501
                    "course_exports_bucket": f"{ol_edxorg_raw_data_bucket}/course_exports",  # noqa: E501
                }
            },
        }
    }


s3_courses_job_def = retrieve_edx_course_exports.to_job(
    name="retrieve_edx_course_exports",
    config=weekly_edx_exports_config(
        "simeon-mitx-pipeline-main", "ol-devops-sandbox/pipeline-storage/", set()
    ),
)

s3_logs_job_def = retrieve_edx_tracking_logs.to_job(
    name="retrieve_edx_logs",
    config=weekly_edx_logs_config(
        "simeon-mitx-pipeline-main", "ol-devops-sandbox/pipeline-storage/", set()
    ),
)

file_regex = {
    "courses": r"COLD/internal-\d{4}-\d{2}-\d{2}.zip$",
    "logs": r"COLD/mitx-edx-events-\d{4}-\d{2}-\d{2}.log.gz$",
}

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
            name="courses_sensor",
            evaluation_fn=partial(
                check_edxorg_data_dumps_sensor,
                "simeon-mitx-pipeline-main",
                bucket_prefix="COLD/",
                object_filter_fn=lambda object_key: re.match(
                    file_regex["courses"], object_key
                ),
                run_config_fn=lambda new_keys: weekly_edx_exports_config(
                    "simeon-mitx-pipeline-main",
                    "ol-devops-sandbox/pipeline-storage/",
                    new_keys,
                ),
            ),
            minimum_interval_seconds=86400,
            job=s3_courses_job_def,
            default_status=DefaultSensorStatus.RUNNING,
        ),
        SensorDefinition(
            name="logs_sensor",
            evaluation_fn=partial(
                check_edxorg_data_dumps_sensor,
                "simeon-mitx-pipeline-main",
                bucket_prefix="COLD/",
                object_filter_fn=lambda object_key: re.match(
                    file_regex["logs"], object_key
                ),
                run_config_fn=lambda new_keys: weekly_edx_logs_config(
                    "simeon-mitx-pipeline-main",
                    "ol-devops-sandbox/pipeline-storage/",
                    new_keys,
                ),
            ),
            minimum_interval_seconds=86400,
            job=s3_logs_job_def,
            default_status=DefaultSensorStatus.RUNNING,
        ),
    ],
    jobs=[s3_courses_job_def, s3_logs_job_def],
)
