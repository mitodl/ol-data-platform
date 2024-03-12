import os
from functools import partial

from dagster import DefaultSensorStatus, Definitions, SensorDefinition, job
from dagster_aws.s3 import S3Resource

from ol_orchestrate.ops.object_storage import (
    S3DownloadConfig,
    S3UploadConfig,
    download_files_from_s3,
    upload_files_to_s3,
)
from ol_orchestrate.resources.outputs import SimpleResultsDir
from ol_orchestrate.sensors.object_storage import s3_multi_file_sensor

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "qa")
download_config = S3DownloadConfig(
    source_bucket="edx-program-reports",
)
upload_config = S3UploadConfig(
    destination_bucket=f"ol-data-lake-landing-zone-{dagster_deployment.lower()}",
    destination_prefix="edxorg-program-credentials",
)


@job(
    name="sync_edxorg_program_reports",
    description="Replicate program credential reports from edx.org into our own S3 "
    "bucket so that it can be ingested into the OL data lake.",
    config={
        "ops": {
            "download_files_from_s3": {"config": download_config.dict()},
            "upload_files_to_s3": {"config": upload_config.dict()},
        }
    },
)
def sync_edxorg_program_reports():
    upload_files_to_s3(download_files_from_s3())


edxorg_program_reports = Definitions(
    resources={
        "s3": S3Resource(profile_name="edxorg"),
        "s3_download": S3Resource(profile_name="edxorg"),
        "s3_upload": S3Resource(),
        "results_dir": SimpleResultsDir.configure_at_launch(),
    },
    sensors=[
        SensorDefinition(
            name="edxorg_program_reports_sensor",
            evaluation_fn=partial(
                s3_multi_file_sensor,
                "edx-program-reports",
                bucket_prefix="reports_v2/MITx/",
                run_config_fn=lambda new_keys: {
                    "ops": {
                        "download_files_from_s3": {
                            "config": {
                                **download_config.dict(),
                                "object_keys": list(new_keys),
                            }
                        },
                        "upload_files_to_s3": {"config": upload_config.dict()},
                    }
                },
            ),
            job=sync_edxorg_program_reports,
            default_status=DefaultSensorStatus.RUNNING,
            minimum_interval_seconds=86400,
        )
    ],
    jobs=[sync_edxorg_program_reports],
)
