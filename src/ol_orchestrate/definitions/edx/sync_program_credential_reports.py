import os
from functools import partial

from dagster import DefaultSensorStatus, Definitions, SensorDefinition, job
from dagster_aws.s3 import S3Resource

from ol_orchestrate.ops.object_storage import SyncS3ObjectsConfig, sync_files_to_s3
from ol_orchestrate.sensors.sync_gcs_to_s3 import check_new_s3_assets_sensor

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "qa")
sync_config = SyncS3ObjectsConfig(
    source_bucket="edx-program-reports",
    destination_bucket=f"ol-data-lake-raw-{dagster_deployment.lower()}",
    destination_prefix="landing_zone",
)


@job(
    name="sync_edxorg_program_reports",
    description="Replicate program credential reports from edx.org into our own S3 "
    "bucket so that it can be ingested into the OL data lake.",
    config={"ops": {"sync_files_to_s3": {"config": sync_config.dict()}}},
)
def sync_edxorg_program_reports():
    sync_files_to_s3()


edxorg_program_reports = Definitions(
    resources={"s3": S3Resource(profile_name="edxorg-programs")},
    sensors=[
        SensorDefinition(
            name="edxorg_program_reports_sensor",
            evaluation_fn=partial(
                check_new_s3_assets_sensor,
                "edx-program-reports",
                bucket_prefix="reports_v2/MITx/",
                run_config_fn=lambda new_keys: {
                    "ops": {
                        "sync_files_to_s3": {
                            "config": {
                                **sync_config.dict(),
                                "object_keys": list(new_keys),
                            }
                        }
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
