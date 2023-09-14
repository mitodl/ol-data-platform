import os

from dagster import DefaultSensorStatus, Definitions, SensorDefinition
from dagster_aws.s3.resources import s3_resource

from ol_orchestrate.jobs.edx_gcs_courses import sync_gcs_to_s3
from ol_orchestrate.lib.yaml_config_helper import load_yaml_config
from ol_orchestrate.resources.gcp_gcs import GCSConnection
from ol_orchestrate.resources.outputs import DailyResultsDir
from ol_orchestrate.sensors.sync_gcs_to_s3 import check_new_gcs_assets_sensor

resources = {
    "gcp_gcs": GCSConnection(
        **load_yaml_config("/etc/dagster/edxorg_gcp.yaml")["resources"]["gcp_gcs"][
            "config"
        ]
    ),
    "s3": s3_resource,
    "results_dir": DailyResultsDir.configure_at_launch(),
}

course_upload_bucket = {
    "qa": "edxorg-qa-edxapp-courses",
    "production": "edxorg-production-edxapp-courses",
}

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "qa")

gcs_sync_job = sync_gcs_to_s3.to_job(
    name="edx_gcs_course_retrieval",
    config={
        "ops": {
            "edx_upload_gcs_course_tarballs": {
                "config": {
                    "edx_etl_results_bucket": course_upload_bucket[dagster_deployment]
                }
            }
        }
    },
)

edx_gcs_courses = Definitions(
    sensors=[
        SensorDefinition(
            evaluation_fn=check_new_gcs_assets_sensor,
            minimum_interval_seconds=86400,
            job=gcs_sync_job,
            default_status=DefaultSensorStatus.RUNNING,
        )
    ],
    resources=resources,
    jobs=[gcs_sync_job],
)
