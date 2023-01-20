import os

from dagster import Definitions, SensorDefinition
from dagster_aws.s3.resources import s3_resource

from ol_orchestrate.jobs.edx_gcs_courses import sync_gcs_to_s3
from ol_orchestrate.resources.gcp_gcs import gcp_gcs_resource
from ol_orchestrate.resources.outputs import daily_dir
from ol_orchestrate.sensors.sync_gcs_to_s3 import check_new_gcs_assets_sensor

environment = {
    "qa": {
        "gcp_gcs": gcp_gcs_resource,
        "s3": s3_resource,
        "results_dir": daily_dir,
    },
    "production": {
        "gcp_gcs": gcp_gcs_resource,
        "s3": s3_resource,
        "results_dir": daily_dir,
    },
}

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "qa")
env_map = environment[dagster_deployment]

gcs_sync_job = sync_gcs_to_s3.to_job(
    name="edx_gcs_course_retrieval", resource_defs=env_map
)

edx_gcs_courses = Definitions(
    sensors=[
        SensorDefinition(
            evaluation_fn=check_new_gcs_assets_sensor,
            minimum_interval_seconds=86400,  # noqa: WPS432 - 1 day
            job=gcs_sync_job,
        )
    ],
    jobs=[gcs_sync_job],
)
