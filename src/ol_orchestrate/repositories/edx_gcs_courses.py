import os

from dagster import DefaultSensorStatus, Definitions, SensorDefinition
from dagster_aws.s3.resources import s3_resource

from ol_orchestrate.jobs.edx_gcs_courses import sync_gcs_to_s3
from ol_orchestrate.lib.yaml_config_helper import load_yaml_config
from ol_orchestrate.resources.gcp_gcs import gcp_gcs_resource
from ol_orchestrate.resources.outputs import daily_dir
from ol_orchestrate.sensors.sync_gcs_to_s3 import check_new_gcs_assets_sensor

resources = {
    "gcp_gcs": gcp_gcs_resource.configured(
        load_yaml_config("/etc/dagster/edxorg_gcp.yaml")["resources"]["gcp_gcs"]
    ),
    "s3": s3_resource,
    "results_dir": daily_dir,
}

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "qa")

gcs_sync_job = sync_gcs_to_s3.to_job(
    name="edx_gcs_course_retrieval", resource_defs=resources
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
    jobs=[gcs_sync_job],
)
