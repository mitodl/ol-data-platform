import os

from dagster import DefaultSensorStatus, Definitions, SensorDefinition
from dagster_aws.s3.resources import s3_resource

from ol_orchestrate.jobs.retrieve_edx_exports import sync_gcs_to_s3
from ol_orchestrate.lib.yaml_config_helper import load_yaml_config
from ol_orchestrate.resources.gcp_gcs import gcp_gcs_resource
from ol_orchestrate.resources.outputs import daily_dir
from ol_orchestrate.sensors.sync_gcs_to_s3 import check_edx_exports_sensor

resources = {
    "gcp_gcs": gcp_gcs_resource.configured(
        # todo: add yaml to deploy.py in ol-infrastructure
        load_yaml_config("/etc/dagster/irx_gcp.yaml")["resources"]["gcp_gcs"]["config"]
    ),
    "s3": s3_resource,
    "exports_dir": daily_dir,
}

edx_exports_bucket = {
    "qa": "ol-devops-sandbox/pipeline-storage/",
    "production": "ol-devops-sandbox/pipeline-storage/",
}

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "qa")

gcs_sync_job = sync_gcs_to_s3.to_job(
    name="edx_gcs_course_retrieval",
    resource_defs=resources,
    config={
        "ops": {
            "download_edx_data_exports": {
                "config": {"edx_exports_bucket": edx_exports_bucket[dagster_deployment]}
            }
        }
    },
)

edx_gcs_courses = Definitions(
    sensors=[
        SensorDefinition(
            evaluation_fn=check_edx_exports_sensor,
            minimum_interval_seconds=86400,
            job=gcs_sync_job,
            default_status=DefaultSensorStatus.RUNNING,
        )
    ],
    jobs=[gcs_sync_job],
)
