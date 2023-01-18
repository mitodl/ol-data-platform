import os

from dagster import repository
from dagster_aws.s3.resources import s3_resource

from ol_orchestrate.jobs.edx_gcs_courses import sync_gcs_to_s3
from ol_orchestrate.resources.gcp_gcs import gcp_gcs_resource
from ol_orchestrate.resources.outputs import daily_dir

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


@repository
def edx_gcs_courses_repository():
    return [
        sync_gcs_to_s3.to_job(resource_defs=env_map),
    ]
