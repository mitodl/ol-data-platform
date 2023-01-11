from dagster import graph
from dagster_aws.s3.resources import s3_resource

from ol_orchestrate.ops.edx_gcs_courses import (
    download_edx_gcs_course_data,
    upload_edx_gcs_course_data_to_s3,
)
from ol_orchestrate.resources.gcp_gcs import gcp_gcs_resource
from ol_orchestrate.resources.healthchecks import (
    healthchecks_dummy_resource,
    healthchecks_io_resource,
)
from ol_orchestrate.resources.outputs import daily_dir


@graph(
    description=(
        "Extract Open edX data maintained by institutional research "
        "from a gcs bucket into a date-stamped paths in S3 on a "
        "nightly basis."
    ),
    tags={
        "source": "gcs",
        "destination": "s3",
        "owner": "institutional-research",
        "consumer": "platform-engineering",
    },
)
def sync_gcs_to_s3():
    upload_edx_gcs_course_data_to_s3(download_edx_gcs_course_data())


dev_resources = {
    "gcp_gcs": gcp_gcs_resource,
    "s3": s3_resource,
    "results_dir": daily_dir,
    "healthchecks": healthchecks_dummy_resource,
}

production_resources = {
    "gcp_gcs": gcp_gcs_resource,
    "s3": s3_resource,
    "results_dir": daily_dir,
    "healthchecks": healthchecks_io_resource,
}

sync_gcs_to_s3_job = sync_gcs_to_s3.to_job(
    name="sync_gcs_to_s3_job_pipeline",
    resource_defs=dev_resources,
)
