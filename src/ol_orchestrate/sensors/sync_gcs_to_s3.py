from dagster import RunRequest, SkipReason, sensor

from ol_orchestrate.jobs.edx_gcs_courses import sync_gcs_to_s3
from ol_orchestrate.resources.gcp_gcs import gcp_gcs_resource


@sensor(job=sync_gcs_to_s3, minimum_interval_seconds=86400)
def check_new_gcs_assets_sensor(context):
    storage_client = context.resources.gcp_gcs.gcp_gcs_resource
    bucket = storage_client.get_bucket("simeon-mitx-course-tarballs")
    new_files = storage_client.list_blobs(bucket)
    if not new_files:
        yield SkipReason("No new files in GCS bucket")
        return
    else:
        yield RunRequest(
            run_key=new_gcs_file,
        )
