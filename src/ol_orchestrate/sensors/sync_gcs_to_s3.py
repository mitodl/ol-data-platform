from dagster import RunRequest, SkipReason, sensor
from google.cloud import storage
from google.oauth2 import service_account

from ol_orchestrate.jobs.sync_assets_and_run_models import sync_gcs_to_s3


@sensor(job=sync_gcs_to_s3, minimum_interval_seconds=86400)
def check_new_gcs_assets_sensor(context):
    credentials = service_account.Credentials.from_service_account_info(access_json)
    storage_client = storage.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    bucket = storage_client.get_bucket("simeon-mitx-course-tarballs")
    new_files = storage_client.list_blobs(bucket)
    if not new_files:
        yield SkipReason("No new files in GCS bucket")
        return
    else:
        yield RunRequest(
            run_key=new_gcs_file,
        )
