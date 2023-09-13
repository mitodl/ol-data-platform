import json

from dagster import RunRequest, SensorEvaluationContext, SkipReason

from ol_orchestrate.resources.gcp_gcs import GCSConnection


def check_new_gcs_assets_sensor(
    context: SensorEvaluationContext, gcp_gcs: GCSConnection
):
    storage_client = gcp_gcs.client
    bucket = storage_client.get_bucket("simeon-mitx-course-tarballs")
    bucket_files = {file.name for file in storage_client.list_blobs(bucket)}
    new_files = bucket_files.difference(set(context.cursor or []))
    if new_files:
        context.update_cursor(json.dumps(list(bucket_files)))
        yield RunRequest(
            run_config={},
        )
    else:
        yield SkipReason("No new files in GCS bucket")
