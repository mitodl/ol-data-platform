import json
import re

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


def check_edxorg_data_dumps_sensor(
    context: SensorEvaluationContext, gcp_gcs: GCSConnection
):
    storage_client = gcp_gcs.client
    bucket = storage_client.get_bucket("simeon-mitx-pipeline-main")
    file_match = r"COLD/mitx-\d{4}-\d{2}-\d{2}.zip$"
    bucket_files = {
        file.name
        for file in storage_client.list_blobs(bucket, prefix="COLD/")
        if not re.match(file_match, file.name)
    }
    new_files = bucket_files.difference(set(context.cursor or []))
    if new_files:
        context.update_cursor(json.dumps(list(bucket_files)))
        yield RunRequest(
            run_config={
                "ops": {
                    "download_edx_data_exports": {
                        "config": {"files_to_sync": list(new_files)}
                    }
                }
            },
        )
    else:
        yield SkipReason("No new files in GCS bucket")
