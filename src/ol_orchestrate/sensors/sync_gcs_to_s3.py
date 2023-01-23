from dagster import RunRequest, SensorEvaluationContext, SkipReason, build_resources

from ol_orchestrate.resources import gcp_gcs


def check_new_gcs_assets_sensor(context: SensorEvaluationContext):
    with build_resources({"gcp_gcs": gcp_gcs}) as resources:
        storage_client = resources.gcp_gcs
        bucket = storage_client.get_bucket("simeon-mitx-course-tarballs")
        new_files = storage_client.list_blobs(bucket)
        if new_files:
            context.update_cursor(str(new_files))
            yield RunRequest(
                run_key="new_gcs_file",
            )
        else:
            yield SkipReason("No new files in GCS bucket")
            return
