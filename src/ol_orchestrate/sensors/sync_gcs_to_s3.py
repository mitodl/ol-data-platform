from dagster import RunRequest, SensorEvaluationContext, SkipReason, build_resources

from ol_orchestrate.resources.gcp_gcs import gcp_gcs_resource
from ol_orchestrate.lib.yaml_config_helper import load_yaml_config


def check_new_gcs_assets_sensor(context: SensorEvaluationContext):
    gcs_config = load_yaml_config("/etc/dagster/edxorg_gcp.yaml")
    with build_resources(
        resources={"gcp_gcs": gcp_gcs_resource}, resource_config=gcs_config["resources"]
    ) as resources:
        storage_client = resources.gcp_gcs
        bucket = storage_client.get_bucket("simeon-mitx-course-tarballs")
        new_files = storage_client.list_blobs(bucket)
        if new_files:
            context.update_cursor(str(new_files))
            yield RunRequest(
                run_key="new_gcs_file",
                run_config=gcs_config,
            )
        else:
            yield SkipReason("No new files in GCS bucket")
