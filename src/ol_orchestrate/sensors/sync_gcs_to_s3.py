import json
from collections.abc import Callable

from dagster import RunRequest, SensorEvaluationContext, SkipReason
from dagster_aws.s3 import S3Resource

from ol_orchestrate.resources.gcp_gcs import GCSConnection


def dummy_filter(object_name: str) -> bool:  # noqa: ARG001
    return True


def dummy_run_config_fn(object_keys: set[str]) -> dict:  # noqa: ARG001
    return {}


def check_new_gcs_assets_sensor(  # noqa: PLR0913
    bucket_name: str,
    context: SensorEvaluationContext,
    gcp_gcs: GCSConnection,
    bucket_prefix: str = "",
    object_filter_fn: Callable[[str], bool] = dummy_filter,
    run_config_fn: Callable[[set[str]], dict] = dummy_run_config_fn,
):
    """Check S3 bucket for new files to operate on.

    :param bucket_name: Name of the Google Cloud Storage bucket to watch
    :param context: The Dagster sensor evaluation context
    :param s3: Configured GCSConnection resource
    :param run_config_fn: Optional function that returns a dictionary of run config
        values when given a `set` object of new keys

    :yields: RunRequest or SkipReason if there are no new files to operate on
    """
    storage_client = gcp_gcs.client
    bucket = storage_client.get_bucket(bucket_name)
    bucket_files = {
        file.name
        for file in storage_client.list_blobs(bucket, prefix=bucket_prefix)
        if object_filter_fn(file.name)
    }
    new_files: set[str] = bucket_files - set(json.loads(context.cursor or "[]"))
    if new_files:
        context.update_cursor(json.dumps(list(bucket_files)))
        yield RunRequest(
            run_config=run_config_fn(new_files),
        )
    else:
        yield SkipReason("No new files in GCS bucket")


def check_new_s3_assets_sensor(  # noqa: PLR0913
    bucket_name: str,
    context: SensorEvaluationContext,
    s3: S3Resource,
    bucket_prefix: str = "",
    object_filter_fn: Callable[[str], bool] = dummy_filter,
    run_config_fn: Callable[[set[str]], dict] = dummy_run_config_fn,
):
    """Check S3 bucket for new files to operate on.

    :param bucket_name: Name of the S3 bucket to watch
    :param context: The Dagster sensor evaluation context
    :param s3: Configured S3 resource
    :param run_config_fn: Optional function that returns a dictionary of run config
        values when given a `set` object of new keys

    :yields: RunRequest or SkipReason if there are no new files to operate on
    """
    bucket_files = {
        obj["Key"]
        for obj in s3.list_objects_v2(Bucket=bucket_name, Prefix=bucket_prefix).get(
            "Contents", []
        )
        if object_filter_fn(obj["Key"])
    }
    new_files: set[str] = bucket_files - set(json.loads(context.cursor or "[]"))
    if new_files:
        context.update_cursor(json.dumps(list(bucket_files)))
        yield RunRequest(
            run_config=run_config_fn(new_files),
        )
    else:
        yield SkipReason("No new files in S3 bucket")
