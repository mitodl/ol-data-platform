import json
from collections.abc import Callable
from typing import Any

from dagster import RunRequest, SensorEvaluationContext, SkipReason
from dagster_aws.s3 import S3Resource

from ol_orchestrate.resources.gcp_gcs import GCSConnection


def dummy_filter(object_name: str) -> bool:  # noqa: ARG001
    return True


def dummy_run_config_fn(object_keys: set[str]) -> dict[Any, Any]:  # noqa: ARG001
    return {}


def gcs_multi_file_sensor(  # noqa: PLR0913
    bucket_name: str,
    context: SensorEvaluationContext,
    gcp_gcs: GCSConnection,
    bucket_prefix: str = "",
    object_filter_fn: Callable[[str], bool] = dummy_filter,
    run_config_fn: Callable[[set[str]], dict[Any, Any]] = dummy_run_config_fn,
):
    """Check GCS bucket for new files to operate on.

    :param bucket_name: Name of the Google Cloud Storage bucket to watch
    :param context: The Dagster sensor evaluation context
    :param gcp_gcs: Configured GCSConnection resource
    :param bucket_prefix: Prefix of the Google Cloud Storage bucket to watch
    :param object_filter_fn: Optional function to filter file names, returns a boolean
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


def s3_multi_file_sensor(  # noqa: PLR0913
    bucket_name: str,
    context: SensorEvaluationContext,
    s3: S3Resource,
    bucket_prefix: str = "",
    object_filter_fn: Callable[[str], bool] = dummy_filter,
    run_config_fn: Callable[[set[str]], dict[Any, Any]] = dummy_run_config_fn,
):
    """Check S3 bucket for new files to operate on.

    :param bucket_name: Name of the S3 bucket to watch
    :param context: The Dagster sensor evaluation context
    :param s3: Configured S3 resource
    :param bucket_prefix: Prefix of the Google Cloud Storage bucket to watch
    :param object_filter_fn: Optional function to filter file names, returns a boolean
    :param run_config_fn: Optional function that returns a dictionary of run config
        values when given a `set` object of new keys

    :yields: RunRequest or SkipReason if there are no new files to operate on
    """
    pages = (
        s3.get_client()
        .get_paginator("list_objects_v2")
        .paginate(Bucket=bucket_name, Prefix=bucket_prefix)
    )
    bucket_files = set()
    for page in pages:
        bucket_files.update(
            {
                obj["Key"]
                for obj in page.get("Contents", [])
                if object_filter_fn(obj["Key"])
            }
        )
    new_files: set[str] = bucket_files - set(json.loads(context.cursor or "[]"))
    if new_files:
        context.update_cursor(json.dumps(list(bucket_files)))
        yield RunRequest(
            run_config=run_config_fn(new_files),
        )
    else:
        yield SkipReason("No new files in S3 bucket")
