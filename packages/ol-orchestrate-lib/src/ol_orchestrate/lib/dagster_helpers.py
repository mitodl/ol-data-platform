import os
import re

from dagster import FilesystemIOManager, IOManager
from dagster._core.definitions.partitions.utils.base import INVALID_PARTITION_SUBSTRINGS
from dagster_aws.s3 import S3PickleIOManager, S3Resource

from ol_orchestrate.io_managers.filepath import (
    LocalFileObjectIOManager,
    S3FileObjectIOManager,
)


def sanitize_mapping_key(mapping_key: str, replacement: str = "__") -> str:
    return re.sub(r"[^A-Za-z0-9_]", replacement, mapping_key)


def contains_invalid_partition_strings(partition_key: str) -> bool:
    return any(substr in partition_key for substr in INVALID_PARTITION_SUBSTRINGS)


def default_io_manager(dagster_env) -> IOManager:
    if dagster_env == "dev":
        return FilesystemIOManager()
    else:
        return S3PickleIOManager(
            s3_resource=S3Resource(),
            s3_bucket=os.environ.get("DAGSTER_BUCKET_NAME", f"dagster-{dagster_env}"),
            s3_prefix="assets",
        )


def default_file_object_io_manager(dagster_env, bucket, path_prefix) -> IOManager:
    if dagster_env == "dev":
        return LocalFileObjectIOManager(
            bucket="/opt/dagster/app/storage",
            path_prefix=path_prefix,
        )
    else:
        return S3FileObjectIOManager(
            bucket=bucket,
            path_prefix=path_prefix,
        )
