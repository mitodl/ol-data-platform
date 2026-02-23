import os
import re
from typing import Literal

from dagster import FilesystemIOManager, IOManager
from dagster._core.definitions.partitions.utils.base import INVALID_PARTITION_SUBSTRINGS
from dagster_aws.s3 import S3PickleIOManager, S3Resource

from ol_orchestrate.io_managers.filepath import (
    LocalFileObjectIOManager,
    S3FileObjectIOManager,
)

DagsterEnvironment = Literal["dev", "qa", "production"]


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


def get_dagster_webserver_url(
    dagster_env: str | None = None,
) -> str:
    """Get the Dagster webserver URL for the current environment.

    Args:
        dagster_env: The Dagster environment. If None, reads from
                     DAGSTER_ENV environment variable.

    Returns:
        The full URL of the Dagster webserver (e.g., https://pipelines.odl.mit.edu)

    """
    env = dagster_env or os.getenv("DAGSTER_ENV", "production")

    if env == "dev":
        return "http://localhost:3000"
    elif env == "qa":
        return "https://pipelines-qa.odl.mit.edu"
    else:  # production
        return "https://pipelines.odl.mit.edu"


def get_dagster_host_and_port(
    dagster_env: str | None = None,
) -> tuple[str, int]:
    """Get the Dagster host and port for the current environment.

    This is useful for services that need separate host and port values
    (e.g., OpenMetadata ingestion).

    Args:
        dagster_env: The Dagster environment. If None, reads from
                     DAGSTER_ENV environment variable.

    Returns:
        A tuple of (host, port)

    Examples:
        >>> get_dagster_host_and_port("production")
        ('pipelines.odl.mit.edu', 443)

        >>> get_dagster_host_and_port("dev")
        ('localhost', 3000)
    """
    env = dagster_env or os.getenv("DAGSTER_ENV", "production")

    if env == "dev":
        return ("localhost", 3000)
    elif env == "qa":
        return ("pipelines-qa.odl.mit.edu", 443)
    else:  # production
        return ("pipelines.odl.mit.edu", 443)
