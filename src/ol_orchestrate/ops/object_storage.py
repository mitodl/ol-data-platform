from typing import Optional

from dagster import Config, op
from pydantic import Field


class SyncS3ObjectsConfig(Config):
    source_bucket: str = Field(description="S3 bucket to sync from")
    destination_bucket: str = Field(description="S3 bucket to sync to")
    object_keys: Optional[list[str]] = Field(
        description="List of S3 object keys to sync"
    )
    destination_prefix: str = Field(
        default="", description="S3 object key prefix to sync to"
    )


@op(
    required_resource_keys={"s3"},
    description="Synchronize a list of files from one bucket to another.",
)
def sync_files_to_s3(context, config: SyncS3ObjectsConfig) -> None:
    """Sync S3 objects between two buckets"""
    for object_key in config.object_keys or []:
        context.log.info(
            "Copying %s from %s to %s",
            object_key,
            config.source_bucket,
            config.destination_bucket,
        )
        context.resources.s3.copy(
            {"Bucket": config.source_bucket, "Key": object_key},
            config.destination_bucket,
            f"{config.destination_prefix}/{object_key}",
        )
