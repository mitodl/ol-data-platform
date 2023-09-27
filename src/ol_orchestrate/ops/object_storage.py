from typing import Optional

from dagster import Config, In, Out, op
from pydantic import Field

from ol_orchestrate.lib.dagster_types.files import DagsterPath


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


class S3DownloadConfig(Config):
    source_bucket: str = Field(description="S3 bucket to sync from")
    object_keys: Optional[list[str]] = Field(
        description="List of S3 object keys to sync"
    )


@op(
    required_resource_keys={"s3_download", "results_dir"},
    description="Synchronize a list of files from one bucket to another.",
    out={"downloaded_objects": Out()},
)
def download_files_from_s3(context, config: S3DownloadConfig) -> DagsterPath:
    """Download a list of objects from an S3 bucket."""
    for object_key in config.object_keys or []:
        context.log.info(
            "Downloading %s from %s",
            object_key,
            config.source_bucket,
        )
        # Ensure that all of the path components are present in the destination
        # directory.
        context.resources.results_dir.path.joinpath(object_key).parent.mkdir(
            parents=True, exist_ok=True
        )
        context.resources.s3_download.download_file(
            Bucket=config.source_bucket,
            Key=object_key,
            Filename=context.resources.results_dir.path.joinpath(object_key),
        )
    return context.resources.results_dir.path


class S3UploadConfig(Config):
    destination_bucket: str = Field(description="S3 bucket to sync to")
    destination_prefix: str = Field(
        default="", description="S3 object key prefix to sync to"
    )


@op(required_resource_keys={"s3_upload"}, ins={"downloaded_objects": In()})
def upload_files_to_s3(
    context, config: S3UploadConfig, downloaded_objects: DagsterPath
) -> None:
    """Upload the contents of a directory to an S3 bucket."""
    for fpath in downloaded_objects.rglob("*"):
        if fpath.is_file():
            object_key = fpath.relative_to(downloaded_objects)
            context.log.info(
                "Uploading %s to %s", object_key, config.destination_bucket
            )
            context.resources.s3_upload.upload_file(
                Filename=fpath,
                Bucket=config.destination_bucket,
                Key=f"{config.destination_prefix}/{object_key}",
            )
