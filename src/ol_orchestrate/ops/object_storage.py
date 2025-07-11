"""DEPRECATION NOTICE: (TMM 2024-08-23)

The logic in this module is deprecated in favor of the IO Manager interface.  For
similar capabilities to what is found here please refer to
src/ol_orchstrate/io_managers/filepath.py
"""

from pathlib import Path

from dagster import Config, In, OpExecutionContext, Out, op
from pydantic import Field

from ol_orchestrate.lib.dagster_types.files import DagsterPath


class SyncS3ObjectsConfig(Config):
    source_bucket: str = Field(description="S3 bucket to sync from")
    destination_bucket: str = Field(description="S3 bucket to sync to")
    object_keys: list[str] | None = Field(description="List of S3 object keys to sync")
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
    object_keys: list[str] | None = Field(description="List of S3 object keys to sync")


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
    destination_key: str | None = Field(description="S3 object key to sync to")


@op(required_resource_keys={"s3_upload"}, ins={"downloaded_objects": In()})
def upload_files_to_s3(
    context, config: S3UploadConfig, downloaded_objects: DagsterPath
) -> None:
    """Upload the contents of a directory to an S3 bucket."""
    if not downloaded_objects.is_dir():
        downloaded_objects = downloaded_objects.parent
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


class GCSDownloadConfig(Config):
    bucket_name: str = Field(description="Google Cloud Storage bucket to sync from")
    object_key: str = Field(description="Google Cloud Storage object key to sync")
    destination_path: str = Field(description="Destination path to sync to")


@op(required_resource_keys={"gcp_gcs"})
def download_file_from_gcs(context, config: GCSDownloadConfig) -> DagsterPath:
    """Download a file from a Google Cloud Storage bucket."""
    context.log.info(
        "Downloading %s from %s",
        config.object_key,
        config.bucket_name,
    )
    # Ensure that all of the path components are present in the destination
    # directory.
    Path(config.destination_path).parent.mkdir(parents=True, exist_ok=True)
    context.resources.gcp_gcs.download_blob(
        bucket_name=config.bucket_name,
        blob_name=config.object_key,
        destination_path=config.destination_path,
    )
    return DagsterPath(config.destination_path)


@op(required_resource_keys={"s3_upload"}, ins={"output_file": In()})
def upload_file_to_s3(
    context: OpExecutionContext, config: S3UploadConfig, output_file: DagsterPath
) -> None:
    """Upload a file to an S3 bucket."""
    context.resources.s3_upload.upload_file(
        Filename=output_file,
        Bucket=config.destination_bucket,
        Key=f"{config.destination_prefix}/{config.destination_key}",
    )
