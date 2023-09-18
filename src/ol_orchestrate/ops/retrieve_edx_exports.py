import zipfile
from pathlib import Path
from typing import Optional

from dagster import (
    AssetMaterialization,
    Config,
    MetadataValue,
    OpExecutionContext,
    Out,
    Output,
    op,
)
from dagster.core.definitions.input import In
from pydantic import Field

from ol_orchestrate.lib.dagster_types.files import DagsterPath


class DownloadConfig(Config):
    edx_exports_bucket: str = Field(
        "irx-edx-exports",
        description="The GCS bucket where IRx stores data exports from edx.org",  # noqa: E501
    )


class UploadConfig(Config):
    # set defaults but make paramaterizable
    edx_irx_exports_bucket: Optional[str] = Field(
        description="The S3 bucket where CSV files will be staged",
    )
    tracking_log_bucket: Optional[str] = Field(
        description="The S3 bucket where tracking log files will be staged",  # noqa: E501
    )
    course_exports_bucket: Optional[str] = Field(
        description="The S3 bucket where course_exports will be stored",
    )


@op(
    name="download_edx_data_exports",
    description="Download zip files from GCS",
    required_resource_keys={"gcp_gcs", "exports_dir"},
    out={"edx_exports_directory": Out(dagster_type=DagsterPath)},
)
def download_edx_data(context: OpExecutionContext, config: DownloadConfig):
    """Download copies of edx.org data from IRx cold storage.

    :yield: The path where files have been downloaded.
    """
    storage_client = context.resources.gcp_gcs
    bucket = storage_client.get_bucket(config.edx_exports_bucket)
    edx_exports_download_path = context.resources.exports_dir.path.joinpath(
        config.edx_exports_bucket
    )
    context.log.info(edx_exports_download_path)
    blobs = bucket.list_blobs(prefix="COLD/")
    for blob in blobs:
        Path(edx_exports_download_path).joinpath(Path(blob.name).parent).mkdir(
            parents=True, exist_ok=True
        )
        if blob.name != "COLD/mitx-2023-03-28.zip":
            continue
        blob.download_to_filename(f"{edx_exports_download_path}/{blob.name}")
        # list all blobs, use regex pattern matching to filter subset
        # COLD/mitx-2023-03-28.zip
        # sensor should pass a complete list of objects
        # is that overall set larger than last? if difference, yield set subtraction new-old
        # pass delta to download fn
        context.log.info(blob.name)
        context.log.info(blob.size)
    yield Output(
        edx_exports_download_path,
        "edx_exports_directory",
    )


@op(
    name="extract_zip_files",
    description="Decompresses zipped files with edx.org data.",
    required_resource_keys={"exports_dir"},
    ins={"edx_exports_directory": In(dagster_type=DagsterPath)},
    out={"edx_exports_directory": Out(dagster_type=DagsterPath)},
)
def extract_files(
    context: OpExecutionContext,
    edx_exports_directory: DagsterPath,
):
    """Extract the contents of the downloaded zip files.

    :param edx_exports_directory: The path where files being processed are downloaded.
    :type edx_exports_directory: DagsterPath

    :yield: The path where files have been decompressed.
    """
    storage_client = context.resources.gcp_gcs
    bucket = storage_client.get_bucket(edx_exports_directory)
    edx_exports_path = context.resources.exports_dir.path.joinpath(
        edx_exports_directory
    )
    for file in edx_exports_directory.glob("*.zip"):
        with zipfile.ZipFile(file, "r") as zippedFile:
            zippedFile.extractall(path=f"./{file.stem}")
            context.log.info(file.name)
    blobs = storage_client.list_blobs(bucket)
    for blob in blobs:
        blob.download_to_filename(f"{edx_exports_path}/{blob.name}")
    yield Output(
        edx_exports_directory,
        "extracted_edx_exports_directory",
    )


@op(
    name="upload_edx_data_exports",
    description="Upload extracted files to S3",
    required_resource_keys={"exports_dir", "s3"},
    ins={"extracted_edx_exports_directory": In(dagster_type=DagsterPath)},
)
def upload_files(
    context: OpExecutionContext,
    extracted_edx_exports_directory: DagsterPath,
    config: UploadConfig,
):
    """Load files to staging locations.
    There are separate S3 buckets for CSV files, tracking logs, and course_exports.

    :param extracted_edx_exports_directory: The path with decompressed edx zip files.
    :type extracted_edx_exports_directory: DagsterPath
    """
    # TODO: iterate through directories and files? what is the file structure?
    # ideal s3 sync command, but we want assetmaterialization
    # load CSV files to s3
    csv_files = Path.glob(extracted_edx_exports_directory, "/*.csv")
    for file in csv_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.csv_staging_bucket,
            Key=f"csvs/{file}",
        )
        Path(file).unlink()
        context.log.info(file)
        # TODO: csv file assets
        yield AssetMaterialization(
            asset_key="irx_edx_exports",
            description="Export directory for IRx edx reports",
            metadata={
                "bucket_path": MetadataValue.path(
                    f"s3://{config.course_exports_bucket}/{context.resources.results_dir.path.name}"  # noqa: E501
                ),
            },
        )

    # load tracking logs to s3
    log_files = Path.glob(extracted_edx_exports_directory, "/*.log")
    for file in log_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.tracking_log_bucket,
            Key=f"logs/{file}",
        )
        Path(file).unlink()
        context.log.info(file)
        # TODO: tracking log assets
    yield AssetMaterialization(
        asset_key="irx_edx_exports",
        description="Export directory for IRx edx reports",
        metadata={
            "bucket_path": MetadataValue.path(
                f"s3://{config.course_exports_bucket}/{context.resources.results_dir.path.name}"  # noqa: E501
            ),
        },
    )

    # load course exports to s3
    # TODO: check for XML?
    course_export_files = Path.glob(extracted_edx_exports_directory, "/*.tar.gz")
    for file in course_export_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.course_exports_bucket,
            Key=f"exports/{file}",
        )
        Path(file).unlink()
        context.log.info(file)
    yield AssetMaterialization(
        asset_key="irx_edx_exports",
        description="Export directory for IRx edx reports",
        metadata={
            "bucket_path": MetadataValue.path(
                f"s3://{config.course_exports_bucket}/{context.resources.results_dir.path.name}"  # noqa: E501
            ),
        },
    )
    context.resources.results_dir.clean_dir()
    yield Output(
        f"{config.course_exports_bucket}/{context.resources.results_dir.path.name}",
        "edx_s3_course_tarball_directory",
    )
