from pathlib import Path

from dagster import op, OpExecutionContext, Out, Output, Config

from dagster.core.definitions.input import In
from pydantic import Field

import glob
import zipfile

from ol_orchestrate.lib.dagster_types.files import DagsterPath


class DownloadConfig(Config):
    edx_exports_bucket: str = Field(
        # todo: should there be a default value?
        default_value="irx-edx-exports",
        description="The GCS bucket where IRx stores data exports from edx.org",  # noqa: E501
    )


class UploadConfig(Config):
    csv_staging_bucket: str = (
        Field(
            description="The S3 bucket where CSV files will be staged",
        ),
    )
    tracking_log_bucket: str = Field(
        description="The S3 bucket where tracking log files will be staged",  # noqa: E501
    )
    course_exports_bucket: str = Field(
        description="The S3 bucket where course_exports will be stored",
    )


# detect files op/sensor?


@op(
    name="download_edx_data_exports",
    description="Downloads zip file from GCS",
    required_resource_keys={"gcp_gcs", "staging_dir"},
    out={"edx_exports_directory": Out(dagster_type=DagsterPath)},
)
def download_edx_data(
    context: OpExecutionContext, config: DownloadConfig
) -> DagsterPath:  # type: ignore
    """Downloads copies of edx.org data from IRx cold storage.

    :yield: The path where files have been downloaded.
    """
    storage_client = context.resources.gcp_gcs
    bucket = storage_client.get_bucket("irx-edx-exports")
    edx_exports_download_path = context.resources.staging_dir.path.joinpath(
        config.edx_exports_bucket
    )
    os.makedirs(edx_exports_download_path, exist_ok=True)
    blobs = storage_client.list_blobs(bucket)
    for blob in blobs:
        blob.download_to_filename(f"{edx_exports_download_path}/{blob.name}")
        context.log.info(f"Downloaded {blob.name} to {edx_exports_download_path}.")
    yield Output(
        edx_exports_download_path,
        "edx_exports_directory",
    )


@op(
    name="extract_zip_files",
    description="Decompresses zipped files with edx.org data.",
    required_resource_keys={"staging_dir"},
    ins={"edx_exports_directory": In(dagster_type=DagsterPath)},
    out={"edx_exports_directory": Out(dagster_type=DagsterPath)},
)
def extract_files(
    context: OpExecutionContext,
    edx_exports_directory: DagsterPath,
) -> DagsterPath:  # type: ignore
    """Extracts the contents of the downloaded zip files.

    :param edx_exports_directory: The path where files being processed are downloaded.
    :type edx_exports_directory: DagsterPath

    :yield: The path where files have been decompressed.
    """
    for file in edx_exports_directory.glob("*.zip"):
        with zipfile.ZipFile(file, "r") as zippedFile:
            zippedFile.extractall(path=f"./{file.stem}")
            context.log.info(f"Extracted contents of '{file.name}' to '{file.stem}'.")
    blobs = storage_client.list_blobs(bucket)
    for blob in blobs:
        blob.download_to_filename(f"{edx_exports_download_path}/{blob.name}")
    yield Output(
        edx_exports_directory,
        "extracted_edx_exports_directory",
    )


@op(
    name="upload_edx_data_exports",
    description="Upload extracted files to S3",
    required_resource_keys={"staging_dir", "s3"},
    ins={"extracted_edx_exports_directory": In(dagster_type=DagsterPath)},
)
def upload_files(
    context: OpExecutionContext,
    extracted_edx_exports_directory: DagsterPath,
    config: UploadConfig,
) -> None:
    """Loads files to staging locations.
    There are separate S3 buckets for CSV files, tracking logs, and course_exports.

    :param extracted_edx_exports_directory: The path with decompressed edx.org zip files.
    :type extracted_edx_exports_directory: DagsterPath
    """
    # load CSV files to s3
    csv_files = glob.glob(extracted_edx_exports_directory + "/*.csv")
    for file in csv_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.csv_staging_bucket,
            Key=f"{file}",
        )
        Path(file).unlink()
        context.log.info(f"Uploaded {file} to {config.csv_staging_bucket}.")

    # load tracking logs to s3
    log_files = glob.glob(extracted_edx_exports_directory + "/*.log")
    for file in log_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.tracking_log_bucket,
            Key=f"{file}",
        )
        Path(file).unlink()
        context.log.info(f"Uploaded {file} to {config.tracking_log_bucket}.")

    # load course exports to s3
    # todo: check for XML?
    course_export_files = glob.glob(extracted_edx_exports_directory + "/*.tar.gz")
    for file in course_export_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.course_exports_bucket,
            Key=f"exports/{file}",
        )
        Path(file).unlink()
        context.log.info(f"Uploaded {file} to {config.course_exports_bucket}.")
