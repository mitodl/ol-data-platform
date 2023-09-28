import re
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
    irx_edxorg_gcs_bucket: str = Field(
        "simeon-mitx-pipeline-main",
        description="The GCS bucket where IRx stores data exports from edx.org",  # noqa: E501
    )
    files_to_sync: Optional[list[str]] = Field(
        description="The list of new files to download", default=None
    )
    file_type: str = Field(
        description="The subset of archive files to process", default=None
    )


class UploadConfig(Config):
    # set defaults but make paramaterizable
    edx_irx_exports_bucket: Optional[str] = Field(
        description="The S3 bucket where CSV files will be staged",
    )
    bucket_prefix: Optional[str] = Field(
        description="The bucket prefix for uploaded edx exports"
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
    storage_client = context.resources.gcp_gcs.client
    bucket = storage_client.get_bucket(config.irx_edxorg_gcs_bucket)
    edx_exports_download_path = context.resources.exports_dir.path.joinpath(
        config.irx_edxorg_gcs_bucket
    )
    context.log.info(edx_exports_download_path)
    for fname in config.files_to_sync or []:
        Path(edx_exports_download_path).joinpath(Path(fname).parent).mkdir(
            parents=True, exist_ok=True
        )
        file_type = {
            "courses": r"COLD/internal-\d{4}-\d{2}-\d{2}.zip$",
            "logs": r"COLD/mitx-edx-events-\d{4}-\d{2}-\d{2}.log.gz$",
        }
        file_match = file_type[config.file_type]
        if re.match(file_match, fname):
            blob = bucket.get_blob(fname)
            blob.download_to_filename(f"{edx_exports_download_path}/{fname}")
            context.log.info(blob.name)
            context.log.info(blob.size)
    yield Output(
        edx_exports_download_path,
        "edx_exports_directory",
    )


@op(
    name="extract_zip_files",
    description="Decompresses zipped files with edx.org course data and csvs.",
    required_resource_keys={"exports_dir"},
    ins={"edx_exports_directory": In(dagster_type=DagsterPath)},
    out={"edx_exports_directory": Out(dagster_type=DagsterPath)},
)
def extract_course_files(
    context: OpExecutionContext,
    edx_exports_directory: DagsterPath,
):
    """Extract the contents of the downloaded zip files.

    :param edx_exports_directory: The path where files being processed are downloaded.
    :type edx_exports_directory: DagsterPath

    :yield: The path where files have been decompressed.
    """
    # course exports
    for file in edx_exports_directory.glob("*.zip"):
        with zipfile.ZipFile(file, "r") as zippedFile:
            zippedFile.extractall(path=f"./{file.stem}")
            context.log.info(file.name)
    yield Output(
        edx_exports_directory,
        "edx_exports_directory",
    )


@op(
    name="upload_edx_data_exports",
    description="Upload extracted files to S3",
    required_resource_keys={"exports_dir", "s3"},
    ins={"extracted_edx_exports_directory": In(dagster_type=DagsterPath)},
    out={"uploaded_edx_exports_directory": Out(dagster_type=DagsterPath)},
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
    # load CSV files to s3
    csv_files = extracted_edx_exports_directory.rglob("*.csv")
    for file in csv_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.edx_irx_exports_bucket,
            Key=f"{config.bucket_prefix}/csv/"
            f"{str(file.relative_to(extracted_edx_exports_directory)).removeprefix('COLD/')}",
        )
        Path(file).unlink()
        context.log.info(file)
        yield AssetMaterialization(
            asset_key="irx_edx_exports",
            description="Export directory for IRx edx csvs",
            metadata={
                "bucket_path": MetadataValue.path(
                    f"s3://{config.edx_irx_exports_bucket}/{config.bucket_prefix}/csv"
                ),
            },
        )

    # load tracking logs to s3
    log_files = extracted_edx_exports_directory.rglob("*.log.gz")
    for file in log_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.edx_irx_exports_bucket,
            Key=f"{config.bucket_prefix}/logs/"
            f"{str(file.relative_to(extracted_edx_exports_directory)).removeprefix('COLD/')}",
        )
        Path(file).unlink()
        context.log.info(file)
        yield AssetMaterialization(
            asset_key="edxorg_tracking_logs",
            description="Export directory for IRx edx tracking logs",
            metadata={
                "bucket_path": MetadataValue.path(
                    f"s3://{config.edx_irx_exports_bucket}/{config.bucket_prefix}/logs"
                ),
            },
        )

    # load course exports to s3
    course_export_files = extracted_edx_exports_directory.rglob("*.tar.gz")
    for file in course_export_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.edx_irx_exports_bucket,
            Key=f"{config.bucket_prefix}/courses/"
            f"{str(file.relative_to(extracted_edx_exports_directory)).removeprefix('COLD/')}",
        )
        Path(file).unlink()
        context.log.info(file)
        yield AssetMaterialization(
            asset_key="irx_edx_exports",
            description="Export directory for IRx edx course exports",
            metadata={
                "bucket_path": MetadataValue.path(
                    f"s3://{config.edx_irx_exports_bucket}/{config.bucket_prefix}/courses"
                ),
            },
        )
    # load forum exports to s3
    forum_files = extracted_edx_exports_directory.rglob("*.[json bson]*")
    for file in forum_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.edx_irx_exports_bucket,
            Key=f"{config.bucket_prefix}/forum/"
            f"{str(file.relative_to(extracted_edx_exports_directory)).removeprefix('COLD/')}",
        )
        Path(file).unlink()
        context.log.info(file)
        yield AssetMaterialization(
            asset_key="irx_edx_exports",
            description="Export directory for IRx edx csvs",
            metadata={
                "bucket_path": MetadataValue.path(
                    f"s3://{config.edx_irx_exports_bucket}/{config.bucket_prefix}/forum"
                ),
            },
        )
    context.resources.exports_dir.clean_dir()
    edx_exports_upload_path = context.resources.exports_dir.path
    yield Output(
        edx_exports_upload_path,
        "uploaded_edx_exports_directory",
    )
