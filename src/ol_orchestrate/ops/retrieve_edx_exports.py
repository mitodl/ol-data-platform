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


class UploadConfig(Config):
    # set defaults but make paramaterizable
    edx_irx_exports_bucket: Optional[str] = Field(
        description="The S3 bucket where CSV files will be staged",
    )
    tracking_log_bucket: Optional[str] = Field(
        description="The S3 bucket where tracking log files will be staged",  # noqa: E501
        default=None,
    )
    course_exports_bucket: Optional[str] = Field(
        description="The S3 bucket where course_exports will be stored", default=None
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
        if fname != "COLD/internal-2023-09-10.zip":
            continue
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
    # TODO: update with logic to handle gpg files?  # noqa: FIX002, TD002, TD003
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
    # TODO: iterate through directories and files? what is the file structure?  # noqa: E501, FIX002, TD002, TD003
    # assetmaterialization
    # load CSV files to s3
    csv_files = extracted_edx_exports_directory.glob("*.csv")
    for file in csv_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.csv_staging_bucket,
            Key=f"csvs/{file}",
        )
        Path(file).unlink()
        context.log.info(file)
        # TODO: csv file assets  # noqa: FIX002, TD002, TD003
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
    log_files = extracted_edx_exports_directory.glob("*.log")
    for file in log_files:
        context.resources.s3.upload_file(
            Filename=file,
            Bucket=config.tracking_log_bucket,
            Key=f"logs/{file}",
        )
        Path(file).unlink()
        context.log.info(file)
        # TODO: tracking log assets  # noqa: FIX002, TD002, TD003
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
    # TODO: check for XML?  # noqa: FIX002, TD002, TD003
    course_export_files = extracted_edx_exports_directory.glob("*.tar.gz")
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
