import re
import zipfile
from pathlib import Path
from typing import Optional

from dagster import (
    AssetMaterialization,
    Config,
    In,
    MetadataValue,
    OpExecutionContext,
    Out,
    Output,
    String,
    op,
)
from pydantic import Field

from ol_orchestrate.lib.dagster_types.files import DagsterPath


class DownloadConfig(Config):
    irx_edxorg_gcs_bucket: str = Field(
        description="The GCS bucket where IRx stores data exports from edx.org",
        default="simeon-mitx-pipeline-main",
    )
    files_to_sync: Optional[list[str]] = Field(
        description="The list of new files to download", default=None
    )
    export_type: str = Field(
        description="The subset of archive files to process", default=None
    )


class UploadConfig(Config):
    edx_irx_exports_bucket: Optional[str] = Field(
        description="The S3 bucket where files will be staged",
        default="ol-devops-sandbox",
    )
    bucket_prefix: Optional[str] = Field(
        description="The bucket prefix for uploaded edx exports",
        default="pipeline-storage",
    )


file_types = {
    "logs": "*.log.gz",
    "csv": "*.csv",
    "courses": "*.tar.gz",
    "forum": "*.[json bson]*",
}


@op(
    name="download_edx_data_exports",
    description="Download zip files from GCS",
    required_resource_keys={"gcp_gcs", "exports_dir"},
    out={"export_type": Out(dagster_type=String)},
)
def download_edx_data(context: OpExecutionContext, config: DownloadConfig):
    """Download copies of edx.org data from IRx cold storage."""
    storage_client = context.resources.gcp_gcs.client
    bucket = storage_client.get_bucket(config.irx_edxorg_gcs_bucket)
    edx_exports_download_path = context.resources.exports_dir.path.joinpath(
        config.export_type
    )
    context.log.info(edx_exports_download_path)
    for file in config.files_to_sync or []:
        fname = file.removeprefix("COLD/")
        Path(edx_exports_download_path).joinpath(Path(fname).parent).mkdir(
            parents=True, exist_ok=True
        )
        export_type = {
            "courses": r"internal-\d{4}-\d{2}-\d{2}.zip$",
            "logs": r"mitx-edx-events-\d{4}-\d{2}-\d{2}.log.gz$",
        }
        file_match = export_type[config.export_type]
        if re.match(file_match, fname):
            blob = bucket.get_blob(file)
            blob.download_to_filename(f"{edx_exports_download_path}/{fname}")
            context.log.info(blob.name)
            context.log.info(blob.size)
    yield Output(
        config.export_type,
        "export_type",
    )


@op(
    name="extract_zip_files",
    description="Decompresses zipped files with edx.org course data and csvs.",
    required_resource_keys={"exports_dir"},
    ins={"export_type": In(dagster_type=String)},
    out={"export_type": Out(dagster_type=String)},
)
def extract_course_files(context: OpExecutionContext, export_type: str):
    """Extract the contents of the downloaded zip files."""
    if export_type == "courses":
        exports_path = context.resources.exports_dir.path.joinpath(export_type)
        context.log.info(exports_path)
        zip_files = exports_path.glob("*.zip")
        bad_files = []
        for file in zip_files:
            context.log.info(file)
            try:
                with zipfile.ZipFile(file, "r") as zippedFile:
                    zippedFile.extractall(path=f"{exports_path}")
            except zipfile.BadZipfile:
                context.log.exception(
                    "zipfile.BadZipfile: %s is not a zip file", file.name
                )
                bad_files.append(file.name)
            context.log.info("Bad Zip Files: %s", bad_files)
    yield Output(
        export_type,
        "export_type",
    )


@op(
    name="upload_edx_data_exports",
    description="Upload extracted files to S3",
    required_resource_keys={"exports_dir", "s3"},
    ins={"export_type": In(dagster_type=String)},
    out={"uploaded_edx_exports_directory": Out(dagster_type=DagsterPath)},
)
def upload_files(context: OpExecutionContext, config: UploadConfig, export_type: str):
    """Load files to staging locations.
    There are separate S3 buckets for CSV files, tracking logs, and course_exports.
    """
    exports_path = context.resources.exports_dir.path.joinpath(export_type)
    context.log.info(exports_path)
    for file_type in file_types:
        files = exports_path.rglob(file_types[file_type])
        context.log.info(files)
        for file in files:
            s3_path = f"s3://{config.edx_irx_exports_bucket}/{config.bucket_prefix}/{file_type}"
            context.log.info(file)
            context.log.info("%s", file.relative_to(exports_path))
            context.log.info(s3_path)
            context.resources.s3.upload_file(
                Filename=file,
                Bucket=config.edx_irx_exports_bucket,
                Key=f"{config.bucket_prefix}/{file_type}/{file.relative_to(exports_path)!s}",
            )
            Path(file).unlink()
            yield AssetMaterialization(
                asset_key=f"edxorg_{file_type}/",
                description=f"Export directory for IRx edx {file_type} data",
                metadata={"bucket_path": MetadataValue.path(s3_path)},
            )
    yield Output(
        exports_path,
        "uploaded_edx_exports_directory",
    )
