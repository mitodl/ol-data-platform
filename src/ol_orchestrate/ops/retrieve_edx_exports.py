import re
import zipfile
from pathlib import Path
from typing import Optional

from dagster import (
    AssetMaterialization,
    Config,
    In,
    List,
    MetadataValue,
    OpExecutionContext,
    Out,
    Output,
    String,
    op,
)
from pydantic import Field


class DownloadConfig(Config):
    irx_edxorg_gcs_bucket: str = Field(
        description="The GCS bucket where IRx stores data exports from edx.org",
        default="simeon-mitx-pipeline-main",
    )
    files_to_sync: Optional[list[str]] = Field(
        description="The list of new files to download", default=None
    )
    export_type_type: str = Field(
        description="The subset of archive files to process",
    )


class UploadConfig(Config):
    edx_irx_exports_bucket: Optional[str] = Field(
        description="The S3 bucket where files will be staged",
        default="ol-devops-sandbox",
    )
    bucket_prefix: Optional[str] = Field(
        description="The bucket prefix for uploaded edx exports",
        default="edxorg-raw-data",
    )


@op(
    name="download_edx_data_exports",
    description="Download zip files from GCS",
    required_resource_keys={"gcp_gcs", "exports_dir"},
    out={"file_list": Out(dagster_type=List[List[String]])},
)
def download_edx_data(context: OpExecutionContext, config: DownloadConfig):
    """Download copies of edx.org data from IRx cold storage.

    This pipeline handles two different types of compressed data: logs and courses.
    All downloaded files are appended to a list that is then passed onto the next op.
    Downloaded log files go straight to uploads without being extracted.
    Downloaded course archives get sent to the extraction op.

    :param context: Dagster execution context for propagating configuration data.
    :type context: OpExecutionContext

    :param config: Dagster execution config for propagating edXorg download
        configuration data.
    :type config: Config

    :yield: file_list: A nested list of files that were downloaded in the previous op.
        Includes export_type, exports_path, file_name, and file_date for each file.
    :type List[List[String]])
    """
    storage_client = context.resources.gcp_gcs.client
    bucket = storage_client.get_bucket(config.irx_edxorg_gcs_bucket)
    exports_path = f"{context.resources.exports_dir.path}/{config.export_type}"
    context.log.info(exports_path)
    export_types = {
        "courses": r"internal-(\d{4}-\d{2}-\d{2}).zip$",
        "logs": r"mitx-edx-events-(\d{4}-\d{2}-\d{2}).log.gz$",
    }
    file_match = export_types[config.export_type]
    downloaded_files = []
    for file in config.files_to_sync or []:
        file_name = file.removeprefix("COLD/")
        Path(exports_path).joinpath(Path(file_name).parent).mkdir(
            parents=True, exist_ok=True
        )
        file_path = f"{exports_path}/{file_name}"
        if match := re.match(file_match, file_name):
            blob = bucket.get_blob(file)
            context.log.info(blob.name)
            context.log.info(blob.size)
            blob.download_to_filename(file_path)
            context.log.info(file_path)
            file_date = str(match.group(1)).replace("-", "")
            downloaded_files.append(
                [config.export_type, exports_path, file_name, file_date]
            )
    context.log.info("Downloaded archives: %s", downloaded_files)
    yield Output(
        downloaded_files,
        "file_list",
    )


@op(
    name="extract_zip_files",
    description="Decompresses zipped files with edx.org course data and csvs.",
    required_resource_keys={"exports_dir"},
    ins={
        "file_list": In(dagster_type=List[List[String]]),
    },
    out={
        "file_list": Out(dagster_type=List[List[String]]),
    },
)
def extract_course_files(context: OpExecutionContext, file_list: list[list[str]]):
    """Extract the contents of the downloaded zip files for course data.

    Extract course archives containing multiple files and subfolders so that
    different file types can be uploaded to their respective S3 buckets.

    :param context: Dagster execution context for propagating configuration data.
    :type context: OpExecutionContext

    :param file_list: A nested list of files that were downloaded in the previous op.
    For each file, we are passing the export_type, exports_path, file_name,
        and file_date.
    :type List[List[String]])

    :yield: file_list: The list of files that were successfully decompressed.
    Includes export_type, exports_path, file_name, and file_date.
    :type List[List[String]])
    """
    extracted_files = []
    bad_zip_files = []
    for export_type, exports_path, file_name, file_date in file_list:
        context.log.info("exports_path: %s", exports_path)
        context.log.info("file_name: %s", file_name)
        if export_type == "courses":
            try:
                with zipfile.ZipFile(f"{exports_path}/{file_name}", "r") as zippedFile:
                    zippedFile.extractall(path=f"{exports_path}")
                    # Append the extracted archive's root to the exports_path
                    (relative_path,) = zipfile.Path(zippedFile).iterdir()
                    extracted_files.append(
                        [
                            export_type,
                            f"{exports_path}/{relative_path.name}",
                            file_name,
                            file_date,
                        ]
                    )
            except zipfile.BadZipfile:
                context.log.exception(
                    "zipfile.BadZipfile: %s is not a zip file", file_name
                )
                bad_zip_files.append([export_type, exports_path, file_name, file_date])
            context.log.info("Bad Zip Files: %s", bad_zip_files)
            context.log.info("Decompressed archives: %s", extracted_files)
    yield Output(
        extracted_files,
        "file_list",
    )


@op(
    name="upload_edx_data_exports",
    description="Upload extracted files to S3",
    required_resource_keys={"exports_dir", "s3"},
    ins={
        "file_list": In(dagster_type=List[List[String]]),
    },
    out={
        "s3_upload_bucket": Out(dagster_type=String),
        "file_list": Out(dagster_type=List[List[String]]),
    },
)
def upload_files(
    context: OpExecutionContext, config: UploadConfig, file_list: list[list[str]]
):
    """Load files to staging locations on S3.

    Upload log and course data to S3 under separate buckets for the file types.

    :param context: Dagster execution context for propagating configuration data.
    :type context: OpExecutionContext

    :param config: Dagster execution config for propagating edXorg upload
        configuration data.
    :type config: Config

    :param file_list: A nested list of files that were processed in the previous op.
    That may be a list of files that were downloaded (.log.gz files) or files that
        were extracted (.zip) internal edXorg export files. For each file, we are
        passing the export_type, exports_path, file_name, and file_date.
    :type List[List[String]])

    :yield: file_list: The list of files that were successfully uploaded.
    Includes export_type, s3_path, file_name, and file_date.
    :type List[List[String]])
    """
    uploaded_files = []
    for export_type, exports_path, file_name, file_date in file_list:
        export_types = {
            "logs": {
                "logs": "*.log.gz",
            },
            "courses": {
                "csv": "*.csv",
                "courses": "*.tar.gz",
                "forum": "*.[json bson]*",
            },
        }
        file_types = export_types[export_type]
        for file_type in file_types:
            files = Path(exports_path).rglob(file_types[file_type])
            for file in files:
                context.log.info(file)
                relative_path = str(file.relative_to(exports_path)).replace(
                    f"{file_type}/", ""
                )
                if export_type == "courses":
                    relative_path = f"{file_date}/{relative_path}"
                context.log.info(relative_path)
                s3_key = f"{config.bucket_prefix}/{file_type}/{relative_path}"
                s3_path = f"s3://{config.edx_irx_exports_bucket}/{s3_key}"
                context.log.info(s3_path)
                context.resources.s3.upload_file(
                    Filename=file,
                    Bucket=config.edx_irx_exports_bucket,
                    Key=s3_key,
                )
                uploaded_files.append([export_type, s3_path, file_name, file_date])
                Path(file).unlink()
                yield AssetMaterialization(
                    asset_key=f"edxorg_{file_type}/",
                    description=f"S3 URI for IRx edx {file_type} upload",
                    metadata={
                        "S3_URI": MetadataValue.path(s3_path),
                        "file_date": file_date,
                    },
                )
    yield Output(
        f"s3://{config.edx_irx_exports_bucket}/{config.bucket_prefix}/",
        "s3_upload_bucket",
    )
    yield Output(
        uploaded_files,
        "file_list",
    )
