import os
from typing import Optional

from dagster import (
    AssetMaterialization,
    Config,
    MetadataValue,
    OpExecutionContext,
    Out,
    Output,
    String,
    op,
)
from dagster.core.definitions.input import In

from ol_orchestrate.lib.dagster_types.files import DagsterPath
from pydantic import Field


class DownloadEdxGcsCourseConfig(Config):
    edx_gcs_course_tarballs: str = Field(
        "simeon-mitx-course-tarballs",
        description="The GCS bucket that contains the MITx course exports from edx.org",  # noqa: E501
    )


class UploadEdxGcsCourseConfig(Config):
    edx_etl_results_bucket: Optional[str] = Field(
        "odl-developer-testing-sandbox",
        description="S3 bucket to use for uploading results of pipeline execution.",
    )


@op(
    name="edx_course_tarballs",
    description="Download edx course tarballs from GCS bucket",
    required_resource_keys={"gcp_gcs", "results_dir"},
    out={"edx_course_tarball_directory": Out(dagster_type=DagsterPath)},
)
def download_edx_gcs_course_data(context, config: DownloadEdxGcsCourseConfig):
    # todo: replace context and config with a new asset config
    # once resources have been migrated
    storage_client = context.resources.gcp_gcs
    bucket = storage_client.get_bucket(config.edx_gcs_course_tarballs)
    edx_course_tarball_path = context.resources.results_dir.path.joinpath(
        config.edx_gcs_course_tarballs
    )
    os.makedirs(edx_course_tarball_path, exist_ok=True)  # noqa: PTH103
    blobs = storage_client.list_blobs(bucket)
    for blob in blobs:
        blob.download_to_filename(f"{edx_course_tarball_path}/{blob.name}")
    yield Output(
        edx_course_tarball_path,
        "edx_course_tarball_directory",
    )


@op(
    name="edx_upload_gcs_course_tarballs",
    description="Upload all data from GCS downloaded course tarballs provided by institutional research.",  # noqa: E501
    required_resource_keys={"results_dir", "s3"},
    ins={
        "edx_gcs_course_tarball_directory": In(dagster_type=DagsterPath),
    },
    out={"edx_s3_course_tarball_directory": Out(dagster_type=String)},
)
def upload_edx_gcs_course_data_to_s3(
    context: OpExecutionContext,
    edx_gcs_course_tarball_directory: DagsterPath,
    config: UploadEdxGcsCourseConfig,
):
    """Upload course tarballs to S3 from gcs provided by institutional research.

    :param context: Dagster execution context for propagating configuration data
    :type context: OpExecutionContext

    :param edx_gcs_course_tarball_directory: Directory path containing course tarballs.
    :type edx_gcs_course_tarball_directory: DagsterPath

    :param config: Directory path containing course tarballs.
    :type Config

    :yield: The S3 path of the uploaded directory
    """
    # todo: replace context and config with a new asset config once
    #  resources have been migrated
    results_bucket = config.edx_etl_results_bucket
    for path_object in context.resources.results_dir.path.iterdir():
        if path_object.is_dir():
            for fpath in path_object.iterdir():
                file_key = str(
                    fpath.relative_to(context.resources.results_dir.root_dir)
                )
                context.resources.s3.upload_file(
                    Filename=str(fpath),
                    Bucket=results_bucket,
                    Key=file_key,
                )
        elif path_object.is_file():
            file_key = str(
                path_object.relative_to(context.resources.results_dir.root_dir)
            )
            context.resources.s3.upload_file(
                Filename=str(path_object),
                Bucket=results_bucket,
                Key=file_key,
            )
    yield AssetMaterialization(
        asset_key="edx_daily_results",
        description="Daily export directory for edX export pipeline",
        metadata={
            "bucket_path": MetadataValue.path(
                f"s3://{results_bucket}/{context.resources.results_dir.path.name}"  # noqa: E501
            ),
        },
    )
    context.resources.results_dir.clean_dir()
    yield Output(
        f"{results_bucket}/{context.resources.results_dir.path.name}",
        "edx_s3_course_tarball_directory",
    )
