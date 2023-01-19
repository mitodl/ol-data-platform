from dagster import (
    AssetMaterialization,
    Field,
    MetadataEntry,
    OpExecutionContext,
    Out,
    Output,
    String,
    op,
)
from dagster.core.definitions.input import In

from ol_orchestrate.lib.dagster_types.files import DagsterPath


@op(
    name="edx_course_tarballs",
    description="Download edx course tarballs from GCS bucket",
    required_resource_keys={"gcp_gcs", "results_dir"},
    config_schema={
        "edx_gcs_course_tarballs": Field(
            String,
            default_value="simeon-mitx-course-tarballs",
            description="The GCS bucket that contains the MITx course exports from edx.org",  # noqa: E501
        ),
    },
    out={"edx_course_tarball_directory": Out(dagster_type=DagsterPath)},
)
def download_edx_gcs_course_data(context):
    storage_client = context.resources.gcp_gcs
    bucket = storage_client.get_bucket("simeon-mitx-course-tarballs")
    edx_course_tarball_path = context.resources.results_dir.path.joinpath(
        context.op_config["edx_gcs_course_tarballs"]
    )
    blobs = storage_client.list_blobs(bucket)
    for blob in blobs:
        blob.download_to_filename(edx_course_tarball_path + blob.name)
    yield Output(
        edx_course_tarball_path,
        "edx_course_tarball_directory",
    )


@op(
    name="edx_upload_gcs_course_tarballs",
    description="Upload all data from GCS downloaded course tarballs provided by institutional research.",  # noqa: E501
    required_resource_keys={"results_dir", "s3"},
    config_schema={
        "edx_etl_results_bucket": Field(
            String,
            default_value="odl-developer-testing-sandbox",
            is_required=False,
            description="S3 bucket to use for uploading results of pipeline execution.",
        ),
    },
    ins={
        "edx_gcs_course_tarball_directory": In(dagster_type=DagsterPath),
    },
    out={"edx_s3_course_tarball_directory": Out(dagster_type=String)},
)
def upload_edx_gcs_course_data_to_s3(
    context: OpExecutionContext,
    edx_gcs_course_tarball_directory: DagsterPath,
):
    """Upload course tarballs to S3 from gcs provided by institutional research.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_gcs_course_tarball_directory: Directory path containing course tarballs.
    :type edx_gcs_course_tarball_directory: DagsterPath

    :yield: The S3 path of the uploaded directory
    """
    results_bucket = context.op_config["edx_etl_results_bucket"]
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
        metadata_entries=[
            MetadataEntry.fspath(
                f"s3://{results_bucket}/{context.resources.results_dir.path.name}"  # noqa: E501, WPS237
            ),
        ],
    )
    context.resources.results_dir.clean_dir()
    yield Output(
        f"{results_bucket}/{context.resources.results_dir.path.name}",  # noqa: WPS237
        "edx_s3_course_tarball_directory",
    )
