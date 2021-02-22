"""Pipeline for uploading open-discussions user and run data to s3."""
from dagster import (  # noqa: WPS235
    Field,
    Int,
    ModeDefinition,
    Noneable,
    Output,
    OutputDefinition,
    PresetDefinition,
    SolidExecutionContext,
    String,
    pipeline,
    solid,
)
from pyarrow import fs
from pypika import PostgreSQLQuery as Query
from pypika import Table, Tables

from ol_data_pipelines.lib.arrow_helper import stream_to_parquet_file
from ol_data_pipelines.lib.yaml_config_helper import load_yaml_config
from ol_data_pipelines.resources.postgres_db import postgres_db_resource


@solid(
    description=("Query postgres and data as parquet files "),
    required_resource_keys={"postgres_db"},
    config_schema={
        "chunksize": Field(
            Int,
            is_required=False,
            default_value=5000,
            description="Number of rows per parquet file",
        ),
        "outputs_dir": Field(
            String, is_required=True, description="Path for output files"
        ),
        "file_base": Field(
            String,
            is_required=False,
            default_value="open_user_data",
            description="base_name_for_file",
        ),
        "minimum_timestamp": Field(
            Noneable(str),
            is_required=False,
            default_value=None,
            description="Minimum minimum_timestamp for user date_joined. Format yyyy-mm-dd",
        ),
    },
    output_defs=[
        OutputDefinition(
            name="query_folder",
            dagster_type=String,
            description="Path to user data rendered as parquet config_files",
        )
    ],
)
def fetch_open_user_data(context: SolidExecutionContext):
    """
    Fetch open user data parquet files.

    :param context: Dagster execution context for configuration data
    :type context: SolidExecutionContext

    :yield: A path definition that points to the the folder containing the data
    """
    users = Table("auth_user")
    query = Query.from_(users).select("id", "email", "date_joined")

    if context.solid_config["minimum_timestamp"]:
        query = query.where(
            users.date_joined > context.solid_config["minimum_timestamp"]
        )

    file_system, output_folder = fs.FileSystem.from_uri(
        context.solid_config["outputs_dir"]
    )

    stream_to_parquet_file(
        context.resources.postgres_db.run_chunked_query(
            query, context.solid_config["chunksize"]
        ),
        context.solid_config["file_base"],
        file_system,
        output_folder,
    )

    yield Output(output_folder, "query_folder")


@solid(
    description=("Retrieve run information from open and write it as parquet files "),
    required_resource_keys={"postgres_db"},
    config_schema={
        "chunksize": Field(
            Int,
            is_required=False,
            default_value=5000,
            description="Number of rows per parquet file",
        ),
        "outputs_dir": Field(
            String, is_required=True, description="Path for output files"
        ),
        "file_base": Field(
            String,
            is_required=False,
            default_value="open_run_data",
            description="base_name_for_file",
        ),
        "minimum_timestamp": Field(
            Noneable(str),
            is_required=False,
            default_value=None,
            description="Minimum minimum_timestamp for run created_on. Format yyyy-mm-dd",
        ),
    },
    output_defs=[
        OutputDefinition(
            name="query_folder",
            dagster_type=String,
            description="Path to user data rendered as parquet config_files",
        )
    ],
)
def fetch_open_run_data(context: SolidExecutionContext):  # noqa: WPS210
    """
    Fetch open run data as parquet files.

    :param context: Dagster execution context for configuration data
    :type context: SolidExecutionContext

    :yield: A path definition that points to the the folder containing the data
    """
    run_offerors, runs, offerors, content_types = Tables(
        "course_catalog_learningresourcerun_offered_by",
        "course_catalog_learningresourcerun",
        "course_catalog_learningresourceofferor",
        "django_content_type",
    )
    query = (
        Query.from_(run_offerors)  # noqa: WPS221
        .join(runs)
        .on(runs.id == run_offerors.learningresourcerun_id)
        .join(offerors)
        .on(offerors.id == run_offerors.learningresourceofferor_id)
        .join(content_types)
        .on(content_types.id == runs.content_type_id)
        .select(
            runs.id,
            runs.run_id,
            runs.created_on,
            (offerors.name).as_("offeror_name"),
            (runs.object_id).as_("course_id"),
        )
        .where(runs.published == True)  # noqa: E712
        .where(content_types.model == "course")
    )

    if context.solid_config["minimum_timestamp"]:
        query = query.where(runs.created_on > context.solid_config["minimum_timestamp"])

    file_system, output_folder = fs.FileSystem.from_uri(
        context.solid_config["outputs_dir"]
    )

    stream_to_parquet_file(
        context.resources.postgres_db.run_chunked_query(
            query, context.solid_config["chunksize"]
        ),
        context.solid_config["file_base"],
        file_system,
        output_folder,
    )

    yield Output(output_folder, "query_folder")


@solid(
    description=(
        "Retrieve course information from open and write it as parquet files "
    ),
    required_resource_keys={"postgres_db"},
    config_schema={
        "chunksize": Field(
            Int,
            is_required=False,
            default_value=5000,
            description="Number of rows per parquet file",
        ),
        "outputs_dir": Field(
            String, is_required=True, description="Path for output files"
        ),
        "file_base": Field(
            String,
            is_required=False,
            default_value="open_course_data",
            description="base_name_for_file",
        ),
        "minimum_timestamp": Field(
            Noneable(str),
            is_required=False,
            default_value=None,
            description="Minimum minimum_timestamp for run created_on. Format yyyy-mm-dd",
        ),
    },
    output_defs=[
        OutputDefinition(
            name="query_folder",
            dagster_type=String,
            description="Path to user data rendered as parquet config_files",
        )
    ],
)
def fetch_open_course_data(context: SolidExecutionContext):  # noqa: WPS210
    """
    Fetch open course data as parquet files.

    :param context: Dagster execution context for configuration data
    :type context: SolidExecutionContext

    :yield: A path definition that points to the the folder containing the data
    """
    course_offerors, courses, offerors = Tables(
        "course_catalog_course_offered_by",
        "course_catalog_course",
        "course_catalog_learningresourceofferor",
    )
    query = (
        Query.from_(course_offerors)  # noqa: WPS221
        .join(courses)
        .on(courses.id == course_offerors.course_id)
        .join(offerors)
        .on(offerors.id == course_offerors.learningresourceofferor_id)
        .select(
            courses.id,
            courses.course_id,
            courses.created_on,
            (offerors.name).as_("offeror_name"),
        )
        .where(courses.published == True)  # noqa: E712
    )

    if context.solid_config["minimum_timestamp"]:
        query = query.where(
            courses.created_on > context.solid_config["minimum_timestamp"]
        )

    file_system, output_folder = fs.FileSystem.from_uri(
        context.solid_config["outputs_dir"]
    )

    stream_to_parquet_file(
        context.resources.postgres_db.run_chunked_query(
            query, context.solid_config["chunksize"]
        ),
        context.solid_config["file_base"],
        file_system,
        output_folder,
    )

    yield Output(output_folder, "query_folder")


@pipeline(
    description="Retrieve user information from open and write it as parquet files",
    mode_defs=[
        ModeDefinition(
            name="production",
            resource_defs={"postgres_db": postgres_db_resource},
        )
    ],
    preset_defs=[
        PresetDefinition(
            name="production",
            run_config=load_yaml_config("/etc/dagster/open-discussions.yaml"),
            mode="production",
        )
    ],
    tags={
        "source": "open_discussions",
        "destination": "s3",
        "owner": "platform-engineering",
    },
)
def pull_open_data_pipeline():
    """Pipeline for pulling open user and run data to s3."""
    fetch_open_run_data()
    fetch_open_course_data()
    fetch_open_user_data()
