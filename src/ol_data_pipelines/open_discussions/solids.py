"""Pipeline for uploading open-discussions user and run data to s3."""
import datetime

import pytz
from dagster import (  # noqa: WPS235
    Field,
    InputDefinition,
    Int,
    List,
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
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
from pyarrow import fs
from pypika import PostgreSQLQuery, Query, Table, Tables
from pypika.functions import Cast, Coalesce, Min

from ol_data_pipelines.lib.arrow_helper import stream_to_parquet_file
from ol_data_pipelines.lib.pypika_helper import Excluded, MinBy, Position, SplitPart
from ol_data_pipelines.lib.yaml_config_helper import load_yaml_config
from ol_data_pipelines.resources.athena_db import athena_db_resource
from ol_data_pipelines.resources.postgres_db import (
    DEFAULT_POSTGRES_QUERY_CHUNKSIZE,
    postgres_db_resource,
)


@solid(
    description=("Query postgres and data as parquet files "),
    required_resource_keys={"postgres_db"},
    config_schema={
        "chunksize": Field(
            Int,
            is_required=False,
            default_value=DEFAULT_POSTGRES_QUERY_CHUNKSIZE,
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

    :yields: A path definition that points to the the folder containing the data
    """
    users = Table("auth_user")
    query = PostgreSQLQuery.from_(users).select("id", "email", "date_joined")

    if context.solid_config["minimum_timestamp"]:  # noqa: WPS204
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
            default_value=DEFAULT_POSTGRES_QUERY_CHUNKSIZE,
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

    :yields: A path definition that points to the the folder containing the data
    """
    run_offerors, runs, offerors, content_types = Tables(
        "course_catalog_learningresourcerun_offered_by",
        "course_catalog_learningresourcerun",
        "course_catalog_learningresourceofferor",
        "django_content_type",
    )
    query = (
        PostgreSQLQuery.from_(run_offerors)
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
            default_value=DEFAULT_POSTGRES_QUERY_CHUNKSIZE,
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
        PostgreSQLQuery.from_(course_offerors)
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


@solid(
    description=(
        "Run athena query joining enrollment data to user, course and run data"
    ),
    required_resource_keys={"athena_db"},
    config_schema={
        "minimum_timestamp": Field(
            Noneable(str),
            is_required=False,
            default_value=None,
            description="Minimum creation timestamp for enrollment or user",
        ),
        "athena_open_database": Field(
            Noneable(str),
            is_required=True,
            description="Athena database for open discussions tables",
        ),
        "athena_open_course_runs_table": Field(
            Noneable(str),
            is_required=True,
            description="Athena table for open course runs",
        ),
        "athena_open_courses_table": Field(
            Noneable(str),
            is_required=True,
            description="Athena table for open courses",
        ),
        "athena_open_users_table": Field(
            Noneable(str),
            is_required=True,
            description="Athena table for open users",
        ),
        "athena_mitx_database": Field(
            Noneable(str),
            is_required=True,
            description="Athena database for open mitx tables",
        ),
        "athena_mitx_enrollments_table": Field(
            Noneable(str),
            is_required=True,
            description="Athena user_info_combo table",
        ),
    },
    input_defs=[
        InputDefinition(
            name="open_user_update",
            dagster_type=String,
            description="Open user data needs to be uploaded to s3/athena before this solid runs.",
        ),
        InputDefinition(
            name="open_course_update",
            dagster_type=String,
            description="Open course data needs to be uploaded to s3/athena before this solid runs.",
        ),
        InputDefinition(
            name="open_run_update",
            dagster_type=String,
            description="Open run data needs to be uploaded to s3/athena before this runs.",
        ),
    ],
    output_defs=[DynamicOutputDefinition(List)],
)
def run_open_enrollments_query(  # noqa: WPS210
    context: SolidExecutionContext,
    open_course_update: String,
    open_run_update: String,
    open_user_update: String,
):
    """
    Query Athena cluster for enrollments and update the open discussions course_catalog_enrollment table.

    :param context: Dagster execution context for configuration data
    :type context: SolidExecutionContext

    :param open_course_update: Output from fetch_open_course_data(). Passed to ensure that solid runs first
    :type open_course_update: String

    :param open_run_update: Output from fetch_open_rundata(). Passed to ensure that solid runs first
    :type open_run_update: String

    :param open_user_update: Output from fetch_open_user_data(). Passed to ensure that solid runs first
    :type open_user_update: String

    :yield: Chunked query results with open enrollments
    """
    runs, courses, users = Tables(
        context.solid_config["athena_open_course_runs_table"],
        context.solid_config["athena_open_courses_table"],
        context.solid_config["athena_open_users_table"],
        schema=context.solid_config["athena_open_database"],
    )

    athena_enrollments = Table(
        context.solid_config["athena_mitx_enrollments_table"],
        schema=context.solid_config["athena_mitx_database"],
    )

    athena_query = (
        Query.from_(athena_enrollments)
        .join(users)
        .on(athena_enrollments.email == users.email)
        .left_join(runs)
        .on(
            (  # noqa: WPS465
                Position(
                    SplitPart(athena_enrollments.enrollment_course_id, "/", 2).isin(
                        runs.run_id
                    )
                )
                < Position(
                    SplitPart(athena_enrollments.enrollment_course_id, "/", 3).isin(
                        runs.run_id
                    )
                )
            )
            & Position(
                SplitPart(athena_enrollments.enrollment_course_id, "/", 2).isin(
                    runs.run_id
                )
            )
            > 0
        )
        .left_join(courses)
        .on(
            runs.id.isnull()
            & Position(
                SplitPart(athena_enrollments.enrollment_course_id, "/", 2).isin(
                    courses.course_id
                )
            )
            > 0
        )
        .select(
            (users.id).as_("open_user_id"),
            athena_enrollments.enrollment_course_id,
            athena_enrollments.enrollment_created,
            Min(runs.id).as_("open_run_id"),
            Coalesce(MinBy(runs.course_id, runs.id), Min(courses.id)).as_(
                "open_course_id"
            ),
        )
        .where(runs.offeror_name.isnull() | runs.offeror_name.isin(["MITx", "xPRO"]))
        .where(
            courses.offeror_name.isnull() | courses.offeror_name.isin(["MITx", "xPRO"])
        )
        .groupby(
            users.id,
            athena_enrollments.enrollment_course_id,
            athena_enrollments.enrollment_created,
        )
    )

    if context.solid_config["minimum_timestamp"]:
        athena_query = athena_query.where(
            (  # noqa: WPS465
                athena_enrollments.enrollment_created
                > Cast(context.solid_config["minimum_timestamp"], "DATE")
            )
            | (
                users.date_joined
                > Cast(context.solid_config["minimum_timestamp"], "DATE")
            )
        )

    query_cursor = context.resources.athena_db.run_query(athena_query)

    chunk = 0
    while True:
        data_chunk = query_cursor.fetchmany(50000)
        if not data_chunk:
            break

        yield DynamicOutput(
            value=data_chunk, mapping_key=f"open_enrollments_data_chunk{chunk}"
        )
        chunk = chunk + 1


@solid(
    description=("Insert open enrollments data to the open database"),
    required_resource_keys={"postgres_db"},
    config_schema={
        "insert_chunksize": Field(
            Int,
            is_required=False,
            default_value=50,
            description="Number of rows to insert into postgres database in one go",
        ),
    },
    input_defs=[
        InputDefinition(
            name="enrollment_data",
            dagster_type=List[dict],
            description="Enrollment data returned in chunks",
        )
    ],
)
def update_open_enrollment_data(  # noqa: WPS210
    context: SolidExecutionContext, enrollment_data: List[dict]
):
    """
    Query Athena cluster for enrollments and update the open discussions course_catalog_enrollment table.

    :param context: Dagster execution context for configuration data
    :type context: SolidExecutionContext

    :param enrollment_data: Output from fetch_open_enrollment_data(). Enrollment data returned in chunks
    :type enrollment_data: List[dict]
    """
    open_enrollments = Table("course_catalog_enrollment")

    insert_count = 0

    timestamp = datetime.datetime.utcnow()
    timezone = pytz.timezone("UTC")

    base_insert_query = (
        PostgreSQLQuery.into(open_enrollments)
        .columns(
            "created_on",
            "updated_on",
            "enrollment_timestamp",
            "run_id",
            "user_id",
            "course_id",
            "enrollments_table_run_id",
        )
        .on_conflict("user_id", "enrollments_table_run_id")
        .do_update("updated_on", timestamp)
        .do_update("enrollment_timestamp", Excluded("enrollment_timestamp"))
        .do_update("run_id", Excluded("run_id"))
        .do_update("course_id", Excluded("course_id"))
    )

    insert_query = base_insert_query

    for enrollment in enrollment_data:
        if enrollment["enrollment_created"]:
            enrollment_timestamp = timezone.localize(enrollment["enrollment_created"])
        else:
            enrollment_timestamp = None  # type: ignore

        insert_query = insert_query.insert(
            timestamp,
            timestamp,
            enrollment_timestamp,
            enrollment["open_run_id"],
            enrollment["open_user_id"],
            enrollment["open_course_id"],
            enrollment["enrollment_course_id"],
        )
        insert_count = insert_count + 1

        if insert_count >= context.solid_config["insert_chunksize"]:
            context.resources.postgres_db.run_write_query(insert_query)
            insert_query = base_insert_query
            insert_count = 0


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


@pipeline(
    description="""
        Update open course, run and user data on S3.
        Merge that data with mitx enrollments from the mitx bigquery database.
        Finally, update data in the open enrollments table.
    """,
    mode_defs=[
        ModeDefinition(
            name="production",
            resource_defs={
                "postgres_db": postgres_db_resource,
                "athena_db": athena_db_resource,
            },
        )
    ],
    preset_defs=[
        PresetDefinition(
            name="production",
            run_config=load_yaml_config(
                "/etc/dagster/open-discussions-enrollment-update.yaml"
            ),
            mode="production",
        )
    ],
    tags={
        "source": "open_discussions",
        "destination": "open_discussions",
        "owner": "platform-engineering",
    },
)
def update_enrollments_pipeline():
    """Pipeline for updating open enrollment data."""
    run_open_enrollments_query(
        fetch_open_run_data(), fetch_open_course_data(), fetch_open_user_data()
    ).map(update_open_enrollment_data)
