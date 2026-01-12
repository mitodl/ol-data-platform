import hashlib
import json
import subprocess
import time
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import jsonlines
from dagster import (
    Config,
    DynamicOut,
    DynamicOutput,
    ExpectationResult,
    Failure,
    List,
    MetadataValue,
    OpExecutionContext,
    Out,
    Output,
    String,
    op,
)
from dagster.core.definitions.input import In
from flatten_dict import flatten
from flatten_dict.reducers import make_reducer
from ol_orchestrate.lib.file_rendering import write_csv
from ol_orchestrate.lib.openedx import un_nest_course_structure
from pydantic import Field
from pypika import MySQLQuery as Query
from pypika import Table, Tables


class ListCoursesConfig(Config):
    edx_course_api_page_size: int = Field(
        default=100,
        description="The number of records to return per API request. This can be "
        "modified to address issues with rate limiting.",
    )


class ExportEdxForumDatabaseConfig(Config):
    mongodump_path: str = Field(
        default="/usr/bin/mongodump",
        description="The mongodump path for a MongoDB replicat set",
    )
    edx_mongodb_uri: str = Field(
        description="The URI for connecting to a MongoDB replicat set",
    )
    edx_mongodb_username: str = Field(
        default="",
        description="Username for account with permissions to read forum database",
    )
    edx_mongodb_password: str = Field(
        default="",
        description="Password for account with permissions to read forum database",
    )
    edx_mongodb_auth_db: str = Field(
        default="admin",
        description="The MongoDB database that contains the account information for the"
        " authenticating user.",
    )
    edx_mongodb_forum_database_name: str = Field(
        description="Name of database that contains forum data for Open edX installation",  # noqa: E501
    )


class ExportEdxCoursesConfig(Config):
    edx_course_bucket: str = Field(
        description="Bucket name that the edX installation uses for uploading "
        "course exports",
    )


class UploadExtractedDataConfig(Config):
    edx_etl_results_bucket: str = Field(
        default="odl-developer-testing-sandbox",
        description="S3 bucket to use for uploading results of pipeline execution.",
    )


@op(
    name="list_edx_courses",
    description=(
        "Retrieve the list of course IDs active in the edX instance "
        "to be used in subsequent steps to pull data per course."
    ),
    required_resource_keys={"openedx"},
    out={"edx_course_ids": Out(description="List of course IDs in the app")},
)
def list_courses(
    context: OpExecutionContext, config: ListCoursesConfig
) -> Generator[ExpectationResult | Output, None, None]:
    """
    Retrieve the list of course IDs active in the edX instance to be used in subsequent
    steps to pull data per course.

    :param config: Client details pertaining to the Open edX API
    :type Config

    :yield: List of edX course IDs
    """
    course_ids = []
    course_id_generator = context.resources.openedx.get_edx_course_ids(
        page_size=config.edx_course_api_page_size,
    )
    for result_set in course_id_generator:
        course_ids.extend([course["id"] for course in result_set])
    yield ExpectationResult(
        success=bool(course_ids),
        label="edx_course_list_not_empty",
        description="Ensure course list is not empty.",
        metadata={
            "number_of_course_ids": MetadataValue.text(text=str(len(course_ids)))
        },
    )
    yield Output(course_ids, "edx_course_ids")


class CourseStructureConfig(Config):
    flattened_dict_delimiter: str = Field(
        default="__",
        description=(
            "The string to use for delimiting the nested fields of the dictionary "
            "representing the course structure."
        ),
    )


@op(
    name="retrieve_edx_course_structure",
    description=(
        "Retrieve the JSON document describing the structure of the selected "
        "course via REST API from a running Open edX instance."
    ),
    required_resource_keys={"openedx", "results_dir"},
    ins={"course_ids": In()},
    out=DynamicOut(),
)
def fetch_edx_course_structure_from_api(
    context: OpExecutionContext, config: CourseStructureConfig, course_ids: list[str]
) -> Generator[DynamicOutput, None, None]:
    """Retrieve the course structure via the REST API of a running Open edX instance.

    :param context: The Dagster execution context
    :param course_ids: The list of course IDs for which to retrieve the structure

    :returns: The path where the document is written to.
    """
    today = datetime.now(tz=UTC)
    structures_file = context.resources.results_dir.path.joinpath(
        f"course_structures_{today.strftime('%Y-%m-%d')}.json"
    )
    blocks_file = context.resources.results_dir.path.joinpath(
        f"course_blocks_{today.strftime('%Y-%m-%d')}.json"
    )
    data_retrieval_timestamp = datetime.now(tz=UTC).isoformat()
    with (
        jsonlines.open(structures_file, mode="w") as structures,
        jsonlines.open(blocks_file, mode="w") as blocks,
    ):
        for course_id in course_ids:
            context.log.info("Retrieving course structure for %s", course_id)
            course_structure = context.resources.openedx.get_course_structure_document(
                course_id
            )
            table_row = {
                "content_hash": hashlib.sha256(
                    json.dumps(course_structure).encode("utf-8")
                ).hexdigest(),
                "course_id": course_id,
                "course_structure": course_structure,
                "course_structure_flattened": flatten(
                    course_structure,
                    reducer=make_reducer(config.flattened_dict_delimiter),
                ),
                "retrieved_at": data_retrieval_timestamp,
            }
            structures.write(table_row)
            for block in un_nest_course_structure(
                course_id, course_structure, data_retrieval_timestamp
            ):
                blocks.write(block)
    yield DynamicOutput(structures_file, mapping_key="course_structures")
    yield DynamicOutput(blocks_file, mapping_key="course_blocks")


@op(
    required_resource_keys={"sqldb", "results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
    out={
        "edx_enrolled_users": Out(
            dagster_type=Path,
            description="Path to user data in tabular format rendered as CSV files",
        )
    },
)
def enrolled_users(
    context: OpExecutionContext, edx_course_ids: list[str]
) -> Generator[Output | ExpectationResult, None, None]:
    """Generate a table showing which students are currently enrolled in which courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_course_ids: List of course IDs to retrieve student enrollments for
    :type edx_course_ids: List[String]

    :yield: A path definition that points to the rendered data table
    """
    course_enrollment, users = Tables("student_courseenrollment", "auth_user")
    users_query = (
        Query.from_(users)
        .join(course_enrollment)
        .on(users.id == course_enrollment.user_id)
        .select(
            users.id,
            users.username,
            users.first_name,
            users.last_name,
            users.email,
            users.is_staff,
            users.is_active,
            users.is_superuser,
            users.last_login,
            users.date_joined,
            course_enrollment.course_id,
        )
        .where(course_enrollment.course_id.isin(edx_course_ids))
    )
    query_fields, users_data = context.resources.sqldb.run_query(str(users_query))
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    enrollments_path = context.resources.results_dir.path.joinpath("users_query.csv")
    write_csv(query_fields, users_data, enrollments_path)
    yield ExpectationResult(
        success=bool(users_data),
        label="enrolled_users_count_non_zero",
        description="Ensure that the number of users is not zero.",
    )
    yield Output(enrollments_path, "edx_enrolled_users")


@op(
    name="open_edx_student_submissions",
    description="Export of student submission events for courses on the specified Open edX installation.",  # noqa: E501
    required_resource_keys={"sqldb", "results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
    out={
        "edx_student_submissions": Out(
            dagster_type=Path,
            description="Path to submissions data in tabular format rendered as CSV files",  # noqa: E501
        )
    },
)
def student_submissions(
    context: OpExecutionContext, edx_course_ids: list[str]
) -> Generator[Output | ExpectationResult, None, None]:
    """Retrieve details of student submissions for the given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :yield: A path definition that points to the rendered data table
    """
    studentmodule = Table("courseware_studentmodule")
    submissions_count = 0
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    submissions_path = context.resources.results_dir.path.joinpath(
        "studentmodule_query.csv"
    )
    for course_id in edx_course_ids:
        submission_query = (
            Query.from_(studentmodule)
            .select(
                "id",
                "module_type",
                "module_id",
                "student_id",
                "state",
                "grade",
                "created",
                "modified",
                "max_grade",
                "done",
                "course_id",
            )
            .where(studentmodule.course_id == course_id)
        )
        query_fields, submission_data = context.resources.sqldb.run_query(
            str(submission_query)
        )
        submissions_count += len(submission_data)
        write_csv(query_fields, submission_data, submissions_path)

    yield ExpectationResult(
        success=submissions_count > 0,
        label="enrolled_students_count_non_zero",
        description="Ensure that the number of enrolled students is not zero.",
    )
    yield Output(submissions_path, "edx_student_submissions")


@op(
    name="open_edx_enrollments",
    description="Export of enrollment records for courses on the specified Open edX installation.",  # noqa: E501
    required_resource_keys={"sqldb", "results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
    out={
        "edx_enrollment_records": Out(
            dagster_type=Path,
            description="Path to enrollment data in tabular format rendered as CSV files",  # noqa: E501
        )
    },
)
def course_enrollments(
    context: OpExecutionContext, edx_course_ids: list[str]
) -> Generator[Output | ExpectationResult, None, None]:
    """Retrieve enrollment records for given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :yield: A path definition that points to the rendered data table
    """
    enrollment = Table("student_courseenrollment")
    enrollments_query = (
        Query.from_(enrollment)
        .select("id", "user_id", "course_id", "created", "is_active", "mode")
        .where(enrollment.course_id.isin(edx_course_ids))
    )
    query_fields, enrollment_data = context.resources.sqldb.run_query(
        str(enrollments_query)
    )
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    enrollments_path = context.resources.results_dir.path.joinpath(
        "enrollment_query.csv"
    )
    write_csv(query_fields, enrollment_data, enrollments_path)
    yield ExpectationResult(
        success=bool(enrollment_data),
        label="enrollments_count_non_zero",
        description="Ensure that the number of enrollment records is not zero.",
    )
    yield Output(enrollments_path, "edx_enrollment_records")


@op(
    name="open_edx_course_roles",
    description="Export of user roles for courses on the specified Open edX installation.",  # noqa: E501
    required_resource_keys={"sqldb", "results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
    out={
        "edx_course_roles": Out(
            dagster_type=Path,
            description="Path to course role data in tabular format rendered as CSV files",  # noqa: E501
        )
    },
)
def course_roles(
    context: OpExecutionContext, edx_course_ids: list[str]
) -> Generator[Output | ExpectationResult, None, None]:
    """Retrieve information about user roles for given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :yield: A path definition that points to the rendered data table
    """
    access_role = Table("student_courseaccessrole")
    roles_query = (
        Query.from_(access_role)
        .select("id", "user_id", "org", "course_id", "role")
        .where(access_role.course_id.isin(edx_course_ids))
    )
    query_fields, roles_data = context.resources.sqldb.run_query(str(roles_query))
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    roles_path = context.resources.results_dir.path.joinpath("role_query.csv")
    write_csv(query_fields, roles_data, roles_path)
    yield ExpectationResult(
        success=bool(roles_data),
        label="course_roles_count_non_zero",
        description="Ensure that the number of course roles is not zero.",
    )
    yield Output(roles_path, "edx_course_roles")


@op(
    name="open_edx_user_roles",
    description="Export of user roles for forums on the specified Open edX installation.",  # noqa: E501
    required_resource_keys={"sqldb", "results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
    out={
        "edx_user_roles": Out(
            dagster_type=Path,
            description="Path to user role data in tabular format rendered as CSV files",  # noqa: E501
        )
    },
)
def user_roles(
    context: OpExecutionContext, edx_course_ids: list[str]
) -> Generator[Output | ExpectationResult, None, None]:
    """Retrieve information about user roles for given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :yield: A path definition that points to the rendered data table
    """
    users, role, course, org = Tables(
        "django_comment_client_role_users",
        "django_comment_client_role",
        "organizations_organizationcourse",
        "organizations_organization",
    )
    user_roles_query = (
        Query.from_(users)
        .join(role)
        .on(users.role_id == role.id)
        .join(course)
        .on(role.course_id == course.course_id)
        .join(org)
        .on(course.organization_id == org.id)
        .select(
            users.id,
            users.user_id,
            org.name.as_("org"),
            role.course_id,
            role.name.as_("role"),
        )
        .where(role.course_id.isin(edx_course_ids))
    )
    query_fields, user_roles_data = context.resources.sqldb.run_query(
        str(user_roles_query)
    )
    user_roles_path = context.resources.results_dir.path.joinpath("role_users.csv")
    write_csv(query_fields, user_roles_data, user_roles_path)
    yield ExpectationResult(
        success=bool(user_roles_data),
        label="user_roles_count_non_zero",
        description="Ensure that the number of user roles is not zero.",
    )
    yield Output(user_roles_path, "edx_user_roles")


@op(
    name="export_edx_forum_database",
    description="Solid to build the command line string for executing mongodump against the Open edX forum database",  # noqa: E501
    required_resource_keys={"results_dir"},
    out={
        "edx_forum_data_directory": Out(
            dagster_type=Path,
            description="Path to exported forum data generated by mongodump command",
        )
    },
)
def export_edx_forum_database(
    context: OpExecutionContext,
    config: ExportEdxForumDatabaseConfig,
) -> Generator[Output, None, None]:
    """Export the edX forum database using mongodump.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param config: Details pertaining to the MongoDB database
    :type Config

    :yield: Path object to the directory where the exported Mongo database is
        located

    :raises Failure: Raise a failure event if the mongo dump returns a non-zero exit
        code
    """
    forum_data_path = context.resources.results_dir.path.joinpath(
        config.edx_mongodb_forum_database_name
    )
    mongo_uri = config.edx_mongodb_uri
    command_array = [
        config.mongodump_path,
        "--uri",
        mongo_uri,
        "--db",
        config.edx_mongodb_forum_database_name,
        "--authenticationDatabase",
        config.edx_mongodb_auth_db,
        "--out",
        context.resources.results_dir.absolute_path,
    ]
    if password := config.edx_mongodb_password:
        command_array.extend(["--password", password])
    if username := config.edx_mongodb_username:
        command_array.extend(["--username", username])

    mongodump_result = subprocess.run(  # noqa: S603
        command_array,
        capture_output=True,
        cwd=str(context.resources.results_dir.root_dir),
        check=False,
    )

    if mongodump_result.returncode != 0:
        raise Failure(
            description="The mongodump command for exporting the Open edX forum database failed.",  # noqa: E501
            metadata={
                "mongodump_command": MetadataValue.text(" ".join(command_array)),
                "mongodump_stdout": MetadataValue.text(
                    text=mongodump_result.stdout.decode("utf8")
                ),
                "mongodump_stderr": MetadataValue.text(
                    text=mongodump_result.stderr.decode("utf8")
                ),
            },
        )

    yield Output(forum_data_path, "edx_forum_data_directory")


@op(
    name="edx_export_courses",
    description="Export the contents of all active courses to S3",
    required_resource_keys={"s3", "openedx"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        ),
        "daily_extracts_dir": In(
            dagster_type=String,
            description="The S3 location for the daily edX extracts",
        ),
    },
)
def export_edx_courses(
    context: OpExecutionContext,
    edx_course_ids: list[str],
    daily_extracts_dir: str,
    config: ExportEdxCoursesConfig,
) -> None:
    exported_courses = context.resources.openedx.export_courses(
        course_ids=edx_course_ids,
    )
    successful_exports: set[str] = set()
    failed_exports: set[str] = set()
    tasks = exported_courses["upload_task_ids"]
    context.log.info("Exporting %s tasks from Open edX", len(tasks))
    # Possible status values found here:
    # https://github.com/openedx/django-user-tasks/blob/master/user_tasks/models.py
    while len(successful_exports.union(failed_exports)) < len(tasks):
        time.sleep(timedelta(seconds=60).seconds)
        for course_id, task_id in tasks.items():
            try:
                task_status = context.resources.openedx.check_course_export_status(
                    course_id,
                    task_id,
                )
            except httpx.HTTPStatusError:
                # Don't fail the whole job if one task status yields an error
                continue
            if task_status["state"] == "Succeeded":
                successful_exports.add(course_id)
            if task_status["state"] in {"Failed", "Canceled", "Retrying"}:
                failed_exports.add(course_id)
    for course_id in successful_exports:
        context.log.info("Moving course %s to %s", course_id, daily_extracts_dir)
        course_file = f"{course_id}.tar.gz"
        source_object = {
            "Bucket": config.edx_course_bucket,
            "Key": course_file,
        }
        dest_bucket, dest_prefix = daily_extracts_dir.split("/", maxsplit=1)
        dest_object = {
            "Bucket": dest_bucket,
            "Key": f"{dest_prefix}/courses/{course_file}",
        }
        context.resources.s3.copy(CopySource=source_object, **dest_object)
        context.resources.s3.delete_object(**source_object)


@op(
    name="edx_write_course_id_csv",
    description="Write a CSV file containing the list of active course IDs on the edX instance",  # noqa: E501
    required_resource_keys={"results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        ),
    },
    out={
        "edx_course_ids_csv": Out(
            dagster_type=Path,
            description="Path to list of course IDs rendered as a CSV file",
        ),
    },
)
def write_course_list_csv(context: OpExecutionContext, edx_course_ids: list[str]):
    course_ids_csv_path = context.resources.results_dir.path.joinpath("course_ids.csv")
    course_ids_dict = [{"course_id": course_id} for course_id in edx_course_ids]
    write_csv(["course_id"], course_ids_dict, course_ids_csv_path)
    yield Output(course_ids_csv_path, "edx_course_ids_csv")


@op(
    name="edx_upload_daily_extracts",
    description="Upload all data from daily extracts to S3 for institutional research.",
    required_resource_keys={"results_dir", "s3"},
    ins={"uploads": In(dagster_type=List[Path])},
    out={"edx_daily_extracts_directory": Out(dagster_type=String)},
)
def upload_extracted_data(
    context: OpExecutionContext,
    config: UploadExtractedDataConfig,
    uploads: list[Path],
):
    """Upload all data exports to S3 so that institutional research can ingest.

    :param context: Dagster execution context for propagaint configuration data
    :param config: The configuration for the operation
    :param uploads: The list of paths to upload to S3

    :yield: The S3 path of the uploaded directory
    """
    results_bucket = config.edx_etl_results_bucket
    for path_object in uploads:
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
    context.resources.results_dir.clean_dir()
    yield Output(
        f"{results_bucket}/{context.resources.results_dir.path.name}",
        "edx_daily_extracts_directory",
    )
