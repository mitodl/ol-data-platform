import time
from datetime import timedelta

from dagster import (
    AssetMaterialization,
    ExpectationResult,
    Failure,
    Field,
    Int,
    List,
    OpExecutionContext,
    Out,
    Output,
    String,
    op,
)
from dagster.core.definitions.input import In
from dagster_shell.utils import execute as run_bash
from pypika import MySQLQuery as Query
from pypika import Table, Tables, Order

from ol_orchestrate.lib.dagster_types.files import DagsterPath
from ol_orchestrate.lib.edx_api_client import (
    check_course_export_status,
    export_courses,
    get_access_token,
    get_edx_course_ids,
)
from ol_orchestrate.lib.file_rendering import write_csv


@op(
    name="list_edx_courses",
    description=(
        "Retrieve the list of course IDs active in the edX instance "
        "to be used in subsequent steps to pull data per course."
    ),
    config_schema={
        "edx_client_id": Field(
            String, is_required=True, description="OAUTH2 Client ID for Open edX API"
        ),
        "edx_client_secret": Field(
            String,
            is_required=True,
            description="OAUTH2 Client secret for Open edX API",
        ),
        "edx_base_url": Field(
            String,
            default_value="lms.mitx.mit.edu",
            is_required=False,
            description="Domain of edX installation",
        ),
        "edx_token_type": Field(
            String,
            default_value="jwt",
            is_required=False,
            description="Type of OAuth token to use for authenticating to the edX API. "
            'Default to "jwt" for edX Juniper and newer, or "bearer" for older releases.',  # noqa: E501
        ),
        "edx_course_api_page_size": Field(
            Int,
            default_value=100,
            is_required=False,
            description="The number of records to return per API request. This can be "
            "modified to address issues with rate limiting.",
        ),
    },
    out={
        "edx_course_ids": Out(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
)
def list_courses(context: OpExecutionContext) -> List[String]:
    """
    Retrieve the list of course IDs active in the edX instance to be used in subsequent
    steps to pull data per course.

    :param context: Dagster context object for passing configuration
    :type context: OpExecutionContext

    :yield: List of edX course IDs
    """
    access_token = get_access_token(
        client_id=context.op_config["edx_client_id"],
        client_secret=context.op_config["edx_client_secret"],
        edx_url=context.op_config["edx_base_url"],
        token_type=context.op_config["edx_token_type"],
    )
    course_ids = []
    course_id_generator = get_edx_course_ids(
        context.op_config["edx_base_url"],
        access_token,
        page_size=context.op_config["edx_course_api_page_size"],
    )
    for result_set in course_id_generator:
        course_ids.extend([course["id"] for course in result_set])
    yield ExpectationResult(
        success=bool(course_ids),
        label="edx_course_list_not_empty",
        description="Ensure course list is not empty.",
    )
    yield Output(course_ids, "edx_course_ids")


@op(
    name="open_edx_enrolled_users",
    required_resource_keys={"sqldb", "results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
    out={
        "edx_enrolled_users": Out(
            dagster_type=DagsterPath,
            description="Path to user data in tabular format rendered as CSV files",
        )
    },
)
def enrolled_users(context: OpExecutionContext, edx_course_ids: List[String]) -> DagsterPath:  # type: ignore  # noqa: E501
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
            ''.as_('password'),
            users.is_staff,
            users.is_active,
            users.is_superuser,
            users.last_login,
            users.date_joined,
            ''.as_('status'),
            'NULL'.as_('email_key'),
            ''.as_('avatar_type'),
            ''.as_('country'),
            '0'.as_('show_country'),
            'NULL'.as_('date_of_birth'),
            ''.as_('interesting_tags'),
            ''.as_('ignored_tags'),
            '0'.as_('email_tag_filter_strategy'),
            '0'.as_('display_tag_filter_strategy'),
            '0'.as_('consecutive_days_visit_count'),
            course_enrollment.course_id,
        )
        .where(course_enrollment.course_id.isin(edx_course_ids))
    )
    query_fields, users_data = context.resources.sqldb.run_query(users_query)
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    enrollments_path = context.resources.results_dir.path.joinpath("users_query.sql")
    write_csv(query_fields, users_data, enrollments_path)
    yield AssetMaterialization(
        asset_key="users_query",
        description="Information of users enrolled in available courses on Open edX installation",  # noqa: E501
    )
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
            dagster_type=DagsterPath,
            description="Path to submissions data in tabular format rendered as CSV files",  # noqa: E501
        )
    },
)
def student_submissions(context: OpExecutionContext, edx_course_ids: List[String]) -> DagsterPath:  # type: ignore  # noqa: E501
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
            submission_query
        )
        submissions_count += len(submission_data)
        write_csv(query_fields, submission_data, submissions_path)
    yield AssetMaterialization(
        asset_key="enrolled_students",
        description="Students enrolled in edX courses",
    )
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
            dagster_type=DagsterPath,
            description="Path to enrollment data in tabular format rendered as CSV files",  # noqa: E501
        )
    },
)
def course_enrollments(context: OpExecutionContext, edx_course_ids: List[String]) -> DagsterPath:  # type: ignore  # noqa: E501
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
        .select("*")
        .where(enrollment.course_id.isin(edx_course_ids))
    )
    query_fields, enrollment_data = context.resources.sqldb.run_query(enrollments_query)
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    enrollments_path = context.resources.results_dir.path.joinpath(
        "enrollment_query.csv"
    )
    write_csv(query_fields, enrollment_data, enrollments_path)
    yield AssetMaterialization(
        asset_key="enrollment_query",
        description="Course enrollment records from Open edX installation",
    )
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
            dagster_type=DagsterPath,
            description="Path to course role data in tabular format rendered as CSV files",  # noqa: E501
        )
    },
)
def course_roles(context: OpExecutionContext, edx_course_ids: List[String]) -> DagsterPath:  # type: ignore  # noqa: E501
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
        .select(
            "org",
            "course_id",
            "user_id",
            "role"
        )
        .where(access_role.course_id.isin(edx_course_ids))
    )
    query_fields, roles_data = context.resources.sqldb.run_query(roles_query)
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    roles_path = context.resources.results_dir.path.joinpath("role_query.csv")
    write_csv(query_fields, roles_data, roles_path)
    yield AssetMaterialization(
        asset_key="role_query",
        description="Course roles records from Open edX installation",
    )
    yield ExpectationResult(
        success=bool(roles_data),
        label="course_roles_count_non_zero",
        description="Ensure that the number of course roles is not zero.",
    )
    yield Output(roles_path, "edx_course_roles")

@op(
    name="open_edx_team_memberships",
    description="Export of team memberships for courses on the specified Open edX installation.",  # noqa: E501
    required_resource_keys={"sqldb", "results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
    out={
        "edx_team_memberships": Out(
            dagster_type=DagsterPath,
            description="Path to team membership data in tabular format rendered as CSV files",  # noqa: E501
        )
    },
)
def team_memberships(context: OpExecutionContext, edx_course_ids: List[String]) -> DagsterPath:  # type: ignore  # noqa: E501
    """Retrieve information about teams membership for given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :yield: A path definition that points to the rendered data table
    """
    course_team, team_membership = Tables("teams_courseteam", "teams_courseteammembership")
    memberships_count = 0
    memberships_path = context.resources.results_dir.path.joinpath(
        "teamsmembership_query.sql"
    )
    for course_id in edx_course_ids:
        membership_query = (
            Query.from_(course_team)
            .join(team_membership)
            .on(course_team.id == team_membership.team_id)
            .select("teams_courseteammembership.*")
            .where(course_team.course_id == course_id)
        )
        query_fields, membership_data = context.resources.sqldb.run_query(
            membership_query
        )
        memberships_count += len(membership_data)
        write_csv(query_fields, membership_data, memberships_path)
    yield AssetMaterialization(
        asset_key="membership_query",
        description="Team memberships in edX courses",
    )
    yield ExpectationResult(
        success=memberships_count > 0,
        label="team_memberships_count_non_zero",
        description="Ensure that the number of team memberships is not zero.",
    )
    yield Output(memberships_path, "edx_team_memberships")

@op(
    name="open_edx_course_grades",
    description="Export of user grades for courses on the specified Open edX installation.",  # noqa: E501
    required_resource_keys={"sqldb", "results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
    out={
        "edx_course_grades": Out(
            dagster_type=DagsterPath,
            description="Path to course grade data in tabular format rendered as CSV files",  # noqa: E501
        )
    },
)
def course_grades(context: OpExecutionContext, edx_course_ids: List[String]) -> DagsterPath:  # type: ignore  # noqa: E501
    """Retrieve information about user grades for given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :yield: A path definition that points to the rendered data table
    """
    course_grade = Table("grades_persistentcoursegrade")
    grades_query = (
        Query.from_(course_grade)
        .select(
            "course_id",
            "user_id",
            "grading_policy_hash",
            "percent_grade",
            "letter_grade",
            "passed_timestamp",
            "created",
            "modified"
        )
        .where(course_grade.course_id.isin(edx_course_ids))
        .orderby("user_id", order=Order.asc)
    )
    query_fields, grades_data = context.resources.sqldb.run_query(grades_query)
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    grades_path = context.resources.results_dir.path.joinpath("coursegrade_query.sql")
    write_csv(query_fields, grades_data, grades_path)
    yield AssetMaterialization(
        asset_key="grades_query",
        description="Course grades records from Open edX installation",
    )
    yield ExpectationResult(
        success=bool(grades_data),
        label="course_grades_count_non_zero",
        description="Ensure that the number of course grades is not zero.",
    )
    yield Output(grades_path, "edx_course_grades")

@op(
    name="open_edx_subsection_grades",
    description="Export of user grades for subsection on the specified Open edX installation.",  # noqa: E501
    required_resource_keys={"sqldb", "results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
    out={
        "edx_subsection_grades": Out(
            dagster_type=DagsterPath,
            description="Path to course grade data in tabular format rendered as CSV files",  # noqa: E501
        )
    },
)
def subsection_grades(context: OpExecutionContext, edx_course_ids: List[String]) -> DagsterPath:  # type: ignore  # noqa: E501
    """Retrieve information about user subsection grades for given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :yield: A path definition that points to the rendered data table
    """
    subsection_grade = Table("grades_persistentsubsectiongrade")
    subsection_query = (
        Query.from_(subsection_grade)
        .select(
            "course_id",
            "user_id",
            "usage_key",
            "earned_all",
            "possible_all",
            "earned_graded",
            "possible_graded",
            "first_attempted",
            "created",
            "modified"
        )
        .where(subsection_grade.course_id.isin(edx_course_ids))
        .orderby("user_id", "first_attempted", order=Order.asc)
    )
    query_fields, subsection_data = context.resources.sqldb.run_query(subsection_query)
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    subsection_path = context.resources.results_dir.path.joinpath("subsectiongrade_query.sql")
    write_csv(query_fields, subsection_data, subsection_path)
    yield AssetMaterialization(
        asset_key="subsection_query",
        description="Subsection grade records from Open edX installation",
    )
    yield ExpectationResult(
        success=bool(subsection_data),
        label="subsection_grades_count_non_zero",
        description="Ensure that the number of subsection grades is not zero.",
    )
    yield Output(subsection_path, "edx_subsection_grades")

@op(
    name="open_edx_generated_certificates",
    description="Export of generated certificates for courses on the specified Open edX installation.",  # noqa: E501
    required_resource_keys={"sqldb", "results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
    out={
        "edx_generated_certificates": Out(
            dagster_type=DagsterPath,
            description="Path to generated certificates data in tabular format rendered as CSV files",  # noqa: E501
        )
    },
)
def generated_certificates(context: OpExecutionContext, edx_course_ids: List[String]) -> DagsterPath:  # type: ignore  # noqa: E501
    """Retrieve information about generated certificates for given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :yield: A path definition that points to the rendered data table
    """
    certificates = Table("certificates_generatedcertificate")
    certificates_query = (
        Query.from_(certificates)
        .select("*")
        .where(certificates.course_id.isin(edx_course_ids))
    )
    query_fields, certificates_data = context.resources.sqldb.run_query(certificates_query)
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    certificates_path = context.resources.results_dir.path.joinpath("generatedcertificate_query.sql")
    write_csv(query_fields, certificates_data, certificates_path)
    yield AssetMaterialization(
        asset_key="certificates_query",
        description="Generated certificate records from Open edX installation",
    )
    yield ExpectationResult(
        success=bool(certificates_data),
        label="generated_certificates_count_non_zero",
        description="Ensure that the number of generated certificates is not zero.",
    )
    yield Output(certificates_path, "edx_generated_certificates")

@op(
    name="open_edx_user_profiles",
    required_resource_keys={"sqldb", "results_dir"},
    ins={
        "edx_course_ids": In(
            dagster_type=List[String],
            description="List of course IDs active on Open edX installation",
        )
    },
    out={
        "edx_user_profiles": Out(
            dagster_type=DagsterPath,
            description="Path to user profile data in tabular format rendered as CSV files",
        )
    },
)
def user_profiles(context: OpExecutionContext, edx_course_ids: List[String]) -> DagsterPath:  # type: ignore  # noqa: E501
    """Generate a table showing user profiles for given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_course_ids: List of course IDs to retrieve student enrollments for
    :type edx_course_ids: List[String]

    :yield: A path definition that points to the rendered data table
    """
    user_profile, course_enrollment = Tables("auth_userprofile", "student_courseenrollment")
    user_profiles_query = (
        Query.from_(user_profile)
        .join(course_enrollment)
        .on(user_profile.user_id == course_enrollment.user_id)
        .select(
            user_profile.id,
            user_profile.user_id,
            user_profile.name,
            user_profile.language,
            user_profile.location,
            user_profile.meta,
            user_profile.courseware,
            user_profile.gender,
            user_profile.mailing_address,
            user_profile.year_of_birth,
            user_profile.level_of_education,
            user_profile.goals,
            user_profile.country,
            user_profile.city,
            user_profile.bio,
            user_profile.profile_image_uploaded_at,
            user_profile.state
        )
        .where(course_enrollment.course_id.isin(edx_course_ids))
    )
    query_fields, user_profiles_data = context.resources.sqldb.run_query(user_profiles_query)
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    user_profiles_path = context.resources.results_dir.path.joinpath("userprofile_query.sql")
    write_csv(query_fields, user_profiles_data, user_profiles_path)
    yield AssetMaterialization(
        asset_key="user_profiles_query",
        description="Profile data of users enrolled in available courses on Open edX installation",  # noqa: E501
    )
    yield ExpectationResult(
        success=bool(user_profiles_data),
        label="enrolled_users_count_non_zero",
        description="Ensure that the number of user profiles is not zero.",
    )
    yield Output(user_profiles_path, "edx_user_profiles")

@op(
    name="export_edx_forum_database",
    description="Solid to build the command line string for executing mongodump against the Open edX forum database",  # noqa: E501
    required_resource_keys={"results_dir"},
    config_schema={
        "mongodump_path": Field(
            String,
            is_required=False,
            description="Local path for mongodump for dev",
            default_value="/usr/bin/mongodump",
        ),
        "edx_mongodb_uri": Field(
            String,
            is_required=True,
            description="The URI for connecting to a MongoDB replicat set",
        ),
        "edx_mongodb_username": Field(
            String,
            is_required=False,
            default_value="",
            description="Username for account with permissions to read forum database",
        ),
        "edx_mongodb_password": Field(
            String,
            is_required=False,
            default_value="",
            description="Password for account with permissions to read forum database",
        ),
        "edx_mongodb_auth_db": Field(
            String,
            is_required=False,
            default_value="admin",
            description="The MongoDB database that contains the account information for the authenticating user.",  # noqa: E501
        ),
        "edx_mongodb_forum_database_name": Field(
            String,
            is_required=True,
            description="Name of database that contains forum data for Open edX installation",  # noqa: E501
        ),
    },
    out={
        "edx_forum_data_directory": Out(
            dagster_type=DagsterPath,
            description="Path to exported forum data generated by mongodump command",
        )
    },
)
def export_edx_forum_database(  # type: ignore
    context: OpExecutionContext,
) -> DagsterPath:
    """Export the edX forum database using mongodump.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :raises Failure: Raise a failure event if the mongo dump returns a non-zero exit
        code

    :yield: Path object to the directory where the exported Mongo database is
        located
    """
    forum_data_path = context.resources.results_dir.path.joinpath(
        context.op_config["edx_mongodb_forum_database_name"]
    )
    mongo_uri = context.op_config["edx_mongodb_uri"]
    command_array = [
        context.op_config["mongodump_path"],
        "--uri",
        f"'{mongo_uri}'",
        "--db",
        context.op_config["edx_mongodb_forum_database_name"],
        "--authenticationDatabase",
        context.op_config["edx_mongodb_auth_db"],
        "--out",
        context.resources.results_dir.absolute_path,
    ]
    if password := context.op_config["edx_mongodb_password"]:
        command_array.extend(["--password", password])
    if username := context.op_config["edx_mongodb_username"]:
        command_array.extend(["--username", username])

    mongodump_output, mongodump_retcode = run_bash(
        " ".join(command_array),
        output_logging="BUFFER",
        log=context.log,
        cwd=str(context.resources.results_dir.root_dir),
    )

    if mongodump_retcode != 0:
        raise Failure(
            description="The mongodump command for exporting the Open edX forum database failed.",  # noqa: E501
        )

    yield AssetMaterialization(
        asset_key="edx_forum_database",
        description="Exported Mongo database of forum data from Open edX installation",
    )

    yield Output(forum_data_path, "edx_forum_data_directory")


@op(
    name="edx_export_courses",
    description="Export the contents of all active courses to S3",
    required_resource_keys={"s3"},
    config_schema={
        "edx_base_url": Field(
            String,
            is_required=True,
            description="Domain of edX installation",
        ),
        "edx_client_id": Field(
            String, is_required=True, description="OAUTH2 Client ID for Open edX API"
        ),
        "edx_client_secret": Field(
            String,
            is_required=True,
            description="OAUTH2 Client secret for Open edX API",
        ),
        "edx_studio_base_url": Field(
            String,
            is_required=True,
            description="Domain of edX studio installation",
        ),
        "edx_token_type": Field(
            String,
            default_value="jwt",
            is_required=False,
            description="Type of OAuth token to use for authenticating to the edX API. "
            'Default to "jwt" for edX Juniper and newer, or "bearer" for older releases.',  # noqa: E501
        ),
        "edx_course_bucket": Field(
            String,
            is_required=True,
            description="Bucket name that the edX installation uses for uploading "
            "course exports",
        ),
    },
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
    context: OpExecutionContext, edx_course_ids: List[str], daily_extracts_dir: str
) -> None:
    access_token = get_access_token(
        client_id=context.op_config["edx_client_id"],
        client_secret=context.op_config["edx_client_secret"],
        edx_url=context.op_config["edx_base_url"],
        token_type=context.op_config["edx_token_type"],
    )
    exported_courses = export_courses(
        context.op_config["edx_studio_base_url"],
        access_token=access_token,
        course_ids=edx_course_ids,
    )
    successful_exports: set[str] = set()
    failed_exports: set[str] = set()
    tasks = exported_courses["upload_task_ids"]
    context.log.info("Exporting %s tasks from Open edX", len(tasks))
    # Possible status values found here:
    # https://github.com/openedx/django-user-tasks/blob/master/user_tasks/models.py
    while len(successful_exports.union(failed_exports)) < len(tasks):
        time.sleep(timedelta(seconds=5).seconds)
        for course_id, task_id in tasks.items():
            task_status = check_course_export_status(
                context.op_config["edx_studio_base_url"],
                access_token,
                course_id,
                task_id,
            )
            if task_status["state"] == "Succeeded":
                successful_exports.add(course_id)
            if task_status["state"] in {"Failed", "Canceled", "Retrying"}:
                failed_exports.add(course_id)
    for course_id in successful_exports:
        context.log.info("Moving course %s to %s", course_id, daily_extracts_dir)
        course_file = f"{course_id}.tar.gz"
        source_object = {
            "Bucket": context.op_config["edx_course_bucket"],
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
            dagster_type=DagsterPath,
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
            dagster_type=DagsterPath,
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
    config_schema={
        "edx_etl_results_bucket": Field(
            String,
            default_value="odl-developer-testing-sandbox",
            is_required=False,
            description="S3 bucket to use for uploading results of pipeline execution.",
        ),
    },
    ins={
        "edx_course_ids_csv": In(dagster_type=DagsterPath),
        "edx_course_roles": In(dagster_type=DagsterPath),
        "edx_enrolled_users": In(dagster_type=DagsterPath),
        "edx_student_submissions": In(dagster_type=DagsterPath),
        "edx_enrollment_records": In(dagster_type=DagsterPath),
        "edx_forum_data_directory": In(dagster_type=DagsterPath),
    },
    out={"edx_daily_extracts_directory": Out(dagster_type=String)},
)
def upload_extracted_data(  # noqa: PLR0913
    context: OpExecutionContext,
    edx_course_ids_csv: DagsterPath,
    edx_course_roles: DagsterPath,
    edx_enrolled_users: DagsterPath,
    edx_student_submissions: DagsterPath,
    edx_enrollment_records: DagsterPath,
    edx_forum_data_directory: DagsterPath,
):
    """Upload all data exports to S3 so that institutional research can ingest.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :param edx_course_ids_csv: Flat file containing a list of course IDs active on the
        Open edX instance.
    :type edx_course_ids_csv: DagsterPath

    :param edx_course_roles: Flat file containing tabular representation of course roles
        in Open edX installation
    :type edx_course_roles: DagsterPath

    :param edx_enrolled_users: Flat file containing tabular representation of users who
        are enrolled in courses in Open edX installation
    :type edx_enrolled_users: DagsterPath

    :param edx_student_submissions: Flat file containing tabular representation of
        student submissions in Open edX installation
    :type edx_student_submissions: DagsterPath

    :param edx_enrollment_records: Flat file containing tabular representation of
        enrollment data in Open edX installation
    :type edx_enrollment_records: DagsterPath

    :param edx_forum_data_directory: Directory containing exported MongoDB database of
        Open edX forum activity
    :type edx_forum_data_directory: DagsterPath

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
    )
    context.resources.results_dir.clean_dir()
    yield Output(
        f"{results_bucket}/{context.resources.results_dir.path.name}",
        "edx_daily_extracts_directory",
    )
