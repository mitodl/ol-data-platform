# -*- coding: utf-8 -*-

from dagster import (
    EventMetadataEntry,
    Failure,
    Field,
    InputDefinition,
    Int,
    List,
    Materialization,
    ModeDefinition,
    Nothing,
    Output,
    OutputDefinition,
    SolidExecutionContext,
    String,
    composite_solid,
    lambda_solid,
    pipeline,
    solid
)
from dagster_aws.s3 import s3_resource
from dagster_bash.utils import execute as run_bash
from pypika import MySQLQuery as Query
from pypika import Table, Tables

from ol_data_pipelines.edx.api_client import (
    get_access_token,
    get_edx_course_ids
)
from ol_data_pipelines.libs.file_rendering import write_csv
from ol_data_pipelines.libs.types import DagsterPath
from ol_data_pipelines.resources.mysql_db import mysql_db_resource
from ol_data_pipelines.resources.outputs import daily_dir


@solid(
    name='list_edx_courses',
    description=('Retrieve the list of course IDs active in the edX instance '
                 'to be used in subsequent steps to pull data per course.'),
    config={
        'edx_client_id': Field(
            String,
            is_required=True,
            description='OAUTH2 Client ID for Open edX API'
        ),
        'edx_client_secret': Field(
            String,
            is_required=True,
            description='OAUTH2 Client secret for Open edX API'
        ),
        'edx_base_url': Field(
            String,
            default_value='lms.mitx.mit.edu',
            is_required=False,
            description='Domain of edX installation'
        )
    },
    output_defs=[
        OutputDefinition(
            dagster_type=List[String],
            name='edx_course_ids',
            description='List of course IDs active on Open edX installation'
        )
    ]
)
def list_courses(context: SolidExecutionContext) -> List[String]:
    """
    Retrieve the list of course IDs active in the edX instance to be used in subsequent steps to pull data per course.

    :param context: Dagster context object for passing configuration
    :type context: SolidExecutionContext

    :returns: List of edX course IDs

    :rtype: List[String]
    """
    access_token = get_access_token(
        context.solid_config['edx_client_id'],
        context.solid_config['edx_client_secret'],
        context.solid_config['edx_base_url'],
    )
    course_ids = []
    course_id_generator = get_edx_course_ids(
        context.solid_config['edx_base_url'],
        access_token)
    for result_set in course_id_generator:
        course_ids.extend([course['id'] for course in result_set])
    yield Output(course_ids, 'edx_course_ids')


@solid
def course_staff(context: SolidExecutionContext, course_id: String) -> String:
    """
    Retrieve a list of the course staff for a given course.

    :param context: Dagster context object
    :type context: SolidExecutionContext

    :param course_id: edX course ID string

    :type course_id: String

    :returns: Path to table of course staff information grouped by course ID rendered as a flat file.

    :rtype: String
    """
    pass


@solid(
    required_resource_keys={'sqldb', 'results_dir'},
    input_defs=[
        InputDefinition(
            name='edx_course_ids',
            dagster_type=List[String],
            description='List of course IDs active on Open edX installation'
        )
    ],
    output_defs=[
        OutputDefinition(
            name='edx_enrolled_users',
            dagster_type=DagsterPath,
            description='Path to user data in tabular format rendered as CSV files'
        )
    ]
)
def enrolled_users(context: SolidExecutionContext, edx_course_ids: List[String]) -> DagsterPath:
    """Generate a table showing which students are currently enrolled in which courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: SolidExecutionContext

    :param edx_course_ids: List of course IDs to retrieve student enrollments for
    :type edx_course_ids: List[String]

    :returns: A path definition that points to the rendered data table

    :rtype: DagsterPath
    """
    course_enrollment, users = Tables('student_courseenrollment', 'auth_user')
    users_query = Query.from_(
        users
    ).join(
        course_enrollment
    ).on(
        users.id == course_enrollment.user_id
    ).select(
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
        course_enrollment.course_id
    ).where(
        course_enrollment.course_id.isin(edx_course_ids)
    )
    query_fields, users_data = context.resources.sqldb.run_query(
        users_query)
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    enrollments_path = context.resources.results_dir.path.joinpath('users_query.csv')
    write_csv(query_fields, users_data, enrollments_path)
    yield Materialization(
        label='users_query.csv',
        description='Information of users enrolled in available courses on Open edX installation',
        metadata_entries=[
            EventMetadataEntry.text(
                label='enrolled_users_count',
                description='Number of users who are enrolled in courses',
                text=str(len(users_data))
            ),
            EventMetadataEntry.path(
                enrollments_path.name, 'enrollment_query_csv_path'
            )
        ]
    )
    yield Output(
        enrollments_path,
        'edx_enrolled_users'
    )


@solid(
    name='open_edx_student_submissions',
    description='Export of student submission events for courses on the specified Open edX installation.',
    required_resource_keys={'sqldb', 'results_dir'},
    input_defs=[
        InputDefinition(
            name='edx_course_ids',
            dagster_type=List[String],
            description='List of course IDs active on Open edX installation'
        )
    ],
    output_defs=[
        OutputDefinition(
            name='edx_student_submissions',
            dagster_type=DagsterPath,
            description='Path to submissions data in tabular format rendered as CSV files'
        )
    ]
)
def student_submissions(context: SolidExecutionContext, edx_course_ids: List[String]) -> DagsterPath:
    """Retrieve details of student submissions for the given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: SolidExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :returns: A path definition that points to the rendered data table

    :rtype: DagsterPath
    """
    studentmodule = Table('courseware_studentmodule')
    submissions_count = 0
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    submissions_path = context.resources.results_dir.path.joinpath('studentmodule_query.csv')
    for course_id in edx_course_ids:
        submission_query = Query.from_(
            studentmodule
        ).select(
            'id',
            'module_type',
            'module_id',
            'student_id',
            'state',
            'grade',
            'created',
            'modified',
            'max_grade',
            'done',
            'course_id'
        ).where(
            studentmodule.course_id == course_id
        )
        query_fields, submission_data = context.resources.sqldb.run_query(
            submission_query)
        submissions_count += len(submission_data)
        write_csv(query_fields, submission_data, submissions_path)
    yield Materialization(
        label='enrolled_students.csv',
        description='Students enrolled in edX courses',
        metadata_entries=[
            EventMetadataEntry.text(
                label='student_submission_count',
                description='Number of student submission records',
                text=str(submissions_count)
            ),
            EventMetadataEntry.path(
                submissions_path.name, 'student_submissions_path'
            )
        ]
    )
    yield Output(
        submissions_path,
        'edx_student_submissions'
    )


@solid(
    name='open_edx_enrollments',
    description='Export of enrollment records for courses on the specified Open edX installation.',
    required_resource_keys={'sqldb', 'results_dir'},
    input_defs=[
        InputDefinition(
            name='edx_course_ids',
            dagster_type=List[String],
            description='List of course IDs active on Open edX installation'
        )
    ],
    output_defs=[
        OutputDefinition(
            name='edx_enrollment_records',
            dagster_type=DagsterPath,
            description='Path to enrollment data in tabular format rendered as CSV files'
        )
    ]
)
def course_enrollments(context: SolidExecutionContext, edx_course_ids: List[String]) -> DagsterPath:
    """Retrieve enrollment records for given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: SolidExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :returns: A path definition that points to the rendered data table

    :rtype: DagsterPath
    """
    enrollment = Table('student_courseenrollment')
    enrollments_query = Query.from_(
        enrollment
    ).select(
        'id',
        'user_id',
        'course_id',
        'created',
        'is_active',
        'mode'
    ).where(
        enrollment.course_id.isin(edx_course_ids)
    )
    query_fields, enrollment_data = context.resources.sqldb.run_query(
        enrollments_query)
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    enrollments_path = context.resources.results_dir.path.joinpath('enrollment_query.csv')
    write_csv(query_fields, enrollment_data, enrollments_path)
    yield Materialization(
        label='enrollment_query.csv',
        description='Course enrollment records from Open edX installation',
        metadata_entries=[
            EventMetadataEntry.text(
                label='course_enrollment_count',
                description='Number of enrollment records',
                text=str(len(enrollment_data))
            ),
            EventMetadataEntry.path(
                enrollments_path.name, 'enrollment_query_csv_path'
            )
        ]
    )
    yield Output(
        enrollments_path,
        'edx_enrollment_records'
    )


@solid(
    name='open_edx_course_roles',
    description='Export of user roles for courses on the specified Open edX installation.',
    required_resource_keys={'sqldb', 'results_dir'},
    input_defs=[
        InputDefinition(
            name='edx_course_ids',
            dagster_type=List[String],
            description='List of course IDs active on Open edX installation'
        )
    ],
    output_defs=[
        OutputDefinition(
            name='edx_course_roles',
            dagster_type=DagsterPath,
            description='Path to course role data in tabular format rendered as CSV files'
        )
    ]
)
def course_roles(context: SolidExecutionContext, edx_course_ids: List[String]) -> DagsterPath:
    """Retrieve information about user roles for given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: SolidExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :returns: A path definition that points to the rendered data table

    :rtype: DagsterPath
    """
    access_role = Table('student_courseaccessrole')
    roles_query = Query.from_(
        access_role
    ).select(
        'id',
        'user_id',
        'org',
        'course_id',
        'role'
    ).where(
        access_role.course_id.isin(edx_course_ids)
    )
    query_fields, roles_data = context.resources.sqldb.run_query(
        roles_query)
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    roles_path = context.resources.results_dir.path.joinpath('role_query.csv')
    write_csv(query_fields, roles_data, roles_path)
    yield Materialization(
        label='role_query.csv',
        description='Course roles records from Open edX installation',
        metadata_entries=[
            EventMetadataEntry.text(
                label='course_roles_count',
                description='Number of course roles records',
                text=str(len(roles_data))
            ),
            EventMetadataEntry.path(
                roles_path.name, 'role_query_csv_path'
            )
        ]
    )
    yield Output(
        roles_path,
        'edx_course_roles'
    )


@solid(
    name='export_edx_forum_database',
    description='Solid to build the command line string for executing mongodump against the Open edX forum database',
    required_resource_keys={'results_dir'},
    config={
        'edx_mongodb_host': Field(
            String,
            is_required=True,
            description='Resolvable host address of MongoDB master'
        ),
        'edx_mongodb_port': Field(
            Int,
            is_required=False,
            default_value=27017,  # noqa WPS4232
            description='TCP port number used to connect to MongoDB server'
        ),
        'edx_mongodb_username': Field(
            String,
            is_required=False,
            default_value='',
            description='Username for account with permissions to read forum database'
        ),
        'edx_mongodb_password': Field(
            String,
            is_required=False,
            default_value='',
            description='Password for account with permissions to read forum database'
        ),
        'edx_mongodb_forum_database_name': Field(
            String,
            is_required=True,
            description='Name of database that contains forum data for Open edX installation'
        )
    },
    output_defs=[
        OutputDefinition(
            name='edx_forum_data_directory',
            dagster_type=DagsterPath,
            description='Path to exported forum data generated by mongodump command'
        )
    ]
)
def export_edx_forum_database(context: SolidExecutionContext) -> DagsterPath:
    """Export the edX forum database using mongodump.

    :param context: Dagster execution context for propagaint configuration data
    :type context: SolidExecutionContext

    :returns: Path object to the directory where the exported Mongo database is located

    :rtype: DagsterPath
    """
    forum_data_path = context.resources.results_dir.path.joinpath(
        context.solid_config['edx_mongodb_forum_database_name'])
    command_array = ['/usr/bin/mongodump',
                     '--host',
                     context.solid_config['edx_mongodb_host'],
                     '--port',
                     str(context.solid_config['edx_mongodb_port']),
                     '--db',
                     context.solid_config['edx_mongodb_forum_database_name'],
                     '--authenticationDatabase',
                     'admin',
                     '--out',
                     context.resources.results_dir.absolute_path]
    if password := context.solid_config['edx_mongodb_password']:
        command_array.extend(['--password', password])
    if username := context.solid_config['edx_mongodb_username']:
        command_array.extend(['--username', username])

    mongodump_output, mongodump_retcode = run_bash(
        ' '.join(command_array),
        output_logging='BUFFER',
        log=context.log,
        cwd=str(context.resources.results_dir.root_dir))

    if mongodump_retcode != 0:
        yield Failure(
            description='The mongodump command for exporting the Open edX forum database failed.',
            metadata_entries=[
                EventMetadataEntry.text(
                    text=mongodump_output,
                    label='mongodump_output',
                    description='Output of the mongodump command'
                )
            ]
        )

    yield Materialization(
        label='edx_forum_database',
        description='Exported Mongo database of forum data from Open edX installation',
        metadata_entries=[
            EventMetadataEntry.path(
                str(forum_data_path), 'edx_forum_database_export_path'
            )
        ]
    )

    yield Output(forum_data_path, 'edx_forum_data_directory')


@solid
def export_course(context: SolidExecutionContext, course_id: String) -> Nothing:
    pass


@solid(
    name='edx_upload_daily_extracts',
    description='Upload all data from daily extracts to S3 for institutional research.',
    required_resource_keys={'sqldb', 'results_dir', 's3'},
    config={
        'edx_etl_results_bucket': Field(
            String,
            default_value='odl-developer-testing-sandbox',
            is_required=False,
            description='S3 bucket to use for uploading results of pipeline execution.'
        )
    },
    input_defs=[
        InputDefinition(name='edx_course_roles', dagster_type=DagsterPath),
        InputDefinition(name='edx_enrolled_users', dagster_type=DagsterPath),
        InputDefinition(name='edx_student_submissions', dagster_type=DagsterPath),
        InputDefinition(name='edx_enrollment_records', dagster_type=DagsterPath),
        InputDefinition(name='edx_forum_data_directory', dagster_type=DagsterPath),
    ]
)
def upload_extracted_data( # noqa WPS211
        context: SolidExecutionContext,
        edx_course_roles: DagsterPath,
        edx_enrolled_users: DagsterPath,
        edx_student_submissions: DagsterPath,
        edx_enrollment_records: DagsterPath,
        edx_forum_data_directory: DagsterPath):
    """Upload all data exports to S3 so that institutional research can ingest into their system.

    :param context: Dagster execution context for propagaint configuration data
    :type context: SolidExecutionContext

    :param edx_course_roles: Flat file containing tabular representation of course roles in Open edX installation
    :type edx_course_roles: DagsterPath

    :param edx_enrolled_users: Flat file containing tabular representation of users who are enrolled in courses in Open
        edX installation
    :type edx_enrolled_users: DagsterPath

    :param edx_student_submissions: Flat file containing tabular representation of student submissions in Open edX
        installation
    :type edx_student_submissions: DagsterPath

    :param edx_enrollment_records: Flat file containing tabular representation of enrollment data in Open edX
        installation
    :type edx_enrollment_records: DagsterPath

    :param edx_forum_data_directory: Directory containing exported MongoDB database of Open edX forum activity
    :type edx_forum_data_directory: DagsterPath
    """
    for path_object in context.resources.results_dir.path.iterdir():
        if path_object.is_file:
            context.resources.s3.upload_file(
                Filename=str(path_object),
                Bucket=context.solid_config['edx_etl_results_bucket'],
                Key=str(path_object.relative_to(context.resources.results_dir.root_dir)))
        elif path_object.is_dir:
            for fpath in path_object.iterdir():
                context.resources.s3.upload_file(
                    Filename=str(path_object),
                    Bucket=context.solid_config['edx_etl_results_bucket'],
                    Key=str(fpath.relative_to(context.resources.results_dir.root_dir)))


@pipeline(
    description=('Extract data and course structure from Open edX for use by institutional research. '
                 'This is ultimately inserted into BigQuery and combined with information from the edX '
                 'tracking logs which get delivered to S3 on an hourly basis via FluentD'),
    mode_defs=[
        ModeDefinition(
            resource_defs={
                'sqldb': mysql_db_resource,
                's3': s3_resource,
                'results_dir': daily_dir
            }
        )
    ]
)
def edx_course_pipeline():
    course_list = list_courses()
    upload_extracted_data(
        enrolled_users(edx_course_ids=course_list),
        student_submissions(edx_course_ids=course_list),
        course_roles(edx_course_ids=course_list),
        course_enrollments(edx_course_ids=course_list),
        export_edx_forum_database())
