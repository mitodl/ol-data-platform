# -*- coding: utf-8 -*-

from pathlib import Path as FSPath

from dagster import (
    EventMetadataEntry,
    Field,
    InputDefinition,
    List,
    Materialization,
    ModeDefinition,
    Nothing,
    Output,
    OutputDefinition,
    Path,
    SolidExecutionContext,
    String,
    pipeline,
    solid
)
from pypika import MySQLQuery as Query
from pypika import Table, Tables

from api_client import get_access_token, get_edx_course_ids
from libs.file_rendering import write_csv
from resources.mysql_db import mysql_db_resource


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
def course_staff(context: SolidExecutionContext, course_id: String) -> Path:
    """
    Retrieve a list of the course staff for a given course.

    :param context: Dagster context object
    :type context: SolidExecutionContext

    :param course_id: edX course ID string

    :type course_id: String

    :returns: Path to table of course staff information grouped by course ID rendered as a flat file.

    :rtype: Path
    """
    pass


@solid(
    required_resource_keys={'sqldb'},
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
            dagster_type=Path,
            description='Path to user data in tabular format rendered as CSV files'
        )
    ]
)
def enrolled_users(context: SolidExecutionContext, edx_course_ids: List[String]) -> Path:
    """Generate a table showing which students are currently enrolled in which courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: SolidExecutionContext

    :param edx_course_ids: List of course IDs to retrieve student enrollments for
    :type edx_course_ids: List[String]

    :returns: A path definition that points to the rendered data table

    :rtype: Path
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
    enrollments_path = 'users_query.csv'
    write_csv(query_fields, users_data, FSPath(enrollments_path))
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
                enrollments_path, 'enrollment_query_csv_path'
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
    required_resource_keys={'sqldb'},
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
            dagster_type=Path,
            description='Path to submissions data in tabular format rendered as CSV files'
        )
    ]
)
def student_submissions(context: SolidExecutionContext, edx_course_ids: List[String]) -> Path:
    """Retrieve details of student submissions for the given courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: SolidExecutionContext

    :param edx_course_ids: List of edX course ID strings
    :type edx_course_ids: List[String]

    :returns: A path definition that points to the rendered data table

    :rtype: Path
    """
    studentmodule = Table('courseware_studentmodule')
    submissions_count = 0
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    submissions_path = 'studentmodule_query.csv'
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
            studentmodule.course_id.isin(edx_course_ids)
        )
        query_fields, submission_data = context.resources.sqldb.run_query(
            submission_query)
        submissions_count += len(submission_data)
        write_csv(query_fields, submission_data, FSPath(submissions_path))
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
                submissions_path, 'student_submissions_path'
            )
        ]
    )
    yield Output(
        submissions_path,
        'edx_student_submissions_csv'
    )


@solid(
    name='open_edx_enrollments',
    description='Export of enrollment records for courses on the specified Open edX installation.',
    required_resource_keys={'sqldb'},
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
            dagster_type=Path,
            description='Path to enrollment data in tabular format rendered as CSV files'
        )
    ]
)
def course_enrollments(context: SolidExecutionContext, edx_course_ids: List[String]) -> Path:
    '''
    select id, user_id, course_id, created, is_active, mode from student_courseenrollment where course_id= :course_id
    '''
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
    enrollments_path = 'enrollment_query.csv'
    write_csv(query_fields, enrollment_data, FSPath(enrollments_path))
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
                enrollments_path, 'enrollment_query_csv_path'
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
    required_resource_keys={'sqldb'},
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
            dagster_type=Path,
            description='Path to course role data in tabular format rendered as CSV files'
        )
    ]
)
def course_roles(context: SolidExecutionContext, edx_course_ids: List[String]) -> Path:
    '''
    select id,user_id,org,course_id,role from student_courseaccessrole where course_id= :course_id
    '''
    access_role = Table('student_courseaccessrole')
    roles_query = Query.from_(
        access_role
    ).select(
        'id',
        'user_id',
        'org',
        'course_id',
        'role',
    ).where(
        access_role.course_id.isin(edx_course_ids)
    )
    query_fields, roles_data = context.resources.sqldb.run_query(
        roles_query)
    # Maintaining previous file name for compatibility (TMM 2020-05-01)
    roles_path = 'role_query.csv'
    write_csv(query_fields, roles_data, FSPath(roles_path))
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
                roles_path, 'role_query_csv_path'
            )
        ]
    )
    yield Output(
        roles_path,
        'edx_course_roles'
    )


@solid
def export_edx_forum_data(context: SolidExecutionContext) -> Path:
    pass


@solid
def export_course(context: SolidExecutionContext, course_id: String) -> Nothing:
    pass


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={'sqldb': mysql_db_resource}
        )
    ]
)
def edx_course_pipeline():
    course_list = list_courses()
    enrolled_users(edx_course_ids=course_list)
    student_submissions(edx_course_ids=course_list)
    course_roles(edx_course_ids=course_list)
    course_enrollments(edx_course_ids=course_list)
