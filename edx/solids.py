# -*- coding: utf-8 -*-

from itertools import chain
from pathlib import Path as FSPath

from dagster import (
    Dict,
    EventMetadataEntry,
    ExpectationResult,
    Field,
    List,
    Materialization,
    ModeDefinition,
    Nothing,
    Path,
    SolidExecutionContext,
    String,
    pipeline,
    solid
)
from pypika import Field as DBField
from pypika import MySQLQuery as Query
from pypika import Table, Tables

from api_client import get_access_token, get_edx_course_ids
from libs.file_rendering import write_csv
from resources.mysql_db import mysql_db_resource


@solid(
    name='list_edx_courses',
    description='Retrieve the list of course IDs active in the edX instance to be used in subsequent steps to pull data per course.',
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
    }
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
    return course_ids


@solid
def export_course(context: SolidExecutionContext, course_id: String) -> Nothing:
    pass


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


@solid(required_resource_keys={'sqldb'})
def enrolled_students(context: SolidExecutionContext, course_ids: List[String]):
    """Generate a table showing which students are currently enrolled in which courses.

    :param context: Dagster execution context for propagaint configuration data
    :type context: SolidExecutionContext

    :param course_id: Course ID to retrieve student enrollments for
    :type course_id: String

    :returns: A path definition that points to the rendered data table

    :rtype: Path
    """
    for course_id in course_ids:
        course_enrollment, users = Tables('student_courseenrollment', 'auth_user')
        enrollment_query = Query.from_(
            users
        ).join(
            course_enrollment
        ).on(
            users.id == course_enrollment.user_id
        ).select(
            users.id, users.username, users.first_name, users.last_name,
            users.email, users.is_staff, users.is_active, users.is_superuser,
            users.last_login, users.date_joined
        ).where(
            course_enrollment.course_id == course_id
        )
        enrollment_data = context.resources.sqldb.run_query(enrollment_query)
        enrollments_path = f'enrolled_students_{course_id}_{context.run_id}.csv'
        write_csv(enrollment_data, FSPath(enrollments_path))
        yield Materialization(
            label=f'enrolled_students_{course_id}.csv',
            description=f'Students enrolled in {course_id}',
            metadata_entries=[
                EventMetadataEntry.path(
                    enrollments_path, f'enrolled_students_{course_id}_path'
                )
            ]
        )



@solid
def student_submissions(context: SolidExecutionContext, course_id: String) -> Path:
    """Retrieve details of student submissions for the given course

    :param context: Dagster execution context for propagaint configuration data
    :type context: SolidExecutionContext

    :param course_id: edX course ID string
    :type course_id: String

    :returns: A path definition that points to the rendered data table

    :rtype: Path
    """
    studentmodule = Table('courseware_studentmodule')
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


@solid()
def enrollments(context: SolidExecutionContext, course_id: String) -> Path:
    '''
    select id, user_id, course_id, created, is_active, mode from student_courseenrollment where course_id= :course_id
    '''
    enrollment = Table('student_courseenrollment')
    enrollments = Query.from_(
        enrollment
    ).select(
        'id',
        'user_id',
        'course_id',
        'created',
        'is_active',
        'mode'
    ).where(
        enrollment.course_id == course_id
    )


@solid()
def course_roles(context: SolidExecutionContext, course_id: String) -> Path:
    '''
    select id,user_id,org,course_id,role from student_courseaccessrole where course_id= :course_id
    '''
    access_role = Table('student_courseaccessrole')
    roles = Query.from_(
        access_role
    ).select(
        'id',
        'user_id',
        'org',
        'course_id',
        'role',
    ).where(
        access_role.course_id == course_id
    )


@solid
def export_edx_forum_data(context: SolidExecutionContext) -> Path:
    pass


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={'sqldb': mysql_db_resource}
        )
    ]
)
def edx_course_pipeline():
    enrolled_students(list_courses())
