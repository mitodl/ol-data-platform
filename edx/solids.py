# -*- coding: utf-8 -*-

from itertools import chain

from dagster import (
    ExpectationResult,
    Field,
    List,
    Nothing,
    Path,
    SolidExecutionContext,
    String,
    pipeline,
    solid
)

from api_client import get_access_token, get_edx_course_ids


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
        course_ids.append([course['id'] for course in result_set])
    return course_ids


@solid
def export_course(context: SolidExecutionContext, course_id: String) -> Nothing:
    pass


@solid
def course_staff(context: SolidExecutionContext, course_ids: List[String]) -> Path:
    """
    Retrieve a list of the course staff for a given course.

    :param context: Dagster context object
    :type context: SolidExecutionContext

    :param course_ids: edX course ID string

    :type course_ids: List[String]

    :returns: Path to table of course staff information grouped by course ID rendered as a flat file.

    :rtype: Path
    """
    


@solid
def enrolled_students(context: SolidExecutionContext, course_id: str) -> Path:
    '''
    select auth_user.id, auth_user.username, auth_user.first_name, auth_user.last_name, auth_user.email,
    auth_user.is_staff, auth_user.is_active, auth_user.is_superuser, auth_user.last_login, auth_user.date_joined from
    auth_user inner join student_courseenrollment on student_courseenrollment.user_id = auth_user.id and
    student_courseenrollment.course_id = :course_id
    '''
    pass


@solid
def student_submissions(context: SolidExecutionContext, course_id: str) -> Path:
    '''
    select id, module_type, module_id, student_id, state, grade, created, modified, max_grade, done, course_id from
    courseware_studentmodule where course_id= :course_id
    '''
    pass


@solid
def enrollments(context: SolidExecutionContext, course_id: str) -> Path:
    '''
    select id, user_id, course_id, created, is_active, mode from student_courseenrollment where course_id= :course_id
    '''
    pass


@solid
def course_roles(context: SolidExecutionContext, course_id: str) -> Path:
    '''
    select id,user_id,org,course_id,role from student_courseaccessrole where course_id= :course_id
    '''
    pass


@pipeline
def edx_course_pipeline():
    list_courses()
