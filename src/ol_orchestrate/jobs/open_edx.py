from dagster import graph

from ol_orchestrate.lib.hooks import (
    notify_healthchecks_io_on_failure,
    notify_healthchecks_io_on_success,
)
from ol_orchestrate.ops.object_storage import upload_files_to_s3
from ol_orchestrate.ops.open_edx import (
    course_enrollments,
    course_roles,
    enrolled_users,
    export_edx_courses,
    export_edx_forum_database,
    fetch_edx_course_structure_from_api,
    list_courses,
    student_submissions,
    upload_extracted_data,
    user_roles,
    write_course_list_csv,
)
from ol_orchestrate.assets.open_edx import (
    course_list,
    course_structure,
    course_list_asset_key,
)


@graph(
    description=(
        "Extract data and course structure from Open edX for use by institutional "
        "research. This is ultimately inserted into BigQuery and combined with "
        "information from the edX tracking logs which get delivered to S3 on an hourly "
        "basis via a log shipping agent"
    ),
    tags={
        "source": "edx",
        "destination": "s3",
        "owner": "platform-engineering",
        "consumer": "institutional-research",
    },
)
def edx_course_pipeline():
    course_list = list_courses()
    extracts_upload = upload_extracted_data(
        uploads=[
            write_course_list_csv(edx_course_ids=course_list),
            enrolled_users(edx_course_ids=course_list),
            student_submissions(edx_course_ids=course_list),
            course_roles(edx_course_ids=course_list),
            user_roles(edx_course_ids=course_list),
            course_enrollments(edx_course_ids=course_list),
            export_edx_forum_database(),
        ]
    )
    export_edx_courses.with_hooks(
        {
            notify_healthchecks_io_on_success,
            notify_healthchecks_io_on_failure,
        }
    )(course_list, extracts_upload)


@graph(
    name="mitol_openedx_data_extracts",
    description=(
        "Extract data from Open edX installations for consumption by the "
        "Open Learning data platform."
    ),
    tags={
        "source": "Open edX",
        "destination": "s3",
        "owner": "platform-engineering",
        "consumer": "ol-data-platform",
    },
)
def extract_open_edx_data_to_ol_data_platform():
    course_structure(course_list())
