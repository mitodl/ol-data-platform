from dagster import graph
from ol_orchestrate.lib.hooks import (
    notify_healthchecks_io_on_failure,
    notify_healthchecks_io_on_success,
)

from legacy_openedx.ops.open_edx import (
    collect_edx_course_exports,
    course_enrollments,
    course_roles,
    enrolled_users,
    export_edx_forum_database,
    export_single_edx_course,
    fan_out_edx_course_exports,
    list_courses,
    student_submissions,
    upload_extracted_data,
    user_roles,
    write_course_list_csv,
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
    dynamic_courses = fan_out_edx_course_exports(course_list)
    results = dynamic_courses.map(export_single_edx_course)
    collect_edx_course_exports.with_hooks(
        {
            notify_healthchecks_io_on_success,
            notify_healthchecks_io_on_failure,
        }
    )(
        exported_course_ids=results.collect(),
        daily_extracts_dir=extracts_upload,
    )
