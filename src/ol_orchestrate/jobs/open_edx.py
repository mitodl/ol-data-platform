from dagster import fs_io_manager, graph
from dagster_aws.s3.io_manager import s3_pickle_io_manager
from dagster_aws.s3.resources import s3_resource

from ol_orchestrate.lib.hooks import (
    notify_healthchecks_io_on_failure,
    notify_healthchecks_io_on_success,
)
from ol_orchestrate.lib.yaml_config_helper import load_yaml_config
from ol_orchestrate.ops.open_edx import (
    course_enrollments,
    course_roles,
    user_roles,
    enrolled_users,
    export_edx_courses,
    export_edx_forum_database,
    list_courses,
    student_submissions,
    upload_extracted_data,
    write_course_list_csv,
)
from ol_orchestrate.resources.healthchecks import (
    healthchecks_dummy_resource,
    healthchecks_io_resource,
)
from ol_orchestrate.resources.mysql_db import mysql_db_resource
from ol_orchestrate.resources.outputs import daily_dir
from ol_orchestrate.resources.sqlite_db import sqlite_db_resource


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
        write_course_list_csv(edx_course_ids=course_list),
        enrolled_users(edx_course_ids=course_list),
        student_submissions(edx_course_ids=course_list),
        course_roles(edx_course_ids=course_list),
        user_roles(edx_course_ids=course_list),
        course_enrollments(edx_course_ids=course_list),
        export_edx_forum_database(),
    )
    export_edx_courses.with_hooks(
        {
            notify_healthchecks_io_on_success,
            notify_healthchecks_io_on_failure,
        }
    )(course_list, extracts_upload)


dev_resources = {
    "sqldb": sqlite_db_resource,
    "s3": s3_resource,
    "results_dir": daily_dir,
    "healthchecks": healthchecks_dummy_resource,
    "io_manager": fs_io_manager,
}

production_resources = {
    "sqldb": mysql_db_resource,
    "s3": s3_resource,
    "results_dir": daily_dir,
    "healthchecks": healthchecks_io_resource,
    "io_manager": s3_pickle_io_manager,
}


residential_edx_job = edx_course_pipeline.to_job(
    name="residential_edx_course_pipeline",
    resource_defs=production_resources,
    config=load_yaml_config("/etc/dagster/residential_edx.yaml"),
)

xpro_edx_job = edx_course_pipeline.to_job(
    name="xpro_edx_course_pipeline",
    resource_defs=production_resources,
    config=load_yaml_config("/etc/dagster/xpro_edx.yaml"),
)

mitxonline_edx_job = edx_course_pipeline.to_job(
    name="mitxonline_edx_course_pipeline",
    resource_defs=production_resources,
    config=load_yaml_config("/etc/dagster/mitxonline_edx.yaml"),
)
