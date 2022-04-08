from dagster import fs_io_manager, repository
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

from ol_data_pipelines.edx.schedule import (
    mitxonline_edx_daily_schedule,
    residential_edx_daily_schedule,
    xpro_edx_daily_schedule,
)
from ol_data_pipelines.edx.solids import edx_course_pipeline
from ol_data_pipelines.lib.yaml_config_helper import load_yaml_config
from ol_data_pipelines.resources.healthchecks import (
    healthchecks_dummy_resource,
    healthchecks_io_resource,
)
from ol_data_pipelines.resources.mysql_db import mysql_db_resource
from ol_data_pipelines.resources.outputs import daily_dir
from ol_data_pipelines.resources.sqlite_db import sqlite_db_resource

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


@repository
def residential_edx_repository():
    return [
        edx_course_pipeline.to_job(
            name="residential_edx_course_pipeline",
            resource_defs=production_resources,
            config=load_yaml_config("/etc/dagster/residential_edx.yaml"),
        ),
        residential_edx_daily_schedule,
    ]


@repository
def xpro_edx_repository():
    return [
        edx_course_pipeline.to_job(
            name="xpro_edx_course_pipeline",
            resource_defs=production_resources,
            config=load_yaml_config("/etc/dagster/xpro_edx.yaml"),
        ),
        xpro_edx_daily_schedule,
    ]


@repository
def mitxonline_edx_repository():
    return [
        edx_course_pipeline.to_job(
            name="mitxonline_edx_course_pipeline",
            resource_defs=production_resources,
            config=load_yaml_config("/etc/dagster/mitxonline_edx.yaml"),
        ),
        mitxonline_edx_daily_schedule,
    ]
