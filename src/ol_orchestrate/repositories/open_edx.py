import os

from dagster import Definitions, fs_io_manager
from dagster_aws.s3 import S3Resource
from dagster_aws.s3.io_manager import s3_pickle_io_manager

from ol_orchestrate.jobs.open_edx import edx_course_pipeline
from ol_orchestrate.lib.yaml_config_helper import load_yaml_config
from ol_orchestrate.resources.healthchecks import (
    HealthchecksIO,
)
from ol_orchestrate.resources.mysql_db import mysql_db_resource
from ol_orchestrate.resources.openedx import OpenEdxApiClient
from ol_orchestrate.resources.outputs import DailyResultsDir
from ol_orchestrate.resources.sqlite_db import sqlite_db_resource
from ol_orchestrate.schedules.open_edx import (
    mitxonline_edx_daily_schedule,
    residential_edx_daily_schedule,
    xpro_edx_daily_schedule,
)

dagster_env = os.environ.get("DAGSTER_ENVIRONMENT", "dev")


dev_resources = {
    "sqldb": sqlite_db_resource,
    "s3": S3Resource(),
    "results_dir": DailyResultsDir.configure_at_launch(),
    "healthchecks": HealthchecksIO.configure_at_launch(),
    "io_manager": fs_io_manager,
    "openedx": OpenEdxApiClient.configure_at_launch(),
}

production_resources = {
    "sqldb": mysql_db_resource,
    "s3": S3Resource(),
    "results_dir": DailyResultsDir.configure_at_launch(),
    "healthchecks": HealthchecksIO.configure_at_launch(),
    "io_manager": s3_pickle_io_manager,
    "openedx": OpenEdxApiClient.configure_at_launch(),
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

open_edx_irx_extracts = Definitions(
    resources=dev_resources if dagster_env == "dev" else production_resources,
    jobs=[residential_edx_job, xpro_edx_job, mitxonline_edx_job],
    schedules=[
        residential_edx_daily_schedule.with_updated_job(residential_edx_job),
        xpro_edx_daily_schedule.with_updated_job(xpro_edx_job),
        mitxonline_edx_daily_schedule.with_updated_job(mitxonline_edx_job),
    ],
)
