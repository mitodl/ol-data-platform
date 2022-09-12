from dagster import RunRequest, schedule

from ol_orchestrate.jobs.open_edx import (
    mitxonline_edx_job,
    residential_edx_job,
    xpro_edx_job,
)
from ol_orchestrate.lib.yaml_config_helper import load_yaml_config


@schedule(
    job=residential_edx_job,
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def residential_edx_daily_schedule(execution_date):
    return RunRequest(
        run_key="residential_edx_course_pipeline",
        run_config=load_yaml_config("/etc/dagster/residential_edx.yaml"),
        tags={"business_unit": "residential"},
    )


@schedule(
    job=xpro_edx_job,
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def xpro_edx_daily_schedule(execution_date):
    return RunRequest(
        run_key="xpro_edx_course_pipeline",
        run_config=load_yaml_config("/etc/dagster/xpro_edx.yaml"),
        tags={"business_unit": "mitxpro"},
    )


@schedule(
    job=mitxonline_edx_job,
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def mitxonline_edx_daily_schedule(execution_date):
    return RunRequest(
        run_key="mitxonline_edx_course_pipeline",
        run_config=load_yaml_config("/etc/dagster/mitxonline_edx.yaml"),
        tags={"business_unit": "mitxonline"},
    )
