from datetime import datetime, time

from dagster import daily_schedule

from ol_orchestrate.lib.yaml_config_helper import load_yaml_config


@daily_schedule(
    pipeline_name="residential_edx_course_pipeline",
    start_date=datetime(2020, 9, 23),
    execution_time=time(3, 0, 0),
    tags_fn_for_date=lambda _: {"business_unit": "residential"},
    execution_timezone="Etc/UTC",
)
def residential_edx_daily_schedule(execution_date):
    return load_yaml_config("/etc/dagster/residential_edx.yaml")


@daily_schedule(
    pipeline_name="xpro_edx_course_pipeline",
    start_date=datetime(2020, 9, 23),
    execution_time=time(0, 0, 0),
    tags_fn_for_date=lambda _: {"business_unit": "mitxpro"},
    execution_timezone="Etc/UTC",
)
def xpro_edx_daily_schedule(execution_date):
    return load_yaml_config("/etc/dagster/xpro_edx.yaml")


@daily_schedule(
    pipeline_name="mitxonline_edx_course_pipeline",
    start_date=datetime(2021, 12, 18),
    execution_time=time(0, 0, 0),
    tags_fn_for_date=lambda _: {"business_unit": "mitxonline"},
    execution_timezone="Etc/UTC",
)
def mitxonline_edx_daily_schedule(execution_date):
    return load_yaml_config("/etc/dagster/mitxonline_edx.yaml")
