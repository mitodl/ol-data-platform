from datetime import datetime, time

from dagster import daily_schedule

from ol_data_pipelines.edx.solids import edx_course_pipeline

residential_preset = edx_course_pipeline.get_preset("residential")
xpro_preset = edx_course_pipeline.get_preset("xpro")
mitxonline_preset = edx_course_pipeline.get_preset("mitxonline")


@daily_schedule(
    pipeline_name="edx_course_pipeline",
    start_date=datetime(2020, 9, 23),
    execution_time=time(3, 0, 0),
    mode="production",
    tags_fn_for_date=lambda _: residential_preset.tags,
    execution_timezone="Etc/UTC",
)
def residential_edx_daily_schedule(execution_date):  # noqa: D103
    return residential_preset.run_config


@daily_schedule(
    pipeline_name="edx_course_pipeline",
    start_date=datetime(2020, 9, 23),
    execution_time=time(0, 0, 0),
    mode="production",
    tags_fn_for_date=lambda _: xpro_preset.tags,
    execution_timezone="Etc/UTC",
)
def xpro_edx_daily_schedule(execution_date):  # noqa: D103
    return xpro_preset.run_config


@daily_schedule(
    pipeline_name="edx_course_pipeline",
    start_date=datetime(2021, 12, 18),
    execution_time=time(0, 0, 0),
    mode="production",
    tags_fn_for_date=lambda _: mitxonline_preset.tags,
    execution_timezone="Etc/UTC",
)
def mitxonline_edx_daily_schedule(execution_date):  # noqa: D103
    return mitxonline_preset.run_config
