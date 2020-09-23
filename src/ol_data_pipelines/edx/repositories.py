from dagster import repository
from ol_data_pipelines.edx.schedule import (
    residential_edx_daily_schedule,
    xpro_edx_daily_schedule
)
from ol_data_pipelines.edx.solids import edx_course_pipeline


@repository
def residential_edx_repository():
    return [edx_course_pipeline, residential_edx_daily_schedule]


@repository
def xpro_edx_repository():
    return [edx_course_pipeline, xpro_edx_daily_schedule]
