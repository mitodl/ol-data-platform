from dagster import repository

from ol_orchestrate.schedules.open_edx import (
    mitxonline_edx_daily_schedule,
    residential_edx_daily_schedule,
    xpro_edx_daily_schedule,
)


@repository
def residential_edx_repository():
    return [
        residential_edx_daily_schedule,
    ]


@repository
def xpro_edx_repository():
    return [
        xpro_edx_daily_schedule,
    ]


@repository
def mitxonline_edx_repository():
    return [
        mitxonline_edx_daily_schedule,
    ]
