"""Schedules for data_loading ingest pipelines."""

import json

import dagster as dg
from ol_dlt.sources import course_xml_blocks
from ol_orchestrate.lib.constants import DAGSTER_ENV

from data_loading.defs.ingestion.assets import MITXONLINE_APP_DLT_ENVIRONMENTS
from data_loading.defs.ingestion.sensor import IN_FLIGHT_RUN_STATUSES

oll_ingest_schedule = dg.ScheduleDefinition(
    name="oll_ingest_daily_schedule",
    target=dg.AssetSelection.keys(
        ["ol_warehouse_raw_data", "raw__oll__google_sheets__courses"]
    ),
    cron_schedule="0 3 * * *",
    execution_timezone="Etc/UTC",
)

mitpe_ingest_schedule = dg.ScheduleDefinition(
    name="mitpe_ingest_daily_schedule",
    target=dg.AssetSelection.keys(
        ["ol_warehouse_raw_data", "raw__mitpe__api__courses"]
    ),
    cron_schedule="15 3 * * *",
    execution_timezone="Etc/UTC",
)

mit_climate_ingest_schedule = dg.ScheduleDefinition(
    name="mit_climate_ingest_daily_schedule",
    target=dg.AssetSelection.keys(
        ["ol_warehouse_raw_data", "raw__mit_climate__api__articles"]
    ),
    cron_schedule="30 3 * * *",
    execution_timezone="Etc/UTC",
)

mit_edx_programs_ingest_schedule = dg.ScheduleDefinition(
    name="mit_edx_programs_ingest_daily_schedule",
    target=dg.AssetSelection.keys(
        ["ol_warehouse_raw_data", "raw__edxorg__discovery__api__programs"]
    ),
    cron_schedule="45 3 * * *",
    execution_timezone="Etc/UTC",
)

podcast_rss_ingest_schedule = dg.ScheduleDefinition(
    name="podcast_rss_ingest_daily_schedule",
    target=dg.AssetSelection.keys(
        ["ol_warehouse_raw_data", "raw__podcast__rss__channels"],
        ["ol_warehouse_raw_data", "raw__podcast__rss__episodes"],
    ),
    cron_schedule="0 4 * * *",
    execution_timezone="Etc/UTC",
)

# The four raw__youtube__api__* tables are materialized by a single @dlt_assets
# run, so schedule the whole youtube source group rather than one table.
youtube_ingest_schedule = dg.ScheduleDefinition(
    name="youtube_ingest_daily_schedule",
    target=dg.AssetSelection.groups("youtube"),
    cron_schedule="15 4 * * *",
    execution_timezone="Etc/UTC",
)

keycloak_ingest_schedule = dg.ScheduleDefinition(
    name="keycloak_ingest_daily_schedule",
    # Selected by group rather than by key so adding a table to KEYCLOAK_SPEC
    # does not also require editing this schedule.
    target=dg.AssetSelection.groups("keycloak"),
    cron_schedule="30 4 * * *",
    execution_timezone="Etc/UTC",
)

# Defined only where the assets are (see MITXONLINE_APP_DLT_ENVIRONMENTS): a
# schedule whose selection matches no asset fails the tick, it does not no-op.
mitxonline_app_ingest_schedule = (
    dg.ScheduleDefinition(
        name="mitxonline_app_ingest_schedule",
        # Selected by group rather than by key so adding a table to
        # MITXONLINE_APP_SPEC does not also require editing this schedule.
        target=dg.AssetSelection.groups("mitxonline"),
        # Every six hours, matching the cadence of the Airbyte connection this
        # replaces (inventory unit mitxonline/app_postgres,
        # sync_interval_hours: 6). Offset off the hour so it does not start
        # alongside the lakehouse dbt runs.
        cron_schedule="20 */6 * * *",
        execution_timezone="Etc/UTC",
    )
    if DAGSTER_ENV in MITXONLINE_APP_DLT_ENVIRONMENTS
    else None
)
# PostHog writes an hour's export object after that hour closes. Across the 168
# objects written 2026-08-28 to 2026-09-03 the lag ran 1.4 to 15.1 minutes,
# median 8.3, so :20 clears the measured maximum. Landing later than that costs
# nothing. An hour that misses its tick entirely is still picked up, because the
# source reads from CURSOR_LOOKBACK behind its cursor rather than trusting the
# high-water mark alone; a window that lands after a later one is not stepped
# over.
POSTHOG_SCHEDULE_NAME = "posthog_events_ingest_hourly_schedule"


def no_posthog_run_in_flight(context: dg.ScheduleEvaluationContext) -> bool:
    """Skip a tick while the previous run is still loading.

    A backlog (the first run, or catching up after the schedule was off) can
    outlast an hour. Two runs starting from the same saved cursor would both
    load the same hours and append them twice.
    """
    return not context.instance.get_run_records(
        dg.RunsFilter(
            tags={"dagster/schedule_name": POSTHOG_SCHEDULE_NAME},
            statuses=list(IN_FLIGHT_RUN_STATUSES),
        ),
        limit=1,
    )


posthog_events_ingest_schedule = dg.ScheduleDefinition(
    name=POSTHOG_SCHEDULE_NAME,
    target=dg.AssetSelection.keys(
        ["ol_warehouse_raw_data", "raw__posthog__learn__s3__events"]
    ),
    cron_schedule="20 * * * *",
    execution_timezone="Etc/UTC",
    should_execute=no_posthog_run_in_flight,
    # A single hour is the smallest load, and the largest measured
    # (2026-09-18 23:00, 346 MB compressed) peaked at 7.1 GB through the Iceberg
    # writer, against the 8Gi default. 16Gi covers that hour at twice its size.
    # A full 256 MB budget should peak near 5 GB, extrapolated from the same
    # ~20x ratio rather than measured. The request stays low,
    # as for edxorg_s3_ingest_job, so the pod does not reserve the ceiling.
    # Schedule tags are strings; dagster-k8s parses this one as JSON.
    #
    # max_runtime bounds a run that hangs with its pod still alive, which would
    # otherwise hold should_execute's skip forever: the instance sets no global
    # limit (run_monitoring max_runtime_seconds: 0). Every load commits its own
    # cursor, so a run cut off mid-backlog loses at most one load.
    tags={
        "dagster/max_runtime": str(6 * 60 * 60),
        "dagster-k8s/config": json.dumps(
            {
                "container_config": {
                    "resources": {
                        "requests": {"memory": "2Gi"},
                        "limits": {"memory": "16Gi"},
                    }
                }
            }
        ),
    },
)

# Loads the course XML blocks the edxorg and openedx archive assets land, ahead
# of the lakehouse's non_airbyte_staging_daily at 06:00. The first run walks
# the whole ~63 GB backlog a budget at a time; later runs read only new course
# versions.
#
# RUNNING by default in production, unlike the schedules above. A schedule
# without default_status starts STOPPED, which is how the PostHog ingest never
# ran after it shipped. Elsewhere it stays stopped: the source always reads the
# production landing zone.
course_xml_blocks_ingest_schedule = dg.ScheduleDefinition(
    name="course_xml_blocks_ingest_daily_schedule",
    target=dg.AssetSelection.keys(
        *(
            ["ol_warehouse_raw_data", raw_table]
            for raw_table in course_xml_blocks.TABLES
        )
    ),
    cron_schedule="45 4 * * *",
    execution_timezone="Etc/UTC",
    default_status=(
        dg.DefaultScheduleStatus.RUNNING
        if DAGSTER_ENV == "production"
        else dg.DefaultScheduleStatus.STOPPED
    ),
)

defs = dg.Definitions(
    schedules=[
        oll_ingest_schedule,
        mitpe_ingest_schedule,
        mit_climate_ingest_schedule,
        mit_edx_programs_ingest_schedule,
        podcast_rss_ingest_schedule,
        youtube_ingest_schedule,
        keycloak_ingest_schedule,
        *([mitxonline_app_ingest_schedule] if mitxonline_app_ingest_schedule else []),
        posthog_events_ingest_schedule,
        course_xml_blocks_ingest_schedule,
    ],
)
