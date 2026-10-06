"""Schedules for data_loading ingest pipelines."""

import json

import dagster as dg
from ol_dlt.sources import course_xml_blocks, ocw_content
from ol_orchestrate.lib.constants import DAGSTER_ENV

from data_loading.defs.ingestion.assets import mitxonline_app_assets
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
        ["ol_warehouse_raw_data", "raw__edxorg__discovery__api__programs"],
        ["ol_warehouse_raw_data", "raw__edxorg__discovery__api__program"],
        ["ol_warehouse_raw_data", "raw__edxorg__discovery__api__program_course"],
        ["ol_warehouse_raw_data", "raw__edxorg__discovery__api__mitx_course"],
        ["ol_warehouse_raw_data", "raw__edxorg__discovery__api__mitx_course_run"],
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

# The raw__youtube__api__* tables are all materialized by a single @dlt_assets
# run, so schedule the whole youtube source group rather than one table.
youtube_ingest_schedule = dg.ScheduleDefinition(
    name="youtube_ingest_daily_schedule",
    target=dg.AssetSelection.groups("youtube"),
    cron_schedule="15 4 * * *",
    execution_timezone="Etc/UTC",
)

# Sloan Executive Education, ahead of the lakehouse's non_airbyte_staging_daily
# at 06:00. RUNNING by default in production, where the delivery location's
# sloan_course_metadata extract has run daily against the same API since before
# this load existed. The API has no QA instance.
see_ingest_schedule = dg.ScheduleDefinition(
    name="see_ingest_daily_schedule",
    target=dg.AssetSelection.keys(
        ["ol_warehouse_raw_data", "raw__see__api__courses"],
        ["ol_warehouse_raw_data", "raw__see__api__course_offerings"],
    ),
    cron_schedule="25 4 * * *",
    execution_timezone="Etc/UTC",
    default_status=(
        dg.DefaultScheduleStatus.RUNNING
        if DAGSTER_ENV == "production"
        else dg.DefaultScheduleStatus.STOPPED
    ),
)

# The four feeds MIT Learn's news_events app polls (news_events/etl/), ahead of
# the lakehouse's non_airbyte_staging_daily at 06:00. Learn polls every three
# hours, but the integrations models only rebuild daily, so a more frequent load
# would change nothing downstream. RUNNING by default in production, where the
# feeds are public and the same in every environment.
news_events_ingest_schedule = dg.ScheduleDefinition(
    name="news_events_ingest_daily_schedule",
    target=dg.AssetSelection.keys(
        ["ol_warehouse_raw_data", "raw__mitpe__api__news"],
        ["ol_warehouse_raw_data", "raw__mitpe__api__events"],
        ["ol_warehouse_raw_data", "raw__openlearning__api__events"],
        ["ol_warehouse_raw_data", "raw__medium__rss__posts"],
    ),
    cron_schedule="50 4 * * *",
    execution_timezone="Etc/UTC",
    default_status=(
        dg.DefaultScheduleStatus.RUNNING
        if DAGSTER_ENV == "production"
        else dg.DefaultScheduleStatus.STOPPED
    ),
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
        # Selected by definition rather than by key so adding a table to
        # MITXONLINE_APP_SPEC does not also require editing this schedule, and
        # not by the "mitxonline" group, which the MITx Online course structure
        # blocks share.
        target=dg.AssetSelection.assets(mitxonline_app_assets),
        # Every six hours, matching the cadence of the Airbyte connection this
        # replaces (inventory unit mitxonline/app_postgres,
        # sync_interval_hours: 6). Offset off the hour so it does not start
        # alongside the lakehouse dbt runs.
        cron_schedule="20 */6 * * *",
        execution_timezone="Etc/UTC",
    )
    if mitxonline_app_assets
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

# Loads the course XML blocks the edxorg and openedx archive assets land, the
# document and transcript text the openedx location extracts from them, and the
# edxorg and openedx course structure blocks, ahead of the lakehouse's
# non_airbyte_staging_daily at 06:00. The first run walks the whole backlog
# (~63 GB of blocks, ~22 GB of text on 2026-10-01, 10 GB of edxorg structure
# blocks on 2026-10-05, 18 GB of openedx structure blocks on 2026-10-06) a
# budget at a time; later runs read only new course versions.
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

OCW_CONTENT_SCHEDULE_NAME = "ocw_content_ingest_daily_schedule"


def no_ocw_content_run_in_flight(context: dg.ScheduleEvaluationContext) -> bool:
    """Skip a tick while the previous run is still reading courses.

    The first run reads every course and can outlast a day. A second run
    starting from the same saved state would read the same courses and append
    them twice.
    """
    return not context.instance.get_run_records(
        dg.RunsFilter(
            tags={"dagster/schedule_name": OCW_CONTENT_SCHEDULE_NAME},
            statuses=list(IN_FLIGHT_RUN_STATUSES),
        ),
        limit=1,
    )


# OCW course pages and resource text for MIT Learn's ContentFiles, ahead of the
# lakehouse's non_airbyte_staging_daily at 06:00. RUNNING by default in
# production only: the source always reads the production OCW bucket, and every
# changed course costs Tika calls.
ocw_content_ingest_schedule = dg.ScheduleDefinition(
    name=OCW_CONTENT_SCHEDULE_NAME,
    target=dg.AssetSelection.keys(["ol_warehouse_raw_data", ocw_content.RAW_TABLE]),
    cron_schedule="35 4 * * *",
    execution_timezone="Etc/UTC",
    should_execute=no_ocw_content_run_in_flight,
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
        see_ingest_schedule,
        news_events_ingest_schedule,
        keycloak_ingest_schedule,
        *([mitxonline_app_ingest_schedule] if mitxonline_app_ingest_schedule else []),
        posthog_events_ingest_schedule,
        course_xml_blocks_ingest_schedule,
        ocw_content_ingest_schedule,
    ],
)
