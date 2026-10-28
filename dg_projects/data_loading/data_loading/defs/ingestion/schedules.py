"""Schedules for data_loading ingest pipelines."""

import json
from collections.abc import Callable

import dagster as dg
from ol_dlt.sources import course_xml_blocks, edxorg_program_reports, ocw_content
from ol_orchestrate.lib.constants import DAGSTER_ENV

from data_loading.defs.ingestion.assets import (
    XPRO_TRACKING_LOGS_TABLE,
    mitxonline_app_assets,
    xpro_app_assets,
)
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
# Defined only where the assets are (see XPRO_APP_DLT_ENVIRONMENTS).
xpro_app_ingest_schedule = (
    dg.ScheduleDefinition(
        name="xpro_app_ingest_schedule",
        # Selected by definition, not by the "xpro" group, which the xPro
        # course structure blocks share.
        target=dg.AssetSelection.assets(xpro_app_assets),
        # Every six hours, matching the Airbyte connection this replaces
        # (inventory unit xpro/app_postgres, sync_interval_hours: 6). Offset from
        # the MITx Online load at :20 so the two do not start together.
        cron_schedule="40 */6 * * *",
        execution_timezone="Etc/UTC",
    )
    if xpro_app_assets
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


def no_run_in_flight(
    schedule_name: str,
) -> Callable[[dg.ScheduleEvaluationContext], bool]:
    """Return a ``should_execute`` that skips a tick while the last run loads.

    For appending loads whose backlog can outlast the schedule's interval. Two
    runs starting from the same saved cursor would both load the same files
    and append them twice.
    """

    def should_execute(context: dg.ScheduleEvaluationContext) -> bool:
        return not context.instance.get_run_records(
            dg.RunsFilter(
                tags={"dagster/schedule_name": schedule_name},
                statuses=list(IN_FLIGHT_RUN_STATUSES),
            ),
            limit=1,
        )

    return should_execute


# A backlog (the first run, or catching up after the schedule was off) can
# outlast an hour.
no_posthog_run_in_flight = no_run_in_flight(POSTHOG_SCHEDULE_NAME)


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
    # otherwise hold should_execute's skip. This tag is the bound for these
    # runs: it overrides the instance's 2-day run_monitoring
    # max_runtime_seconds, which only applies to runs with no tag. Every load
    # commits its own cursor, so a run cut off mid-backlog loses at most one
    # load.
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

# edX.org's learner reports reach the landing zone at about 15:00 UTC (S3
# modification times 14:51 to 15:02 on 2026-10-14 to 2026-10-23), so the day's
# reports are loaded well ahead of the next non_airbyte_staging_daily at 06:00.
# The first runs walk the 4.93 TB backlog, at most 430 GB per run at the
# 200-batch cap (the first run, 2026-10-27, read 385 GB); later runs read one
# day of reports. RUNNING by default in production only, as for the course XML
# blocks: the source always reads the production landing zone.
#
# That first run peaked at 8.06 GB RSS against the 8Gi default limit, with the
# largest batch at 2.05 GiB, so a load peaks near 3.7x its batch. A batch can
# reach the 2 GiB budget plus the unread files in the cursor's second, which
# edxorg_files exempts from the budget (up to 1.06 GiB of reports share one
# second on recent days), so ~3.1 GiB and ~12 GB. 16Gi covers that. The
# request stays low, as for the PostHog schedule above.
edxorg_program_reports_ingest_schedule = dg.ScheduleDefinition(
    name="edxorg_program_reports_ingest_daily_schedule",
    target=dg.AssetSelection.keys(
        ["ol_warehouse_raw_data", edxorg_program_reports.RAW_TABLE]
    ),
    cron_schedule="30 16 * * *",
    execution_timezone="Etc/UTC",
    tags={
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
    default_status=(
        dg.DefaultScheduleStatus.RUNNING
        if DAGSTER_ENV == "production"
        else dg.DefaultScheduleStatus.STOPPED
    ),
)

# Every 12 hours, the cadence of the Airbyte connection it replaces (inventory
# unit xpro/tracking_logs). The first production runs walk the 44.3 GB backlog
# at most 200 batches of 128 MiB gzip a run, so it takes about two runs, and
# the guard keeps a run that outlasts the interval from overlapping the next.
# The guard sees only this schedule's runs and their retries, not a run launched
# by hand. max_runtime fails a hung run after 11 hours, and run_retries
# relaunches it with the same tags, so a hang that repeats on every attempt
# holds the schedule off until the retries run out.
#
# A batch should peak near 4.3 GB at the worst gzip ratio measured (see
# tracking_logs.BUDGET_BYTES), and the run also holds its listing (474 MB for
# the whole bucket). Nothing has been measured in a deployed pod, and the other
# two batched loads each passed 7 GB against the 8Gi default, so this takes
# their 16Gi limit. The request stays low, as for those schedules.
#
# RUNNING in production and QA, which reads the QA deployment's bucket (51,857
# files, 0.9 GB on 2026-10-28). Stopped elsewhere, where the source would read
# production.
XPRO_TRACKING_LOGS_SCHEDULE_NAME = "xpro_tracking_logs_ingest_schedule"
no_xpro_tracking_logs_run_in_flight = no_run_in_flight(XPRO_TRACKING_LOGS_SCHEDULE_NAME)
xpro_tracking_logs_ingest_schedule = dg.ScheduleDefinition(
    name=XPRO_TRACKING_LOGS_SCHEDULE_NAME,
    target=dg.AssetSelection.keys(["ol_warehouse_raw_data", XPRO_TRACKING_LOGS_TABLE]),
    cron_schedule="50 */12 * * *",
    execution_timezone="Etc/UTC",
    should_execute=no_xpro_tracking_logs_run_in_flight,
    tags={
        "dagster/max_runtime": str(11 * 60 * 60),
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
    default_status=(
        dg.DefaultScheduleStatus.RUNNING
        if DAGSTER_ENV in {"qa", "production"}
        else dg.DefaultScheduleStatus.STOPPED
    ),
)

OCW_CONTENT_SCHEDULE_NAME = "ocw_content_ingest_daily_schedule"
# Defined in ocw_content_sensor, named here so the schedule can wait for its runs.
OCW_CONTENT_SENSOR_NAME = "ocw_content_publish_sensor"
# Carried by the sweep and by every scoped load. The run coordinator's
# tag_concurrency_limits in ol-infrastructure (dagster_instance.yaml) caps runs
# with this key at one, so a load requested while another is starting waits in
# the queue. The in-flight checks below only keep the sensor and schedule from
# requesting runs that would sit there.
OCW_CONTENT_RUN_TAGS = {"ocw/content_load": "true"}


def ocw_content_run_in_flight(instance: dg.DagsterInstance) -> bool:
    """Tell whether a sweep or a scoped OCW content load has not finished.

    Each load saves the source's whole dlt state, and a load restores the one
    saved by the newest load id. So of two loads at once, one drops what the
    other recorded: the sweep's resume point,
    the prefixes it saw empty, the gone courses it unpublished (which the next
    sweep would then unpublish and count against the limit again). A run
    started by hand is seen, and queued behind the others, only if it is
    launched with ``OCW_CONTENT_RUN_TAGS``. The schedule's own name is checked
    too because a sweep started before that tag existed carries only it, and
    the first sweep can outlast a day.
    """
    return any(
        instance.get_run_records(
            dg.RunsFilter(tags=tags, statuses=list(IN_FLIGHT_RUN_STATUSES)),
            limit=1,
        )
        for tags in (
            OCW_CONTENT_RUN_TAGS,
            {"dagster/schedule_name": OCW_CONTENT_SCHEDULE_NAME},
        )
    )


def ocw_content_sweep_due(context: dg.ScheduleEvaluationContext) -> bool:
    """Start the day's sweep at the first tick no OCW content load is running.

    The first sweep reads every course and can outlast a day, and a scoped load
    from ocw_content_publish_sensor can be running when the 04:35 tick fires.
    Either way the tick waits (see ``ocw_content_run_in_flight``), and the
    hourly ticks after it start the sweep once nothing is running, unless one
    already started that day.
    """
    if ocw_content_run_in_flight(context.instance):
        return False
    tick = context.scheduled_execution_time
    if tick is None:
        msg = "The OCW content schedule was evaluated without a tick time."
        raise ValueError(msg)
    return not context.instance.get_run_records(
        dg.RunsFilter(
            tags={"dagster/schedule_name": OCW_CONTENT_SCHEDULE_NAME},
            created_after=tick.replace(hour=0, minute=0, second=0, microsecond=0),
        ),
        limit=1,
    )


# OCW course pages and resource text for MIT Learn's ContentFiles, ahead of the
# lakehouse's non_airbyte_staging_daily at 06:00. One sweep a day, at the first
# hourly tick from 04:35 that nothing else is loading (see
# ocw_content_sweep_due), so it can start as late as 09:35. RUNNING by default in
# production only: the source always reads the production OCW bucket, and every
# changed course costs Tika calls.
ocw_content_ingest_schedule = dg.ScheduleDefinition(
    name=OCW_CONTENT_SCHEDULE_NAME,
    target=dg.AssetSelection.keys(["ol_warehouse_raw_data", ocw_content.RAW_TABLE]),
    cron_schedule="35 4-9 * * *",
    execution_timezone="Etc/UTC",
    should_execute=ocw_content_sweep_due,
    tags=OCW_CONTENT_RUN_TAGS,
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
        *([xpro_app_ingest_schedule] if xpro_app_ingest_schedule else []),
        posthog_events_ingest_schedule,
        course_xml_blocks_ingest_schedule,
        edxorg_program_reports_ingest_schedule,
        xpro_tracking_logs_ingest_schedule,
        ocw_content_ingest_schedule,
    ],
)
