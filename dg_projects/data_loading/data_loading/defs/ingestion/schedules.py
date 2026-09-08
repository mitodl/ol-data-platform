"""Schedules for data_loading ingest pipelines."""

import dagster as dg
from ol_orchestrate.lib.constants import DAGSTER_ENV

from data_loading.defs.ingestion.assets import MITXONLINE_APP_DLT_ENVIRONMENTS

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

defs = dg.Definitions(
    schedules=[
        oll_ingest_schedule,
        mitpe_ingest_schedule,
        mit_climate_ingest_schedule,
        mit_edx_programs_ingest_schedule,
        podcast_rss_ingest_schedule,
        keycloak_ingest_schedule,
        *([mitxonline_app_ingest_schedule] if mitxonline_app_ingest_schedule else []),
    ],
)
