"""Schedules for data_loading ingest pipelines."""

import dagster as dg

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

defs = dg.Definitions(
    schedules=[
        oll_ingest_schedule,
        mitpe_ingest_schedule,
        mit_climate_ingest_schedule,
        mit_edx_programs_ingest_schedule,
        podcast_rss_ingest_schedule,
    ],
)
