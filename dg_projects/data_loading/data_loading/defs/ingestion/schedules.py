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

# The four raw__youtube__api__* tables are materialized by a single @dlt_assets
# run, so schedule the whole youtube source group rather than one table.
youtube_ingest_schedule = dg.ScheduleDefinition(
    name="youtube_ingest_daily_schedule",
    target=dg.AssetSelection.groups("youtube"),
    cron_schedule="30 3 * * *",
    execution_timezone="Etc/UTC",
)

defs = dg.Definitions(
    schedules=[oll_ingest_schedule, mitpe_ingest_schedule, youtube_ingest_schedule],
)
