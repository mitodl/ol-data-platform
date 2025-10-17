"""Schedules for OpenMetadata ingestion workflows.

These schedules define when metadata should be ingested from various sources.
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

# Job for metadata ingestion
metadata_ingestion_job = define_asset_job(
    name="openmetadata_ingestion",
    selection=AssetSelection.groups("openmetadata"),
    description="Ingest metadata from all data sources into OpenMetadata",
)

# Schedule to run metadata ingestion daily at 2 AM
metadata_ingestion_schedule = ScheduleDefinition(
    job=metadata_ingestion_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    default_status=DefaultScheduleStatus.STOPPED,
    description="Daily metadata ingestion from all data sources",
)

# Schedule for more frequent updates of critical sources (every 4 hours)
critical_metadata_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="critical_metadata_ingestion",
        selection=AssetSelection.keys(
            ["openmetadata", "trino", "metadata"],
            ["openmetadata", "dbt", "metadata"],
            ["openmetadata", "dbt", "lineage"],
            ["openmetadata", "dagster", "metadata"],
        ),
        description="Ingest metadata from critical data sources",
    ),
    cron_schedule="0 */4 * * *",  # Every 4 hours
    default_status=DefaultScheduleStatus.STOPPED,
    description="Frequent updates for Trino, dbt, and Dagster metadata",
)
