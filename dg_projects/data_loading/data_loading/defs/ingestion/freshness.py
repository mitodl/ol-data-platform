"""Freshness checks on ingest assets that must load every day."""

from datetime import timedelta

import dagster as dg

# Google Ads keeps 30 days of change events and no past snapshots, so a lapse
# loses them for good. The daily reports load after both in the same run, so
# checking them catches a run that fails partway. Each check is on when the asset
# last materialized, not on its newest row: a day without account changes is
# normal and loads no change events.
google_ads_freshness_checks = dg.build_last_update_freshness_checks(
    assets=[
        dg.AssetKey(["ol_warehouse_raw_data", f"raw__google_ads__api__{table}"])
        for table in (
            "change_events",
            "keyword_snapshots",
            "keyword_daily",
            "campaign_daily",
        )
    ],
    lower_bound_delta=timedelta(days=2),
    severity=dg.AssetCheckSeverity.ERROR,
)

# Stopped with google_ads_ingest_daily_schedule; start both together.
google_ads_freshness_sensor = dg.build_sensor_for_freshness_checks(
    freshness_checks=google_ads_freshness_checks,
    name="google_ads_freshness_sensor",
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)

defs = dg.Definitions(
    asset_checks=google_ads_freshness_checks,
    sensors=[google_ads_freshness_sensor],
)
