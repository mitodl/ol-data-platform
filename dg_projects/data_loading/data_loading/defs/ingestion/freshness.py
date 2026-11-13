"""Freshness checks on ingest assets."""

from datetime import timedelta

import dagster as dg

# One table per Google Ads load step; change events can't be re-pulled after 30 days.
google_ads_freshness_checks = dg.build_last_update_freshness_checks(
    assets=[
        dg.AssetKey(["ol_warehouse_raw_data", "raw__google_ads__api__change_events"]),
        dg.AssetKey(["ol_warehouse_raw_data", "raw__google_ads__api__keyword_daily"]),
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
