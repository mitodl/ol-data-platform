"""Freshness checks on ingest assets whose data can't be re-pulled once it's late."""

from datetime import timedelta

import dagster as dg

# Google Ads keeps 30 days of change events, so a lapse in the daily load loses
# history for good once it outlasts that window. The check is on when the asset
# last materialized, not on the newest event: a day without account changes is
# normal and loads no rows.
google_ads_change_events_freshness_checks = dg.build_last_update_freshness_checks(
    assets=[
        dg.AssetKey(["ol_warehouse_raw_data", "raw__google_ads__api__change_events"])
    ],
    lower_bound_delta=timedelta(days=2),
    severity=dg.AssetCheckSeverity.ERROR,
)

# Stopped with google_ads_ingest_daily_schedule; start both together.
google_ads_freshness_sensor = dg.build_sensor_for_freshness_checks(
    freshness_checks=google_ads_change_events_freshness_checks,
    name="google_ads_freshness_sensor",
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)

defs = dg.Definitions(
    asset_checks=google_ads_change_events_freshness_checks,
    sensors=[google_ads_freshness_sensor],
)
