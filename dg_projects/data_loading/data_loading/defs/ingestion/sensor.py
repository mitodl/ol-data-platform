"""Sensor for edxorg S3 ingest triggered by upstream asset materializations."""

import dagster as dg
from ol_orchestrate.lib.constants import EDXORG_DB_TABLES

_EDXORG_S3_ASSET_KEYS = [
    dg.AssetKey(["ol_warehouse_raw_data", f"raw__edxorg__s3__tables__{t}"])
    for t in EDXORG_DB_TABLES
]

_EDXORG_UPSTREAM_ASSET_KEYS = [
    dg.AssetKey(["edxorg", "raw_data", "db_table", t]) for t in EDXORG_DB_TABLES
]

edxorg_s3_ingest_job = dg.define_asset_job(
    name="edxorg_s3_ingest_job",
    selection=dg.AssetSelection.keys(*_EDXORG_S3_ASSET_KEYS),
    description=(
        "Loads all edxorg database tables from S3 into the raw warehouse layer."
    ),
)


@dg.multi_asset_sensor(
    monitored_assets=_EDXORG_UPSTREAM_ASSET_KEYS,
    job=edxorg_s3_ingest_job,
    minimum_interval_seconds=86400,
    name="edxorg_upstream_changes_sensor",
    description=(
        "Triggers edxorg S3 ingest when any upstream edxorg/raw_data/db_table/* "
        "asset is materialized. Polls at most once per day."
    ),
)
def edxorg_upstream_changes_sensor(
    context: dg.MultiAssetSensorEvaluationContext,
) -> dg.RunRequest | dg.SkipReason:
    """Trigger the edxorg S3 ingest job when upstream archive assets change."""
    latest_records = context.latest_materialization_records_by_key()

    has_new = any(record is not None for record in latest_records.values())
    if not has_new:
        return dg.SkipReason("No new upstream edxorg materializations since last check")

    context.advance_all_cursors()
    return dg.RunRequest()


defs = dg.Definitions(
    jobs=[edxorg_s3_ingest_job],
    sensors=[edxorg_upstream_changes_sensor],
)
