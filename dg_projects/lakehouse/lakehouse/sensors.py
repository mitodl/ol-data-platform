"""Dagster sensors for the data lakehouse."""

from __future__ import annotations

import datetime
import json
import logging

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    job,
    op,
    sensor,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.iceberg_maintenance import (
    advance_snapshot_pointer,
    find_snapshot_pointer_lag,
)

log = logging.getLogger(__name__)

# dev mirrors production so engineers can test against live data without a
# separate QA raw database (same pattern as the iceberg_maintenance asset).
_RAW_DATABASE_BY_ENV: dict[str, str] = {
    "production": "ol_warehouse_production_raw",
    "qa": "ol_warehouse_qa_raw",
    "dev": "ol_warehouse_production_raw",
    "ci": "ol_warehouse_qa_raw",
}
RAW_DATABASE = _RAW_DATABASE_BY_ENV.get(DAGSTER_ENV, "ol_warehouse_production_raw")

# Alert when the pointer lags behind the latest snapshot by more than this.
# Two hours provides a buffer for the normal write window without masking a
# genuine stuck-pointer condition (which gaps by days, not minutes).
_MIN_LAG_HOURS = 2.0


@op
def repair_lagging_snapshot_pointers(context):
    """Find and repair all raw-layer Iceberg tables with a stale current-snapshot-id.

    Re-discovers lagging tables at run time so the repair is always applied to
    the current state, even if some tables cleared between the sensor tick and
    this run.  Raises at the end if any table could not be repaired, which
    triggers the existing Slack run-failure alert.
    """
    lagging = find_snapshot_pointer_lag(
        glue_database=RAW_DATABASE,
        min_lag_hours=_MIN_LAG_HOURS,
    )

    if not lagging:
        context.log.info("No snapshot pointer lag detected — nothing to repair.")
        return

    context.log.info("Repairing snapshot pointer for %d table(s)...", len(lagging))

    failed: list[str] = []
    for info in lagging:
        context.log.info(
            "Advancing %s (%.1fh lag, current=%s, latest=%s)",
            info.table_name,
            info.lag_hours,
            info.current_snapshot_id,
            info.latest_snapshot_id,
        )
        try:
            result = advance_snapshot_pointer(
                glue_database=info.database,
                table_name=info.table_name,
            )
        except Exception:
            context.log.exception("Failed to repair %s", info.table_name)
            failed.append(info.table_name)
            continue

        if result.get("skipped"):
            reason = result.get("reason", "unknown")
            if reason == "already current":
                # Resolved between sensor tick and job run — harmless.
                context.log.info(
                    "%s pointer is already current, skipping.", info.table_name
                )
            else:
                # Unexpected skip: treat as failure so the Slack run-failure
                # alert fires and the issue is not silently masked.
                context.log.error(
                    "Unexpected skip for %s: %s — treating as failure.",
                    info.table_name,
                    reason,
                )
                failed.append(info.table_name)
        else:
            context.log.info(
                "Repaired %s: %s → %s",
                info.table_name,
                result["old_snapshot_id"],
                result["new_snapshot_id"],
            )

    if failed:
        raise Exception(  # noqa: TRY002
            f"Snapshot pointer repair failed for {len(failed)} table(s): "
            + ", ".join(failed)
            + " — check the run logs for details."
        )


@job(name="iceberg_snapshot_pointer_repair")
def iceberg_snapshot_pointer_repair_job():
    repair_lagging_snapshot_pointers()


@sensor(
    name="iceberg_snapshot_pointer_lag",
    job=iceberg_snapshot_pointer_repair_job,
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Detects Iceberg tables in the raw layer where current-snapshot-id lags "
        "behind the latest snapshot (the Airbyte full-refresh pin bug) and "
        "triggers an automatic repair run.  If the repair run itself fails, the "
        "existing Slack run-failure alert fires."
    ),
)
def iceberg_snapshot_pointer_lag_sensor(
    context: SensorEvaluationContext,
) -> SensorResult:
    lagging = find_snapshot_pointer_lag(
        glue_database=RAW_DATABASE,
        min_lag_hours=_MIN_LAG_HOURS,
    )

    cursor_data: dict[str, list[str]] = json.loads(context.cursor or "{}")
    previously_lagging: set[str] = set(cursor_data.get("lagging", []))
    currently_lagging: set[str] = {t.table_name for t in lagging}

    new_lagging = currently_lagging - previously_lagging
    recovered = previously_lagging - currently_lagging

    if new_lagging:
        context.log.warning(
            "Snapshot pointer lag detected in %d new table(s): %s",
            len(new_lagging),
            ", ".join(sorted(new_lagging)),
        )
    if currently_lagging - new_lagging:
        context.log.warning(
            "Snapshot pointer lag persists in %d table(s): %s",
            len(currently_lagging - new_lagging),
            ", ".join(sorted(currently_lagging - new_lagging)),
        )
    if recovered:
        context.log.info(
            "Snapshot pointer lag resolved for %d table(s): %s",
            len(recovered),
            ", ".join(sorted(recovered)),
        )

    run_requests = []
    if currently_lagging:
        # Include the UTC hour in the run_key so that failed repair runs are
        # retried on the next hourly tick rather than being silently skipped
        # (Dagster's run_key deduplication prevents re-running the same key).
        hour_bucket = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d%H")
        run_requests.append(RunRequest(run_key=f"repair-{hour_bucket}"))

    return SensorResult(
        run_requests=run_requests,
        cursor=json.dumps({"lagging": sorted(currently_lagging)}),
    )
