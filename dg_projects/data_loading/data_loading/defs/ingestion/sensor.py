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

# Runs in these statuses haven't finished yet -- wait rather than deciding.
_IN_FLIGHT_RUN_STATUSES = frozenset(
    {
        dg.DagsterRunStatus.QUEUED,
        dg.DagsterRunStatus.NOT_STARTED,
        dg.DagsterRunStatus.STARTING,
        dg.DagsterRunStatus.STARTED,
        dg.DagsterRunStatus.CANCELING,
    }
)

# Tags every launched run with the exact set of upstream materializations it
# was launched for. Querying runs by this tag (rather than relying on
# context.last_run_key, which reflects the sensor DAEMON's own tick-history
# bookkeeping) is what lets this sensor decide, from persisted run state
# alone, whether the run gating the current unconsumed batch succeeded,
# failed, or is still in flight -- and how many times that exact batch has
# already been attempted.
_BATCH_ID_TAG = "edxorg_s3/upstream_batch_id"


def _batch_id(new_records: dict[dg.AssetKey, dg.EventLogRecord]) -> str:
    """Return a deterministic id for a set of new upstream materializations."""
    return "-".join(
        f"{key.to_user_string()}:{record.storage_id}"
        for key, record in sorted(
            new_records.items(), key=lambda kv: kv[0].to_user_string()
        )
    )


def _attempts_for_batch(
    instance: dg.DagsterInstance, batch_id: str
) -> list[dg.RunRecord]:
    """Return every run previously launched for ``batch_id``, oldest first."""
    records = instance.get_run_records(dg.RunsFilter(tags={_BATCH_ID_TAG: batch_id}))
    return sorted(records, key=lambda r: r.create_timestamp)


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
    """Trigger the edxorg S3 ingest job when upstream archive assets change.

    Cursors advance only once the run they gated is confirmed to have
    succeeded -- never at launch time -- so a failed or canceled run is
    retried on the very next tick instead of silently waiting for the next
    genuinely new upstream materialization (which, for a daily archive
    process, can be a full day away).

    Deliberately does not rely on ``context.last_run_key`` (the sensor
    daemon's own tick-history bookkeeping): whether the batch of
    materializations currently unconsumed has already been attempted, and
    with what outcome, is instead derived entirely from real run records
    tagged with ``_BATCH_ID_TAG``, which is both simpler to reason about and
    directly testable without a full daemon loop.
    """
    latest_records = context.latest_materialization_records_by_key()
    new_records = {key: record for key, record in latest_records.items() if record}
    if not new_records:
        return dg.SkipReason("No new upstream edxorg materializations since last check")

    batch_id = _batch_id(new_records)
    attempts = _attempts_for_batch(context.instance, batch_id)

    if attempts:
        latest_attempt = attempts[-1]
        status = latest_attempt.dagster_run.status
        if status in _IN_FLIGHT_RUN_STATUSES:
            return dg.SkipReason(
                f"edxorg_s3 ingest for this batch (run_id="
                f"{latest_attempt.dagster_run.run_id}) is still {status.value}; "
                "waiting for it to finish."
            )
        if status == dg.DagsterRunStatus.SUCCESS:
            context.advance_all_cursors()
            return dg.SkipReason(
                "Most recent edxorg_s3 ingest attempt for this batch succeeded"
            )
        context.log.warning(
            "edxorg_s3 ingest attempt %d for this batch (run_id=%s) finished "
            "with status %s; retrying with a fresh run_key.",
            len(attempts) - 1,
            latest_attempt.dagster_run.run_id,
            status.value,
        )
        # Cursor stays put: new_records remains "new" on the next tick too, so
        # a fresh attempt is requested below instead of waiting for more
        # upstream data.

    run_key = f"edxorg_s3_ingest__{batch_id}__attempt{len(attempts)}"
    return dg.RunRequest(run_key=run_key, tags={_BATCH_ID_TAG: batch_id})


defs = dg.Definitions(
    jobs=[edxorg_s3_ingest_job],
    sensors=[edxorg_upstream_changes_sensor],
)
