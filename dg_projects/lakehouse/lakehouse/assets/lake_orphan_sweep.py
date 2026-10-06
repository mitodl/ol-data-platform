"""Dagster asset that sweeps orphaned table directories out of the data lake.

A table directory is orphaned when no Glue table location or metadata pointer
references it: a dbt run killed between writing its files and registering the
table, or a table dropped by an engine that removed the Glue entry and left the
files. Iceberg's per-table orphan removal reaches neither, because the table it
would walk does not exist.

The sweep is scoped to the warehouse environment this code location runs in and
reads its scan targets from Glue (see
:func:`ol_orchestrate.lib.lake_orphan_sweep.warehouse_scan_targets`).

It reports everywhere and deletes only where
:data:`LAKE_ORPHAN_SWEEP_DELETE_ENVIRONMENTS` says so. That set starts empty:
the first deletion list for an environment gets read by a person before the
environment is added.
"""

from datetime import UTC, datetime
from typing import Any

import boto3
from dagster import AssetExecutionContext, Config, MetadataValue, Output, asset
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.iceberg_maintenance import warehouse_env_for
from ol_orchestrate.lib.lake_orphan_sweep import SweepResult, sweep_warehouse

WAREHOUSE_ENV = warehouse_env_for(DAGSTER_ENV)

# Keyed on the Dagster environment, not the warehouse one: `dev` resolves to the
# production warehouse, and a laptop must never be able to delete from it.
LAKE_ORPHAN_SWEEP_DELETE_ENVIRONMENTS: frozenset[str] = frozenset()

# The floor has to exceed the longest time a writer leaves files unregistered.
# Measured 2026-10-06 over the 1,084 production tables Glue registered in the
# previous 30 days whose directory carries dbt's uuid suffix: 200 seconds at
# most between a directory's first object and its Glue CreateTime (p99 35 s).
# Seven days is what the manual sweeps used, about 3,000 times that.
LAKE_ORPHAN_SWEEP_MIN_AGE_DAYS = 7

# Enough rows to review a normal week in the Dagster UI without making the
# event too large to render. The counts above it are never capped.
METADATA_DETAIL_ROWS = 200


class LakeOrphanSweepConfig(Config):
    """Run configuration for :func:`lake_orphan_sweep`."""

    # No default on purpose: a floor nobody chose is how a sweep deletes a
    # table that was about to be registered.
    min_age_days: int
    delete: bool = False


def _aws_clients() -> tuple[Any, Any]:
    """Return the Glue and S3 clients the sweep runs with."""
    return boto3.client("glue"), boto3.client("s3")


def _detail_rows(result: SweepResult) -> list[dict[str, Any]]:
    rows = sorted(result.orphans, key=lambda row: -row["bytes"])
    return [
        {
            "path": f"s3://{row['bucket']}/{row['prefix']}/",
            "objects": row["objects"],
            "bytes": row["bytes"],
            "age_days": row["age_days"],
            "eligible": row["eligible"],
        }
        for row in rows[:METADATA_DETAIL_ROWS]
    ]


@asset(
    group_name="lakehouse_maintenance",
    description=(
        "Finds table directories in this environment's data lake buckets that no "
        "Glue table references, and deletes the ones older than the configured "
        "floor where deletion is enabled for the environment."
    ),
)
def lake_orphan_sweep(
    context: AssetExecutionContext, config: LakeOrphanSweepConfig
) -> Output[None]:
    """Report, and where enabled delete, orphaned data-lake table directories."""
    if config.delete and DAGSTER_ENV not in LAKE_ORPHAN_SWEEP_DELETE_ENVIRONMENTS:
        msg = (
            f"Deletion is not enabled for the {DAGSTER_ENV} environment "
            f"(enabled: {sorted(LAKE_ORPHAN_SWEEP_DELETE_ENVIRONMENTS)}). Run "
            "with delete=false to report."
        )
        raise ValueError(msg)

    glue, s3 = _aws_clients()
    result = sweep_warehouse(
        glue,
        s3,
        warehouse_env=WAREHOUSE_ENV,
        min_age_days=config.min_age_days,
        now=datetime.now(UTC),
        delete=config.delete,
    )
    # Glue naming no bucket-root database for the environment means the scope
    # resolved to nothing, which is a fault and not a clean lake.
    if not result.targets:
        msg = (
            f"No ol_warehouse_{WAREHOUSE_ENV}_* Glue database is located at a "
            "bucket root, so there is nothing to scan. Refusing to report "
            "success for zero work."
        )
        raise RuntimeError(msg)

    eligible = result.eligible
    metadata: dict[str, Any] = {
        "warehouse_env": MetadataValue.text(WAREHOUSE_ENV),
        "min_age_days": MetadataValue.int(config.min_age_days),
        "directories_scanned": MetadataValue.int(len(result.targets)),
        "prefixes_scanned": MetadataValue.int(result.prefixes_scanned),
        "orphan_prefixes": MetadataValue.int(len(result.orphans)),
        "orphan_bytes": MetadataValue.int(sum(r["bytes"] for r in result.orphans)),
        "eligible_prefixes": MetadataValue.int(len(eligible)),
        "eligible_objects": MetadataValue.int(sum(r["objects"] for r in eligible)),
        "eligible_bytes": MetadataValue.int(sum(r["bytes"] for r in eligible)),
        "orphan_details": MetadataValue.json(_detail_rows(result)),
        # Unreferenced, but without dbt's uuid suffix, so never deleted and
        # never measured. Listed because a person may still want to look.
        "unsuffixed_unreferenced_prefixes": MetadataValue.int(len(result.unsuffixed)),
        "unsuffixed_unreferenced_details": MetadataValue.json(
            result.unsuffixed[:METADATA_DETAIL_ROWS]
        ),
    }

    if result.outcomes is None:
        # No deleted_* keys in a report run. A zero there would read as "ran
        # and found nothing to delete".
        metadata["delete_status"] = MetadataValue.text("not run (report only)")
        context.log.info(
            "Report only: %d orphan prefixes, %d eligible (%d bytes).",
            len(result.orphans),
            len(eligible),
            sum(r["bytes"] for r in eligible),
        )
        return Output(value=None, metadata=metadata)

    deleted = [o for o in result.outcomes if o.action == "deleted"]
    kept = [o for o in result.outcomes if o.action != "deleted"]
    errors = [error for o in deleted for error in o.errors]
    metadata |= {
        "delete_status": MetadataValue.text("ran"),
        "deleted_prefixes": MetadataValue.int(len(deleted)),
        "deleted_objects": MetadataValue.int(sum(o.objects for o in deleted)),
        "deleted_bytes": MetadataValue.int(sum(o.bytes for o in deleted)),
        # Eligible at scan time and left alone at delete time, e.g. registered
        # in Glue in between.
        "kept_at_delete_time": MetadataValue.json(
            [f"s3://{o.bucket}/{o.prefix}/: {o.reason}" for o in kept]
        ),
        "delete_error_count": MetadataValue.int(len(errors)),
    }
    if errors:
        msg = (
            f"{len(errors)} objects could not be deleted across "
            f"{sum(1 for o in deleted if o.errors)} prefixes. First errors: "
            f"{errors[:5]}"
        )
        raise RuntimeError(msg)
    return Output(value=None, metadata=metadata)
