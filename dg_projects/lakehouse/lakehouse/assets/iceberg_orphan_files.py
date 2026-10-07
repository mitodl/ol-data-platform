"""Dagster asset that removes orphan files from inside live raw Iceberg tables.

Snapshot expiry (``iceberg_raw_layer_maintenance``) only rewrites a table's
metadata: pyiceberg deletes none of the files the expired snapshots referenced.
``lake_orphan_sweep`` removes whole directories no Glue table references, so it
never looks inside a table that still exists. This asset is the pass in
between. For each Iceberg table in the raw database it lists the table's
directory and compares it with what the table's metadata can still reach (see
:mod:`ol_orchestrate.lib.iceberg_orphan_files`).

It reports everywhere and deletes only where
:data:`ICEBERG_ORPHAN_FILES_DELETE_ENVIRONMENTS` says so. That set starts
empty: the first report for an environment gets read by a person before the
environment is added.

Raw only. The dbt layers lose files to expiry the same way, but a ``table``
materialization moves to a new directory on every build and the sweep collects
the old one, so the raw layer is where the bytes are.
"""

from datetime import UTC, datetime
from typing import Any

import boto3
from dagster import AssetExecutionContext, Config, MetadataValue, Output, asset
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.iceberg_maintenance import (
    get_glue_catalog,
    maintenance_failure_threshold,
    warehouse_env_for,
)
from ol_orchestrate.lib.iceberg_orphan_files import (
    DatabaseOrphanFiles,
    S3ObjectStore,
    remove_database_orphan_files,
)

WAREHOUSE_ENV = warehouse_env_for(DAGSTER_ENV)
RAW_GLUE_DATABASE = f"ol_warehouse_{WAREHOUSE_ENV}_raw"

# Keyed on the Dagster environment, not the warehouse one: `dev` resolves to the
# production warehouse, and a laptop must never be able to delete from it.
ICEBERG_ORPHAN_FILES_DELETE_ENVIRONMENTS: frozenset[str] = frozenset()

# The floor has to exceed the longest time a writer holds a file before the
# commit that references it. Not measured for the raw writers (an Airbyte sync
# writes data files for as long as it runs and commits at the end, dlt
# likewise). Seven days is the sweep's floor and more than twice Spark's
# three-day default for the same operation.
ICEBERG_ORPHAN_FILES_MIN_AGE_DAYS = 7

# Each worker holds one table's reachable-file set and its eligible orphans in
# memory, so this bounds memory as much as it bounds S3 and Glue request rates.
ICEBERG_ORPHAN_FILES_WORKERS = 4

# Enough rows to review in the Dagster UI without making the event too large to
# render. The counts above it are never capped.
METADATA_DETAIL_ROWS = 200


class IcebergOrphanFilesConfig(Config):
    """Run configuration for :func:`iceberg_raw_orphan_files`."""

    # No default on purpose: a floor nobody chose is how this deletes a file a
    # writer was about to commit. The pass refuses less than 1.
    min_age_days: int
    delete: bool = False


def _aws_clients() -> tuple[Any, Any]:
    """Return the Glue and S3 clients the pass runs with."""
    return boto3.client("glue"), boto3.client("s3")


def _detail_rows(result: DatabaseOrphanFiles) -> list[dict[str, Any]]:
    rows = sorted(
        (table for table in result.examined if table.orphan_objects),
        key=lambda table: -table.eligible_bytes,
    )
    return [
        {
            "table": table.table,
            "objects_listed": table.objects_listed,
            "bytes_listed": table.bytes_listed,
            "orphan_objects": table.orphan_objects,
            "orphan_bytes": table.orphan_bytes,
            "eligible_objects": table.eligible_objects,
            "eligible_bytes": table.eligible_bytes,
        }
        for table in rows[:METADATA_DETAIL_ROWS]
    ]


@asset(
    group_name="lakehouse_maintenance",
    description=(
        "Finds files inside this environment's raw Iceberg table directories "
        "that no retained snapshot references (left by snapshot expiry and by "
        "writes that never committed), and deletes the ones older than the "
        "configured floor where deletion is enabled for the environment."
    ),
)
def iceberg_raw_orphan_files(
    context: AssetExecutionContext, config: IcebergOrphanFilesConfig
) -> Output[None]:
    """Report, and where enabled delete, orphan files in raw Iceberg tables."""
    if config.delete and DAGSTER_ENV not in ICEBERG_ORPHAN_FILES_DELETE_ENVIRONMENTS:
        msg = (
            f"Deletion is not enabled for the {DAGSTER_ENV} environment "
            f"(enabled: {sorted(ICEBERG_ORPHAN_FILES_DELETE_ENVIRONMENTS)}). Run "
            "with delete=false to report."
        )
        raise ValueError(msg)

    glue, s3 = _aws_clients()
    result = remove_database_orphan_files(
        glue,
        get_glue_catalog,
        S3ObjectStore(s3),
        database=RAW_GLUE_DATABASE,
        min_age_days=config.min_age_days,
        now=datetime.now(UTC),
        delete=config.delete,
        workers=ICEBERG_ORPHAN_FILES_WORKERS,
        logger=context.log,
    )
    attempted = len(result.tables) + len(result.failures)
    if not attempted:
        msg = (
            f"Glue lists no Iceberg table in {RAW_GLUE_DATABASE}. Refusing to "
            "report success for zero work."
        )
        raise RuntimeError(msg)

    examined = result.examined
    if not examined:
        msg = (
            f"None of the {attempted} Iceberg tables in {RAW_GLUE_DATABASE} was "
            f"examined: {len(result.refused)} refused, {len(result.failures)} "
            f"failed. First reasons: "
            f"{[t.refused for t in result.refused[:3]] + result.failures[:3]}"
        )
        raise RuntimeError(msg)
    metadata: dict[str, Any] = {
        "glue_database": MetadataValue.text(RAW_GLUE_DATABASE),
        "min_age_days": MetadataValue.int(config.min_age_days),
        "tables_examined": MetadataValue.int(len(examined)),
        "objects_listed": MetadataValue.int(sum(t.objects_listed for t in examined)),
        "bytes_listed": MetadataValue.int(sum(t.bytes_listed for t in examined)),
        "orphan_objects": MetadataValue.int(sum(t.orphan_objects for t in examined)),
        "orphan_bytes": MetadataValue.int(sum(t.orphan_bytes for t in examined)),
        "eligible_objects": MetadataValue.int(
            sum(t.eligible_objects for t in examined)
        ),
        "eligible_bytes": MetadataValue.int(sum(t.eligible_bytes for t in examined)),
        "orphan_details": MetadataValue.json(_detail_rows(result)),
        # Left alone, each for the reason given. Their orphans are not counted
        # above, so the totals are a lower bound while any are listed.
        "tables_refused": MetadataValue.int(len(result.refused)),
        "refused_details": MetadataValue.json(
            [
                f"{table.table}: {table.refused}"
                for table in result.refused[:METADATA_DETAIL_ROWS]
            ]
        ),
        "failure_count": MetadataValue.int(len(result.failures)),
        "failure_details": MetadataValue.json(result.failures[:METADATA_DETAIL_ROWS]),
        # Databases this environment's role may not read. Their tables were not
        # checked for overlap with the raw tables, so the pass refuses to
        # delete with any listed here.
        "unreadable_glue_databases": MetadataValue.json(result.unreadable_databases),
    }

    delete_errors = [error for t in examined for error in t.delete_errors]
    if config.delete:
        metadata |= {
            "delete_status": MetadataValue.text("ran"),
            "deleted_objects": MetadataValue.int(
                sum(t.deleted_objects or 0 for t in examined)
            ),
            "deleted_bytes": MetadataValue.int(
                sum(t.deleted_bytes or 0 for t in examined)
            ),
            "delete_error_count": MetadataValue.int(len(delete_errors)),
        }
    else:
        # No deleted_* keys in a report run. A zero there would read as "ran
        # and found nothing to delete".
        metadata["delete_status"] = MetadataValue.text("not run (report only)")

    # Raising drops the metadata above. The pass has already logged every table
    # it failed on or deleted from to context.log, so the run's log is the
    # record.
    threshold = maintenance_failure_threshold(attempted)
    if len(result.failures) >= threshold:
        msg = (
            f"The orphan-file pass failed for {len(result.failures)}/{attempted} "
            f"tables (threshold {threshold}). First failures: "
            f"{result.failures[:5]}"
        )
        raise RuntimeError(msg)
    if delete_errors:
        msg = (
            f"{len(delete_errors)} objects could not be deleted across "
            f"{sum(1 for t in examined if t.delete_errors)} tables. First errors: "
            f"{delete_errors[:5]}"
        )
        raise RuntimeError(msg)
    return Output(value=None, metadata=metadata)
