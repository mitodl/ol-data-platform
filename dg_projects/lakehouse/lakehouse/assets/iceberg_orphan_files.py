"""Dagster assets that remove orphan files from inside live Iceberg tables.

Snapshot expiry (``iceberg_raw_layer_maintenance`` and
``iceberg_dbt_layer_maintenance``) only rewrites a table's metadata: pyiceberg
deletes none of the files the expired snapshots referenced. ``lake_orphan_sweep``
removes whole directories no Glue table references, so it never looks inside a
table that still exists. These assets are the pass in between. For each Iceberg
table in their databases they list the table's directory and compare it with
what the table's metadata can still reach (see
:mod:`ol_orchestrate.lib.iceberg_orphan_files`).

- ``iceberg_raw_orphan_files`` covers the raw database. It deletes where
  :data:`ICEBERG_ORPHAN_FILES_DELETE_ENVIRONMENTS` says so (QA and production).
  The raw buckets are versioned with a 90-day noncurrent expiry.
- ``iceberg_dbt_layer_orphan_files`` covers the dbt-layer databases in
  :data:`DBT_LAYER_GLUE_DATABASES`. A ``table`` materialization moves to a new
  directory on every build and the sweep collects the old one, but an
  ``incremental`` model keeps its directory, so its expired snapshots' files
  stay inside it (14.2 TB eligible in production staging, measured
  2026-11-16). It deletes where
  :data:`ICEBERG_DBT_LAYER_ORPHAN_FILES_DELETE_ENVIRONMENTS` says so, and that
  set is empty until a person has read an in-cluster report. The staging bucket
  is versioned with a 30-day noncurrent expiry.

Both report everywhere they run. An environment is added to a delete set after
a person has read a report for it.
"""

import os
from datetime import UTC, datetime
from typing import Any

import boto3
from dagster import (
    AssetExecutionContext,
    Config,
    DagsterInstance,
    MetadataValue,
    Output,
    asset,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.iceberg_maintenance import (
    get_glue_catalog,
    maintenance_failure_threshold,
    warehouse_env_for,
)
from ol_orchestrate.lib.iceberg_orphan_files import (
    DatabaseOrphanFiles,
    S3DeletionManifests,
    S3ObjectStore,
    TableOrphanFiles,
    remove_database_orphan_files,
)

WAREHOUSE_ENV = warehouse_env_for(DAGSTER_ENV)
RAW_GLUE_DATABASE = f"ol_warehouse_{WAREHOUSE_ENV}_raw"

# Every dbt layer whose tables live in a Glue database of its own. Not history:
# its tables are raw snapshots that cannot be rebuilt, and the pass refuses to
# delete from it. A layer the environment lacks is examined as zero tables.
DBT_LAYERS = (
    "staging",
    "intermediate",
    "dimensional",
    "mart",
    "reporting",
    "external",
    "integrations",
    "migration",
)
DBT_LAYER_GLUE_DATABASES = tuple(
    f"ol_warehouse_{WAREHOUSE_ENV}_{layer}" for layer in DBT_LAYERS
)

# Keyed on the Dagster environment, not the warehouse one: `dev` resolves to the
# production warehouse, and a laptop must never be able to delete from it.
ICEBERG_ORPHAN_FILES_DELETE_ENVIRONMENTS: frozenset[str] = frozenset(
    {"qa", "production"}
)
ICEBERG_DBT_LAYER_ORPHAN_FILES_DELETE_ENVIRONMENTS: frozenset[str] = frozenset()

# The floor has to exceed the longest time a writer holds a file before the
# commit that references it. A delete run refuses to start unless the Dagster
# instance's run_monitoring cap is on and below it. Airbyte's cap (airbyte-cron's
# SYNC_WORKLOAD_TIMEOUT, in ol-infrastructure) can't be read from here. Neither
# bounds a run tagged with a longer dagster/max_runtime or a writer outside
# Dagster and Airbyte (e.g. StarRocks writing a dbt-layer table). Seven days is
# the floor the sweep uses.
ICEBERG_ORPHAN_FILES_MIN_AGE_DAYS = 7
ICEBERG_ORPHAN_FILES_MIN_AGE_SECONDS = ICEBERG_ORPHAN_FILES_MIN_AGE_DAYS * 24 * 60 * 60

# Each worker holds one table's reachable-file set and its eligible orphans in
# memory, so this bounds memory as much as it bounds S3 and Glue request rates.
ICEBERG_ORPHAN_FILES_WORKERS = 4

# Under the Dagster bucket, not the lake: a prefix in a lake bucket that no Glue
# table references is what lake_orphan_sweep reports. One run's manifests sit
# under <prefix>/<run_id>/, one object per table.
DELETION_MANIFEST_PREFIX = "iceberg-orphan-files/deletions"

# Enough rows to review in the Dagster UI without making the event too large to
# render. The counts above it are never capped.
METADATA_DETAIL_ROWS = 200


class IcebergOrphanFilesConfig(Config):
    """Run configuration for the orphan-file assets."""

    # No default on purpose: a floor nobody chose is how this deletes a file a
    # writer was about to commit. The pass refuses less than 1.
    min_age_days: int
    delete: bool = False


def _aws_clients() -> tuple[Any, Any]:
    """Return the Glue and S3 clients the pass runs with."""
    return boto3.client("glue"), boto3.client("s3")


def _run_cap_seconds(instance: DagsterInstance) -> int:
    """Return the instance's default run time limit, or 0 when nothing enforces one.

    The limit only applies while run monitoring is enabled: the daemon's
    monitoring loop is what terminates a run that exceeds it.
    """
    if not instance.run_monitoring_enabled:
        return 0
    return instance.run_monitoring_max_runtime_seconds


def _detail_rows(examined: list[TableOrphanFiles]) -> list[dict[str, Any]]:
    rows = sorted(
        (table for table in examined if table.orphan_objects),
        key=lambda table: -table.eligible_bytes,
    )
    return [
        {
            "database": table.database,
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


def _log_summary(
    context: AssetExecutionContext,
    result: DatabaseOrphanFiles,
    manifests: S3DeletionManifests | None,
) -> None:
    """Log one database's totals.

    The metadata is only in the event log, and every raise in the caller drops
    it. This line, and the pass's own per-table lines (every table it failed on
    or deleted from), reach the compute log either way.
    """
    examined = result.examined
    delete_errors = sum(len(t.delete_errors) for t in examined)
    context.log.info(
        "%s: %d tables examined, %d refused, %d failed. Orphan %d objects "
        "(%d bytes), eligible %d objects (%d bytes). Delete: %s. Unreadable "
        "Glue databases: %s.",
        result.database,
        len(examined),
        len(result.refused),
        len(result.failures),
        sum(t.orphan_objects for t in examined),
        sum(t.orphan_bytes for t in examined),
        sum(t.eligible_objects for t in examined),
        sum(t.eligible_bytes for t in examined),
        (
            f"ran, {sum(t.deleted_objects or 0 for t in examined)} objects "
            f"deleted, {sum(t.deleted_bytes or 0 for t in examined)} bytes sent "
            f"for deletion, error count {delete_errors}, keys under "
            f"{manifests.uri}"
            if manifests is not None
            else "not run (report only)"
        ),
        sorted(result.unreadable_databases),
    )


def _orphan_file_pass(
    context: AssetExecutionContext,
    config: IcebergOrphanFilesConfig,
    *,
    databases: tuple[str, ...],
    delete_environments: frozenset[str],
) -> Output[None]:
    """Report, and where enabled delete, orphan files in ``databases``' tables."""
    if config.delete and DAGSTER_ENV not in delete_environments:
        msg = (
            f"Deletion is not enabled for the {DAGSTER_ENV} environment "
            f"(enabled: {sorted(delete_environments)}). Run with delete=false to "
            "report."
        )
        raise ValueError(msg)
    if config.delete and config.min_age_days < ICEBERG_ORPHAN_FILES_MIN_AGE_DAYS:
        msg = (
            f"A delete run needs min_age_days of at least "
            f"{ICEBERG_ORPHAN_FILES_MIN_AGE_DAYS}, got {config.min_age_days}. A "
            "report can use less."
        )
        raise ValueError(msg)
    run_cap = _run_cap_seconds(context.instance)
    if config.delete and not 0 < run_cap < ICEBERG_ORPHAN_FILES_MIN_AGE_SECONDS:
        msg = (
            "A delete run needs the Dagster instance to cap runs below the "
            f"{ICEBERG_ORPHAN_FILES_MIN_AGE_SECONDS}s floor (run_monitoring enabled "
            f"with max_runtime_seconds set), got {run_cap}. A report can run "
            "without one."
        )
        raise ValueError(msg)

    glue, s3 = _aws_clients()
    manifests = (
        S3DeletionManifests(
            s3,
            bucket=os.environ["DAGSTER_BUCKET_NAME"],
            prefix=f"{DELETION_MANIFEST_PREFIX}/{context.run_id}",
        )
        if config.delete
        else None
    )
    results: list[DatabaseOrphanFiles] = []
    for database in databases:
        result = remove_database_orphan_files(
            glue,
            get_glue_catalog,
            S3ObjectStore(s3),
            database=database,
            min_age_days=config.min_age_days,
            now=datetime.now(UTC),
            delete=config.delete,
            workers=ICEBERG_ORPHAN_FILES_WORKERS,
            record_deletion=manifests,
            logger=context.log,
        )
        _log_summary(context, result, manifests)
        results.append(result)

    named = ", ".join(databases)
    examined = [t for result in results for t in result.examined]
    refused = [t for result in results for t in result.refused]
    # The pass names only the table, so add the database it was in.
    failures = [
        f"{result.database}.{failure}"
        for result in results
        for failure in result.failures
    ]
    unreadable = sorted({d for result in results for d in result.unreadable_databases})
    attempted = sum(len(result.tables) + len(result.failures) for result in results)
    delete_errors = [error for t in examined for error in t.delete_errors]

    if not attempted:
        msg = (
            f"Glue lists no Iceberg table in {named}. Refusing to report success "
            "for zero work."
        )
        raise RuntimeError(msg)
    if not examined:
        msg = (
            f"None of the {attempted} Iceberg tables in {named} was examined: "
            f"{len(refused)} refused, {len(failures)} failed. First reasons: "
            f"{[t.refused for t in refused[:3]] + failures[:3]}"
        )
        raise RuntimeError(msg)
    metadata: dict[str, Any] = {
        "glue_database": MetadataValue.text(named),
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
        "orphan_details": MetadataValue.json(_detail_rows(examined)),
        # Left alone, each for the reason given. Their orphans are not counted
        # above, so the totals are a lower bound while any are listed.
        "tables_refused": MetadataValue.int(len(refused)),
        "refused_details": MetadataValue.json(
            [
                f"{table.database}.{table.table}: {table.refused}"
                for table in refused[:METADATA_DETAIL_ROWS]
            ]
        ),
        "failure_count": MetadataValue.int(len(failures)),
        "failure_details": MetadataValue.json(failures[:METADATA_DETAIL_ROWS]),
        # Databases this environment's role may not read. Their tables were not
        # checked for overlap with the tables examined, so the pass refuses to
        # delete with any listed here.
        "unreadable_glue_databases": MetadataValue.json(unreadable),
    }
    if manifests is not None:
        metadata |= {
            "delete_status": MetadataValue.text("ran"),
            "deletion_manifests": MetadataValue.path(manifests.uri),
            "deleted_objects": MetadataValue.int(
                sum(t.deleted_objects or 0 for t in examined)
            ),
            # Bytes a delete was sent for, including objects whose delete failed.
            "deleted_bytes": MetadataValue.int(
                sum(t.deleted_bytes or 0 for t in examined)
            ),
            "delete_error_count": MetadataValue.int(len(delete_errors)),
        }
    else:
        # No deleted_* keys in a report run. A zero there would read as "ran
        # and found nothing to delete".
        metadata["delete_status"] = MetadataValue.text("not run (report only)")

    threshold = maintenance_failure_threshold(attempted)
    if len(failures) >= threshold:
        msg = (
            f"The orphan-file pass failed for {len(failures)}/{attempted} "
            f"tables (threshold {threshold}). First failures: {failures[:5]}"
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
    return _orphan_file_pass(
        context,
        config,
        databases=(RAW_GLUE_DATABASE,),
        delete_environments=ICEBERG_ORPHAN_FILES_DELETE_ENVIRONMENTS,
    )


@asset(
    group_name="lakehouse_maintenance",
    description=(
        "Finds files inside this environment's dbt-layer Iceberg table "
        "directories that no retained snapshot references (mostly incremental "
        "models' expired snapshots), and deletes the ones older than the "
        "configured floor where deletion is enabled for the environment."
    ),
)
def iceberg_dbt_layer_orphan_files(
    context: AssetExecutionContext, config: IcebergOrphanFilesConfig
) -> Output[None]:
    """Report, and where enabled delete, orphan files in dbt-layer Iceberg tables."""
    return _orphan_file_pass(
        context,
        config,
        databases=DBT_LAYER_GLUE_DATABASES,
        delete_environments=ICEBERG_DBT_LAYER_ORPHAN_FILES_DELETE_ENVIRONMENTS,
    )
