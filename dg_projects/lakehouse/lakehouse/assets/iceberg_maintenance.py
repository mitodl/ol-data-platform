"""Dagster assets for Iceberg table maintenance across the OL data lakehouse.

Two assets run on staggered nightly schedules:

``iceberg_dbt_layer_maintenance`` (02:00 UTC)
    Processes all dbt-managed tables discovered from ``manifest.json`` plus the
    non-dbt singleton tables listed in ``NON_DBT_SINGLETON_TABLES``.

    For each table it runs:
    - OPTIMIZE  (Trino)   — compact small files; triggered when materialization
                            count since last maintenance >= optimize_after_every_n_runs
    - ANALYZE   (Trino)   — refresh query statistics; same threshold logic with
                            analyze_after_every_n_runs
    - EXPIRE SNAPSHOTS (pyiceberg) — always runs; uses snapshot_retention_days
    - REMOVE ORPHAN FILES (pyiceberg) — always runs; uses orphan_retention_days

    Config comes from ``dbt_project.yml`` ``+meta.iceberg_maintenance`` blocks,
    compiled into each node's ``config.meta.iceberg_maintenance`` in the manifest.
    Per-model overrides in schema YAML files are shallow-merged on top.

``iceberg_raw_layer_maintenance`` (03:00 UTC)
    Processes all Iceberg tables in ``ol_warehouse_production_raw`` (1,300+
    Airbyte-ingested tables).  Uses a live Glue catalog scan rather than the
    Dagster event log because the OLAirbyteTranslator slugifies connection names
    in a way that doesn't map cleanly back to Glue table names.

    Runs EXPIRE SNAPSHOTS and REMOVE ORPHAN FILES only — OPTIMIZE and ANALYZE
    are not needed for raw tables since they are not Trino analytics targets and
    Airbyte writes complete files per sync (no small-file accumulation).

    Tables are processed in a ThreadPoolExecutor(max_workers=8) to handle the
    volume without overwhelming the Glue API rate limits.
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DagsterEventType,
    EventRecordsFilter,
    MetadataValue,
    Output,
    asset,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.iceberg_maintenance import (
    NON_DBT_SINGLETON_TABLES,
    TableMaintenanceConfig,
    expire_snapshots,
    get_glue_catalog,
    load_maintenance_configs_from_manifest,
    load_raw_layer_maintenance_work,
    raw_config_for_table,
    remove_orphan_files,
)
from ol_orchestrate.resources.trino_maintenance import TrinoMaintenanceResource
from pyiceberg.catalog.glue import GlueCatalog

from lakehouse.assets.lakehouse.dbt import dbt_project

log = logging.getLogger(__name__)

# Each environment runs maintenance only against its own catalog and schema set.
# dev uses production because DAGSTER_ENV="dev" targets the production dbt profile
# (dev_production), meaning developers run against real production Iceberg tables.
_RAW_GLUE_DATABASE_MAP: dict[str, str] = {
    "production": "ol_warehouse_production_raw",
    "qa": "ol_warehouse_qa_raw",
    "dev": "ol_warehouse_production_raw",
    "ci": "ol_warehouse_qa_raw",
}

RAW_GLUE_DATABASE = _RAW_GLUE_DATABASE_MAP.get(
    DAGSTER_ENV, "ol_warehouse_production_raw"
)
# Parallelism for raw layer: enough to be fast across 1,300+ tables without
# hitting Glue API rate limits (default per-account limit is ~200 req/s).
RAW_LAYER_WORKERS = 8


# ── Helpers ───────────────────────────────────────────────────────────────────


def _get_last_maintenance_cursor(
    instance, maintenance_asset_key: AssetKey
) -> int | None:
    """Return the storage_id of the most recent maintenance materialization.

    This cursor is passed to ``_count_materializations_since`` so we only count
    dbt model materializations that occurred *after* the previous maintenance run.
    Returns ``None`` on the first ever run, which the caller treats as "run all
    operations unconditionally".
    """
    records = instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=maintenance_asset_key,
        ),
        limit=1,
        ascending=False,
    )
    return records[0].storage_id if records else None


def _count_materializations_since(
    instance,
    asset_key: AssetKey,
    after_cursor: int | None,
) -> int:
    """Count how many times *asset_key* was materialized since *after_cursor*.

    Up to 500 records are fetched — enough to saturate any reasonable
    optimize_after_every_n_runs or analyze_after_every_n_runs threshold.
    """
    if after_cursor is not None:
        records = instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key,
                after_cursor=after_cursor,
            ),
            limit=500,
            ascending=False,
        )
    else:
        records = instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key,
            ),
            limit=500,
            ascending=False,
        )
    return len(records)


def _run_table_maintenance(
    cfg: TableMaintenanceConfig,
    instance,
    last_cursor: int | None,
    trino_maintenance: TrinoMaintenanceResource,
    catalog: GlueCatalog,
) -> dict[str, Any]:
    """Run all maintenance operations for one dbt-layer table.

    The shared ``catalog`` is created once by the asset and reused across all
    tables: constructing a fresh GlueCatalog (and its S3 FileIO) per table is
    both wasteful and a known trigger for native-client hangs.

    Returns a summary dict used to aggregate the asset's output metadata.
    """
    summary: dict[str, Any] = {
        "model": cfg.model_name,
        "optimized": False,
        "analyzed": False,
        "snapshots_expired": 0,
        "orphans_removed": 0,
        "errors": [],
    }

    model_key = AssetKey(cfg.asset_key)
    mat_count = _count_materializations_since(instance, model_key, last_cursor)

    # On the first maintenance run (last_cursor is None) treat effective count
    # as the maximum threshold so every operation runs regardless of history.
    if last_cursor is None:
        effective_count = max(
            cfg.optimize_after_every_n_runs,
            cfg.analyze_after_every_n_runs,
            1,
        )
    else:
        effective_count = mat_count

    # EXPIRE SNAPSHOTS — always, uses its own time-based filter
    try:
        exp = expire_snapshots(
            catalog=catalog,
            database=cfg.schema_name,
            table_name=cfg.model_name,
            retention_days=cfg.snapshot_retention_days,
        )
        if not exp.get("skipped"):
            summary["snapshots_expired"] = exp.get("eligible_count", 0)
    except Exception as exc:  # noqa: BLE001
        summary["errors"].append(f"expire_snapshots: {exc}")

    # REMOVE ORPHAN FILES — always, uses its own time-based filter
    try:
        orp = remove_orphan_files(
            catalog=catalog,
            database=cfg.schema_name,
            table_name=cfg.model_name,
            retention_days=cfg.orphan_retention_days,
        )
        if not orp.get("skipped"):
            summary["orphans_removed"] = orp.get("deleted-files", 0)
    except Exception as exc:  # noqa: BLE001
        summary["errors"].append(f"remove_orphan_files: {exc}")

    # OPTIMIZE — only if enough materializations have accumulated
    if effective_count >= cfg.optimize_after_every_n_runs:
        try:
            trino_maintenance.optimize(schema=cfg.schema_name, table=cfg.model_name)
            summary["optimized"] = True
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "OPTIMIZE failed for %s.%s: %s", cfg.schema_name, cfg.model_name, exc
            )
            summary["errors"].append(f"optimize: {exc}")

    # ANALYZE — only if enough materializations have accumulated
    if effective_count >= cfg.analyze_after_every_n_runs:
        try:
            trino_maintenance.analyze(schema=cfg.schema_name, table=cfg.model_name)
            summary["analyzed"] = True
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "ANALYZE failed for %s.%s: %s", cfg.schema_name, cfg.model_name, exc
            )
            summary["errors"].append(f"analyze: {exc}")

    return summary


# ── Assets ────────────────────────────────────────────────────────────────────


@asset(
    group_name="lakehouse_maintenance",
    description=(
        "Nightly Iceberg maintenance for all dbt-managed tables and non-dbt "
        "singletons.  Runs OPTIMIZE and ANALYZE via Trino (threshold-gated by "
        "materialization count) and EXPIRE SNAPSHOTS via pyiceberg.  Config is "
        "read from manifest.json iceberg_maintenance meta blocks."
    ),
)
def iceberg_dbt_layer_maintenance(
    context: AssetExecutionContext,
    trino_maintenance: TrinoMaintenanceResource,
) -> Output[None]:
    """Run Iceberg maintenance for all dbt-managed tables and non-dbt singletons."""
    configs = load_maintenance_configs_from_manifest(
        manifest_path=dbt_project.manifest_path,
    )
    all_configs = [*configs, *NON_DBT_SINGLETON_TABLES]
    context.log.info(
        "Loaded %d table configs (%d dbt, %d singleton)",
        len(all_configs),
        len(configs),
        len(NON_DBT_SINGLETON_TABLES),
    )

    # Cursor of the previous maintenance run — used to count dbt materializations
    # that occurred since the last time this asset ran.
    own_key = AssetKey(["iceberg_dbt_layer_maintenance"])
    last_cursor = _get_last_maintenance_cursor(context.instance, own_key)
    if last_cursor is None:
        context.log.info(
            "First maintenance run — all operations will execute unconditionally."
        )

    catalog = get_glue_catalog()

    tables_processed = 0
    tables_optimized = 0
    tables_analyzed = 0
    snapshots_expired = 0
    orphans_removed = 0
    failures: list[str] = []

    for cfg in all_configs:
        if not cfg.enabled:
            continue
        try:
            result = _run_table_maintenance(
                cfg, context.instance, last_cursor, trino_maintenance, catalog
            )
            tables_processed += 1
            if result["optimized"]:
                tables_optimized += 1
            if result["analyzed"]:
                tables_analyzed += 1
            snapshots_expired += result["snapshots_expired"]
            orphans_removed += result["orphans_removed"]
            failures.extend(
                f"{cfg.schema_name}.{cfg.model_name}: {err}" for err in result["errors"]
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "Maintenance failed for %s.%s: %s", cfg.schema_name, cfg.model_name, exc
            )
            failures.append(f"{cfg.schema_name}.{cfg.model_name}: {exc}")

    context.log.info(
        "dbt layer maintenance complete: %d tables processed, %d optimized, "
        "%d analyzed, %d snapshot batches expired, %d failures",
        tables_processed,
        tables_optimized,
        tables_analyzed,
        snapshots_expired,
        len(failures),
    )

    # Fail the asset if maintenance failures exceed 5% of tables processed.
    # This surfaces systemic failures (broken Trino credentials, Glue outage)
    # that would otherwise accumulate silently in metadata while Dagster marks
    # the run SUCCESS, advancing the cursor and blocking retries.
    if failures:
        failure_threshold = max(1, int(tables_processed * 0.05))
        if len(failures) >= failure_threshold:
            context.log.error(
                "Maintenance failed for %d/%d tables (threshold: %d). Failing asset.",
                len(failures),
                tables_processed,
                failure_threshold,
            )
            msg = (
                f"Iceberg maintenance failed for "
                f"{len(failures)}/{tables_processed} "
                f"tables (threshold {failure_threshold}). "
                f"First failures: {failures[:5]}"
            )
            raise RuntimeError(msg)

    return Output(
        value=None,
        metadata={
            "tables_processed": MetadataValue.int(tables_processed),
            "tables_optimized": MetadataValue.int(tables_optimized),
            "tables_analyzed": MetadataValue.int(tables_analyzed),
            "snapshots_expired": MetadataValue.int(snapshots_expired),
            "orphans_removed": MetadataValue.int(orphans_removed),
            "failure_count": MetadataValue.int(len(failures)),
            # Cap at 20 entries so the Dagster UI doesn't time out rendering
            "failure_details": MetadataValue.json(failures[:20]),
        },
    )


@asset(
    group_name="lakehouse_maintenance",
    description=(
        "Nightly Iceberg maintenance for all raw/Airbyte-ingested tables in "
        "ol_warehouse_production_raw (~1,300 tables).  Runs EXPIRE SNAPSHOTS and "
        "REMOVE ORPHAN FILES only — OPTIMIZE and ANALYZE are not needed for raw "
        "tables since they are not Trino analytics targets.  Tables are processed "
        "in parallel (max_workers=8) and sorted worst-offender-first."
    ),
)
def iceberg_raw_layer_maintenance(context: AssetExecutionContext) -> Output[None]:
    """Run EXPIRE SNAPSHOTS and REMOVE ORPHAN FILES for all raw layer Iceberg tables."""
    context.log.info("Scanning Glue catalog for raw layer Iceberg tables...")
    tables = load_raw_layer_maintenance_work(glue_database=RAW_GLUE_DATABASE)
    context.log.info(
        "Found %d Iceberg tables; processing with %d workers.",
        len(tables),
        RAW_LAYER_WORKERS,
    )

    tables_cleaned = 0
    snapshots_expired = 0
    orphans_removed = 0
    failures: list[str] = []

    # A GlueCatalog (and its underlying S3 FileIO / boto3 client) must not be
    # shared across worker threads: concurrent commits mutate catalog state and
    # the native client deadlocks under the nested ThreadPoolExecutor. Give each
    # of the RAW_LAYER_WORKERS threads its own catalog, created once and reused
    # for every table that thread processes.
    _thread_local = threading.local()

    def _worker_catalog():
        if not hasattr(_thread_local, "catalog"):
            _thread_local.catalog = get_glue_catalog()
        return _thread_local.catalog

    def _process_one(table_info) -> dict[str, Any]:
        cfg = raw_config_for_table(table_info.table_name)
        catalog = _worker_catalog()
        result: dict[str, Any] = {
            "table": table_info.table_name,
            "ok": True,
            "errors": [],
        }
        try:
            exp = expire_snapshots(
                catalog=catalog,
                database=table_info.database,
                table_name=table_info.table_name,
                retention_days=cfg.snapshot_retention_days,
            )
            result["expire"] = exp

            orp = remove_orphan_files(
                catalog=catalog,
                database=table_info.database,
                table_name=table_info.table_name,
                retention_days=cfg.orphan_retention_days,
            )
            result["orphan"] = orp
        except Exception as exc:  # noqa: BLE001
            result["ok"] = False
            result["errors"].append(str(exc))
        return result

    with ThreadPoolExecutor(max_workers=RAW_LAYER_WORKERS) as executor:
        future_to_table = {executor.submit(_process_one, t): t for t in tables}
        for future in as_completed(future_to_table):
            table_info = future_to_table[future]
            try:
                res = future.result()
                if res["ok"]:
                    tables_cleaned += 1
                    exp = res.get("expire", {})
                    orp = res.get("orphan", {})
                    snapshots_expired += exp.get("eligible_count", 0)
                    orphans_removed += orp.get("deleted-files", 0)
                else:
                    failures.extend(
                        f"{table_info.table_name}: {err}" for err in res["errors"]
                    )
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{table_info.table_name}: {exc}")

    context.log.info(
        "Raw layer maintenance complete: %d/%d tables cleaned, %d failures.",
        tables_cleaned,
        len(tables),
        len(failures),
    )

    # Fail the asset if maintenance failures exceed 5% of tables scanned.
    # Prevents silent SUCCESS when Glue/S3 is unavailable for a large fraction
    # of tables, which would advance the snapshot timestamp cursor.
    tables_processed = len(tables)
    if failures:
        failure_threshold = max(1, int(tables_processed * 0.05))
        if len(failures) >= failure_threshold:
            context.log.error(
                "Maintenance failed for %d/%d tables (threshold: %d). Failing asset.",
                len(failures),
                tables_processed,
                failure_threshold,
            )
            msg = (
                f"Iceberg raw layer maintenance failed for "
                f"{len(failures)}/{tables_processed} tables "
                f"(threshold {failure_threshold}). "
                f"First failures: {failures[:5]}"
            )
            raise RuntimeError(msg)

    return Output(
        value=None,
        metadata={
            "tables_scanned": MetadataValue.int(len(tables)),
            "tables_cleaned": MetadataValue.int(tables_cleaned),
            "snapshots_expired": MetadataValue.int(snapshots_expired),
            "orphans_removed": MetadataValue.int(orphans_removed),
            "failure_count": MetadataValue.int(len(failures)),
            "failure_details": MetadataValue.json(failures[:20]),
        },
    )
