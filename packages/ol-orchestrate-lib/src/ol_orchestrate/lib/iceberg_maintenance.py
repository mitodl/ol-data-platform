"""Iceberg table maintenance utilities for the OL data lakehouse.

This module provides the building blocks for the two nightly maintenance assets:

- ``iceberg_dbt_layer_maintenance``: OPTIMIZE + ANALYZE (Trino) + EXPIRE + ORPHAN
  removal (pyiceberg) for all dbt-managed tables and non-dbt singletons.
- ``iceberg_raw_layer_maintenance``: EXPIRE + ORPHAN removal (pyiceberg) for the
  1,300+ Airbyte-ingested tables in ``ol_warehouse_production_raw``.

Three sources of truth are used deliberately — each layer of the lakehouse has
a natural authoritative registry, and we use each one directly:

- dbt-managed tables  → ``manifest.json`` + Dagster materialization event log
- Raw / Airbyte tables → Glue catalog live scan + Iceberg snapshot timestamps
- Non-dbt singletons  → ``NON_DBT_SINGLETON_TABLES`` module-level constant

The Airbyte layer cannot use the Dagster event log for timing because
``OLAirbyteTranslator`` prefixes asset keys with ``ol_warehouse_raw_data`` and
sluggifies connection names — there is no reliable programmatic mapping from
those asset keys back to Glue table names like
``raw__mitxonline__openedx__api__course_blocks``.  The Iceberg snapshot
timestamp is more accurate anyway: it records exactly when Airbyte last
committed data.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from pyiceberg.catalog.glue import GlueCatalog

log = logging.getLogger(__name__)

AWS_REGION = "us-east-1"


# ── Configuration Dataclasses ─────────────────────────────────────────────────


@dataclass(frozen=True)
class TableMaintenanceConfig:
    """Maintenance configuration for a single dbt model or non-dbt singleton.

    ``asset_key`` mirrors the Dagster AssetKey used by ``DbtAutomationTranslator``
    (group = config.schema, name = model name from unique_id). This is used to
    query the Dagster materialization event log.
    """

    model_name: str
    schema_name: str  # full Glue/Trino schema, e.g. "ol_warehouse_production_mart"
    materialized: str  # "table" or "incremental"
    asset_key: list[str]  # Dagster AssetKey path components
    enabled: bool = True
    snapshot_retention_days: int = 7
    orphan_retention_days: int = 7
    # Run OPTIMIZE after this many materializations since last maintenance
    optimize_after_every_n_runs: int = 1
    # Run ANALYZE after this many materializations since last maintenance
    analyze_after_every_n_runs: int = 7


@dataclass(frozen=True)
class RawLayerGroupConfig:
    """Maintenance config for a raw-layer source group.

    OPTIMIZE and ANALYZE are intentionally omitted: raw tables are not Trino
    analytics targets and Airbyte writes complete files per sync.
    """

    snapshot_retention_days: int = 7
    orphan_retention_days: int = 7


@dataclass
class RawLayerTableInfo:
    """Metadata about a raw-layer Iceberg table, populated during the catalog scan."""

    table_name: str
    database: str
    snapshot_count: int
    eligible_snapshot_count: int
    latest_snapshot_timestamp_ms: int | None = None


# ── Constants ─────────────────────────────────────────────────────────────────

# Source-group-level overrides for raw-layer maintenance.
# Key = table name prefix (matched with str.startswith).
# "_default" is the fallback for tables not matching any prefix.
RAW_LAYER_GROUP_CONFIGS: dict[str, RawLayerGroupConfig] = {
    # High-frequency syncs (6-12 h interval) -- shorter retention to control growth
    "raw__mitxonline__app": RawLayerGroupConfig(snapshot_retention_days=3),
    "raw__xpro__app": RawLayerGroupConfig(snapshot_retention_days=3),
    "raw__mitlearn__app": RawLayerGroupConfig(snapshot_retention_days=3),
    "raw__learn_ai__app": RawLayerGroupConfig(snapshot_retention_days=3),
    "raw__ocw__studio": RawLayerGroupConfig(snapshot_retention_days=3),
    # External / third-party — 14 days for audit and reprocessing window
    "raw__thirdparty__salesforce": RawLayerGroupConfig(snapshot_retention_days=14),
    "raw__thirdparty__zendesk_support": RawLayerGroupConfig(snapshot_retention_days=14),
    # Fallback for all other raw__ groups
    "_default": RawLayerGroupConfig(snapshot_retention_days=7),
}

# Tables written outside dbt and Airbyte that still need Iceberg maintenance.
# Add new entries here as additional inference pipelines or ad-hoc writers land.
NON_DBT_SINGLETON_TABLES: list[TableMaintenanceConfig] = [
    TableMaintenanceConfig(
        model_name="student_risk_probability",
        schema_name="ol_warehouse_production_reporting",
        materialized="table",
        # Asset key matches the Dagster asset in the student_risk_probability
        # code location.
        asset_key=["reporting", "student_risk_probability"],
        snapshot_retention_days=7,
        orphan_retention_days=7,
        optimize_after_every_n_runs=1,
        analyze_after_every_n_runs=7,
    ),
]


# ── Catalog Factory ───────────────────────────────────────────────────────────


def get_glue_catalog(region: str = AWS_REGION) -> GlueCatalog:
    """Return a configured pyiceberg GlueCatalog backed by boto3.

    This is the single factory used by both the maintenance library and
    ``glue_helper.get_dbt_model_as_dataframe``.  When the latter is refactored,
    it should delegate here.
    """
    return GlueCatalog(
        "default",
        client=boto3.client("glue", region_name=region),
        **{
            "s3.connect-timeout": "10",
            "s3.request-timeout": "120",
        },
    )


# ── Maintenance Operations ────────────────────────────────────────────────────


def expire_snapshots(
    catalog: GlueCatalog,
    database: str,
    table_name: str,
    retention_days: int,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Expire old Iceberg snapshots for a table via pyiceberg.

    Uses the pyiceberg >= 0.10.0 API:
        ``table.maintenance.expire_snapshots().older_than(cutoff_dt).commit()``

    Returns a result dict with the following keys:

    - ``skipped`` (bool): True if no action was taken.
    - ``dry_run`` (bool, optional): Present when dry_run=True.
    - ``reason`` (str, optional): Why the operation was skipped.
    - ``total_snapshots`` (int): Total snapshot count before expiry.
    - ``eligible_count`` (int): Number of snapshots that qualified for expiry.
    - ``error`` (str, optional): Exception message on failure.
    """
    cutoff_dt = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(
        days=retention_days
    )
    cutoff_ms = int(cutoff_dt.timestamp() * 1000)

    try:
        table = catalog.load_table(f"{database}.{table_name}")
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Could not load %s.%s for snapshot expiry: %s", database, table_name, exc
        )
        return {"skipped": True, "error": str(exc)}

    snapshots = table.metadata.snapshots
    current_id = table.metadata.current_snapshot_id
    eligible = [
        s
        for s in snapshots
        if s.snapshot_id != current_id and s.timestamp_ms < cutoff_ms
    ]

    if dry_run:
        return {
            "skipped": True,
            "dry_run": True,
            "total_snapshots": len(snapshots),
            "eligible_count": len(eligible),
        }

    if not eligible:
        return {
            "skipped": True,
            "reason": "no eligible snapshots",
            "total_snapshots": len(snapshots),
            "eligible_count": 0,
        }

    try:
        table.maintenance.expire_snapshots().older_than(cutoff_dt).commit()
    except Exception as exc:  # noqa: BLE001
        log.warning("expire_snapshots failed for %s.%s: %s", database, table_name, exc)
        return {
            "skipped": True,
            "error": str(exc),
            "total_snapshots": len(snapshots),
            "eligible_count": len(eligible),
        }

    return {
        "skipped": False,
        "total_snapshots": len(snapshots),
        "eligible_count": len(eligible),
    }


def remove_orphan_files(
    catalog: GlueCatalog,
    database: str,
    table_name: str,
    retention_days: int,  # noqa: ARG001 (reserved for future implementation)
    *,
    dry_run: bool = False,  # noqa: ARG001 (reserved for future implementation)
) -> dict[str, Any]:
    """Remove orphan S3 files for a table — files not referenced by any snapshot.

    pyiceberg 0.11.x does not yet expose a built-in ``remove_orphan_files()``
    API on ``Table``.  This function is implemented as a graceful no-op stub that
    logs a notice and returns ``{"skipped": True, "reason": "not_implemented"}``.

    When pyiceberg adds orphan-file removal (tracked upstream), replace this
    stub body with:
        table.remove_orphan_files().older_than(cutoff_dt).execute()

    Callers should handle ``skipped=True`` without treating it as an error.
    """
    try:
        # Validate the table exists and is loadable before returning.
        catalog.load_table(f"{database}.{table_name}")
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Could not load %s.%s for orphan removal: %s", database, table_name, exc
        )
        return {"skipped": True, "error": str(exc)}

    log.debug(
        "remove_orphan_files: skipped for %s.%s — not yet implemented in pyiceberg %s",
        database,
        table_name,
        _pyiceberg_version(),
    )
    return {
        "skipped": True,
        "reason": "not_implemented",
        "note": (
            "pyiceberg does not yet expose a remove_orphan_files() API on Table. "
            "See https://github.com/apache/iceberg-python for upstream status."
        ),
    }


def _pyiceberg_version() -> str:
    try:
        import pyiceberg  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        return "unknown"
    else:
        return pyiceberg.__version__


# ── Manifest Parsing ──────────────────────────────────────────────────────────


def load_maintenance_configs_from_manifest(
    manifest_path: str | Path,
) -> list[TableMaintenanceConfig]:
    """Parse dbt ``manifest.json`` and return a ``TableMaintenanceConfig`` per model.

    Only models whose ``iceberg_maintenance`` config has been compiled into
    ``config.meta`` are included.  This makes ``dbt_project.yml`` the single
    source of truth for maintenance defaults: models without the key are
    skipped rather than having Python-side defaults applied silently, which
    would diverge from the dbt config over time.

    The Glue/Trino schema name is read directly from ``node['schema']``, which
    dbt fully resolves at compile time (e.g. ``ol_warehouse_production_mart``).
    Using the resolved value is safer than reconstructing it from
    ``config.schema`` + an env prefix, because it reflects the actual target
    the manifest was compiled against.

    The Dagster AssetKey path is ``[config.schema, model_name]`` to match
    ``DbtAutomationTranslator.get_group_name``, which returns ``config.schema``
    (the bare schema suffix, e.g. ``mart``).
    """
    import json  # noqa: PLC0415

    manifest = json.loads(Path(manifest_path).read_text())
    nodes = manifest.get("nodes", {})

    configs: list[TableMaintenanceConfig] = []
    for unique_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue

        materialized = node.get("config", {}).get("materialized", "table")
        if materialized not in ("table", "incremental"):
            continue  # skip view, ephemeral, seed-backed models

        # Use the fully-resolved schema from the manifest node (e.g.
        # "ol_warehouse_production_mart"), not config.schema (bare suffix).
        schema_name = node.get("schema", "")
        if not schema_name:
            log.debug("Skipping %s — no schema on manifest node", unique_id)
            continue

        # config.schema is the bare suffix (e.g. "mart") used as the Dagster
        # asset group name by DbtAutomationTranslator.get_group_name.
        schema_suffix = node.get("config", {}).get("schema", "")
        model_name = unique_id.split(".")[-1]

        # dbt_project.yml is the source of truth.  Only include models whose
        # iceberg_maintenance meta was compiled into the manifest.  Skip models
        # without it so Python never silently falls back to stale defaults.
        node_meta = node.get("config", {}).get("meta", {})
        iceberg_cfg = node_meta.get("iceberg_maintenance")
        if iceberg_cfg is None:
            log.debug(
                "Skipping %s — no iceberg_maintenance in compiled meta", model_name
            )
            continue

        if not iceberg_cfg.get("enabled", True):
            log.debug("Skipping %s — iceberg_maintenance.enabled=false", model_name)
            continue

        configs.append(
            TableMaintenanceConfig(
                model_name=model_name,
                schema_name=schema_name,
                materialized=materialized,
                # AssetKey: [schema_suffix, model_name] per DbtAutomationTranslator
                asset_key=[schema_suffix, model_name],
                enabled=iceberg_cfg.get("enabled", True),
                snapshot_retention_days=iceberg_cfg["snapshot_retention_days"],
                orphan_retention_days=iceberg_cfg["orphan_retention_days"],
                optimize_after_every_n_runs=iceberg_cfg["optimize_after_every_n_runs"],
                analyze_after_every_n_runs=iceberg_cfg["analyze_after_every_n_runs"],
            )
        )

    log.info("Loaded %d dbt model maintenance configs from manifest", len(configs))
    return configs


# ── Raw Layer ─────────────────────────────────────────────────────────────────


def raw_config_for_table(table_name: str) -> RawLayerGroupConfig:
    """Return the ``RawLayerGroupConfig`` for *table_name* by prefix match.

    Iterates ``RAW_LAYER_GROUP_CONFIGS`` in insertion order; the first matching
    prefix wins.  Falls back to ``_default`` if no prefix matches.
    """
    for prefix, config in RAW_LAYER_GROUP_CONFIGS.items():
        if prefix == "_default":
            continue
        if table_name.startswith(prefix):
            return config
    return RAW_LAYER_GROUP_CONFIGS["_default"]


def load_raw_layer_maintenance_work(
    glue_database: str,
    region: str = AWS_REGION,
) -> list[RawLayerTableInfo]:
    """Scan the Glue catalog and return RawLayerTableInfo sorted by eligible snapshots.

    This is a read-only inspection — it does not run any maintenance.  Pass the
    returned list to the maintenance asset for parallel processing.

    Tables are sorted descending by ``eligible_snapshot_count`` so the worst
    offenders are processed first (fail-fast semantics for long-running jobs).

    The per-table snapshot eligibility is computed using each table's
    ``RawLayerGroupConfig.snapshot_retention_days`` from ``RAW_LAYER_GROUP_CONFIGS``.
    """
    glue_client = boto3.client("glue", region_name=region)
    catalog = get_glue_catalog(region=region)

    # ── 1. Enumerate Iceberg tables via Glue paginator ──────────────────────
    iceberg_table_names: list[str] = []
    paginator = glue_client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=glue_database):
        for table in page.get("TableList", []):
            params = table.get("Parameters", {})
            if params.get("table_type", "").upper() == "ICEBERG":
                iceberg_table_names.append(table["Name"])

    log.info("Found %d Iceberg tables in %s", len(iceberg_table_names), glue_database)

    # ── 2. Load each table's snapshot metadata ────────────────────────────
    now_ms = int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)
    results: list[RawLayerTableInfo] = []

    for table_name in iceberg_table_names:
        group_cfg = raw_config_for_table(table_name)
        cutoff_ms = now_ms - int(group_cfg.snapshot_retention_days * 86_400 * 1_000)

        try:
            table = catalog.load_table(f"{glue_database}.{table_name}")
            snapshots = table.metadata.snapshots
            current_id = table.metadata.current_snapshot_id

            eligible = [
                s
                for s in snapshots
                if s.snapshot_id != current_id and s.timestamp_ms < cutoff_ms
            ]
            latest_ts = max((s.timestamp_ms for s in snapshots), default=None)

            results.append(
                RawLayerTableInfo(
                    table_name=table_name,
                    database=glue_database,
                    snapshot_count=len(snapshots),
                    eligible_snapshot_count=len(eligible),
                    latest_snapshot_timestamp_ms=latest_ts,
                )
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not inspect %s.%s: %s", glue_database, table_name, exc)

    # Worst offenders first so they are processed before a potential timeout
    results.sort(key=lambda t: t.eligible_snapshot_count, reverse=True)
    log.info(
        "Snapshot scan complete: %d tables inspected, %d total snapshots eligible",
        len(results),
        sum(t.eligible_snapshot_count for t in results),
    )
    return results
