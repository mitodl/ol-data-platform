"""Asset index — builds UUID-keyed lookups for Superset asset YAML files."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class DatasetAsset:
    """Superset dataset as parsed from assets/datasets/**/*.yaml."""

    uuid: str
    table_name: str
    schema: str
    catalog: str
    database: str  # subdirectory name, e.g. "Trino"
    columns: set[str] = field(default_factory=set)
    sql: str | None = None  # non-None means virtual dataset
    path: Path = field(default_factory=Path)


@dataclass
class ChartAsset:
    """Superset chart as parsed from assets/charts/*.yaml."""

    uuid: str
    name: str
    dataset_uuid: str | None
    # Plain column names referenced in chart params (SQL-expression cols excluded)
    column_refs: set[str] = field(default_factory=set)
    path: Path = field(default_factory=Path)


@dataclass
class DashboardAsset:
    """Superset dashboard as parsed from assets/dashboards/*.yaml."""

    uuid: str
    title: str
    chart_uuids: set[str] = field(default_factory=set)
    path: Path = field(default_factory=Path)


@dataclass
class AssetIndex:
    """UUID-keyed indexes for all local Superset assets."""

    datasets: dict[str, DatasetAsset] = field(default_factory=dict)
    charts: dict[str, ChartAsset] = field(default_factory=dict)
    dashboards: dict[str, DashboardAsset] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Column-reference extraction from chart params
# ---------------------------------------------------------------------------

_SIMPLE_COLUMN_KEYS = ("groupby", "x_axis")
"""Chart param keys whose values are plain column name strings."""


def _is_sql_expression(item: Any) -> bool:
    """Return True if item is an adhoc column dict with expressionType=SQL."""
    return isinstance(item, dict) and item.get("expressionType") in ("SQL", "CUSTOM")


def extract_chart_column_refs(params: dict[str, Any]) -> set[str]:
    """
    Extract plain column name references from chart params.

    SQL-expression columns (dicts with ``expressionType: SQL``) are skipped
    because they compute new values and don't reference raw dataset columns
    by name.

    Columns extracted from:
    - ``groupby`` — list of str
    - ``x_axis`` — str
    - ``all_columns`` — mixed list; only plain str items are included
    - ``adhoc_filters`` — subject field, only when expressionType is SIMPLE

    Args:
        params: Parsed chart params dict.

    Returns:
        Set of plain column name strings.
    """
    refs: set[str] = set()

    for key in _SIMPLE_COLUMN_KEYS:
        value = params.get(key)
        if isinstance(value, str) and value:
            refs.add(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item:
                    refs.add(item)

    # all_columns is a mixed list of str | dict
    for item in params.get("all_columns", []):
        if isinstance(item, str) and item:
            refs.add(item)
        # dicts are SQL expressions — skip

    # adhoc_filters: only SIMPLE type filters reference a column by name
    for filt in params.get("adhoc_filters", []):
        if not isinstance(filt, dict):
            continue
        if filt.get("expressionType") == "SIMPLE":
            subject = filt.get("subject")
            if isinstance(subject, str) and subject:
                refs.add(subject)

    return refs


# ---------------------------------------------------------------------------
# Asset loading
# ---------------------------------------------------------------------------


def _load_datasets(datasets_dir: Path) -> dict[str, DatasetAsset]:
    """Parse all dataset YAML files and return UUID-keyed dict."""
    result: dict[str, DatasetAsset] = {}
    if not datasets_dir.exists():
        return result

    for yaml_file in sorted(datasets_dir.rglob("*.yaml")):
        try:
            data: Any = yaml.safe_load(yaml_file.read_text())
        except Exception:  # noqa: BLE001, S112
            continue
        if not isinstance(data, dict):
            continue

        uuid = data.get("uuid")
        if not uuid:
            continue

        columns: set[str] = set()
        for col in data.get("columns", []):
            if isinstance(col, dict) and col.get("column_name"):
                columns.add(col["column_name"])

        raw_sql = data.get("sql")
        sql: str | None = (
            raw_sql if isinstance(raw_sql, str) and raw_sql.strip() else None
        )

        database = yaml_file.parent.name  # e.g. "Trino"

        result[uuid] = DatasetAsset(
            uuid=uuid,
            table_name=data.get("table_name", ""),
            schema=data.get("schema", ""),
            catalog=data.get("catalog", ""),
            database=database,
            columns=columns,
            sql=sql,
            path=yaml_file,
        )

    return result


def _load_charts(charts_dir: Path) -> dict[str, ChartAsset]:
    """Parse all chart YAML files and return UUID-keyed dict."""
    result: dict[str, ChartAsset] = {}
    if not charts_dir.exists():
        return result

    for yaml_file in sorted(charts_dir.glob("*.yaml")):
        try:
            data: Any = yaml.safe_load(yaml_file.read_text())
        except Exception:  # noqa: BLE001, S112
            continue
        if not isinstance(data, dict):
            continue

        uuid = data.get("uuid")
        if not uuid:
            continue

        params = data.get("params") or {}
        column_refs = (
            extract_chart_column_refs(params) if isinstance(params, dict) else set()
        )

        result[uuid] = ChartAsset(
            uuid=uuid,
            name=data.get("slice_name", ""),
            dataset_uuid=data.get("dataset_uuid"),
            column_refs=column_refs,
            path=yaml_file,
        )

    return result


def _load_dashboards(dashboards_dir: Path) -> dict[str, DashboardAsset]:
    """Parse all dashboard YAML files and return UUID-keyed dict."""
    result: dict[str, DashboardAsset] = {}
    if not dashboards_dir.exists():
        return result

    for yaml_file in sorted(dashboards_dir.glob("*.yaml")):
        try:
            data: Any = yaml.safe_load(yaml_file.read_text())
        except Exception:  # noqa: BLE001, S112
            continue
        if not isinstance(data, dict):
            continue

        uuid = data.get("uuid")
        if not uuid:
            continue

        chart_uuids: set[str] = set()
        for _elem_id, elem in (data.get("position") or {}).items():
            if not isinstance(elem, dict):
                continue
            if elem.get("type") == "CHART":
                meta = elem.get("meta") or {}
                chart_uuid = meta.get("uuid")
                if chart_uuid:
                    chart_uuids.add(chart_uuid)

        result[uuid] = DashboardAsset(
            uuid=uuid,
            title=data.get("dashboard_title", ""),
            chart_uuids=chart_uuids,
            path=yaml_file,
        )

    return result


def build_asset_index(assets_dir: Path) -> AssetIndex:
    """
    Build a complete UUID-keyed index from all local Superset asset YAML files.

    Args:
        assets_dir: Path to the assets directory (contains charts/, datasets/,
            dashboards/ subdirectories).

    Returns:
        AssetIndex with populated datasets, charts, and dashboards dicts.
    """
    return AssetIndex(
        datasets=_load_datasets(assets_dir / "datasets"),
        charts=_load_charts(assets_dir / "charts"),
        dashboards=_load_dashboards(assets_dir / "dashboards"),
    )
