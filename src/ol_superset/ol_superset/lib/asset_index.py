"""Asset index — builds UUID-keyed lookups for Superset asset YAML files."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NamedTuple

import sqlglot
import sqlglot.expressions as exp
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
    # Columns with a non-empty ``expression`` field — computed by Superset at
    # query time and not present as raw columns in the underlying table.
    calculated_columns: set[str] = field(default_factory=set)
    sql: str | None = None  # non-None means virtual dataset
    # Output column names parsed from the virtual dataset SQL (lowercase).
    # None means the SQL contains a wildcard (SELECT * / table.*) or could
    # not be parsed — column-level chart validation is skipped in that case.
    virtual_columns: set[str] | None = None
    # True when virtual_columns is None specifically because the SQL uses a
    # SELECT * or table.* wildcard (as opposed to a parse failure).
    sql_has_wildcard: bool = False
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
# Virtual dataset SQL column extraction
# ---------------------------------------------------------------------------


class VirtualColumnsResult(NamedTuple):
    """Result of parsing a virtual dataset SQL query."""

    columns: set[str] | None
    """Lowercase output column names, or None when the set is indeterminate."""
    has_wildcard: bool
    """True when None is due to a SELECT * or table.* wildcard."""


def extract_virtual_dataset_columns(sql: str) -> VirtualColumnsResult:
    """
    Parse a virtual dataset SQL query and return its output column names.

    Uses sqlglot (Trino dialect) to parse the outermost SELECT and extract
    the names of all output columns.  Column names are returned in **lowercase**
    to allow case-insensitive comparison.

    ``columns`` is ``None`` when the complete set cannot be determined:

    - The query contains a wildcard (``SELECT *`` or ``table.*``).
      ``has_wildcard`` will be ``True`` in this case.
    - The SQL cannot be parsed (e.g. proprietary syntax).
      ``has_wildcard`` will be ``False`` in this case.

    Unnamed expressions (e.g. un-aliased aggregates) are silently ignored —
    they cannot be referenced by name in chart params anyway.

    Args:
        sql: Raw SQL string from the Superset dataset YAML ``sql`` field.

    Returns:
        :class:`VirtualColumnsResult` with ``columns`` and ``has_wildcard``.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="trino")
    except Exception:  # noqa: BLE001
        return VirtualColumnsResult(columns=None, has_wildcard=False)

    cols: set[str] = set()
    for sel in tree.selects:
        if isinstance(sel, exp.Star):
            return VirtualColumnsResult(columns=None, has_wildcard=True)
        if isinstance(sel, exp.Column) and sel.name == "*":
            # table.* pattern
            return VirtualColumnsResult(columns=None, has_wildcard=True)
        if isinstance(sel, exp.Alias):
            cols.add(sel.alias.lower())
        elif isinstance(sel, exp.Column):
            cols.add(sel.name.lower())
        # else: unnamed expression — skip

    return VirtualColumnsResult(columns=cols, has_wildcard=False)


# ---------------------------------------------------------------------------
# Column-reference extraction from chart params
# ---------------------------------------------------------------------------

_SIMPLE_COLUMN_KEYS = ("x_axis",)
"""Chart param keys whose values are a single plain column name string."""

_SIMPLE_LIST_KEYS = ("groupby",)
"""Chart param keys whose values are lists of str | adhoc-column-dict."""

_METRIC_KEYS = (
    "metric",
    "metrics",
    "timeseries_limit_metric",
    "secondary_metric",
    "percent_metrics",
)
"""Chart param keys: single metric dict, or list of metric dicts/strings."""


def _cols_from_sql_expression(sql: str) -> set[str]:
    """
    Extract column name references from a SQL expression fragment.

    Uses sqlglot (Trino dialect) to parse the expression and locate all
    ``Column`` nodes.  Column names are returned in their original case
    (callers normalise as needed).

    Returns an empty set when the SQL is blank or cannot be parsed — we
    never raise on bad input, we just extract fewer refs.

    Examples::

        _cols_from_sql_expression("AVG(percent_problems_attempted)")
        # -> {"percent_problems_attempted"}

        _cols_from_sql_expression(
            "COUNT(DISTINCT CASE WHEN active THEN user_id END)"
        )
        # -> {"active", "user_id"}

        _cols_from_sql_expression("COUNT(*)")  # Star is not a Column
        # -> set()
    """
    if not sql or not sql.strip():
        return set()
    try:
        tree = sqlglot.parse_one(sql, dialect="trino")
    except Exception:  # noqa: BLE001
        return set()
    if tree is None:
        return set()
    return {
        col.name for col in tree.find_all(exp.Column) if col.name and col.name != "*"
    }


def _cols_from_adhoc(item: Any) -> set[str]:
    """
    Return dataset column names referenced by an adhoc column/filter dict.

    Adhoc column dicts appear in ``groupby``, ``all_columns``, and metric
    positions.  Two expression types are handled:

    * ``SIMPLE`` — the column is identified directly via ``column.column_name``.
    * ``SQL`` / ``CUSTOM`` — the ``sqlExpression`` SQL fragment is parsed with
      sqlglot and all ``Column`` nodes are returned.

    Returns an empty set for any unrecognised shape.
    """
    if not isinstance(item, dict):
        return set()
    expr_type = item.get("expressionType")
    if expr_type == "SIMPLE":
        col = item.get("column")
        if isinstance(col, dict):
            name = col.get("column_name")
            if name:
                return {str(name)}
        return set()
    if expr_type in ("SQL", "CUSTOM"):
        sql_expr = item.get("sqlExpression")
        if isinstance(sql_expr, str):
            return _cols_from_sql_expression(sql_expr)
    return set()


def _cols_from_metric(item: Any) -> set[str]:
    """
    Return dataset column names referenced by a metric value.

    Metric values can be:

    * A plain string — a *saved metric* name (e.g. ``"count"``).  These are
      server-side named metrics, not raw column references; returns empty set.
    * A metric dict — delegates to :func:`_cols_from_adhoc`.
    """
    if isinstance(item, str):
        return set()  # saved metric name — not a raw column
    return _cols_from_adhoc(item)


def _cols_from_order_by_cols(order_by_cols: list[Any]) -> set[str]:
    """
    Parse ``order_by_cols`` entries to extract column names.

    Each entry is a JSON-encoded string like ``'["column_name", false]'``
    where the first element is the column name and the second is a bool
    indicating descending sort.
    """
    import json

    refs: set[str] = set()
    for entry in order_by_cols:
        if not isinstance(entry, str):
            continue
        try:
            parsed = json.loads(entry)
        except (ValueError, TypeError):
            continue
        if isinstance(parsed, list) and parsed and isinstance(parsed[0], str):
            name = parsed[0].strip()
            if name:
                refs.add(name)
    return refs


def extract_chart_column_refs(params: dict[str, Any]) -> set[str]:
    """
    Extract dataset column name references from all chart params locations.

    Covers every param key where a missing dataset column would cause a query
    error in Superset:

    - ``x_axis`` — str: time or categorical axis column
    - ``entity`` — str: column used in map/scatter charts (e.g. country code)
    - ``groupby`` — list of str or adhoc-column-dict
    - ``all_columns`` — list of str or adhoc-column-dict
    - ``metrics`` / ``metric`` / ``timeseries_limit_metric`` /
      ``secondary_metric`` / ``percent_metrics`` — metric dicts
    - ``adhoc_filters`` — ``subject`` (SIMPLE) or ``sqlExpression`` (SQL/CUSTOM)
    - ``order_by_cols`` — JSON-encoded ``["column_name", bool]`` strings

    For ``SIMPLE`` expression types the column is taken directly from the
    structured ``column.column_name`` field.  For ``SQL``/``CUSTOM`` expression
    types the ``sqlExpression`` fragment is parsed with sqlglot (Trino dialect)
    and all ``Column`` AST nodes are returned — this correctly handles cases
    like ``AVG(percent_problems_attempted)`` or complex CASE expressions.

    Saved metric name strings (e.g. ``"count"``) and items that cannot be
    parsed are silently ignored.

    Args:
        params: Parsed chart params dict.

    Returns:
        Set of column name strings as they appear in the params.
    """
    refs: set[str] = set()

    # ── Single plain-string column keys ──────────────────────────────────────
    for key in _SIMPLE_COLUMN_KEYS:
        value = params.get(key)
        if isinstance(value, str) and value:
            refs.add(value)

    # entity is a plain string column used in map/scatter charts
    entity = params.get("entity")
    if isinstance(entity, str) and entity:
        refs.add(entity)

    # ── List keys: str | adhoc-column-dict ───────────────────────────────────
    for key in _SIMPLE_LIST_KEYS:
        for item in params.get(key) or []:
            if isinstance(item, str) and item:
                refs.add(item)
            else:
                refs.update(_cols_from_adhoc(item))

    # all_columns is a mixed list of str | adhoc-column-dict
    for item in params.get("all_columns") or []:
        if isinstance(item, str) and item:
            refs.add(item)
        else:
            refs.update(_cols_from_adhoc(item))

    # ── Metric keys ──────────────────────────────────────────────────────────
    for key in _METRIC_KEYS:
        value = params.get(key)
        if value is None:
            continue
        if isinstance(value, list):
            for item in value:
                refs.update(_cols_from_metric(item))
        else:
            refs.update(_cols_from_metric(value))

    # ── adhoc_filters ─────────────────────────────────────────────────────────
    # SIMPLE: column name is in the structured ``subject`` field.
    # SQL/CUSTOM: parse sqlExpression to find column references.
    for filt in params.get("adhoc_filters") or []:
        if not isinstance(filt, dict):
            continue
        expr_type = filt.get("expressionType")
        if expr_type == "SIMPLE":
            subject = filt.get("subject")
            if isinstance(subject, str) and subject:
                refs.add(subject)
        elif expr_type in ("SQL", "CUSTOM"):
            sql_expr = filt.get("sqlExpression")
            if isinstance(sql_expr, str):
                refs.update(_cols_from_sql_expression(sql_expr))

    # ── order_by_cols: JSON-encoded ["column_name", bool] strings ────────────
    order_by = params.get("order_by_cols")
    if isinstance(order_by, list):
        refs.update(_cols_from_order_by_cols(order_by))

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
        calculated_columns: set[str] = set()
        for col in data.get("columns", []):
            if isinstance(col, dict) and col.get("column_name"):
                name = col["column_name"]
                if col.get("expression"):
                    calculated_columns.add(name)
                else:
                    columns.add(name)

        raw_sql = data.get("sql")
        sql: str | None = (
            raw_sql if isinstance(raw_sql, str) and raw_sql.strip() else None
        )

        virtual_columns: set[str] | None = None
        sql_has_wildcard = False
        if sql:
            vcr = extract_virtual_dataset_columns(sql)
            virtual_columns = vcr.columns
            sql_has_wildcard = vcr.has_wildcard

        database = yaml_file.parent.name  # e.g. "Trino"

        result[uuid] = DatasetAsset(
            uuid=uuid,
            table_name=data.get("table_name", ""),
            schema=data.get("schema", ""),
            catalog=data.get("catalog", ""),
            database=database,
            columns=columns,
            calculated_columns=calculated_columns,
            sql=sql,
            virtual_columns=virtual_columns,
            sql_has_wildcard=sql_has_wildcard,
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
