"""Tests for asset_index module."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ol_superset.lib.asset_index import (
    build_asset_index,
    extract_chart_column_refs,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data))


@pytest.fixture
def assets_dir(tmp_path: Path) -> Path:
    """Create a minimal assets directory for testing."""
    base = tmp_path / "assets"

    # Dataset: simple table
    _write_yaml(
        base / "datasets" / "Trino" / "orders_abc123.yaml",
        {
            "uuid": "abc123",
            "table_name": "marts__orders",
            "schema": "ol_warehouse_production_mart",
            "catalog": "ol_data_lake_production",
            "sql": None,
            "columns": [
                {"column_name": "order_id"},
                {"column_name": "user_email"},
                {"column_name": "total_amount"},
            ],
        },
    )

    # Dataset: virtual (custom SQL)
    _write_yaml(
        base / "datasets" / "Trino" / "virtual_def456.yaml",
        {
            "uuid": "def456",
            "table_name": "virtual_report",
            "schema": "ol_warehouse_production_mart",
            "catalog": "ol_data_lake_production",
            "sql": "SELECT * FROM ol_warehouse_production_mart.marts__orders",
            "columns": [
                {"column_name": "order_id"},
            ],
        },
    )

    # Chart referencing the simple dataset
    _write_yaml(
        base / "charts" / "chart_ghi789.yaml",
        {
            "uuid": "ghi789",
            "slice_name": "Order Summary",
            "dataset_uuid": "abc123",
            "params": {
                "groupby": ["user_email"],
                "all_columns": ["order_id", "total_amount"],
                "adhoc_filters": [
                    {
                        "expressionType": "SIMPLE",
                        "subject": "user_email",
                        "operator": "==",
                        "comparator": "test@example.com",
                    }
                ],
            },
        },
    )

    # Chart with SQL expression columns (should be excluded)
    _write_yaml(
        base / "charts" / "chart_sql_expr.yaml",
        {
            "uuid": "jkl012",
            "slice_name": "SQL Chart",
            "dataset_uuid": "abc123",
            "params": {
                "all_columns": [
                    "order_id",  # plain column - included
                    {  # SQL expression - excluded
                        "expressionType": "SQL",
                        "label": "computed",
                        "sqlExpression": "UPPER(user_email)",
                    },
                ],
                "adhoc_filters": [
                    {
                        "expressionType": "SQL",  # SQL filter - excluded
                        "sqlExpression": "total_amount > 100",
                    }
                ],
            },
        },
    )

    # Dashboard referencing both charts
    _write_yaml(
        base / "dashboards" / "dashboard_mno345.yaml",
        {
            "uuid": "mno345",
            "dashboard_title": "Sales Dashboard",
            "position": {
                "CHART-1": {
                    "type": "CHART",
                    "meta": {"uuid": "ghi789", "sliceName": "Order Summary"},
                    "children": [],
                },
                "CHART-2": {
                    "type": "CHART",
                    "meta": {"uuid": "jkl012", "sliceName": "SQL Chart"},
                    "children": [],
                },
                "HEADER_ID": {
                    "type": "HEADER",  # Non-chart element - should be ignored
                    "meta": {"text": "Sales"},
                },
            },
        },
    )

    return base


# ---------------------------------------------------------------------------
# Tests: build_asset_index
# ---------------------------------------------------------------------------


def test_build_asset_index_counts(assets_dir: Path) -> None:
    index = build_asset_index(assets_dir)

    assert len(index.datasets) == 2
    assert len(index.charts) == 2
    assert len(index.dashboards) == 1


def test_build_asset_index_dataset_columns(assets_dir: Path) -> None:
    index = build_asset_index(assets_dir)

    ds = index.datasets["abc123"]
    assert ds.table_name == "marts__orders"
    assert ds.columns == {"order_id", "user_email", "total_amount"}
    assert ds.sql is None


def test_build_asset_index_virtual_dataset(assets_dir: Path) -> None:
    index = build_asset_index(assets_dir)

    ds = index.datasets["def456"]
    assert ds.sql is not None
    assert "marts__orders" in ds.sql


def test_build_asset_index_chart_dataset_ref(assets_dir: Path) -> None:
    index = build_asset_index(assets_dir)

    chart = index.charts["ghi789"]
    assert chart.dataset_uuid == "abc123"
    assert chart.name == "Order Summary"


def test_build_asset_index_dashboard_chart_refs(assets_dir: Path) -> None:
    index = build_asset_index(assets_dir)

    dash = index.dashboards["mno345"]
    assert dash.title == "Sales Dashboard"
    assert "ghi789" in dash.chart_uuids
    assert "jkl012" in dash.chart_uuids
    assert len(dash.chart_uuids) == 2  # HEADER_ID should not be included


def test_build_asset_index_empty_dir(tmp_path: Path) -> None:
    index = build_asset_index(tmp_path / "assets")
    assert len(index.datasets) == 0
    assert len(index.charts) == 0
    assert len(index.dashboards) == 0


# ---------------------------------------------------------------------------
# Tests: extract_chart_column_refs
# ---------------------------------------------------------------------------


def test_extract_column_refs_plain_strings() -> None:
    params = {
        "groupby": ["user_email", "platform"],
        "all_columns": ["order_id", "total_amount"],
    }
    refs = extract_chart_column_refs(params)
    assert refs == {"user_email", "platform", "order_id", "total_amount"}


def test_extract_column_refs_x_axis() -> None:
    params = {"x_axis": "event_date"}
    refs = extract_chart_column_refs(params)
    assert "event_date" in refs


def test_extract_column_refs_adhoc_simple_only() -> None:
    params = {
        "adhoc_filters": [
            {
                "expressionType": "SIMPLE",
                "subject": "platform",
                "operator": "==",
                "comparator": "MITx Online",
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "total > 0",
            },
        ]
    }
    refs = extract_chart_column_refs(params)
    assert "platform" in refs
    # SQL expression filter should not add any column
    assert "total" not in refs


def test_extract_column_refs_skips_sql_expression_dicts() -> None:
    params = {
        "all_columns": [
            "order_id",  # plain string - included
            {
                "expressionType": "SQL",
                "label": "computed_col",
                "sqlExpression": "UPPER(user_email)",
            },  # dict - excluded
        ]
    }
    refs = extract_chart_column_refs(params)
    assert refs == {"order_id"}
    assert "computed_col" not in refs


def test_extract_column_refs_empty_params() -> None:
    assert extract_chart_column_refs({}) == set()


def test_extract_column_refs_deduplicates() -> None:
    params = {
        "groupby": ["user_email"],
        "all_columns": ["user_email", "order_id"],  # user_email appears twice
    }
    refs = extract_chart_column_refs(params)
    assert refs == {"user_email", "order_id"}


def test_extract_column_refs_ignores_empty_strings() -> None:
    params = {
        "groupby": ["", "user_email"],
        "all_columns": [""],
    }
    refs = extract_chart_column_refs(params)
    assert refs == {"user_email"}
