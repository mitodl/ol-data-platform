"""Tests for asset_index module."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ol_superset.lib.asset_index import (
    build_asset_index,
    extract_chart_column_refs,
    extract_virtual_dataset_columns,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, data: dict[str, object]) -> None:
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
                # Calculated column — has an expression
                {
                    "column_name": "order_year",
                    "expression": "substring(order_created_on, 1, 4)",
                },
            ],
        },
    )

    # Dataset: virtual (custom SQL) — fully parseable, no wildcard
    _write_yaml(
        base / "datasets" / "Trino" / "virtual_def456.yaml",
        {
            "uuid": "def456",
            "table_name": "virtual_report",
            "schema": "ol_warehouse_production_mart",
            "catalog": "ol_data_lake_production",
            "sql": (
                "SELECT a.order_id, a.user_email, "
                "sum(a.total_amount) AS total_amount "
                "FROM ol_warehouse_production_mart.marts__orders a "
                "GROUP BY a.order_id, a.user_email"
            ),
            "columns": [
                {"column_name": "order_id"},
            ],
        },
    )

    # Dataset: virtual with SELECT * wildcard
    _write_yaml(
        base / "datasets" / "Trino" / "wildcard_pqr999.yaml",
        {
            "uuid": "pqr999",
            "table_name": "wildcard_report",
            "schema": "ol_warehouse_production_mart",
            "catalog": "ol_data_lake_production",
            "sql": "SELECT a.* FROM ol_warehouse_production_mart.marts__orders a",
            "columns": [],
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

    assert len(index.datasets) == 3  # simple + virtual + wildcard
    assert len(index.charts) == 2
    assert len(index.dashboards) == 1


def test_build_asset_index_dataset_columns(assets_dir: Path) -> None:
    index = build_asset_index(assets_dir)

    ds = index.datasets["abc123"]
    assert ds.table_name == "marts__orders"
    # Plain columns only — calculated columns are in calculated_columns
    assert ds.columns == {"order_id", "user_email", "total_amount"}
    assert ds.calculated_columns == {"order_year"}
    assert ds.sql is None


def test_build_asset_index_calculated_columns_not_in_columns(assets_dir: Path) -> None:
    index = build_asset_index(assets_dir)

    ds = index.datasets["abc123"]
    # Calculated columns must not appear in the plain columns set
    assert "order_year" not in ds.columns
    assert "order_year" in ds.calculated_columns


def test_build_asset_index_virtual_dataset(assets_dir: Path) -> None:
    index = build_asset_index(assets_dir)

    ds = index.datasets["def456"]
    assert ds.sql is not None
    assert ds.virtual_columns == {"order_id", "user_email", "total_amount"}
    assert ds.sql_has_wildcard is False


def test_build_asset_index_wildcard_dataset(assets_dir: Path) -> None:
    index = build_asset_index(assets_dir)

    ds = index.datasets["pqr999"]
    assert ds.sql is not None
    assert ds.virtual_columns is None
    assert ds.sql_has_wildcard is True


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


def test_extract_column_refs_entity() -> None:
    params = {"entity": "user_country_code"}
    refs = extract_chart_column_refs(params)
    assert "user_country_code" in refs


def test_extract_column_refs_adhoc_sql_filter_parsed() -> None:
    """SQL adhoc_filters are now parsed — columns inside SQL are extracted."""
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
    # SQL expression filter column is now also extracted
    assert "total" in refs


def test_extract_column_refs_sql_dicts_in_all_columns_parsed() -> None:
    """SQL-expression dicts in all_columns: sqlExpression is parsed for columns."""
    params = {
        "all_columns": [
            "order_id",  # plain string - always included
            {
                "expressionType": "SQL",
                "label": "computed_col",
                "sqlExpression": "UPPER(user_email)",
            },
        ]
    }
    refs = extract_chart_column_refs(params)
    assert "order_id" in refs
    # user_email is parsed from the SQL expression
    assert "user_email" in refs
    # The label name itself is not a column reference
    assert "computed_col" not in refs


def test_extract_column_refs_simple_adhoc_in_all_columns() -> None:
    """SIMPLE adhoc-column dicts in all_columns reference the underlying column."""
    params = {
        "all_columns": [
            "order_id",
            {
                "expressionType": "SIMPLE",
                "column": {"column_name": "user_email"},
                "label": "user_email",
            },
        ]
    }
    refs = extract_chart_column_refs(params)
    assert "order_id" in refs
    assert "user_email" in refs


def test_extract_column_refs_groupby_sql_dict_parsed() -> None:
    """SQL-expression dicts in groupby: sqlExpression is parsed for columns."""
    params = {
        "groupby": [
            "platform",  # plain string — always included
            {
                "expressionType": "SQL",
                "label": "is_current",
                "sqlExpression": "CASE WHEN active = 1 THEN true ELSE false END",
            },
        ]
    }
    refs = extract_chart_column_refs(params)
    assert "platform" in refs
    # active is parsed from the SQL CASE expression
    assert "active" in refs
    # The label is not a column reference
    assert "is_current" not in refs


def test_extract_column_refs_groupby_simple_dict() -> None:
    """SIMPLE adhoc-column dicts in groupby expose column.column_name."""
    params = {
        "groupby": [
            {
                "expressionType": "SIMPLE",
                "column": {"column_name": "enrollment_status"},
                "label": "enrollment_status",
            }
        ]
    }
    refs = extract_chart_column_refs(params)
    assert "enrollment_status" in refs


def test_extract_column_refs_metrics_simple() -> None:
    """SIMPLE metric dicts expose the aggregated column."""
    params = {
        "metrics": [
            {
                "expressionType": "SIMPLE",
                "aggregate": "SUM",
                "column": {"column_name": "enrollment_count"},
                "label": "SUM(enrollment_count)",
            },
            {
                "expressionType": "SIMPLE",
                "aggregate": "COUNT_DISTINCT",
                "column": {"column_name": "user_id"},
                "label": "COUNT_DISTINCT(user_id)",
            },
        ]
    }
    refs = extract_chart_column_refs(params)
    assert "enrollment_count" in refs
    assert "user_id" in refs


def test_extract_column_refs_metric_singular_simple() -> None:
    """Single ``metric`` dict (not list) exposes its column."""
    params = {
        "metric": {
            "expressionType": "SIMPLE",
            "aggregate": "SUM",
            "column": {"column_name": "revenue"},
        }
    }
    refs = extract_chart_column_refs(params)
    assert "revenue" in refs


def test_extract_column_refs_metrics_sql_parsed() -> None:
    """SQL-expression metric dicts: sqlExpression is parsed for columns."""
    params = {
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "SUM(revenue) / COUNT(*)",
                "label": "avg_revenue",
            }
        ]
    }
    refs = extract_chart_column_refs(params)
    # revenue is parsed from the SQL; COUNT(*) Star node contributes nothing
    assert "revenue" in refs
    assert "avg_revenue" not in refs


def test_extract_column_refs_metrics_saved_metric_string_skipped() -> None:
    """Saved metric names (plain strings) are not raw column refs."""
    params = {"metrics": ["count", "sum__revenue"]}
    refs = extract_chart_column_refs(params)
    assert not refs


def test_extract_column_refs_timeseries_limit_metric() -> None:
    params = {
        "timeseries_limit_metric": {
            "expressionType": "SIMPLE",
            "aggregate": "MAX",
            "column": {"column_name": "activity_date"},
        }
    }
    refs = extract_chart_column_refs(params)
    assert "activity_date" in refs


def test_extract_column_refs_secondary_metric() -> None:
    params = {
        "secondary_metric": {
            "expressionType": "SIMPLE",
            "aggregate": "COUNT",
            "column": {"column_name": "session_count"},
        }
    }
    refs = extract_chart_column_refs(params)
    assert "session_count" in refs


def test_extract_column_refs_percent_metrics() -> None:
    params = {
        "percent_metrics": [
            {
                "expressionType": "SIMPLE",
                "aggregate": "SUM",
                "column": {"column_name": "completion_count"},
            }
        ]
    }
    refs = extract_chart_column_refs(params)
    assert "completion_count" in refs


def test_extract_column_refs_order_by_cols() -> None:
    params = {
        "order_by_cols": [
            '["created_on", false]',
            '["message_index", true]',
        ]
    }
    refs = extract_chart_column_refs(params)
    assert "created_on" in refs
    assert "message_index" in refs


def test_extract_column_refs_order_by_cols_invalid_json_ignored() -> None:
    params = {"order_by_cols": ["not-json", '["valid_col", true]']}
    refs = extract_chart_column_refs(params)
    assert "valid_col" in refs
    assert len(refs) == 1


def test_extract_column_refs_empty_params() -> None:
    assert extract_chart_column_refs({}) == set()


def test_extract_column_refs_sql_complex_case_expression() -> None:
    """Multi-column CASE expressions are fully parsed."""
    params = {
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": (
                    "COUNT(DISTINCT CASE WHEN num_video_played > 0 "
                    "THEN user_username ELSE null END)"
                ),
                "label": "active_users",
            }
        ]
    }
    refs = extract_chart_column_refs(params)
    assert "num_video_played" in refs
    assert "user_username" in refs
    assert "active_users" not in refs


def test_extract_column_refs_sql_arithmetic_expression() -> None:
    """Arithmetic SQL expressions with multiple column operands are parsed."""
    params = {
        "groupby": [
            {
                "expressionType": "SQL",
                "label": "capacity",
                "sqlExpression": "num_discount_codes * discount_max_redemptions",
            }
        ]
    }
    refs = extract_chart_column_refs(params)
    assert "num_discount_codes" in refs
    assert "discount_max_redemptions" in refs


def test_extract_column_refs_sql_function_only_no_columns() -> None:
    """SQL expressions with no column references (pure functions) yield nothing."""
    params = {
        "metrics": [
            {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "cnt"}
        ],
        "adhoc_filters": [
            {"expressionType": "SQL", "sqlExpression": "CURRENT_DATE > '2024-01-01'"}
        ],
    }
    refs = extract_chart_column_refs(params)
    assert not refs


def test_extract_column_refs_sql_parse_failure_is_silent() -> None:
    """Unparseable SQL expressions do not raise — they yield no columns."""
    params = {
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "{{jinja_template_not_valid_sql}}",
                "label": "broken",
            }
        ]
    }
    # Must not raise
    refs = extract_chart_column_refs(params)
    assert isinstance(refs, set)


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


def test_extract_column_refs_comprehensive() -> None:
    """Integration test: all extraction paths including SQL expression parsing."""
    params = {
        "x_axis": "activity_date",
        "entity": "user_country_code",
        "groupby": [
            "platform",
            {
                "expressionType": "SIMPLE",
                "column": {"column_name": "course_run_id"},
                "label": "course_run_id",
            },
            {
                "expressionType": "SQL",
                "label": "derived",
                # platform is already in refs; this still gets parsed
                "sqlExpression": "UPPER(platform)",
            },
        ],
        "all_columns": [
            "user_email",
            {
                "expressionType": "SIMPLE",
                "column": {"column_name": "enrollment_mode"},
                "label": "enrollment_mode",
            },
        ],
        "metrics": [
            {
                "expressionType": "SIMPLE",
                "aggregate": "COUNT_DISTINCT",
                "column": {"column_name": "user_id"},
            },
            # COUNT(*) — Star node contributes nothing; label "cnt" is not a col
            {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "cnt"},
        ],
        "timeseries_limit_metric": {
            "expressionType": "SIMPLE",
            "aggregate": "MAX",
            "column": {"column_name": "last_activity_date"},
        },
        "adhoc_filters": [
            {"expressionType": "SIMPLE", "subject": "is_active"},
            # SQL filter: revenue is now extracted
            {"expressionType": "SQL", "sqlExpression": "revenue > 0"},
        ],
        "order_by_cols": ['["enrollment_count", false]'],
    }
    refs = extract_chart_column_refs(params)
    assert refs == {
        "activity_date",
        "user_country_code",
        "platform",  # plain groupby str AND from UPPER(platform) SQL
        "course_run_id",
        "user_email",
        "enrollment_mode",
        "user_id",
        "last_activity_date",
        "is_active",
        "revenue",  # extracted from SQL adhoc_filter
        "enrollment_count",
    }
    # Label names are not column references
    assert "derived" not in refs
    assert "cnt" not in refs


# ---------------------------------------------------------------------------
# Tests: extract_virtual_dataset_columns
# ---------------------------------------------------------------------------


def test_extract_virtual_columns_simple_select() -> None:
    sql = (
        "SELECT a.platform, h.course_title, a.courserun_readable_id "
        "FROM schema_a.table_one a JOIN schema_b.table_two h ON a.id = h.id"
    )
    result = extract_virtual_dataset_columns(sql)
    assert result.has_wildcard is False
    assert result.columns == {"platform", "course_title", "courserun_readable_id"}


def test_extract_virtual_columns_aliases() -> None:
    sql = (
        "SELECT a.platform, sum(a.cnt) AS enrollment_count, "
        "substring(a.dt, 1, 4) AS year "
        "FROM schema.tbl a GROUP BY a.platform"
    )
    result = extract_virtual_dataset_columns(sql)
    assert result.has_wildcard is False
    assert result.columns == {"platform", "enrollment_count", "year"}


def test_extract_virtual_columns_case_insensitive() -> None:
    sql = "SELECT a.UserEmail, a.CourseName FROM schema.tbl a"
    result = extract_virtual_dataset_columns(sql)
    assert result.has_wildcard is False
    assert result.columns == {"useremail", "coursename"}


def test_extract_virtual_columns_cte() -> None:
    sql = "WITH base AS (SELECT * FROM schema.tbl) SELECT b.col1, b.col2 FROM base b"
    result = extract_virtual_dataset_columns(sql)
    # Outer SELECT has no wildcard — columns are deterministic
    assert result.has_wildcard is False
    assert result.columns == {"col1", "col2"}


def test_extract_virtual_columns_bare_star_returns_none() -> None:
    result = extract_virtual_dataset_columns("SELECT * FROM schema.tbl")
    assert result.columns is None
    assert result.has_wildcard is True


def test_extract_virtual_columns_table_dot_star_returns_none() -> None:
    result = extract_virtual_dataset_columns(
        "SELECT a.*, b.extra FROM schema.tbl a JOIN schema.tbl2 b ON a.id = b.id"
    )
    assert result.columns is None
    assert result.has_wildcard is True


def test_extract_virtual_columns_parse_error_returns_none() -> None:
    result = extract_virtual_dataset_columns("THIS IS NOT SQL %%%")
    assert result.columns is None
    assert result.has_wildcard is False


def test_extract_virtual_columns_empty_sql() -> None:
    # Empty string cannot be parsed — returns None with no wildcard flag
    result = extract_virtual_dataset_columns("")
    assert result.columns is None
    assert result.has_wildcard is False
