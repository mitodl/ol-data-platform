"""Tests for dbt_registry module."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from ol_superset.lib.dbt_registry import (
    build_dbt_registry,
    extract_sql_table_refs,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def dbt_project(tmp_path: Path) -> Path:
    """Create a minimal dbt project structure for testing."""
    # dbt_project.yml
    project_yml = tmp_path / "dbt_project.yml"
    project_yml.write_text(
        textwrap.dedent("""\
            name: test_project
            version: '0.1.0'
            profile: test_project
            model-paths: ["models"]
            models:
              test_project:
                marts:
                  +schema: mart
                dimensional:
                  +schema: dimensional
                reporting:
                  +schema: reporting
        """)
    )

    # profiles.yml with a production target
    profiles_yml = tmp_path / "profiles.yml"
    profiles_yml.write_text(
        textwrap.dedent("""\
            test_project:
              outputs:
                production:
                  type: trino
                  schema: ol_warehouse_production
        """)
    )

    # models/marts/_marts__models.yml
    marts_dir = tmp_path / "models" / "marts"
    marts_dir.mkdir(parents=True)
    (marts_dir / "_marts__models.yml").write_text(
        textwrap.dedent("""\
            version: 2
            models:
              - name: marts__orders
                description: Orders mart
                columns:
                  - name: order_id
                    description: Primary key
                  - name: user_email
                    description: User email
                  - name: total_amount
                    description: Total order amount
        """)
    )

    # models/dimensional/_dim__models.yml
    dim_dir = tmp_path / "models" / "dimensional"
    dim_dir.mkdir(parents=True)
    (dim_dir / "_dim__models.yml").write_text(
        textwrap.dedent("""\
            version: 2
            models:
              - name: dim_user
                description: User dimension
                columns:
                  - name: user_id
                    description: Primary key
                  - name: email
                    description: Email address
        """)
    )

    return tmp_path


# ---------------------------------------------------------------------------
# Tests: build_dbt_registry
# ---------------------------------------------------------------------------


def test_build_dbt_registry_loads_models(dbt_project: Path) -> None:
    registry = build_dbt_registry(dbt_project)

    assert "marts__orders" in registry.models
    assert "dim_user" in registry.models


def test_build_dbt_registry_correct_columns(dbt_project: Path) -> None:
    registry = build_dbt_registry(dbt_project)

    orders = registry.models["marts__orders"]
    assert orders.columns == {"order_id", "user_email", "total_amount"}

    user = registry.models["dim_user"]
    assert user.columns == {"user_id", "email"}


def test_build_dbt_registry_layer_assignment(dbt_project: Path) -> None:
    registry = build_dbt_registry(dbt_project)

    assert registry.models["marts__orders"].layer == "marts"
    assert registry.models["dim_user"].layer == "dimensional"


def test_build_dbt_registry_expected_schema(dbt_project: Path) -> None:
    registry = build_dbt_registry(dbt_project)

    assert (
        registry.models["marts__orders"].expected_schema
        == "ol_warehouse_production_mart"
    )
    assert (
        registry.models["dim_user"].expected_schema
        == "ol_warehouse_production_dimensional"
    )


def test_build_dbt_registry_schema_to_layer(dbt_project: Path) -> None:
    registry = build_dbt_registry(dbt_project)

    assert registry.schema_to_layer["ol_warehouse_production_mart"] == "marts"
    assert (
        registry.schema_to_layer["ol_warehouse_production_dimensional"] == "dimensional"
    )


def test_build_dbt_registry_missing_dbt_project(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="dbt_project.yml not found"):
        build_dbt_registry(tmp_path)


def test_build_dbt_registry_get_model(dbt_project: Path) -> None:
    registry = build_dbt_registry(dbt_project)

    assert registry.get_model("marts__orders") is not None
    assert registry.get_model("nonexistent_model") is None


def test_build_dbt_registry_empty_models_dir(tmp_path: Path) -> None:
    """Registry with no _*.yml files returns empty models dict."""
    (tmp_path / "dbt_project.yml").write_text(
        "name: empty\nversion: '0.1.0'\nprofile: empty\n"
        "models:\n  empty:\n    marts:\n      +schema: mart\n"
    )
    (tmp_path / "models").mkdir()
    (tmp_path / "profiles.yml").write_text(
        "empty:\n  outputs:\n    production:\n      type: trino\n"
        "      schema: ol_warehouse_production\n"
    )

    registry = build_dbt_registry(tmp_path)
    assert len(registry.models) == 0


# ---------------------------------------------------------------------------
# Tests: extract_sql_table_refs
# ---------------------------------------------------------------------------


def test_extract_sql_table_refs_simple_join() -> None:
    sql = (
        "SELECT a.col FROM schema_a.table_one a "
        "JOIN schema_b.table_two b ON a.id = b.id"
    )
    refs = extract_sql_table_refs(sql)
    assert "table_one" in refs
    assert "table_two" in refs


def test_extract_sql_table_refs_three_part() -> None:
    sql = "SELECT * FROM catalog.schema.my_table"
    refs = extract_sql_table_refs(sql)
    assert "my_table" in refs


def test_extract_sql_table_refs_skips_column_aliases() -> None:
    sql = (
        "SELECT a.user_id, b.email FROM schema_a.orders a "
        "JOIN schema_b.users b ON a.user_id = b.id"
    )
    refs = extract_sql_table_refs(sql)
    # Should only have table names, not column alias refs like user_id/email/id
    assert "orders" in refs
    assert "users" in refs
    assert "user_id" not in refs
    assert "email" not in refs


def test_extract_sql_table_refs_empty_sql() -> None:
    assert extract_sql_table_refs("") == set()
    assert extract_sql_table_refs(None) == set()  # type: ignore[arg-type]


def test_extract_sql_table_refs_multiple_joins() -> None:
    sql = textwrap.dedent("""\
        SELECT *
        FROM schema_a.fact_table f
        INNER JOIN schema_b.dim_user u ON f.user_id = u.id
        LEFT JOIN schema_c.dim_course c ON f.course_id = c.id
    """)
    refs = extract_sql_table_refs(sql)
    assert "fact_table" in refs
    assert "dim_user" in refs
    assert "dim_course" in refs


def test_extract_sql_table_refs_case_insensitive() -> None:
    sql = "SELECT * FROM Schema_A.My_Table INNER JOIN Schema_B.Other_Table t ON 1=1"
    refs = extract_sql_table_refs(sql)
    assert "my_table" in refs
    assert "other_table" in refs
