"""Tests for lib/yaml_registry.py — YAML schema registry."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from ol_dbt_cli.lib.yaml_registry import build_yaml_registry


@pytest.fixture()
def models_dir(tmp_path: Path) -> Path:
    """Create a temporary models directory with sample YAML schema files."""
    staging = tmp_path / "staging" / "myplatform"
    staging.mkdir(parents=True)

    (staging / "_myplatform__models.yml").write_text(
        textwrap.dedent("""
        version: 2
        models:
        - name: stg__myplatform__users
          description: User accounts
          columns:
          - name: user_id
            description: int, primary key
            tests:
            - unique
            - not_null
          - name: user_email
            description: str, email
            tests:
            - not_null
          - name: user_is_active
            description: boolean
        - name: stg__myplatform__orders
          description: Orders table
          columns:
          - name: order_id
            description: int, primary key
        """).strip()
    )

    (staging / "_myplatform__sources.yml").write_text(
        textwrap.dedent("""
        version: 2
        sources:
        - name: ol_warehouse_raw_data
          tables:
          - name: raw__myplatform__users
            description: Raw user table
            columns:
            - name: id
            - name: email
            - name: is_active
          - name: raw__myplatform__orders
            description: Raw orders table
            columns:
            - name: order_id
            - name: user_id
        """).strip()
    )
    return tmp_path


class TestYamlRegistry:
    def test_loads_models(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        assert "stg__myplatform__users" in registry.models
        assert "stg__myplatform__orders" in registry.models

    def test_column_names(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        m = registry.get_model("stg__myplatform__users")
        assert m is not None
        assert m.column_names == {"user_id", "user_email", "user_is_active"}

    def test_column_tests(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        m = registry.get_model("stg__myplatform__users")
        assert m is not None
        assert "unique" in m.columns["user_id"].tests
        assert "not_null" in m.columns["user_id"].tests

    def test_unknown_model_returns_none(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        assert registry.get_model("nonexistent_model") is None

    def test_file_to_models_mapping(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        yaml_file = models_dir / "staging" / "myplatform" / "_myplatform__models.yml"
        names = registry.file_to_models.get(yaml_file, [])
        assert "stg__myplatform__users" in names
        assert "stg__myplatform__orders" in names

    def test_empty_models_dir(self, tmp_path: Path) -> None:
        (tmp_path / "staging").mkdir()
        registry = build_yaml_registry(tmp_path)
        assert len(registry.models) == 0

    def test_invalid_yaml_is_skipped(self, tmp_path: Path) -> None:
        (tmp_path / "staging").mkdir()
        (tmp_path / "staging" / "_bad.yml").write_text("{ invalid yaml: [")
        registry = build_yaml_registry(tmp_path)
        assert len(registry.models) == 0


class TestYamlRegistrySources:
    def test_loads_source(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        assert "ol_warehouse_raw_data" in registry.sources

    def test_source_tables(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        src = registry.sources.get("ol_warehouse_raw_data")
        assert src is not None
        assert "raw__myplatform__users" in src.tables
        assert "raw__myplatform__orders" in src.tables

    def test_get_source_columns(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        cols = registry.get_source_columns("ol_warehouse_raw_data", "raw__myplatform__users")
        assert cols == {"id", "email", "is_active"}

    def test_get_source_columns_unknown_source(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        assert registry.get_source_columns("nonexistent", "some_table") is None

    def test_get_source_columns_unknown_table(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        assert registry.get_source_columns("ol_warehouse_raw_data", "nonexistent") is None

    def test_get_source_table(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        tbl = registry.get_source_table("ol_warehouse_raw_data", "raw__myplatform__users")
        assert tbl is not None
        assert tbl.source_name == "ol_warehouse_raw_data"
        assert "id" in tbl.columns


class TestNestedModelDefinitions:
    """Nested model definitions inside a columns: list are handled gracefully."""

    def test_nested_model_excluded_from_parent_columns(self, tmp_path: Path) -> None:
        """A column entry with its own columns: key is NOT added to the parent's columns."""
        staging = tmp_path / "staging"
        staging.mkdir()
        (staging / "_models.yml").write_text(
            "version: 2\n"
            "models:\n"
            "- name: parent_model\n"
            "  columns:\n"
            "  - name: real_column\n"
            "    description: A real column\n"
            "  - name: nested_model\n"
            "    description: Accidentally nested model\n"
            "    columns:\n"
            "    - name: nested_col\n"
        )
        registry = build_yaml_registry(tmp_path)
        parent = registry.models.get("parent_model")
        assert parent is not None
        assert "real_column" in parent.columns
        assert "nested_model" not in parent.columns  # must NOT appear as a column

    def test_nested_model_rescued_as_top_level(self, tmp_path: Path) -> None:
        """A column entry with its own columns: key IS registered as its own model."""
        staging = tmp_path / "staging"
        staging.mkdir()
        (staging / "_models.yml").write_text(
            "version: 2\n"
            "models:\n"
            "- name: parent_model\n"
            "  columns:\n"
            "  - name: real_column\n"
            "  - name: nested_model\n"
            "    description: Accidentally nested model\n"
            "    columns:\n"
            "    - name: nested_col\n"
            "      description: A column in the nested model\n"
        )
        registry = build_yaml_registry(tmp_path)
        nested = registry.models.get("nested_model")
        assert nested is not None, "nested model should be rescued into the registry"
        assert "nested_col" in nested.columns
