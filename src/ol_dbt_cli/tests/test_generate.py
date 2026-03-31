"""Tests for commands/generate.py — source/staging scaffold helpers."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ol_dbt_cli.commands.generate import (
    _adjust_source_schema_pattern,
    _build_sources_content_from_local,
    _build_staging_sql_from_columns,
    _describe_local_view,
    _discover_tables_from_local_db,
    _extract_domain,
    _merge_sources_content,
)


class TestExtractDomain:
    def test_standard_prefix(self) -> None:
        assert _extract_domain("raw__mitlearn__app__postgres__users") == "mitlearn"

    def test_learn_ai_prefix(self) -> None:
        assert _extract_domain("raw__learn_ai__app__postgres__oauth2_grant") == "learn_ai"

    def test_two_part_prefix(self) -> None:
        assert _extract_domain("raw__edxorg") == "edxorg"

    def test_single_segment_returns_empty(self) -> None:
        """Prefix without __ delimiter — second part doesn't exist."""
        assert _extract_domain("raw") == ""

    def test_empty_string_returns_empty(self) -> None:
        assert _extract_domain("") == ""


class TestAdjustSourceSchemaPattern:
    _BASE = "version: 2\n\nsources:\n- name: ol_warehouse_production_raw\n  tables:\n  - name: my_table\n"

    def test_replaces_production_schema_name(self) -> None:
        result = _adjust_source_schema_pattern(self._BASE)
        assert "ol_warehouse_raw_data" in result
        assert "ol_warehouse_production_raw" not in result

    def test_injects_loader_airbyte(self) -> None:
        result = _adjust_source_schema_pattern(self._BASE)
        assert "loader: airbyte" in result

    def test_injects_database_jinja(self) -> None:
        result = _adjust_source_schema_pattern(self._BASE)
        assert "{{ target.database }}" in result

    def test_injects_schema_jinja(self) -> None:
        result = _adjust_source_schema_pattern(self._BASE)
        assert "schema_suffix" in result
        assert "_raw" in result

    def test_custom_original_schema(self) -> None:
        content = "sources:\n- name: my_custom_schema\n  tables:\n  - name: foo\n"
        result = _adjust_source_schema_pattern(content, original_schema="my_custom_schema")
        assert "ol_warehouse_raw_data" in result
        assert "my_custom_schema" not in result

    def test_does_not_duplicate_loader_if_already_present(self) -> None:
        """Idempotent: running twice doesn't insert loader/database/schema twice."""
        first = _adjust_source_schema_pattern(self._BASE)
        second = _adjust_source_schema_pattern(first)
        assert second.count("loader: airbyte") == 1


class TestMergeSourcesContent:
    """Tests for _merge_sources_content — incremental YAML merge logic."""

    _EXISTING = """\
version: 2

sources:
- name: ol_warehouse_raw_data
  loader: airbyte
  database: '{{ target.database }}'
  schema: '{{ target.schema.replace(var("schema_suffix", ""), "").rstrip("_") }}_raw'
  tables:
  - name: existing_table
    columns:
    - name: id
      data_type: bigint
    - name: created_at
      data_type: timestamp
"""

    _NEW_TABLE = """\
version: 2

sources:
- name: ol_warehouse_production_raw
  tables:
  - name: new_table
    columns:
    - name: record_id
      data_type: varchar
    - name: updated_at
      data_type: timestamp
"""

    def test_adds_new_table_to_existing_source(self) -> None:
        result = _merge_sources_content(self._EXISTING, self._NEW_TABLE)
        assert "new_table" in result
        assert "existing_table" in result

    def test_preserves_existing_table_columns(self) -> None:
        result = _merge_sources_content(self._EXISTING, self._NEW_TABLE)
        assert "id" in result
        assert "created_at" in result

    def test_new_table_columns_present(self) -> None:
        result = _merge_sources_content(self._EXISTING, self._NEW_TABLE)
        assert "record_id" in result

    def test_merges_new_column_into_existing_table(self) -> None:
        """New column on an existing table should be added."""
        new_with_extra_col = """\
version: 2

sources:
- name: ol_warehouse_production_raw
  tables:
  - name: existing_table
    columns:
    - name: id
      data_type: bigint
    - name: brand_new_col
      data_type: boolean
"""
        result = _merge_sources_content(self._EXISTING, new_with_extra_col)
        assert "brand_new_col" in result
        assert "id" in result

    def test_preserves_column_with_description(self) -> None:
        """Existing column with a description should not have its description overwritten."""
        existing_with_desc = """\
version: 2

sources:
- name: ol_warehouse_raw_data
  loader: airbyte
  tables:
  - name: existing_table
    columns:
    - name: id
      data_type: bigint
      description: "Primary key"
"""
        new_changing_type = """\
version: 2

sources:
- name: ol_warehouse_production_raw
  tables:
  - name: existing_table
    columns:
    - name: id
      data_type: varchar
"""
        result = _merge_sources_content(existing_with_desc, new_changing_type)
        # Description should be preserved
        assert "Primary key" in result
        # Data type should NOT be overwritten when description exists
        assert "bigint" in result

    def test_updates_data_type_when_no_description(self) -> None:
        """Column without a description should have its data_type updated from new content."""
        new_updated_type = """\
version: 2

sources:
- name: ol_warehouse_production_raw
  tables:
  - name: existing_table
    columns:
    - name: created_at
      data_type: timestamp with time zone
"""
        result = _merge_sources_content(self._EXISTING, new_updated_type)
        assert "timestamp with time zone" in result

    def test_invalid_existing_yaml_falls_back_to_new_content(self) -> None:
        result = _merge_sources_content("not: valid: yaml: [{", self._NEW_TABLE)
        assert "new_table" in result

    def test_invalid_new_yaml_returns_existing_content(self) -> None:
        result = _merge_sources_content(self._EXISTING, "not: valid: yaml: [{")
        assert "existing_table" in result

    def test_source_not_in_existing_is_appended(self) -> None:
        """New source with a different name is appended, not merged."""
        new_different_source = """\
version: 2

sources:
- name: a_different_source
  tables:
  - name: some_table
    columns: []
"""
        result = _merge_sources_content(self._EXISTING, new_different_source)
        assert "a_different_source" in result
        assert "ol_warehouse_raw_data" in result

    @pytest.mark.parametrize(
        ("prefix", "expected_domain"),
        [
            ("raw__mitlearn__app__postgres__users", "mitlearn"),
            ("raw__learn_ai__app__postgres__grant", "learn_ai"),
            ("raw__edxorg__tracking", "edxorg"),
        ],
    )
    def test_extract_domain_parametrized(self, prefix: str, expected_domain: str) -> None:
        assert _extract_domain(prefix) == expected_domain


class TestDiscoverTablesFromLocalDb:
    """Tests for _discover_tables_from_local_db — reads from the _glue_source_registry."""

    def _mock_duckdb_connect(self, mock_connect: MagicMock, rows: Sequence[tuple[str, ...]]) -> MagicMock:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = rows
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_connect.return_value.__exit__.return_value = False
        return mock_conn

    def test_raises_if_db_not_found(self, tmp_path: Path) -> None:
        missing = tmp_path / "nonexistent.duckdb"
        with pytest.raises(FileNotFoundError, match="Run `ol-dbt local register`"):
            _discover_tables_from_local_db(missing, "my_db", "raw__prefix__")

    def test_returns_matching_table_names(self, tmp_path: Path) -> None:
        db = tmp_path / "local.duckdb"
        db.touch()
        with patch("ol_dbt_cli.commands.generate.duckdb.connect") as mock_connect:
            self._mock_duckdb_connect(mock_connect, [("raw__prefix__table_a",), ("raw__prefix__table_b",)])
            result = _discover_tables_from_local_db(db, "my_db", "raw__prefix__")
        assert result == ["raw__prefix__table_a", "raw__prefix__table_b"]

    def test_strips_trailing_wildcards_from_prefix(self, tmp_path: Path) -> None:
        """Trailing _ and % are stripped before appending the LIKE wildcard."""
        db = tmp_path / "local.duckdb"
        db.touch()
        with patch("ol_dbt_cli.commands.generate.duckdb.connect") as mock_connect:
            mock_conn = self._mock_duckdb_connect(mock_connect, [])
            _discover_tables_from_local_db(db, "my_db", "raw__prefix__%")
        # The LIKE parameter should end with a single % (clean + appended)
        executed_args = mock_conn.execute.call_args[0]
        assert executed_args[1][1] == "raw__prefix%"

    def test_returns_empty_list_when_no_matches(self, tmp_path: Path) -> None:
        db = tmp_path / "local.duckdb"
        db.touch()
        with patch("ol_dbt_cli.commands.generate.duckdb.connect") as mock_connect:
            self._mock_duckdb_connect(mock_connect, [])
            result = _discover_tables_from_local_db(db, "my_db", "raw__nomatch__")
        assert result == []

    def test_opens_db_in_read_only_mode(self, tmp_path: Path) -> None:
        db = tmp_path / "local.duckdb"
        db.touch()
        with patch("ol_dbt_cli.commands.generate.duckdb.connect") as mock_connect:
            self._mock_duckdb_connect(mock_connect, [])
            _discover_tables_from_local_db(db, "my_db", "raw__prefix__")
        mock_connect.assert_called_once_with(str(db), read_only=True)


class TestDescribeLocalView:
    """Tests for _describe_local_view — DESCRIBE against a registered DuckDB view."""

    def _mock_duckdb_connect(
        self, mock_connect: MagicMock, describe_rows: Sequence[tuple[str | None, ...]]
    ) -> MagicMock:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = describe_rows
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_connect.return_value.__exit__.return_value = False
        return mock_conn

    def test_returns_column_name_and_type(self, tmp_path: Path) -> None:
        db = tmp_path / "local.duckdb"
        db.touch()
        rows = [("id", "BIGINT", "YES", None, None, None), ("name", "VARCHAR", "YES", None, None, None)]
        with patch("ol_dbt_cli.commands.generate.duckdb.connect") as mock_connect:
            self._mock_duckdb_connect(mock_connect, rows)
            result = _describe_local_view(db, "my_db", "users")
        assert result == [{"name": "id", "data_type": "BIGINT"}, {"name": "name", "data_type": "VARCHAR"}]

    def test_view_name_uses_glue_convention(self, tmp_path: Path) -> None:
        db = tmp_path / "local.duckdb"
        db.touch()
        with patch("ol_dbt_cli.commands.generate.duckdb.connect") as mock_connect:
            mock_conn = self._mock_duckdb_connect(mock_connect, [])
            _describe_local_view(db, "ol_warehouse_production_raw", "my_table")
        describe_call = mock_conn.execute.call_args_list[-1]
        # View name is double-quoted to handle non-identifier characters.
        assert '"glue__ol_warehouse_production_raw__my_table"' in describe_call[0][0]

    def test_loads_required_extensions(self, tmp_path: Path) -> None:
        db = tmp_path / "local.duckdb"
        db.touch()
        with patch("ol_dbt_cli.commands.generate.duckdb.connect") as mock_connect:
            mock_conn = self._mock_duckdb_connect(mock_connect, [])
            _describe_local_view(db, "my_db", "orders")
        load_calls = [c for c in mock_conn.execute.call_args_list if c[0][0].startswith("LOAD ")]
        loaded = [c[0][0].split()[-1] for c in load_calls]
        assert "httpfs" in loaded
        assert "aws" in loaded
        assert "iceberg" in loaded

    def test_calls_load_aws_credentials(self, tmp_path: Path) -> None:
        db = tmp_path / "local.duckdb"
        db.touch()
        with patch("ol_dbt_cli.commands.generate.duckdb.connect") as mock_connect:
            mock_conn = self._mock_duckdb_connect(mock_connect, [])
            _describe_local_view(db, "my_db", "orders")
        mock_conn.execute.assert_any_call("CALL load_aws_credentials()")


class TestBuildSourcesContentFromLocal:
    """Tests for _build_sources_content_from_local — YAML generation from local schemas."""

    _TABLES_WITH_COLS: list[tuple[str, list[dict[str, str]]]] = [
        ("raw__app__users", [{"name": "id", "data_type": "BIGINT"}, {"name": "email", "data_type": "VARCHAR"}]),
        ("raw__app__orders", [{"name": "order_id", "data_type": "BIGINT"}]),
    ]

    def test_source_name_matches_glue_database(self) -> None:
        result = _build_sources_content_from_local("my_glue_db", self._TABLES_WITH_COLS)
        assert "name: my_glue_db" in result

    def test_all_tables_present(self) -> None:
        result = _build_sources_content_from_local("my_glue_db", self._TABLES_WITH_COLS)
        assert "raw__app__users" in result
        assert "raw__app__orders" in result

    def test_column_names_present(self) -> None:
        result = _build_sources_content_from_local("my_glue_db", self._TABLES_WITH_COLS)
        assert "name: id" in result
        assert "name: email" in result
        assert "name: order_id" in result

    def test_data_types_present(self) -> None:
        result = _build_sources_content_from_local("my_glue_db", self._TABLES_WITH_COLS)
        assert "BIGINT" in result
        assert "VARCHAR" in result

    def test_version_field_present(self) -> None:
        result = _build_sources_content_from_local("my_glue_db", self._TABLES_WITH_COLS)
        assert "version: 2" in result

    def test_empty_tables_list(self) -> None:
        result = _build_sources_content_from_local("my_glue_db", [])
        assert "my_glue_db" in result
        assert "tables:" in result


class TestBuildStagingSqlFromColumns:
    """Tests for _build_staging_sql_from_columns — minimal staging SQL scaffold."""

    _COLS = [{"name": "id", "data_type": "BIGINT"}, {"name": "email", "data_type": "VARCHAR"}]

    def test_contains_with_source_as(self) -> None:
        result = _build_staging_sql_from_columns("my_source", "users", self._COLS)
        assert "with source as" in result

    def test_contains_renamed_as(self) -> None:
        result = _build_staging_sql_from_columns("my_source", "users", self._COLS)
        assert "renamed as" in result

    def test_source_macro_call_present(self) -> None:
        result = _build_staging_sql_from_columns("my_source", "users", self._COLS)
        assert "source('my_source', 'users')" in result

    def test_all_column_names_in_select(self) -> None:
        result = _build_staging_sql_from_columns("my_source", "users", self._COLS)
        assert "id" in result
        assert "email" in result

    def test_ends_with_select_star_from_renamed(self) -> None:
        result = _build_staging_sql_from_columns("my_source", "users", self._COLS)
        assert "select * from renamed" in result

    def test_empty_columns_produces_valid_sql(self) -> None:
        result = _build_staging_sql_from_columns("my_source", "users", [])
        assert "with source as" in result
        # Empty columns: falls back to select * from source (no renamed CTE)
        assert "select * from source" in result
        assert "renamed as" not in result

    def test_different_source_and_table_names(self) -> None:
        result = _build_staging_sql_from_columns("ol_warehouse_raw_data", "raw__app__orders", self._COLS)
        assert "source('ol_warehouse_raw_data', 'raw__app__orders')" in result
