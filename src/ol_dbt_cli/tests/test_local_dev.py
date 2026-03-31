"""Tests for commands/local_dev.py — DuckDB/Iceberg local development helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ol_dbt_cli.commands.local_dev import (
    PROTECTED_SCHEMAS,
    _register_single_table,
    _validate_schema_safety,
)


class TestValidateSchemaSafety:
    """Tests for _validate_schema_safety — guards against dropping production schemas."""

    def test_allows_exact_suffixed_schema(self) -> None:
        assert _validate_schema_safety("ol_warehouse_production_tmacey", "tmacey", "ol_warehouse_production") is True

    def test_allows_suffixed_schema_with_layer_suffix(self) -> None:
        """Schema like base_suffix_raw (a sub-layer of the dev schema) is also safe."""
        assert (
            _validate_schema_safety("ol_warehouse_production_tmacey_raw", "tmacey", "ol_warehouse_production") is True
        )

    def test_blocks_protected_production_schema(self) -> None:
        assert _validate_schema_safety("ol_warehouse_production", "tmacey", "ol_warehouse_production") is False

    def test_blocks_all_protected_schemas(self) -> None:
        """Every entry in PROTECTED_SCHEMAS must be blocked regardless of suffix."""
        for schema in PROTECTED_SCHEMAS:
            base = schema.rsplit("_", 1)[0] if "_" in schema else schema
            assert _validate_schema_safety(schema, "tmacey", base) is False, (
                f"Expected {schema!r} to be blocked but it was allowed"
            )

    def test_blocks_empty_suffix(self) -> None:
        """Empty suffix means no developer scope — always blocked."""
        assert _validate_schema_safety("ol_warehouse_production_something", "", "ol_warehouse_production") is False

    def test_blocks_wrong_suffix(self) -> None:
        """Schema belonging to a different developer's suffix must not be cleaned."""
        assert _validate_schema_safety("ol_warehouse_production_alice", "bob", "ol_warehouse_production") is False

    def test_blocks_base_schema_without_suffix(self) -> None:
        """The bare base schema itself (no suffix appended) must never be dropped."""
        assert _validate_schema_safety("ol_warehouse_qa", "tmacey", "ol_warehouse_qa") is False

    @pytest.mark.parametrize(
        ("schema", "suffix", "base", "expected"),
        [
            ("ol_warehouse_production_dev123", "dev123", "ol_warehouse_production", True),
            ("ol_warehouse_production_dev123_staging", "dev123", "ol_warehouse_production", True),
            ("ol_warehouse_production", "dev123", "ol_warehouse_production", False),
            ("ol_warehouse_production_qa", "dev123", "ol_warehouse_production", False),
            ("ol_warehouse_production_dev123x", "dev123", "ol_warehouse_production", False),
        ],
    )
    def test_parametrized_safety_cases(self, schema: str, suffix: str, base: str, expected: bool) -> None:
        assert _validate_schema_safety(schema, suffix, base) is expected


class TestRegisterSingleTable:
    """Tests for _register_single_table — per-thread DuckDB view registration."""

    def _make_table(self, name: str = "users", location: str = "s3://bucket/users/v1.json") -> dict[str, str]:
        return {"name": name, "metadata_location": location}

    def test_skips_unchanged_table(self, tmp_path: Path) -> None:
        table = self._make_table()
        existing = {"glue__my_db__users": "s3://bucket/users/v1.json"}

        status, view_name, extra = _register_single_table(
            table, "my_db", tmp_path / "test.duckdb", existing, force=False
        )

        assert status == "skipped"
        assert view_name == "glue__my_db__users"
        assert extra is None

    def test_skips_check_uses_view_name_not_table_name(self, tmp_path: Path) -> None:
        """Skip comparison key is the full view_name, not just the table name."""
        table = self._make_table(name="orders", location="s3://bucket/orders/v2.json")
        # Different metadata location → should NOT skip
        existing = {"glue__my_db__orders": "s3://bucket/orders/v1.json"}

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            status, view_name, extra = _register_single_table(
                table, "my_db", tmp_path / "test.duckdb", existing, force=False
            )

        assert status != "skipped"
        assert view_name == "glue__my_db__orders"

    def test_registers_new_table_calls_duckdb(self, tmp_path: Path) -> None:
        """New table (not in existing_registrations) triggers DuckDB CREATE VIEW."""
        table = self._make_table()
        existing: dict[str, str] = {}

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            status, view_name, extra = _register_single_table(
                table, "my_db", tmp_path / "test.duckdb", existing, force=False
            )

        assert status == "success"
        assert view_name == "glue__my_db__users"
        assert extra == "new"
        mock_conn.execute.assert_any_call(
            "CREATE OR REPLACE VIEW glue__my_db__users AS\nSELECT * FROM iceberg_scan('s3://bucket/users/v1.json')\n"
        )

    def test_registers_updated_table_when_location_changed(self, tmp_path: Path) -> None:
        """Table with a changed metadata_location should be re-registered and marked 'updated'."""
        table = self._make_table(location="s3://bucket/users/v2.json")
        existing = {"glue__my_db__users": "s3://bucket/users/v1.json"}

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            status, view_name, extra = _register_single_table(
                table, "my_db", tmp_path / "test.duckdb", existing, force=False
            )

        assert status == "success"
        assert extra == "updated"

    def test_force_re_registers_unchanged_table(self, tmp_path: Path) -> None:
        """force=True bypasses the skip check, re-registering even unchanged tables."""
        table = self._make_table()
        existing = {"glue__my_db__users": "s3://bucket/users/v1.json"}

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            status, view_name, extra = _register_single_table(
                table, "my_db", tmp_path / "test.duckdb", existing, force=True
            )

        assert status == "success"
        assert extra == "updated"  # it was in existing_registrations → "updated"

    def test_returns_error_on_duckdb_exception(self, tmp_path: Path) -> None:
        """DuckDB errors (e.g. bad Iceberg manifest) are caught and returned as 'error'."""
        table = self._make_table()
        existing: dict[str, str] = {}

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = RuntimeError("Iceberg manifest read failed")
            mock_connect.return_value = mock_conn

            status, view_name, extra = _register_single_table(
                table, "my_db", tmp_path / "test.duckdb", existing, force=False
            )

        assert status == "error"
        assert view_name == "glue__my_db__users"
        assert "Iceberg manifest read failed" in (extra or "")

    def test_loads_extensions_and_credentials_per_connection(self, tmp_path: Path) -> None:
        """Every worker thread must LOAD extensions and call load_aws_credentials()."""
        table = self._make_table()

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            _register_single_table(table, "my_db", tmp_path / "test.duckdb", {}, force=False)

        execute_calls = [str(c) for c in mock_conn.execute.call_args_list]
        assert any("LOAD httpfs" in c for c in execute_calls)
        assert any("LOAD aws" in c for c in execute_calls)
        assert any("LOAD iceberg" in c for c in execute_calls)
        assert any("load_aws_credentials" in c for c in execute_calls)

    def test_view_name_uses_database_prefix(self, tmp_path: Path) -> None:
        """View name is always glue__{database}__{table}."""
        table = self._make_table(name="enrollments")

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect"):
            _, view_name, _ = _register_single_table(
                table, "ol_warehouse_production_raw", tmp_path / "test.duckdb", {}, force=False
            )

        assert view_name == "glue__ol_warehouse_production_raw__enrollments"

    def test_inserts_registry_row_on_success(self, tmp_path: Path) -> None:
        """Successful registration persists a row into _glue_source_registry."""
        table = self._make_table()

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            _register_single_table(table, "my_db", tmp_path / "test.duckdb", {}, force=False)

        registry_insert_call = next(
            (c for c in mock_conn.execute.call_args_list if "_glue_source_registry" in str(c)),
            None,
        )
        assert registry_insert_call is not None, "Expected INSERT into _glue_source_registry"
        # Ensure the correct values were passed
        _, kwargs = registry_insert_call
        args_positional = registry_insert_call[0]
        if len(args_positional) > 1:
            row = args_positional[1]
            assert "glue__my_db__users" in row
            assert "my_db" in row
            assert "users" in row
