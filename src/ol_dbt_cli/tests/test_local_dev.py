"""Tests for commands/local_dev.py — DuckDB/Iceberg local development helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ol_dbt_cli.commands.local_dev import (
    PROTECTED_SCHEMAS,
    REGISTRY_STALE_AFTER_DAYS,
    _classify_registration,
    _describe_registry_age,
    _register_single_table,
    _registry_last_refreshed,
    _show_registry,
    _stale_threshold_phrase,
    _validate_schema_safety,
    snapshot,
)


class TestSnapshot:
    def test_runs_ctas_with_validated_identifiers(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        import subprocess

        (tmp_path / "dbt_project.yml").write_text("name: test\nprofile: test\n")
        calls: list[list[str]] = []

        def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            calls.append(cmd)
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", fake_run)
        snapshot(
            "enrollment_detail_report",
            as_name="enrollment_detail_report_baseline",
            dbt_dir_path=str(tmp_path),
        )
        assert len(calls) == 1
        inline_idx = calls[0].index("--inline") + 1
        inline_sql = calls[0][inline_idx]
        assert "identifier='enrollment_detail_report_baseline'" in inline_sql
        assert "ref('enrollment_detail_report')" in inline_sql
        assert "--limit" in calls[0]
        assert calls[0][calls[0].index("--limit") + 1] == "-1"

    def test_rejects_invalid_identifier(self, tmp_path: Path) -> None:
        with pytest.raises(SystemExit):
            snapshot("m; drop table x", as_name="baseline", dbt_dir_path=str(tmp_path))


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

    def _mock_duckdb_connect(self, mock_connect: MagicMock, mock_conn: MagicMock) -> None:
        """Wire mock_connect so that `with duckdb.connect(...) as conn:` yields mock_conn."""
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_connect.return_value.__exit__.return_value = False

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
            self._mock_duckdb_connect(mock_connect, mock_conn)

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
            self._mock_duckdb_connect(mock_connect, mock_conn)

            status, view_name, extra = _register_single_table(
                table, "my_db", tmp_path / "test.duckdb", existing, force=False
            )

        assert status == "success"
        assert view_name == "glue__my_db__users"
        assert extra == "new"
        mock_conn.execute.assert_any_call(
            "CREATE OR REPLACE VIEW \"glue__my_db__users\" AS\nSELECT * FROM iceberg_scan('s3://bucket/users/v1.json')\n"
        )

    def test_registers_updated_table_when_location_changed(self, tmp_path: Path) -> None:
        """Table with a changed metadata_location should be re-registered and marked 'updated'."""
        table = self._make_table(location="s3://bucket/users/v2.json")
        existing = {"glue__my_db__users": "s3://bucket/users/v1.json"}

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect") as mock_connect:
            mock_conn = MagicMock()
            self._mock_duckdb_connect(mock_connect, mock_conn)

            status, view_name, extra = _register_single_table(
                table, "my_db", tmp_path / "test.duckdb", existing, force=False
            )

        assert status == "success"
        assert extra == "updated"

    def test_force_re_registers_unchanged_table_as_refreshed(self, tmp_path: Path) -> None:
        """force=True re-registers an unchanged table, reported as 'refreshed' not 'updated'.

        'updated' is reserved for a table whose Glue metadata location actually
        moved. Reporting a forced no-op re-registration as 'updated' would let a
        forced run claim upstream changes that never happened.
        """
        table = self._make_table()
        existing = {"glue__my_db__users": "s3://bucket/users/v1.json"}

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect") as mock_connect:
            mock_conn = MagicMock()
            self._mock_duckdb_connect(mock_connect, mock_conn)

            status, view_name, extra = _register_single_table(
                table, "my_db", tmp_path / "test.duckdb", existing, force=True
            )

        assert status == "success"
        assert extra == "refreshed"

    def test_force_still_reports_a_moved_pointer_as_updated(self, tmp_path: Path) -> None:
        """Under force, a genuinely changed location is still 'updated', not 'refreshed'."""
        table = self._make_table(location="s3://bucket/users/v2.json")
        existing = {"glue__my_db__users": "s3://bucket/users/v1.json"}

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect") as mock_connect:
            mock_conn = MagicMock()
            self._mock_duckdb_connect(mock_connect, mock_conn)

            _status, _view_name, extra = _register_single_table(
                table, "my_db", tmp_path / "test.duckdb", existing, force=True
            )

        assert extra == "updated"

    def test_returns_error_on_duckdb_exception(self, tmp_path: Path) -> None:
        """DuckDB errors (e.g. bad Iceberg manifest) are caught and returned as 'error'."""
        table = self._make_table()
        existing: dict[str, str] = {}

        with patch("ol_dbt_cli.commands.local_dev.duckdb.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = RuntimeError("Iceberg manifest read failed")
            self._mock_duckdb_connect(mock_connect, mock_conn)

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
            self._mock_duckdb_connect(mock_connect, mock_conn)

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
            self._mock_duckdb_connect(mock_connect, mock_conn)

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


class TestClassifyRegistration:
    """Tests for _classify_registration — the shared new/updated/refreshed decision."""

    def test_unregistered_table_is_new(self) -> None:
        assert _classify_registration(None, "s3://bucket/v1.json") == "new"

    def test_moved_pointer_is_updated(self) -> None:
        assert _classify_registration("s3://bucket/v1.json", "s3://bucket/v2.json") == "updated"

    def test_identical_pointer_is_refreshed(self) -> None:
        assert _classify_registration("s3://bucket/v1.json", "s3://bucket/v1.json") == "refreshed"


class TestRegistryAge:
    """Tests for registry staleness reporting — the signal the incident lacked."""

    def _make_registry(self, db_path: Path, scanned_at_sql: str, glue_database: str = "my_db") -> None:
        import duckdb

        with duckdb.connect(str(db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS _glue_registry_scans (
                    glue_database VARCHAR PRIMARY KEY,
                    scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(
                f"INSERT OR REPLACE INTO _glue_registry_scans VALUES (?, {scanned_at_sql})",  # noqa: S608
                (glue_database,),
            )

    def test_reports_age_through_show_registry(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """_show_registry must report the age, not degrade it to "unknown".

        Regression: _show_registry holds a read-write connection while asking for
        the age. When _registry_last_refreshed opened its own read_only handle to
        the same file, DuckDB refused it ("Can't open a connection to same
        database file with a different configuration") and the broad except
        turned a readable 3-day-old registry into "unknown" -- so list-sources
        could never report an age or warn, on either a fresh or a stale registry.
        """
        import duckdb

        db = tmp_path / "local.duckdb"
        with duckdb.connect(str(db)) as conn:
            conn.execute("""
                CREATE TABLE _glue_source_registry (
                    view_name VARCHAR PRIMARY KEY,
                    glue_database VARCHAR,
                    glue_table VARCHAR,
                    metadata_location VARCHAR,
                    registered_at TIMESTAMP
                )
            """)
            conn.execute(
                "INSERT INTO _glue_source_registry VALUES ('glue__my_db__t', 'my_db', 't', 's3://x', CURRENT_TIMESTAMP)"
            )
            conn.execute("""
                CREATE TABLE _glue_registry_scans (
                    glue_database VARCHAR PRIMARY KEY,
                    scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("INSERT INTO _glue_registry_scans VALUES ('my_db', CURRENT_TIMESTAMP - INTERVAL 3 DAY)")

        _show_registry(db)
        out = capsys.readouterr().out
        assert "unknown" not in out
        assert "3.0 days ago" in out
        assert "More than a day old" in out

    def test_returns_none_when_no_database_file(self, tmp_path: Path) -> None:
        assert _registry_last_refreshed(tmp_path / "missing.duckdb", ["my_db"]) is None

    def test_returns_none_when_no_registry_table(self, tmp_path: Path) -> None:
        """An existing DuckDB file with no registry table must not raise."""
        import duckdb

        db = tmp_path / "local.duckdb"
        with duckdb.connect(str(db)) as conn:
            conn.execute("CREATE TABLE unrelated (x INTEGER)")
        assert _registry_last_refreshed(db, ["my_db"]) is None

    def test_returns_none_for_unregistered_database(self, tmp_path: Path) -> None:
        """max() over zero matching rows is NULL, which must surface as None."""
        db = tmp_path / "local.duckdb"
        self._make_registry(db, "CURRENT_TIMESTAMP", glue_database="my_db")
        assert _registry_last_refreshed(db, ["some_other_db"]) is None

    def test_reads_newest_timestamp_for_targeted_databases(self, tmp_path: Path) -> None:
        db = tmp_path / "local.duckdb"
        self._make_registry(db, "CURRENT_TIMESTAMP")
        assert _registry_last_refreshed(db, ["my_db"]) is not None

    def test_unknown_age_is_not_reported_as_stale(self) -> None:
        """No readable registration is 'unknown', not 'stale' — do not cry wolf on first run."""
        phrase, is_stale = _describe_registry_age(None)
        assert is_stale is False
        assert "unknown" in phrase

    def test_threshold_phrase_is_not_ungrammatical_at_one_day(self) -> None:
        """The banner interpolates this; at the default of 1 it must not read '1 days'."""
        assert "1 days" not in _stale_threshold_phrase()

    def test_fresh_registry_is_not_stale(self) -> None:
        import datetime

        recent = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(hours=1)
        phrase, is_stale = _describe_registry_age(recent)
        assert is_stale is False
        assert "hours ago" in phrase

    def test_old_registry_is_stale(self) -> None:
        import datetime

        old = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(days=REGISTRY_STALE_AFTER_DAYS + 1)
        phrase, is_stale = _describe_registry_age(old)
        assert is_stale is True
        assert "days ago" in phrase

    def test_naive_timestamp_does_not_raise(self) -> None:
        """DuckDB hands back naive datetimes; subtracting an aware 'now' would raise."""
        import datetime

        naive = datetime.datetime.now() - datetime.timedelta(days=1)  # noqa: DTZ005
        phrase, _is_stale = _describe_registry_age(naive)
        assert "ago" in phrase


class TestRegisterTablesInDuckdbDryRun:
    """Tests for _register_tables_in_duckdb dry-run counter accuracy."""

    def test_dry_run_counts_new_tables(self, tmp_path: Path) -> None:
        """Dry-run should increment results['new'] for tables not in existing_registrations."""
        from ol_dbt_cli.commands.local_dev import _register_tables_in_duckdb

        tables = [
            {"name": "users", "metadata_location": "s3://bucket/users/v1.json"},
            {"name": "orders", "metadata_location": "s3://bucket/orders/v1.json"},
        ]
        results = _register_tables_in_duckdb(tables, "my_db", tmp_path / "local.duckdb", dry_run=True, verbose=False)
        assert results["success"] == 2
        assert results["new"] == 2
        assert results["updated"] == 0
        assert results["skipped"] == 0

    def test_dry_run_counts_updated_tables(self, tmp_path: Path) -> None:
        """Dry-run should increment results['updated'] when metadata_location changed."""
        from ol_dbt_cli.commands.local_dev import _register_tables_in_duckdb

        db = tmp_path / "local.duckdb"
        import duckdb

        with duckdb.connect(str(db)) as conn:
            conn.execute("""
                CREATE TABLE _glue_source_registry (
                    view_name VARCHAR PRIMARY KEY,
                    glue_database VARCHAR,
                    glue_table VARCHAR,
                    metadata_location VARCHAR,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(
                "INSERT INTO _glue_source_registry VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
                ("glue__my_db__users", "my_db", "users", "s3://bucket/users/v1.json"),
            )

        tables = [{"name": "users", "metadata_location": "s3://bucket/users/v2.json"}]
        results = _register_tables_in_duckdb(tables, "my_db", db, dry_run=True, verbose=False)
        assert results["success"] == 1
        assert results["updated"] == 1
        assert results["new"] == 0

    def test_dry_run_counts_skipped_tables(self, tmp_path: Path) -> None:
        """Dry-run should increment results['skipped'] when metadata_location unchanged."""
        from ol_dbt_cli.commands.local_dev import _register_tables_in_duckdb

        db = tmp_path / "local.duckdb"
        import duckdb

        with duckdb.connect(str(db)) as conn:
            conn.execute("""
                CREATE TABLE _glue_source_registry (
                    view_name VARCHAR PRIMARY KEY,
                    glue_database VARCHAR,
                    glue_table VARCHAR,
                    metadata_location VARCHAR,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(
                "INSERT INTO _glue_source_registry VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
                ("glue__my_db__users", "my_db", "users", "s3://bucket/users/v1.json"),
            )

        tables = [{"name": "users", "metadata_location": "s3://bucket/users/v1.json"}]
        results = _register_tables_in_duckdb(tables, "my_db", db, dry_run=True, verbose=False)
        assert results["skipped"] == 1
        assert results["success"] == 0
        assert results["new"] == 0
        assert results["updated"] == 0
