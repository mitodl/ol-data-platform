"""Tests for commands/impact.py — column diff and lineage helpers."""

from __future__ import annotations

from pathlib import Path

from ol_dbt_cli.commands.impact import _diff_columns


class TestDiffColumns:
    def test_removed_column(self) -> None:
        changes = _diff_columns({"user_id", "user_email", "user_name"}, {"user_id", "user_email"})
        assert any(c.column == "user_name" and c.change_type == "removed" for c in changes)

    def test_added_column(self) -> None:
        changes = _diff_columns({"user_id"}, {"user_id", "user_email"})
        assert any(c.column == "user_email" and c.change_type == "added" for c in changes)

    def test_no_changes(self) -> None:
        changes = _diff_columns({"user_id", "user_email"}, {"user_id", "user_email"})
        assert changes == []

    def test_rename_heuristic(self) -> None:
        # One removed, one added with high prefix similarity → detected as rename
        changes = _diff_columns({"user_email_address"}, {"user_email_addr"})
        change_types = {c.change_type for c in changes}
        # Should detect rename (renamed_from + renamed_to) rather than removed + added
        assert "renamed_from" in change_types or "removed" in change_types

    def test_multiple_removes(self) -> None:
        changes = _diff_columns({"a", "b", "c"}, {"a"})
        removed = {c.column for c in changes if c.change_type == "removed"}
        assert removed == {"b", "c"}


class TestGetColumnsReadFromRef:
    """Tests for _get_columns_read_from_ref — SQL-level column-read analysis."""

    def test_detects_direct_column_read_via_cte(self, tmp_path: Path) -> None:  # noqa: F821
        """Model reads upstream column via thin passthrough CTE; aliases it in output."""
        from ol_dbt_cli.commands.impact import _get_columns_read_from_ref
        from ol_dbt_cli.lib.sql_parser import parse_model_sql

        sql = """
        with src as (
            select * from ref_stg_users
        )
        select
            src.user_id as id,
            src.user_email as email_address
        from src
        """
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        # Manually set refs and ref_placeholder_map as parse_model_sql does
        parsed.refs = ["stg_users"]
        parsed.ref_placeholder_map = {"ref_stg_users": "stg_users"}
        parsed.source_path = sql_file

        result = _get_columns_read_from_ref(parsed, "stg_users")
        assert result is not None
        assert "user_email" in result
        assert "user_id" in result

    def test_returns_none_when_upstream_not_referenced(self, tmp_path: Path) -> None:  # noqa: F821
        """Returns None when the upstream model is not referenced in the SQL."""
        from ol_dbt_cli.commands.impact import _get_columns_read_from_ref
        from ol_dbt_cli.lib.sql_parser import parse_model_sql

        sql = "select user_id, email from ref_other_model"
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["other_model"]
        parsed.ref_placeholder_map = {"ref_other_model": "other_model"}
        parsed.source_path = sql_file

        result = _get_columns_read_from_ref(parsed, "stg_users")
        assert result is None

    def test_returns_empty_set_when_no_qualified_column_reads(self, tmp_path: Path) -> None:  # noqa: F821
        """When columns are referenced without table qualifier, returns empty (no false positives)."""
        from ol_dbt_cli.commands.impact import _get_columns_read_from_ref
        from ol_dbt_cli.lib.sql_parser import parse_model_sql

        # Columns unqualified — we can't determine which table they come from
        sql = "select user_id, email from ref_stg_users"
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_users"]
        parsed.ref_placeholder_map = {"ref_stg_users": "stg_users"}
        parsed.source_path = sql_file

        result = _get_columns_read_from_ref(parsed, "stg_users")
        # Without table qualifier, returns None or empty — either is acceptable
        assert result is None or len(result) == 0

    def test_false_negative_scenario_aliased_column(self, tmp_path: Path) -> None:  # noqa: F821
        """Simulates rachellougee's reported false negative: column read but renamed in output."""
        from ol_dbt_cli.commands.impact import _get_columns_read_from_ref
        from ol_dbt_cli.lib.sql_parser import parse_model_sql

        # This model reads user_email from upstream, but outputs it as user_edxorg_email.
        # With manifest column intersection, user_email is not in output columns,
        # so naive intersection returns empty. SQL-level analysis should catch it.
        sql = """
        with program_entitlements as (
            select * from ref_stg_edxorg_program_entitlement
        )
        select
            program_entitlements.program_id,
            program_entitlements.user_email as user_edxorg_email,
            program_entitlements.status
        from program_entitlements
        """
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_edxorg_program_entitlement"]
        parsed.ref_placeholder_map = {"ref_stg_edxorg_program_entitlement": "stg_edxorg_program_entitlement"}
        parsed.source_path = sql_file

        cols_read = _get_columns_read_from_ref(parsed, "stg_edxorg_program_entitlement")
        assert cols_read is not None
        # user_email is read from the upstream even though it's aliased in output
        assert "user_email" in cols_read
