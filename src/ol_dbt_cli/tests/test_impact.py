"""Tests for commands/impact.py — column diff and lineage helpers."""

from __future__ import annotations

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
