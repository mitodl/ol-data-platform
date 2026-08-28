"""Cursor-viability classification. No AWS: the Glue read lives at the command edge."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ol_dbt_cli.lib.cursor_audit import Verdict, audit, classify_table, select_units

AIRBYTE_COLUMNS = [
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta",
    "_airbyte_generation_id",
]


def classify(columns: list[str] | None, declared: str | list[str] | None = None) -> Any:
    """Classify one table, wrapping a bare `declared` string into a path."""
    if isinstance(declared, str):
        declared = [declared]
    return classify_table(
        unit_key="mitxonline/app_postgres",
        raw_table="raw__mitxonline__app__postgres__thing",
        stream="thing",
        columns=columns,
        declared_cursor=declared,
    )


class TestCandidateDetection:
    def test_django_modification_timestamp_is_a_candidate(self) -> None:
        f = classify([*AIRBYTE_COLUMNS, "id", "title", "created_on", "updated_on"])
        assert f.verdict is Verdict.CURSOR_AVAILABLE
        assert f.candidate == "updated_on"

    def test_modification_wins_over_creation(self) -> None:
        # Both present is the common Django shape; keying on created_on would
        # capture inserts and never reflect an edit.
        f = classify([*AIRBYTE_COLUMNS, "id", "created_at", "updated_at"])
        assert f.candidate == "updated_at"

    def test_creation_only_is_its_own_bucket(self) -> None:
        # Valid for an append-only ledger, a trap for a mutable row, and the
        # schema cannot tell which -- so it must not be folded into either side.
        f = classify([*AIRBYTE_COLUMNS, "id", "amount", "created_at"])
        assert f.verdict is Verdict.INSERT_ONLY
        assert f.candidate == "created_at"

    def test_wagtail_revision_timestamp_is_only_a_secondary_candidate(self) -> None:
        f = classify([*AIRBYTE_COLUMNS, "id", "title", "latest_revision_created_at", "live"])
        assert f.verdict is Verdict.SECONDARY_AVAILABLE
        assert f.candidate == "latest_revision_created_at"

    def test_no_time_column_means_replace(self) -> None:
        f = classify([*AIRBYTE_COLUMNS, "id", "course_id", "topic_id"])
        assert f.verdict is Verdict.REPLACE
        assert f.candidate is None

    def test_loader_bookkeeping_columns_are_not_source_columns(self) -> None:
        # Airbyte's _airbyte_extracted_at would otherwise read as a cursor, and
        # it describes the load rather than the row.
        f = classify([*AIRBYTE_COLUMNS, "id", "course_id"])
        assert f.verdict is Verdict.REPLACE
        assert f.source_columns == 2

    def test_dlt_bookkeeping_columns_are_also_excluded(self) -> None:
        f = classify(["_dlt_id", "_dlt_load_id", "id", "course_id"])
        assert f.verdict is Verdict.REPLACE
        assert f.source_columns == 2


class TestDeclaredCursorIsCheckedAgainstReality:
    """The standing check: schemas change, and a dead cursor fails silently."""

    def test_declared_and_present_is_settled(self) -> None:
        f = classify([*AIRBYTE_COLUMNS, "id", "updated_on"], declared="updated_on")
        assert f.verdict is Verdict.CURSOR_OK

    def test_declared_but_column_is_gone(self) -> None:
        f = classify([*AIRBYTE_COLUMNS, "id", "modified"], declared="updated_on")
        assert f.verdict is Verdict.CURSOR_MISSING
        assert f.declared_cursor == "updated_on"

    def test_a_declaration_is_not_second_guessed(self) -> None:
        # A better-looking candidate does not override an explicit choice: the
        # author may know something about which column is actually maintained.
        f = classify([*AIRBYTE_COLUMNS, "id", "updated_on", "modified"], declared="modified")
        assert f.verdict is Verdict.CURSOR_OK
        assert f.candidate == "modified"

    def test_declared_column_matching_is_case_insensitive(self) -> None:
        f = classify([*AIRBYTE_COLUMNS, "id", "updated_on"], declared="UPDATED_ON")
        assert f.verdict is Verdict.CURSOR_OK

    def test_table_declared_but_never_landed(self) -> None:
        f = classify(None, declared="updated_on")
        assert f.verdict is Verdict.NOT_LANDED

    def test_a_nested_cursor_path_is_reported_not_guessed(self) -> None:
        # cursor_field is a PATH. Checking only the first segment would report
        # cursor_ok whenever the struct still exists, even after the field
        # inside it vanished — the failure this module exists to catch, but
        # inverted into a false all-clear.
        f = classify([*AIRBYTE_COLUMNS, "id", "payload"], declared=["payload", "updated_at"])
        assert f.verdict is Verdict.CURSOR_NESTED
        assert f.declared_cursor == "payload.updated_at"
        assert f.needs_attention

    def test_a_nested_path_is_not_rescued_by_a_present_first_segment(self) -> None:
        f = classify([*AIRBYTE_COLUMNS, "payload"], declared=["payload", "gone_at"])
        assert f.verdict is not Verdict.CURSOR_OK

    def test_a_single_segment_path_is_checked_normally(self) -> None:
        assert classify([*AIRBYTE_COLUMNS, "updated_on"], declared=["updated_on"]).verdict is Verdict.CURSOR_OK


class TestNeedsAttention:
    def test_a_narrow_replace_table_is_not_worth_a_human(self) -> None:
        # id + two FKs is a Django M2M join table: replace is correct, not a
        # compromise, and 213 of them should not be a review queue.
        f = classify([*AIRBYTE_COLUMNS, "id", "course_id", "topic_id"])
        assert f.verdict is Verdict.REPLACE
        assert not f.needs_attention

    def test_a_wide_replace_table_is(self) -> None:
        f = classify([*AIRBYTE_COLUMNS, *(f"col_{n}" for n in range(12))])
        assert f.verdict is Verdict.REPLACE
        assert f.needs_attention

    def test_a_broken_declaration_always_is(self) -> None:
        f = classify([*AIRBYTE_COLUMNS, "id"], declared="updated_on")
        assert f.needs_attention

    def test_a_settled_cursor_is_not(self) -> None:
        f = classify([*AIRBYTE_COLUMNS, "id", "updated_on"], declared="updated_on")
        assert not f.needs_attention


@dataclass
class FakeUnit:
    data: dict[str, Any]

    @property
    def tables(self) -> list[dict[str, Any]]:
        return self.data.get("tables", [])


class TestSelectUnits:
    """A partial match must not read as success — the caller audits what it was asked for."""

    def _units(self) -> list[FakeUnit]:
        return [
            FakeUnit({"deployment": "mitxonline", "layer": "app_postgres"}),
            FakeUnit({"deployment": "xpro", "layer": "mysql"}),
        ]

    def test_selects_the_requested_units(self) -> None:
        selected, missing = select_units(self._units(), ["xpro/mysql"])
        assert not missing
        assert [u.data["deployment"] for u in selected] == ["xpro"]

    def test_a_typo_alongside_a_valid_key_is_reported(self) -> None:
        # The regression: filtering and checking only for an empty result left
        # this silent, so the command audited one unit and reported success.
        selected, missing = select_units(self._units(), ["mitxonline/app_postgres", "xpro/mysqlx"])
        assert missing == ["xpro/mysqlx"]
        assert len(selected) == 1

    def test_every_key_missing_is_reported_too(self) -> None:
        _, missing = select_units(self._units(), ["nope/nothing"])
        assert missing == ["nope/nothing"]


class TestAudit:
    def test_walks_units_and_sorts_stably(self) -> None:
        unit = FakeUnit(
            {
                "deployment": "mitxonline",
                "layer": "app_postgres",
                "table_prefix": "raw__mitxonline__app__postgres__",
                "tables": [
                    {"name": "zeta", "raw_table": "raw__z"},
                    {"name": "alpha", "raw_table": "raw__a"},
                ],
            }
        )
        result = audit(
            [unit],
            {
                "raw__z": [*AIRBYTE_COLUMNS, "id", "updated_on"],
                "raw__a": [*AIRBYTE_COLUMNS, "id", "b_id"],
            },
        )
        assert [f.raw_table for f in result.findings] == ["raw__a", "raw__z"]
        assert result.counts[Verdict.CURSOR_AVAILABLE] == 1
        assert result.counts[Verdict.REPLACE] == 1

    def test_cursor_field_is_read_from_the_inventory_list_form(self) -> None:
        # The schema stores cursor_field as a list of path segments.
        unit = FakeUnit(
            {
                "deployment": "d",
                "layer": "l",
                "tables": [
                    {
                        "name": "t",
                        "raw_table": "raw__t",
                        "cursor_field": ["updated_on"],
                    }
                ],
            }
        )
        result = audit([unit], {"raw__t": [*AIRBYTE_COLUMNS, "id", "updated_on"]})
        assert result.findings[0].verdict is Verdict.CURSOR_OK

    def test_broken_collects_only_vanished_cursors(self) -> None:
        unit = FakeUnit(
            {
                "deployment": "d",
                "layer": "l",
                "tables": [
                    {"name": "ok", "raw_table": "raw__ok", "cursor_field": ["updated_on"]},
                    {"name": "bad", "raw_table": "raw__bad", "cursor_field": ["gone_on"]},
                    {"name": "plain", "raw_table": "raw__plain"},
                ],
            }
        )
        result = audit(
            [unit],
            {
                "raw__ok": [*AIRBYTE_COLUMNS, "updated_on"],
                "raw__bad": [*AIRBYTE_COLUMNS, "id"],
                "raw__plain": [*AIRBYTE_COLUMNS, "id"],
            },
        )
        assert [f.stream for f in result.broken] == ["bad"]
