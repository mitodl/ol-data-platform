"""Tests for the raw history snapshot generator and its inventory rules."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.lib.inventory import (
    LEARNER_ACTIVITY_TABLES,
    RenderError,
    Unit,
    keyset_raw_tables,
    load_units,
    reconcile_dbt,
    reconcile_warehouse,
    validate_inventory,
)
from ol_dbt_cli.lib.raw_history import (
    DELETE_FILTER_MACRO_PATH,
    KEYSET_SOURCES_PATH,
    SNAPSHOT_DIR,
    history_tables,
    render_history,
    stale_history_files,
    write_history_files,
)
from ol_dbt_cli.lib.validation import Severity, ValidationReport

REPO_ROOT = Path(__file__).resolve().parents[3]
REAL_INVENTORY = REPO_ROOT / "ingestion" / "inventory"

PILOT_TABLES = {
    "cms_certificatepage",
    "cms_externalcoursepage",
    "cms_externalcoursepage_topics",
    "courses_courserun",
    "courses_coursetopic",
    "ecommerce_product",
    "ecommerce_productversion",
    "wagtailcore_page",
}

DB_UNIT: dict[str, Any] = {
    "schema_version": 1,
    "deployment": "mitxonline",
    "layer": "mysql",
    "scope": "scoped",
    "strategies": {"qa": "ingest", "local": "fixture"},
    "loader": "airbyte",
    "table_prefix": "raw__mitxonline__openedx__mysql__",
    "airbyte": {
        "source_kind": "source-mysql",
        "replication_method": "cursor",
        "connections": [
            {
                "environment": "production",
                "name": "MITx Online Open edX DB → S3 Data Lake",
                "status": "active",
                "sync_interval_hours": 12,
                "streams": ["auth_user", "course_overviews", "student_courseenrollment"],
            }
        ],
    },
    "tables": [
        {
            "name": "auth_user",
            "raw_table": "raw__mitxonline__openedx__mysql__auth_user",
            "sync_mode": "incremental_deduped_history",
            "cursor_field": ["id"],
            "primary_key": [["id"]],
            "modeled": True,
        },
        {
            "name": "course_overviews",
            "raw_table": "raw__mitxonline__openedx__mysql__course_overviews",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": [["id"]],
            "modeled": True,
        },
        {
            "name": "student_courseenrollment",
            "raw_table": "raw__mitxonline__openedx__mysql__student_courseenrollment",
            "sync_mode": "full_refresh_overwrite",
            "modeled": True,
        },
    ],
}

FEED_UNIT: dict[str, Any] = {
    "schema_version": 1,
    "deployment": "youtube",
    "layer": "api",
    "scope": "scoped",
    "strategies": {"qa": "ingest", "local": "fixture"},
    "loader": "dlt",
    "table_prefix": "raw__youtube__api__",
    "history": {"absence": "deleted"},
    "dlt": {"source_module": "ol_dlt.sources.youtube", "write_disposition": "merge"},
    "tables": [
        {
            "name": "raw__youtube__api__videos",
            "raw_table": "raw__youtube__api__videos",
            "sync_mode": "full_refresh_overwrite_deduped",
            "primary_key": [["video_id"]],
            "modeled": True,
        }
    ],
}


def _unit(data: dict[str, Any], name: str = "unit.yml") -> Unit:
    return Unit(path=Path(name), data=data)


def _by_name(units: list[Unit]) -> dict[str, Any]:
    return {table.name: table for rows in history_tables(units).values() for table in rows}


@pytest.fixture
def inventory(tmp_path: Path) -> Path:
    root = tmp_path / "inventory"
    (root / "units").mkdir(parents=True)
    (root / "schema").mkdir()
    (root / "schema" / "unit.schema.json").write_text((REAL_INVENTORY / "schema" / "unit.schema.json").read_text())
    (root / "vocabulary.yml").write_text((REAL_INVENTORY / "vocabulary.yml").read_text())
    return root


def _validate(root: Path, *units: tuple[str, dict[str, Any]]) -> ValidationReport:
    for name, data in units:
        (root / "units" / f"{name}.yml").write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True))
    report = ValidationReport()
    validate_inventory(root, report)
    return report


def _errors(report: ValidationReport) -> list[str]:
    return [issue.message for issue in report.issues if issue.severity == Severity.ERROR]


class TestCommittedFiles:
    def test_generated_files_are_current(self) -> None:
        """A hand edit, or an inventory change not regenerated, fails here and in CI."""
        rendered = render_history(load_units(REAL_INVENTORY))
        assert stale_history_files(REPO_ROOT, rendered) == [], "Run `ol-dbt inventory history --write`."

    def test_real_scope(self) -> None:
        """334 tables: 339 on 2026-11-10, less mitx's five unmodeled assessment_ai* tables."""
        selected = history_tables(load_units(REAL_INVENTORY))
        raw_tables = {table.raw_table for rows in selected.values() for table in rows}
        assert len(raw_tables) == 334  # noqa: PLR2004
        assert not raw_tables & LEARNER_ACTIVITY_TABLES

    def test_pilot_snapshots_keep_their_names_and_alone_are_live(self) -> None:
        """Renaming a pilot snapshot would orphan its production table."""
        selected = history_tables(load_units(REAL_INVENTORY))
        live = {table.snapshot_name for rows in selected.values() for table in rows if table.live}
        assert live == {f"snapshot_raw__xpro__app__postgres__{name}" for name in PILOT_TABLES}


class TestScope:
    def test_rule(self) -> None:
        selected = _by_name([_unit(DB_UNIT)])
        # No primary key, so not yet.
        assert set(selected) == {"auth_user", "course_overviews"}

    def test_opting_out(self) -> None:
        data = copy.deepcopy(DB_UNIT)
        data["tables"][0]["history"] = {"in_scope": False, "reason": "too big"}
        assert "auth_user" not in _by_name([_unit(data)])

    def test_learner_activity_tables_are_out(self) -> None:
        data = copy.deepcopy(DB_UNIT)
        data["tables"][0]["raw_table"] = "raw__mitxonline__openedx__mysql__courseware_studentmodule"
        assert "auth_user" not in _by_name([_unit(data)])

    def test_append_and_unmodeled_tables_are_out(self) -> None:
        data = copy.deepcopy(DB_UNIT)
        data["tables"][0]["sync_mode"] = "incremental_append"
        data["tables"][1]["modeled"] = False
        assert _by_name([_unit(data)]) == {}


class TestSettings:
    def test_database_unit_defaults_to_deleted_and_off(self) -> None:
        table = _by_name([_unit(DB_UNIT)])["course_overviews"]
        assert (table.absence, table.live) == ("deleted", False)

    def test_table_live_overrides_the_unit(self) -> None:
        data = copy.deepcopy(DB_UNIT)
        data["history"] = {"live": True}
        data["tables"][1]["history"] = {"live": False}
        selected = _by_name([_unit(data)])
        assert (selected["auth_user"].live, selected["course_overviews"].live) == (True, False)

    def test_incremental_database_table_gets_a_keyset_filter(self) -> None:
        spec = _by_name([_unit(DB_UNIT)])["auth_user"].delete_filter
        assert spec is not None
        assert (spec.kind, spec.key, spec.loader_time_column, spec.max_age_hours) == (
            "keyset",
            "id",
            "_airbyte_extracted_at",
            24,
        )

    def test_full_read_needs_no_filter(self) -> None:
        assert _by_name([_unit(DB_UNIT)])["course_overviews"].delete_filter is None

    def test_dlt_merge_feed_reads_the_latest_load(self) -> None:
        spec = _by_name([_unit(FEED_UNIT)])["raw__youtube__api__videos"].delete_filter
        assert spec is not None
        assert spec.kind == "latest_load"

    def test_aged_out_feed_reads_every_load(self) -> None:
        data = copy.deepcopy(FEED_UNIT)
        data["history"]["absence"] = "aged_out"
        assert _by_name([_unit(data)])["raw__youtube__api__videos"].delete_filter is None

    def test_non_database_unit_without_absence_stops_the_render(self) -> None:
        data = copy.deepcopy(FEED_UNIT)
        del data["history"]
        with pytest.raises(RenderError, match=r"history\.absence"):
            history_tables([_unit(data)])

    def test_markers_reach_only_their_tables(self) -> None:
        data = copy.deepcopy(DB_UNIT)
        marker = {"at": "2026-11-12T14:00Z", "kind": "loader_cutover", "tables": ["auth_user"], "note": "dlt"}
        data["history"] = {"markers": [marker]}
        selected = _by_name([_unit(data)])
        assert selected["auth_user"].markers == (marker,)
        assert selected["course_overviews"].markers == ()


class TestRender:
    def test_snapshot_call_spells_every_setting(self) -> None:
        data = copy.deepcopy(DB_UNIT)
        data["history"] = {"live": True}
        data["tables"][1]["history"] = {"exclude_columns": ["notes"], "ignore_columns": ["modified"]}
        sql = render_history([_unit(data)])[SNAPSHOT_DIR / "mitxonline__mysql.sql"]
        assert "{% snapshot snapshot_raw__mitxonline__openedx__mysql__course_overviews %}" in sql
        assert "    exclude_columns=['notes'],\n    ignore_columns=['modified'],\n    live=true\n" in sql
        assert "absence=" not in sql

    def test_composite_key_is_a_list(self) -> None:
        data = copy.deepcopy(DB_UNIT)
        data["tables"][1]["primary_key"] = [["Email"], ["Program Title"]]
        sql = render_history([_unit(data)])[SNAPSHOT_DIR / "mitxonline__mysql.sql"]
        assert "unique_key=['Email', 'Program Title']," in sql

    def test_aged_out_docs_have_no_deleted_column(self) -> None:
        data = copy.deepcopy(FEED_UNIT)
        data["history"]["absence"] = "aged_out"
        rendered = render_history([_unit(data)])
        docs = yaml.safe_load(rendered[SNAPSHOT_DIR / "_youtube__api__snapshots.yml"])
        snapshot = docs["snapshots"][0]
        assert "dbt_is_deleted" not in {column["name"] for column in snapshot["columns"]}
        assert snapshot["config"]["meta"]["raw_history"]["delete_capture"] == "aged_out"
        assert "absence='aged_out'" in rendered[SNAPSHOT_DIR / "youtube__api.sql"]

    def test_keyset_sources_and_filter_map(self) -> None:
        rendered = render_history([_unit(DB_UNIT), _unit(FEED_UNIT)])
        sources = yaml.safe_load(rendered[KEYSET_SOURCES_PATH])["sources"][0]
        assert [table["name"] for table in sources["tables"]] == ["raw__mitxonline__openedx__mysql__auth_user__keyset"]
        macro = rendered[DELETE_FILTER_MACRO_PATH]
        assert (
            "'raw__mitxonline__openedx__mysql__auth_user': {'kind': 'keyset', "
            "'loader_time_column': '_airbyte_extracted_at', 'key': 'id', 'max_age_hours': 24}," in macro
        )
        assert "'raw__youtube__api__videos': {'kind': 'latest_load', 'loader_time_column': '_dlt_load_id'}," in macro


class TestFiles:
    def test_an_empty_inventory_renders_nothing(self) -> None:
        """A wrong --inventory-dir loads no units, and --write would then delete every file."""
        with pytest.raises(RenderError, match="no inventory units"):
            render_history([])

    def test_hand_edit_and_leftover_are_stale_and_write_fixes_both(self, tmp_path: Path) -> None:
        rendered = render_history([_unit(DB_UNIT)])
        write_history_files(tmp_path, rendered)
        assert stale_history_files(tmp_path, rendered) == []

        sql = tmp_path / SNAPSHOT_DIR / "mitxonline__mysql.sql"
        sql.write_text(sql.read_text() + "-- edited\n")
        leftover = tmp_path / SNAPSHOT_DIR / "retired__unit.sql"
        leftover.write_text("")
        assert stale_history_files(tmp_path, rendered) == [
            SNAPSHOT_DIR / "mitxonline__mysql.sql",
            SNAPSHOT_DIR / "retired__unit.sql",
        ]

        write_history_files(tmp_path, rendered)
        assert not leftover.exists()
        assert stale_history_files(tmp_path, rendered) == []


class TestValidation:
    def test_valid_units_pass(self, inventory: Path) -> None:
        assert _errors(_validate(inventory, ("mitxonline__mysql", DB_UNIT), ("youtube__api", FEED_UNIT))) == []

    def test_non_database_unit_must_state_absence(self, inventory: Path) -> None:
        data = copy.deepcopy(FEED_UNIT)
        del data["history"]
        assert any(
            "states no history.absence" in message for message in _errors(_validate(inventory, ("youtube__api", data)))
        )

    def test_opting_out_needs_a_reason(self, inventory: Path) -> None:
        data = copy.deepcopy(DB_UNIT)
        data["tables"][0]["history"] = {"in_scope": False}
        assert any("reason" in message for message in _errors(_validate(inventory, ("mitxonline__mysql", data))))

    def test_settings_on_a_table_out_of_scope(self, inventory: Path) -> None:
        data = copy.deepcopy(DB_UNIT)
        data["tables"][2]["history"] = {"exclude_columns": ["notes"]}
        errors = _errors(_validate(inventory, ("mitxonline__mysql", data)))
        assert any("is not in history scope" in message for message in errors)

    def test_key_columns_cannot_be_excluded(self, inventory: Path) -> None:
        data = copy.deepcopy(DB_UNIT)
        data["tables"][1]["history"] = {"ignore_columns": ["ID"]}
        errors = _errors(_validate(inventory, ("mitxonline__mysql", data)))
        assert any("key column(s) ['id']" in message for message in errors)

    def test_marker_names_unknown_table(self, inventory: Path) -> None:
        data = copy.deepcopy(DB_UNIT)
        data["history"] = {
            "markers": [{"at": "2026-11-12T14:00Z", "kind": "schema_change", "tables": ["nope"], "note": "x"}]
        }
        errors = _errors(_validate(inventory, ("mitxonline__mysql", data)))
        assert any("unknown tables ['nope']" in message for message in errors)

    def test_nested_key_fails_without_a_history_block(self, inventory: Path) -> None:
        data = copy.deepcopy(DB_UNIT)
        data["tables"][1]["primary_key"] = [["payload", "id"]]
        errors = _errors(_validate(inventory, ("mitxonline__mysql", data)))
        assert any("has a nested path" in message for message in errors)

    def test_keyless_candidate_is_reported_as_info(self, inventory: Path) -> None:
        report = _validate(inventory, ("mitxonline__mysql", DB_UNIT))
        infos = [issue.message for issue in report.issues if issue.severity == Severity.INFO]
        assert any("student_courseenrollment is left out of raw history" in message for message in infos)


class TestReconcile:
    def test_keyset_tables_are_accounted_for(self, inventory: Path) -> None:
        units = [_unit(DB_UNIT)]
        keysets = keyset_raw_tables(units)
        assert keysets == {"raw__mitxonline__openedx__mysql__auth_user__keyset"}
        declared = {str(table["raw_table"]) for table in DB_UNIT["tables"]}

        report = ValidationReport()
        reconcile_dbt(units, declared | keysets, inventory, report)
        reconcile_warehouse(units, declared | keysets, report)
        assert report.issues == []
