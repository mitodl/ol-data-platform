"""Tests for the §7.2 removal/rename check and the §5 render targets.

The removal check is the one rule in the inventory whose absence is invisible:
every other finding shows up as a broken build somewhere, while a silently
dropped table just stops arriving. So each way a removal can be acknowledged —
and each way it can fail to be — gets its own case.
"""

from __future__ import annotations

import copy
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.lib.inventory import (
    RenderError,
    Snapshot,
    Unit,
    check_removals,
    dagster_group_name,
    load_snapshot,
    load_snapshot_at_ref,
    render_airbyte,
    render_dagster_intervals,
    validate_inventory,
)
from ol_dbt_cli.lib.validation import Severity, ValidationReport

REPO_ROOT = Path(__file__).resolve().parents[3]
REAL_INVENTORY = REPO_ROOT / "ingestion" / "inventory"

UNIT: dict[str, Any] = {
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
                "name": "MITx Online Open edX DB → S3 Data Lake",
                "status": "active",
                "sync_interval_hours": 12,
                "streams": ["auth_user", "courseware_studentmodule"],
            }
        ],
    },
    "tables": [
        {
            "name": "auth_user",
            "namespace": "edxapp",
            "raw_table": "raw__mitxonline__openedx__mysql__auth_user",
            "sync_mode": "incremental_append",
            "cursor_field": ["id"],
            "primary_key": [["id"]],
            "modeled": True,
        },
        {
            "name": "courseware_studentmodule",
            "namespace": "edxapp",
            "raw_table": "raw__mitxonline__openedx__mysql__courseware_studentmodule",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": [["course_id", "id"], ["revision.with.dots"]],
            "excluded_columns": ["state"],
            "modeled": False,
        },
    ],
}


def _unit(data: dict[str, Any], name: str = "mitxonline__mysql.yml") -> Unit:
    return Unit(path=Path("ingestion/inventory/units") / name, data=data)


def _snapshot(data: dict[str, Any], retired: list[dict[str, Any]] | None = None) -> Snapshot:
    return Snapshot(units=[_unit(data)], retired=retired or [])


def _without_second_table() -> dict[str, Any]:
    """Return the unit with `courseware_studentmodule` dropped, connection included."""
    after = copy.deepcopy(UNIT)
    after["tables"] = [after["tables"][0]]
    after["airbyte"]["connections"][0]["streams"] = ["auth_user"]
    return after


def _run(previous: Snapshot, current: Snapshot) -> ValidationReport:
    report = ValidationReport()
    check_removals(previous, current, report)
    return report


def _messages(report: ValidationReport, severity: Severity = Severity.ERROR) -> str:
    return " | ".join(issue.message for issue in report.issues if issue.severity == severity)


RETIRED_ENTRY = {
    "deployment": "mitxonline",
    "layer": "mysql",
    "raw_tables": ["raw__mitxonline__openedx__mysql__courseware_studentmodule"],
    "retired_on": "2026-08-18",
    "reason": "Superseded by the studentmodulehistory extract; the table has not been read since.",
}


class TestRemovals:
    def test_an_unchanged_inventory_reports_nothing(self) -> None:
        report = _run(_snapshot(UNIT), _snapshot(UNIT))
        assert report.issues == []

    def test_a_dropped_table_is_an_error(self) -> None:
        report = _run(_snapshot(UNIT), _snapshot(_without_second_table()))
        assert "courseware_studentmodule disappeared" in _messages(report)

    def test_a_dropped_table_is_forgiven_by_a_retired_entry(self) -> None:
        report = _run(_snapshot(UNIT), _snapshot(_without_second_table(), [RETIRED_ENTRY]))
        assert report.errors == [], _messages(report)

    def test_a_retired_entry_for_a_different_unit_does_not_forgive(self) -> None:
        # The graveyard is keyed on (deployment, layer, raw_table): the same
        # table name under another unit is a different table.
        entry = {**RETIRED_ENTRY, "layer": "app_postgres"}
        report = _run(_snapshot(UNIT), _snapshot(_without_second_table(), [entry]))
        assert "disappeared" in _messages(report)

    def test_deleting_the_whole_unit_is_an_error_per_table(self) -> None:
        report = _run(_snapshot(UNIT), Snapshot())
        assert len(report.errors) == 2  # noqa: PLR2004

    def test_a_first_ever_inventory_removes_nothing(self) -> None:
        report = _run(Snapshot(), _snapshot(UNIT))
        assert report.issues == []


class TestRenames:
    def test_a_rename_without_acknowledgement_looks_like_a_removal(self) -> None:
        after = copy.deepcopy(UNIT)
        after["tables"][1]["raw_table"] = "raw__mitxonline__openedx__mysql__studentmodule"
        report = _run(_snapshot(UNIT), _snapshot(after))
        assert "courseware_studentmodule disappeared" in _messages(report)

    def test_renamed_from_acknowledges_it(self) -> None:
        after = copy.deepcopy(UNIT)
        after["tables"][1]["raw_table"] = "raw__mitxonline__openedx__mysql__studentmodule"
        after["tables"][1]["renamed_from"] = "raw__mitxonline__openedx__mysql__courseware_studentmodule"
        report = _run(_snapshot(UNIT), _snapshot(after))
        assert report.errors == [], _messages(report)
        assert "was renamed to" in _messages(report, Severity.INFO)

    def test_renamed_from_in_another_unit_does_not_acknowledge_it(self) -> None:
        # `renamed_from` is scoped to the unit: a table moving between units is
        # a removal from one and an addition to the other, and the removal side
        # still has to be stated.
        after = copy.deepcopy(UNIT)
        after["tables"] = [after["tables"][0]]
        after["airbyte"]["connections"][0]["streams"] = ["auth_user"]
        other = copy.deepcopy(UNIT)
        other["layer"] = "openedx_notes"
        other["tables"][1]["renamed_from"] = "raw__mitxonline__openedx__mysql__courseware_studentmodule"
        current = Snapshot(units=[_unit(after), _unit(other, "mitxonline__openedx_notes.yml")])
        report = _run(_snapshot(UNIT), current)
        assert "courseware_studentmodule disappeared" in _messages(report)

    def test_a_renamed_from_that_matches_no_removal_warns(self) -> None:
        after = copy.deepcopy(UNIT)
        after["tables"][1]["renamed_from"] = "raw__mitxonline__openedx__mysql__never_existed"
        report = _run(_snapshot(UNIT), _snapshot(after))
        assert report.errors == []
        assert "did not disappear in this change" in _messages(report, Severity.WARNING)


class TestGraveyardIsNeverEmptied:
    def test_deleting_a_retired_entry_is_an_error(self) -> None:
        previous = _snapshot(_without_second_table(), [RETIRED_ENTRY])
        current = _snapshot(_without_second_table(), [])
        report = _run(previous, current)
        assert "was deleted" in _messages(report)

    def test_a_retired_table_the_unit_still_declares_is_a_contradiction(self, tmp_path: Path) -> None:
        root = _materialize(tmp_path, UNIT, [RETIRED_ENTRY])
        report = ValidationReport()
        validate_inventory(root, report)
        assert "is retired but the unit still declares it" in _messages(report)

    def test_a_graveyard_entry_without_a_reason_is_rejected(self, tmp_path: Path) -> None:
        entry = {key: value for key, value in RETIRED_ENTRY.items() if key != "reason"}
        root = _materialize(tmp_path, _without_second_table(), [entry])
        report = ValidationReport()
        validate_inventory(root, report)
        assert "'reason' is a required property" in _messages(report)

    def test_a_missing_retired_schema_fails_loudly_rather_than_skipping_validation(self, tmp_path: Path) -> None:
        # An optional schema means deleting retired.schema.json silently waives
        # every date/reason requirement, letting a malformed entry acknowledge a
        # removal and pass check-removals. The schema is mandatory, like the
        # unit schema already is.
        root = _materialize(tmp_path, _without_second_table(), [RETIRED_ENTRY])
        (root / "schema" / "retired.schema.json").unlink()
        with pytest.raises(FileNotFoundError):
            validate_inventory(root, ValidationReport())


def _materialize(tmp_path: Path, unit: dict[str, Any], retired: list[dict[str, Any]]) -> Path:
    """Write a one-unit inventory on disk, carrying the real schemas and vocabulary."""
    root = tmp_path / "inventory"
    (root / "units").mkdir(parents=True)
    (root / "schema").mkdir()
    for name in ("unit.schema.json", "retired.schema.json"):
        (root / "schema" / name).write_text((REAL_INVENTORY / "schema" / name).read_text())
    (root / "vocabulary.yml").write_text((REAL_INVENTORY / "vocabulary.yml").read_text())
    (root / "units" / "mitxonline__mysql.yml").write_text(yaml.safe_dump(unit, sort_keys=False))
    (root / "retired.yml").write_text(yaml.safe_dump({"schema_version": 1, "retired": retired}, sort_keys=False))
    return root


class TestReadingTheBaseRefFromGit:
    """The one case the in-memory tests cannot cover: reading the *deleted* side.

    A unit file the branch removed exists only in the tree, so the base-ref
    snapshot has to come from `git ls-tree` rather than from disk — the mistake
    this guards against is globbing the worktree and finding nothing to compare.
    """

    def test_a_deleted_unit_file_is_still_read_at_the_base_ref(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        inventory = _materialize(repo, UNIT, [])
        _git(repo, "init", "--initial-branch=main")
        _git(repo, "add", "-A")
        _git(repo, "-c", "user.email=t@example.com", "-c", "user.name=T", "commit", "-m", "inventory")
        base = _git(repo, "rev-parse", "HEAD").strip()

        (inventory / "units" / "mitxonline__mysql.yml").unlink()

        previous = load_snapshot_at_ref(inventory, base, repo_root=repo)
        report = ValidationReport()
        check_removals(previous, load_snapshot(inventory), report)
        assert len(previous.units) == 1
        assert len(report.errors) == 2  # noqa: PLR2004


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(  # noqa: S603
        ["git", *args],  # noqa: S607
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


class TestRenderAirbyte:
    @pytest.fixture
    def rendered(self) -> dict[str, Any]:
        return render_airbyte([_unit(UNIT)])

    def test_it_carries_the_streams_of_each_connection(self, rendered: dict[str, Any]) -> None:
        streams = rendered["units"][0]["connections"][0]["streams"]
        assert [stream["name"] for stream in streams] == ["auth_user", "courseware_studentmodule"]

    def test_namespace_survives(self, rendered: dict[str, Any]) -> None:
        # 790 of 1,518 live streams carry one, and a render that drops it cannot
        # reproduce the imported connection, so §6.4's empty preview never lands.
        assert all(stream["namespace"] == "edxapp" for stream in rendered["units"][0]["connections"][0]["streams"])

    def test_a_composite_primary_key_round_trips_without_ambiguity(self, rendered: dict[str, Any]) -> None:
        streams = rendered["units"][0]["connections"][0]["streams"]
        assert streams[0]["primary_key"] == [["id"]]
        # A two-segment nested path and a single segment containing a literal
        # "." are stored and rendered as distinct shapes — neither collapses
        # into the other, unlike the old dotted-string encoding.
        assert streams[1]["primary_key"] == [["course_id", "id"], ["revision.with.dots"]]

    def test_excluded_columns_are_emitted_as_the_exclusion(self, rendered: dict[str, Any]) -> None:
        # Not as `selected_fields`: the complement needs the discovered schema,
        # which the inventory does not hold.
        streams = rendered["units"][0]["connections"][0]["streams"]
        assert streams[1]["excluded_columns"] == ["state"]
        assert "selected_fields" not in streams[1]

    def test_an_undeclared_stream_stops_the_render(self) -> None:
        # `render` does not validate first, so it meets inventories `validate`
        # would have rejected. Skipping the stream would be worse than failing:
        # this JSON is applied to production Airbyte, so a dropped stream is a
        # connection reconfigured to stop carrying a table — silently, in a file
        # a human reviewed.
        broken = copy.deepcopy(UNIT)
        broken["airbyte"]["connections"][0]["streams"].append("never_declared")
        with pytest.raises(RenderError, match="never_declared"):
            render_airbyte([_unit(broken)])

    def test_dlt_units_are_not_rendered(self) -> None:
        dlt_unit = copy.deepcopy(UNIT)
        dlt_unit["loader"] = "dlt"
        del dlt_unit["airbyte"]
        dlt_unit["dlt"] = {"source_module": "ol_dlt.sources.mitxonline", "write_disposition": "merge"}
        assert render_airbyte([_unit(dlt_unit)])["units"] == []


class TestRenderDagsterIntervals:
    def test_the_group_name_matches_the_dagster_translator(self) -> None:
        # definitions.py drops Airbyte's U+2192 arrow entirely rather than
        # turning it into a separator, which is why the doubled underscore is
        # correct and "fixing" it would break the lookup.
        assert dagster_group_name("MITx Online Open edX DB → S3 Data Lake") == "mitx_online_open_edx_db__s3_data_lake"

    def test_it_maps_the_derived_group_to_its_cadence(self) -> None:
        assert render_dagster_intervals([_unit(UNIT)]) == {"mitx_online_open_edx_db__s3_data_lake": 12}

    def test_dagster_invisible_units_are_omitted(self) -> None:
        hidden = copy.deepcopy(UNIT)
        hidden["dagster_visible"] = False
        hidden["airbyte"]["connections"][0]["name"] = "MITx Online Open edX DB → Snowflake"
        assert render_dagster_intervals([_unit(hidden)]) == {}

    def test_a_paused_connection_keeps_its_interval(self) -> None:
        # Dagster selects on the connection name alone, so a paused connection
        # still produces a group; dropping its entry hands it the silent
        # 24-hour default the moment somebody re-enables it.
        paused = copy.deepcopy(UNIT)
        paused["airbyte"]["connections"][0]["status"] = "inactive"
        assert render_dagster_intervals([_unit(paused)]) == {"mitx_online_open_edx_db__s3_data_lake": 12}

    def test_two_connection_names_colliding_on_the_same_interval_is_fine(self) -> None:
        # Dashes and spaces both collapse to "_", so distinct connection names
        # legitimately derive the same group when they agree on cadence.
        collided = copy.deepcopy(UNIT)
        collided["airbyte"]["connections"][0]["name"] = "MITx Online-Open edX DB S3 Data Lake"
        second = copy.deepcopy(collided["airbyte"]["connections"][0])
        second["name"] = "MITx Online Open edX DB S3 Data Lake"
        collided["airbyte"]["connections"].append(second)
        assert render_dagster_intervals([_unit(collided)]) == {"mitx_online_open_edx_db_s3_data_lake": 12}

    def test_two_connection_names_colliding_on_different_intervals_stops_the_render(
        self,
    ) -> None:
        # A group name is only unique modulo dashes/whitespace, so two distinct
        # connections that disagree on cadence would otherwise have one silently
        # overwrite the other based on sort order — recreating the wrong-schedule
        # failure this renderer exists to prevent.
        collided = copy.deepcopy(UNIT)
        collided["airbyte"]["connections"][0]["name"] = "MITx Online-Open edX DB S3 Data Lake"
        second = copy.deepcopy(collided["airbyte"]["connections"][0])
        second["name"] = "MITx Online Open edX DB S3 Data Lake"
        second["sync_interval_hours"] = 6
        collided["airbyte"]["connections"].append(second)
        with pytest.raises(RenderError, match="mitx_online_open_edx_db_s3_data_lake"):
            render_dagster_intervals([_unit(collided)])
