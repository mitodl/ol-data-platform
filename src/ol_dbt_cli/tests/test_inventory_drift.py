"""Tests for `ol-dbt inventory drift` (INGESTION_INVENTORY_SPEC §4, step 8).

Steps 5 and 6 were struck (§6.0), so nothing applies the inventory to Airbyte
and this check is the only thing that notices a UI edit. Its first run against
production found a real error in the hand-written edxorg units, which is the
argument for the cases below: each one is a way the file can quietly stop
describing reality.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.lib.inventory import check_drift, load_units
from ol_dbt_cli.lib.validation import Severity, ValidationReport

PREFIX = "raw__mitxonline__openedx__mysql__"

UNIT: dict[str, Any] = {
    "schema_version": 1,
    "deployment": "mitxonline",
    "layer": "mysql",
    "scope": "scoped",
    "strategies": {"qa": "omit", "local": "fixture"},
    "loader": "airbyte",
    "table_prefix": PREFIX,
    "airbyte": {
        "source_kind": "source-mysql",
        "replication_method": "cursor",
        "connections": [
            {
                "name": "MITx Online Open edX DB → S3 Data Lake",
                "status": "active",
                "sync_interval_hours": 12,
                "streams": ["auth_user"],
            }
        ],
    },
    "tables": [
        {
            "name": "auth_user",
            "raw_table": f"{PREFIX}auth_user",
            "sync_mode": "incremental_append",
            "cursor_field": ["id"],
            "modeled": True,
        }
    ],
}

CONNECTION_NAME = "MITx Online Open edX DB → S3 Data Lake"


def live(**overrides: Any) -> dict[str, Any]:
    """Build a snapshot that agrees with UNIT, before overrides are applied."""
    connection = {
        "name": CONNECTION_NAME,
        "status": "active",
        "prefix": PREFIX,
        "schedule": {"scheduleType": "manual"},
        "configurations": {
            "streams": [
                {
                    "name": "auth_user",
                    "syncMode": "incremental_append",
                    "cursorField": ["id"],
                    "primaryKey": [],
                    "selectedFields": [],
                }
            ]
        },
    }
    connection.update(overrides)
    return {"connections": [connection]}


@pytest.fixture
def units(tmp_path: Path) -> list[Any]:
    root = tmp_path / "inventory"
    (root / "units").mkdir(parents=True)
    (root / "units" / "mitxonline__mysql.yml").write_text(yaml.safe_dump(UNIT, sort_keys=False))
    return load_units(root)


def _run(snapshot: dict[str, Any], units: list[Any]) -> ValidationReport:
    report = ValidationReport()
    check_drift(snapshot, units, report)
    return report


def _messages(report: ValidationReport, severity: Severity) -> list[str]:
    return [i.message for i in report.issues if i.severity == severity]


class TestAgreement:
    def test_a_workspace_matching_the_inventory_is_silent(self, units: list[Any]) -> None:
        assert _run(live(), units).issues == []

    def test_an_empty_cursor_matches_an_undeclared_one(self, units: list[Any]) -> None:
        # Airbyte returns `[]` where the render omits the field. Comparing those
        # raw would report drift on nearly every stream, since most have no
        # primary key.
        no_key = copy.deepcopy(UNIT)
        del no_key["tables"][0]["cursor_field"]
        snapshot = live()
        snapshot["connections"][0]["configurations"]["streams"][0]["cursorField"] = []
        report = ValidationReport()
        check_drift(snapshot, [type(units[0])(path=Path("u.yml"), data=no_key)], report)
        assert report.issues == []


class TestConnectionDrift:
    def test_a_declared_connection_missing_from_airbyte_is_an_error(self, units: list[Any]) -> None:
        report = _run({"connections": []}, units)
        assert "is declared but no longer exists in Airbyte" in _messages(report, Severity.ERROR)[0]

    def test_an_undeclared_live_connection_is_a_warning(self, units: list[Any]) -> None:
        # Usually config someone forgot to delete, and it costs nothing until
        # something depends on it — unlike a declaration that has gone missing.
        snapshot = live()
        snapshot["connections"].append({"name": "Something Someone Made In The UI", "configurations": {}})
        report = _run(snapshot, units)
        assert report.errors == []
        assert "no unit declares it" in _messages(report, Severity.WARNING)[0]

    def test_a_paused_connection_is_reported(self, units: list[Any]) -> None:
        report = _run(live(status="inactive"), units)
        assert "'inactive' in Airbyte but declared 'active'" in _messages(report, Severity.ERROR)[0]


class TestScheduleDrift:
    def test_an_active_connection_with_its_own_schedule_is_an_error(self, units: list[Any]) -> None:
        report = _run(live(schedule={"scheduleType": "basic"}), units)
        assert "carries its own Airbyte schedule" in _messages(report, Severity.ERROR)[0]

    def test_a_paused_one_is_only_a_warning(self, units: list[Any]) -> None:
        # It cannot double-schedule anything while paused; resuming it would.
        report = _run(live(status="inactive", schedule={"scheduleType": "basic"}), units)
        schedule = [m for m in _messages(report, Severity.WARNING) if "schedule" in m]
        assert schedule, _messages(report, Severity.WARNING)


class TestPrefixDrift:
    def test_a_narrower_live_prefix_is_not_drift(self, units: list[Any]) -> None:
        # Real case: the mongodb units declare `…__mongodb__` while Airbyte
        # writes `…__mongodb__forum_`. `table_prefix` is declared documentation
        # covering a unit's tables (§1.1), not a copy of Airbyte's literal.
        report = _run(live(prefix=f"{PREFIX}forum_"), units)
        assert [m for m in _messages(report, Severity.ERROR) if "prefix" in m] == []

    def test_a_live_prefix_outside_the_declared_one_is_an_error(self, units: list[Any]) -> None:
        report = _run(live(prefix="raw__somewhere__else__"), units)
        assert "outside the unit's declared" in [m for m in _messages(report, Severity.ERROR) if "prefix" in m][0]


class TestStreamDrift:
    def test_a_dropped_stream_is_an_error(self, units: list[Any]) -> None:
        snapshot = live()
        snapshot["connections"][0]["configurations"]["streams"] = []
        report = _run(snapshot, units)
        assert "no longer carries declared stream" in _messages(report, Severity.ERROR)[0]

    def test_an_extra_stream_is_a_warning(self, units: list[Any]) -> None:
        snapshot = live()
        snapshot["connections"][0]["configurations"]["streams"].append(
            {"name": "auth_group", "syncMode": "incremental_append"}
        )
        report = _run(snapshot, units)
        assert report.errors == []
        assert "which the unit does not declare" in _messages(report, Severity.WARNING)[0]

    def test_a_changed_sync_mode_is_an_error(self, units: list[Any]) -> None:
        # This is the case that caught the hand-written edxorg units: seven
        # streams declared full_refresh_overwrite that Airbyte runs incrementally.
        snapshot = live()
        snapshot["connections"][0]["configurations"]["streams"][0]["syncMode"] = "full_refresh_overwrite"
        report = _run(snapshot, units)
        assert "sync_mode is 'full_refresh_overwrite' in Airbyte" in _messages(report, Severity.ERROR)[0]

    def test_a_changed_cursor_is_an_error(self, units: list[Any]) -> None:
        snapshot = live()
        snapshot["connections"][0]["configurations"]["streams"][0]["cursorField"] = ["updated_at"]
        report = _run(snapshot, units)
        assert "cursor_field is ['updated_at'] in Airbyte" in _messages(report, Severity.ERROR)[0]
