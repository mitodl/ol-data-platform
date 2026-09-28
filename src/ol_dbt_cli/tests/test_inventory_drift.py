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

from ol_dbt_cli.lib.inventory import Unit, check_drift, load_units
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


def _unit(data: dict[str, Any]) -> Unit:
    """Build a Unit in memory, so a case can vary one field without a fixture file."""
    return Unit(path=Path("unit.yml"), data=data)


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


class TestWhereTablesLand:
    """`prefix + stream` against the declared `raw_table`, not prefix against prefix.

    Comparing `table_prefix` to Airbyte's `prefix` cannot work in either
    direction: the mongodb units declare `…__mongodb__` while Airbyte writes
    `…__mongodb__forum_`, and twelve connections carry no prefix at all. What
    matters is the table a stream lands in, which reproduces all 949 declared
    raw tables exactly across the real workspace.
    """

    def test_a_prefix_the_declared_raw_table_already_accounts_for_is_not_drift(self) -> None:
        # The real mongodb shape: the unit's raw_table already contains
        # `forum_`, so the longer live prefix lands exactly where declared.
        unit = copy.deepcopy(UNIT)
        unit["tables"][0]["raw_table"] = f"{PREFIX}forum_auth_user"
        units = [_unit(unit)]
        report = _run(live(prefix=f"{PREFIX}forum_"), units)
        assert report.errors == [], _messages(report, Severity.ERROR)

    def test_a_moved_table_is_an_error(self, units: list[Any]) -> None:
        report = _run(live(prefix="raw__somewhere__else__"), units)
        assert "now lands in 'raw__somewhere__else__auth_user'" in _messages(report, Severity.ERROR)[0]

    def test_a_cleared_prefix_is_caught(self, units: list[Any]) -> None:
        # The earlier check skipped a falsey prefix, so clearing one on a
        # database connection — which moves every table it lands — was silent.
        report = _run(live(prefix=""), units)
        assert "now lands in 'auth_user'" in _messages(report, Severity.ERROR)[0]

    def test_a_prefixless_connection_matching_its_declaration_is_fine(self) -> None:
        # The S3 shape: no prefix, because the stream name is already the whole
        # raw table name.
        unit = copy.deepcopy(UNIT)
        unit["tables"][0].update(name="raw__edxorg__s3__tracking_logs", raw_table="raw__edxorg__s3__tracking_logs")
        unit["airbyte"]["connections"][0]["streams"] = ["raw__edxorg__s3__tracking_logs"]
        snapshot = live(prefix="")
        snapshot["connections"][0]["configurations"]["streams"][0]["name"] = "raw__edxorg__s3__tracking_logs"
        report = _run(snapshot, [_unit(unit)])
        assert report.errors == [], _messages(report, Severity.ERROR)


class TestSourceDrift:
    """Source-level state, which no stream describes."""

    def test_a_changed_replication_method_is_an_error(self, units: list[Any]) -> None:
        # §3.4 records this so `rg replication_method: xmin` answers which
        # connections need a replacement cursor before dlt. Flipping a source
        # from xmin to a cursor column changes no stream at all.
        snapshot = live(sourceId="src-1")
        snapshot["sources"] = [
            {"sourceId": "src-1", "sourceType": "mysql", "configuration": {"replication_method": {"method": "Xmin"}}}
        ]
        report = _run(snapshot, units)
        assert "replicates by 'xmin', unit declares 'cursor'" in _messages(report, Severity.ERROR)[0]

    def test_a_changed_connector_is_an_error(self, units: list[Any]) -> None:
        snapshot = live(sourceId="src-1")
        snapshot["sources"] = [{"sourceId": "src-1", "sourceType": "postgres", "configuration": {}}]
        report = _run(snapshot, units)
        assert "now uses connector 'source-postgres'" in _messages(report, Severity.ERROR)[0]

    def test_a_matching_source_is_silent(self, units: list[Any]) -> None:
        snapshot = live(sourceId="src-1")
        snapshot["sources"] = [
            {
                "sourceId": "src-1",
                "sourceType": "mysql",
                "configuration": {"replication_method": {"method": "STANDARD"}},
            }
        ]
        assert _run(snapshot, units).issues == []


class TestIncompleteSnapshot:
    def test_missing_stream_configuration_is_not_read_as_zero_streams(self, units: list[Any]) -> None:
        # The dumper re-fetches a connection whose list response omitted its
        # streams, but leaves the key absent if that GET also fails. Collapsing
        # that to `[]` would turn one transient API failure into "every declared
        # stream has been dropped" across the workspace.
        snapshot = live()
        del snapshot["connections"][0]["configurations"]
        report = _run(snapshot, units)
        assert "holds no stream configuration" in _messages(report, Severity.ERROR)[0]
        assert not [m for m in _messages(report, Severity.ERROR) if "no longer carries" in m]

    def test_an_explicitly_empty_stream_list_is_still_drift(self, units: list[Any]) -> None:
        snapshot = live()
        snapshot["connections"][0]["configurations"]["streams"] = []
        report = _run(snapshot, units)
        assert "no longer carries declared stream" in _messages(report, Severity.ERROR)[0]


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
