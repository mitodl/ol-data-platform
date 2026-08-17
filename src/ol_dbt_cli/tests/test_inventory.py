"""Tests for the ingestion inventory checks.

Every rule in INGESTION_INVENTORY_SPEC §3.3 gets a case that fails it, because a
validator nobody has watched reject anything is a validator that passes
everything.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.lib.inventory import validate_inventory
from ol_dbt_cli.lib.validation import ValidationReport

REPO_ROOT = Path(__file__).resolve().parents[3]
REAL_INVENTORY = REPO_ROOT / "ingestion" / "inventory"

# A valid Airbyte-backed unit with two connections, the shape §3.5 established
# from the live workspace: a bulk connection plus one huge table split out onto
# its own cadence.
APP_UNIT: dict[str, Any] = {
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
                "streams": ["auth_user"],
            },
            {
                "name": "MITx Online Production Open edX Student Module History → S3 Data Lake",
                "status": "active",
                "sync_interval_hours": 24,
                "streams": ["coursewarehistoryextended_studentmodulehistoryextended"],
            },
        ],
    },
    "tables": [
        {
            "name": "auth_user",
            "raw_table": "raw__mitxonline__openedx__mysql__auth_user",
            "sync_mode": "incremental_append",
            "cursor_field": ["id"],
            "primary_key": ["id"],
            "modeled": True,
        },
        {
            "name": "coursewarehistoryextended_studentmodulehistoryextended",
            "raw_table": ("raw__mitxonline__openedx__mysql__coursewarehistoryextended_studentmodulehistoryextended"),
            "sync_mode": "incremental_append",
            "cursor_field": ["id"],
            "modeled": False,
        },
    ],
}

# A dlt-backed singleton, so the loader-conditional rules have both sides.
DLT_UNIT: dict[str, Any] = {
    "schema_version": 1,
    "deployment": "edxorg",
    "layer": "s3",
    "scope": "singleton",
    "strategies": {"qa": "omit", "local": "fixture"},
    "loader": "dlt",
    "table_prefix": "raw__edxorg__s3__",
    "dlt": {
        "source_module": "ol_dlt.sources.edxorg_s3",
        "write_disposition": "merge",
    },
    "tables": [
        {
            "name": "tables__auth_user",
            "raw_table": "raw__edxorg__s3__tables__auth_user",
            "sync_mode": "full_refresh_overwrite",
            "modeled": True,
        }
    ],
}


@pytest.fixture
def inventory(tmp_path: Path) -> Path:
    """Build a two-unit inventory carrying the real schema and vocabulary."""
    root = tmp_path / "inventory"
    (root / "units").mkdir(parents=True)
    (root / "schema").mkdir()
    (root / "schema" / "unit.schema.json").write_text((REAL_INVENTORY / "schema" / "unit.schema.json").read_text())
    (root / "vocabulary.yml").write_text((REAL_INVENTORY / "vocabulary.yml").read_text())
    _write(root, "mitxonline__mysql", APP_UNIT)
    _write(root, "edxorg__s3", DLT_UNIT)
    return root


def _write(root: Path, name: str, unit: dict[str, Any]) -> None:
    (root / "units" / f"{name}.yml").write_text(yaml.safe_dump(unit, sort_keys=False))


def _run(root: Path) -> ValidationReport:
    report = ValidationReport()
    validate_inventory(root, report)
    return report


def _mutate(root: Path, name: str, base: dict[str, Any], **changes: Any) -> None:
    unit = copy.deepcopy(base)
    unit.update(changes)
    _write(root, name, unit)


def _messages(report: ValidationReport) -> str:
    return " | ".join(issue.message for issue in report.errors)


class TestValidInventory:
    def test_two_unit_fixture_passes(self, inventory: Path) -> None:
        report = _run(inventory)
        assert report.errors == [], _messages(report)

    def test_the_real_vocabulary_and_schema_parse(self) -> None:
        # Guards against a broken JSON Schema or vocabulary landing unnoticed:
        # every fixture above depends on both, so a syntax error here fails
        # everything else confusingly.
        json.loads((REAL_INVENTORY / "schema" / "unit.schema.json").read_text())
        vocabulary = yaml.safe_load((REAL_INVENTORY / "vocabulary.yml").read_text())
        assert vocabulary["deployments"]
        assert "openedx_notes" in vocabulary["layers"]


class TestSchemaShape:
    def test_unknown_key_is_rejected(self, inventory: Path) -> None:
        _mutate(inventory, "mitxonline__mysql", APP_UNIT, sync_interval_hours=6)
        report = _run(inventory)
        assert report.errors
        assert "sync_interval_hours" in _messages(report)

    def test_local_mirror_is_rejected_by_the_schema(self, inventory: Path) -> None:
        # RFC 12711 §3 rule 1 — the enum omits the value rather than a rule
        # rejecting it, so it cannot be re-litigated per entry.
        _mutate(
            inventory,
            "mitxonline__mysql",
            APP_UNIT,
            strategies={"qa": "ingest", "local": "mirror"},
        )
        report = _run(inventory)
        assert report.errors
        assert "mirror" in _messages(report)


class TestRules:
    def test_local_ingest_requires_dlt(self, inventory: Path) -> None:
        _mutate(
            inventory,
            "mitxonline__mysql",
            APP_UNIT,
            strategies={"qa": "ingest", "local": "ingest"},
        )
        report = _run(inventory)
        assert "not dlt-backed" in _messages(report)

    def test_mirror_requires_max_age(self, inventory: Path) -> None:
        _mutate(
            inventory,
            "mitxonline__mysql",
            APP_UNIT,
            strategies={"qa": "mirror", "local": "fixture"},
        )
        report = _run(inventory)
        assert "mirror_max_age_days is not set" in _messages(report)

    def test_max_age_without_mirror_is_rejected(self, inventory: Path) -> None:
        _mutate(inventory, "mitxonline__mysql", APP_UNIT, mirror_max_age_days=30)
        report = _run(inventory)
        assert "no strategy is `mirror`" in _messages(report)

    def test_unknown_layer_is_rejected(self, inventory: Path) -> None:
        _mutate(inventory, "mitxonline__mysql", APP_UNIT, layer="notes")
        report = _run(inventory)
        assert "not in vocabulary" in _messages(report)

    def test_unknown_deployment_is_rejected(self, inventory: Path) -> None:
        _mutate(inventory, "mitxonline__mysql", APP_UNIT, deployment="thirdparty")
        report = _run(inventory)
        assert "not in vocabulary" in _messages(report)

    def test_raw_table_must_start_with_the_prefix(self, inventory: Path) -> None:
        unit = copy.deepcopy(APP_UNIT)
        unit["tables"][0]["raw_table"] = "raw__somewhere__else__auth_user"
        _write(inventory, "mitxonline__mysql", unit)
        report = _run(inventory)
        assert "does not start with the unit's table_prefix" in _messages(report)

    def test_incremental_without_cursor_is_rejected(self, inventory: Path) -> None:
        unit = copy.deepcopy(APP_UNIT)
        del unit["tables"][0]["cursor_field"]
        _write(inventory, "mitxonline__mysql", unit)
        report = _run(inventory)
        assert "declares no cursor_field" in _messages(report)

    def test_connection_name_must_survive_the_dagster_selector(self, inventory: Path) -> None:
        unit = copy.deepcopy(APP_UNIT)
        unit["airbyte"]["connections"][0]["name"] = "MITx Online Open edX DB → Somewhere"
        _write(inventory, "mitxonline__mysql", unit)
        report = _run(inventory)
        assert "does not end with" in _messages(report)

    def test_dagster_visible_false_permits_a_dropped_connection(self, inventory: Path) -> None:
        unit = copy.deepcopy(APP_UNIT)
        unit["airbyte"]["connections"][0]["name"] = "MITx Online Open edX DB → Somewhere"
        unit["dagster_visible"] = False
        _write(inventory, "mitxonline__mysql", unit)
        report = _run(inventory)
        assert report.errors == [], _messages(report)

    def test_loader_block_must_match_loader(self, inventory: Path) -> None:
        unit = copy.deepcopy(APP_UNIT)
        unit["dlt"] = {
            "source_module": "ol_dlt.sources.edxorg_s3",
            "write_disposition": "merge",
        }
        _write(inventory, "mitxonline__mysql", unit)
        report = _run(inventory)
        assert "`dlt:` is present but loader is 'airbyte'" in _messages(report)

    def test_table_carried_by_no_connection_is_rejected(self, inventory: Path) -> None:
        unit = copy.deepcopy(APP_UNIT)
        unit["airbyte"]["connections"][1]["streams"] = ["something_else"]
        _write(inventory, "mitxonline__mysql", unit)
        report = _run(inventory)
        assert "no connection carries that stream" in _messages(report)

    def test_duplicate_stream_name_within_a_unit_is_rejected(self, inventory: Path) -> None:
        # Two same-named streams in different source namespaces cannot be
        # expressed — raw_table is prefix + name, so Airbyte would write both
        # into one destination table — and the renderer resolves a connection's
        # stream to its namespace by name.
        unit = copy.deepcopy(APP_UNIT)
        second = copy.deepcopy(unit["tables"][0])
        second["raw_table"] = "raw__mitxonline__openedx__mysql__auth_user_other"
        second["namespace"] = "edxapp_csmh"
        unit["tables"].append(second)
        unit["airbyte"]["connections"][0]["streams"] = ["auth_user", "auth_user"]
        _write(inventory, "mitxonline__mysql", unit)
        report = _run(inventory)
        assert "is declared by 2 tables in this unit" in _messages(report)

    def test_table_carried_by_two_connections_is_rejected(self, inventory: Path) -> None:
        unit = copy.deepcopy(APP_UNIT)
        unit["airbyte"]["connections"][1]["streams"] = ["auth_user"]
        unit["tables"] = [unit["tables"][0]]
        _write(inventory, "mitxonline__mysql", unit)
        report = _run(inventory)
        assert "is carried 2 times within this unit" in _messages(report)


class TestCrossUnit:
    def test_overlapping_prefixes_are_rejected(self, inventory: Path) -> None:
        # `raw__edxorg__s3__tables__` sits underneath `raw__edxorg__s3__`, so a
        # raw table would map to two units.
        overlapping = copy.deepcopy(DLT_UNIT)
        overlapping["layer"] = "api"
        overlapping["table_prefix"] = "raw__edxorg__s3__tables__"
        overlapping["tables"] = [
            {
                "name": "auth_user",
                "raw_table": "raw__edxorg__s3__tables__auth_user_two",
                "sync_mode": "full_refresh_overwrite",
            }
        ]
        _write(inventory, "edxorg__api", overlapping)
        report = _run(inventory)
        assert "overlaps" in _messages(report)

    def test_duplicate_raw_table_across_units_is_rejected(self, inventory: Path) -> None:
        duplicate = copy.deepcopy(DLT_UNIT)
        duplicate["deployment"] = "oll"
        duplicate["table_prefix"] = "raw__oll__google_sheets__"
        duplicate["layer"] = "google_sheets"
        duplicate["tables"] = [
            {
                # Same raw table as the edxorg unit, reached from a different unit.
                "name": "tables__auth_user",
                "raw_table": "raw__edxorg__s3__tables__auth_user",
                "sync_mode": "full_refresh_overwrite",
            }
        ]
        _write(inventory, "oll__google_sheets", duplicate)
        report = _run(inventory)
        assert "is also declared by" in _messages(report)

    def test_a_scalar_tables_field_is_a_schema_error_not_a_crash(self, inventory: Path) -> None:
        # Malformed units are still read while reporting, so the accessors have
        # to tolerate the wrong type rather than assume the schema passed.
        _mutate(inventory, "mitxonline__mysql", APP_UNIT, tables=3)
        report = _run(inventory)
        assert report.errors
        assert "tables" in _messages(report)

    def test_malformed_units_do_not_masquerade_as_duplicate_keys(self, inventory: Path) -> None:
        # Two units missing `deployment` both key as `?/?`. Reporting that as a
        # duplicate key blames the wrong thing — the shape errors are the finding.
        for name in ("broken_one", "broken_two"):
            broken = copy.deepcopy(DLT_UNIT)
            del broken["deployment"]
            _write(inventory, name, broken)
        report = _run(inventory)
        assert "already defined by" not in _messages(report)
        assert "'deployment' is a required property" in _messages(report)

    def test_duplicate_unit_key_is_rejected(self, inventory: Path) -> None:
        clone = copy.deepcopy(DLT_UNIT)
        clone["table_prefix"] = "raw__edxorg__discovery__"
        clone["tables"] = [
            {
                "name": "programs",
                "raw_table": "raw__edxorg__discovery__programs",
                "sync_mode": "full_refresh_overwrite",
            }
        ]
        _write(inventory, "edxorg__s3_again", clone)
        report = _run(inventory)
        assert "already defined by" in _messages(report)
