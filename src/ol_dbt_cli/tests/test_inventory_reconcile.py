"""Tests for `ol-dbt inventory reconcile` (INGESTION_INVENTORY_SPEC §5).

Reconcile compares the inventory against two independent observations — the
warehouse and the dbt sources — and reports disagreement without editing
either. The cases below pin which direction of disagreement is an ERROR,
because that is the whole judgement in the command: a bucket reported at the
wrong severity either blocks work that is fine or waves through the thing the
inventory exists to catch.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.lib.inventory import (
    load_units,
    reconcile_dbt,
    reconcile_warehouse,
    unit_for_table,
)
from ol_dbt_cli.lib.validation import Severity, ValidationReport
from ol_dbt_cli.lib.yaml_registry import collect_source_tables

MITX_PREFIX = "raw__mitxonline__openedx__mysql__"
EDX_PREFIX = "raw__edxorg__s3__"

MITX_UNIT: dict[str, Any] = {
    "schema_version": 1,
    "deployment": "mitxonline",
    "layer": "mysql",
    "scope": "scoped",
    "strategies": {"qa": "ingest", "local": "fixture"},
    "loader": "airbyte",
    "table_prefix": MITX_PREFIX,
    "tables": [
        {
            "name": "auth_user",
            "raw_table": f"{MITX_PREFIX}auth_user",
            "sync_mode": "incremental_append",
            "modeled": True,
        },
        {
            "name": "courseware_studentmodule",
            "raw_table": f"{MITX_PREFIX}courseware_studentmodule",
            "sync_mode": "incremental_append",
            "modeled": False,
        },
    ],
}

EDX_UNIT: dict[str, Any] = {
    "schema_version": 1,
    "deployment": "edxorg",
    "layer": "s3",
    "scope": "singleton",
    "strategies": {"qa": "omit", "local": "fixture"},
    "loader": "dlt",
    "table_prefix": EDX_PREFIX,
    "tables": [
        {
            "name": "tables__auth_user",
            "raw_table": f"{EDX_PREFIX}tables__auth_user",
            "sync_mode": "full_refresh_overwrite",
            "modeled": True,
        }
    ],
}

DECLARED = {
    f"{MITX_PREFIX}auth_user",
    f"{MITX_PREFIX}courseware_studentmodule",
    f"{EDX_PREFIX}tables__auth_user",
}


@pytest.fixture
def inventory(tmp_path: Path) -> Path:
    """Build a two-unit inventory with an empty graveyard."""
    root = tmp_path / "inventory"
    (root / "units").mkdir(parents=True)
    (root / "units" / "mitxonline__mysql.yml").write_text(yaml.safe_dump(MITX_UNIT, sort_keys=False))
    (root / "units" / "edxorg__s3.yml").write_text(yaml.safe_dump(EDX_UNIT, sort_keys=False))
    _write_retired(root, [])
    return root


def _write_retired(root: Path, entries: list[dict[str, Any]]) -> None:
    (root / "retired.yml").write_text(yaml.safe_dump({"schema_version": 1, "retired": entries}, sort_keys=False))


def _messages(report: ValidationReport, severity: Severity) -> list[str]:
    return [issue.message for issue in report.issues if issue.severity == severity]


class TestWarehouseReconciliation:
    def test_three_buckets_are_reported(self, inventory: Path) -> None:
        units = load_units(inventory)
        report = ValidationReport()
        warehouse = {
            f"{MITX_PREFIX}auth_user",  # in both
            f"{MITX_PREFIX}courseware_studentmodule",  # in both
            f"{MITX_PREFIX}unexpected_table",  # drift
        }
        # `edxorg` declares a table the warehouse does not hold.
        result = reconcile_warehouse(units, warehouse, report)

        assert result.both == {
            f"{MITX_PREFIX}auth_user",
            f"{MITX_PREFIX}courseware_studentmodule",
        }
        assert result.declared_not_observed == {f"{EDX_PREFIX}tables__auth_user"}
        assert result.observed_not_declared == {f"{MITX_PREFIX}unexpected_table"}

    def test_neither_direction_is_an_error(self, inventory: Path) -> None:
        # A missing table may just be a new entry awaiting its first sync, and
        # an undeclared one may be years-old drift. Neither is fixable in the
        # pull request that trips it, which is the line for ERROR.
        units = load_units(inventory)
        report = ValidationReport()
        reconcile_warehouse(units, {f"{MITX_PREFIX}unexpected_table"}, report)

        assert report.errors == []
        assert len(report.warnings) == 4  # noqa: PLR2004  three declared-but-absent, one undeclared

    def test_undeclared_table_names_the_unit_its_prefix_points_at(self, inventory: Path) -> None:
        units = load_units(inventory)
        report = ValidationReport()
        reconcile_warehouse(units, DECLARED | {f"{MITX_PREFIX}unexpected_table"}, report)

        drift = next(issue for issue in report.warnings if "unexpected_table" in issue.message)
        assert drift.model == "mitxonline/mysql"
        assert "table_prefix" in drift.detail

    def test_undeclared_table_under_no_prefix_is_unclaimed(self, inventory: Path) -> None:
        units = load_units(inventory)
        report = ValidationReport()
        reconcile_warehouse(units, DECLARED | {"raw__ovs__postgres__ui_collection"}, report)

        drift = next(issue for issue in report.warnings if "ui_collection" in issue.message)
        assert drift.model == "(unclaimed)"
        assert "needs a new unit" in drift.detail

    def test_full_agreement_is_silent(self, inventory: Path) -> None:
        units = load_units(inventory)
        report = ValidationReport()
        result = reconcile_warehouse(units, DECLARED, report)

        assert report.issues == []
        assert result.both == DECLARED


class TestDbtReconciliation:
    def test_dbt_source_no_unit_loads_is_an_error(self, inventory: Path) -> None:
        # Step 3's acceptance criterion: every dbt-declared raw table maps to
        # exactly one unit. dbt reading a table the inventory says never
        # arrives means one of the two is wrong, and both are text in this repo.
        units = load_units(inventory)
        report = ValidationReport()
        reconcile_dbt(units, DECLARED | {"raw__ovs__postgres__ui_collection"}, inventory, report)

        assert _messages(report, Severity.ERROR) == [
            "raw__ovs__postgres__ui_collection is a dbt source but no unit declares it"
        ]

    def test_retired_dbt_source_is_a_warning_not_an_error(self, inventory: Path) -> None:
        # The graveyard explains the absence, so it is not an undeclared table.
        # It is a stale model, which is a different and non-blocking finding.
        _write_retired(
            inventory,
            [
                {
                    "deployment": "ovs",
                    "layer": "app_postgres",
                    "raw_tables": ["raw__ovs__postgres__ui_collection"],
                    "retired_on": "2026-08-14",
                    "reason": "OVS ingestion was switched off.",
                }
            ],
        )
        units = load_units(inventory)
        report = ValidationReport()
        reconcile_dbt(units, DECLARED | {"raw__ovs__postgres__ui_collection"}, inventory, report)

        assert report.errors == []
        assert _messages(report, Severity.WARNING) == [
            "raw__ovs__postgres__ui_collection is a dbt source but the inventory retired it"
        ]

    def test_modeled_flag_with_no_dbt_source_is_a_warning(self, inventory: Path) -> None:
        # `modeled:` is what step 7 generates the sources YAML from, so a stale
        # flag silently drops a table from that generation.
        units = load_units(inventory)
        report = ValidationReport()
        reconcile_dbt(units, {f"{MITX_PREFIX}auth_user"}, inventory, report)

        assert report.errors == []
        assert _messages(report, Severity.WARNING) == [
            f"{EDX_PREFIX}tables__auth_user is marked modeled but dbt declares no source for it"
        ]

    def test_loaded_but_unmodeled_is_not_reported(self, inventory: Path) -> None:
        # dbt declares 372 of ~2,090 loaded tables (§1.4), so the gap is the
        # normal state — reporting it would bury every real finding.
        units = load_units(inventory)
        report = ValidationReport()
        reconcile_dbt(units, {f"{MITX_PREFIX}auth_user", f"{EDX_PREFIX}tables__auth_user"}, inventory, report)

        assert report.issues == []


class TestPrefixOwnership:
    def test_longest_prefix_wins(self) -> None:
        units = load_units_from(
            {
                "broad": {**MITX_UNIT, "table_prefix": "raw__mitxonline__"},
                "narrow": MITX_UNIT,
            }
        )
        assert unit_for_table(units, f"{MITX_PREFIX}auth_user") == "mitxonline/mysql"

    def test_no_prefix_matches_returns_none(self, inventory: Path) -> None:
        assert unit_for_table(load_units(inventory), "raw__ovs__postgres__ui_collection") is None


def load_units_from(units: dict[str, dict[str, Any]]) -> list[Any]:
    """Build Unit objects in-memory without touching the filesystem."""
    from ol_dbt_cli.lib.inventory import Unit  # noqa: PLC0415

    return [Unit(path=Path(f"{name}.yml"), data=data) for name, data in units.items()]


class TestSourceTableCollection:
    def test_tables_are_unioned_across_files_declaring_one_source(self, tmp_path: Path) -> None:
        # `build_yaml_registry` keys sources by name, so the last file wins.
        # `ol_warehouse_raw_data` is re-declared per staging directory, and
        # reading it off the registry yields one directory's tables instead of
        # all 374 — which reports every other table as undeclared drift.
        models = tmp_path / "models"
        (models / "staging" / "a").mkdir(parents=True)
        (models / "staging" / "b").mkdir(parents=True)
        _write_source(models / "staging" / "a" / "_a__sources.yml", ["raw__a__one"])
        _write_source(models / "staging" / "b" / "_b__sources.yml", ["raw__b__two"])

        assert collect_source_tables(models, "ol_warehouse_raw_data") == {"raw__a__one", "raw__b__two"}

    def test_other_sources_are_ignored(self, tmp_path: Path) -> None:
        models = tmp_path / "models"
        models.mkdir()
        _write_source(models / "_raw__sources.yml", ["raw__a__one"])
        _write_source(models / "_dim__sources.yml", ["dim_user"], source_name="dimensional")

        assert collect_source_tables(models, "ol_warehouse_raw_data") == {"raw__a__one"}


def _write_source(path: Path, tables: list[str], source_name: str = "ol_warehouse_raw_data") -> None:
    path.write_text(
        yaml.safe_dump(
            {
                "version": 2,
                "sources": [{"name": source_name, "tables": [{"name": table} for table in tables]}],
            },
            sort_keys=False,
        )
    )
