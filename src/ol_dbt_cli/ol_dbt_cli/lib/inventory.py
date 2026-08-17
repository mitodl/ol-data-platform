"""Load and validate the ingestion inventory.

The inventory declares what we load: one file per `(deployment, layer)` unit under
``ingestion/inventory/units/``, keyed exactly as RFC 12711 §3 fixes it. See
``docs/specs/INGESTION_INVENTORY_SPEC.md`` §3 for the entry shape and the rules
enforced here.

This module deliberately imports neither dbt nor duckdb. Phase 2 of the Airbyte→dlt
migration builds dlt sources in a separately packaged Dagster code location, which
must be able to read the inventory without acquiring a dbt toolchain — keeping the
imports narrow now is what makes that a move rather than a rewrite.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator

from ol_dbt_cli.lib.validation import Severity, ValidationReport

CHECK = "inventory"

DEFAULT_INVENTORY_DIR = Path("ingestion/inventory")
UNITS_SUBDIR = "units"
SCHEMA_PATH = Path("schema") / "unit.schema.json"
VOCABULARY_FILENAME = "vocabulary.yml"

DAGSTER_SELECTOR_SUFFIX = "s3 data lake"
INCREMENTAL_PREFIX = "incremental"
MIRROR_STRATEGY = "mirror"


@dataclass
class Unit:
    """One parsed unit file, kept as plain data plus where it came from."""

    path: Path
    data: dict[str, Any]

    @property
    def key(self) -> str:
        return f"{self.data.get('deployment', '?')}/{self.data.get('layer', '?')}"

    # Both accessors tolerate the wrong type rather than assuming the schema has
    # already passed: they are read while reporting on units that failed it, and
    # a hand-edited `tables: 3` should produce a schema error, not a TypeError.

    @property
    def tables(self) -> list[dict[str, Any]]:
        tables = self.data.get("tables")
        return tables if isinstance(tables, list) else []

    @property
    def connections(self) -> list[dict[str, Any]]:
        airbyte = self.data.get("airbyte")
        connections = airbyte.get("connections") if isinstance(airbyte, dict) else None
        return connections if isinstance(connections, list) else []


@dataclass
class Vocabulary:
    deployments: set[str] = field(default_factory=set)
    layers: set[str] = field(default_factory=set)


def load_vocabulary(inventory_dir: Path) -> Vocabulary:
    path = inventory_dir / VOCABULARY_FILENAME
    if not path.exists():
        msg = f"No vocabulary at {path}"
        raise FileNotFoundError(msg)
    raw = yaml.safe_load(path.read_text()) or {}
    return Vocabulary(
        deployments=set(raw.get("deployments") or []),
        layers=set(raw.get("layers") or []),
    )


def load_units(inventory_dir: Path) -> list[Unit]:
    """Read every unit file, sorted by path so findings are reported stably."""
    units_dir = inventory_dir / UNITS_SUBDIR
    if not units_dir.exists():
        return []
    units = []
    for path in sorted(units_dir.glob("*.yml")):
        parsed = yaml.safe_load(path.read_text())
        units.append(Unit(path=path, data=parsed if isinstance(parsed, dict) else {}))
    return units


def _load_schema(inventory_dir: Path) -> Draft202012Validator:
    return Draft202012Validator(json.loads((inventory_dir / SCHEMA_PATH).read_text()))


def _check_shape(unit: Unit, validator: Draft202012Validator, report: ValidationReport) -> bool:
    """Validate against the JSON Schema. Returns False if the unit is unusable."""
    errors = sorted(validator.iter_errors(unit.data), key=lambda e: list(e.absolute_path))
    for error in errors:
        location = "/".join(str(part) for part in error.absolute_path) or "(root)"
        report.add(
            CHECK,
            Severity.ERROR,
            unit.key,
            f"{unit.path.name}: {location} {error.message}",
        )
    return not errors


def _check_vocabulary(unit: Unit, vocabulary: Vocabulary, report: ValidationReport) -> None:
    deployment = unit.data.get("deployment")
    layer = unit.data.get("layer")
    if deployment not in vocabulary.deployments:
        report.add(
            CHECK,
            Severity.ERROR,
            unit.key,
            f"deployment {deployment!r} is not in vocabulary.yml",
            "Add it there if the deployment is real; the vocabulary is the "
            "controlled list precisely so a typo cannot invent one.",
        )
    if layer not in vocabulary.layers:
        report.add(
            CHECK,
            Severity.ERROR,
            unit.key,
            f"layer {layer!r} is not in vocabulary.yml",
        )


def _check_strategies(unit: Unit, report: ValidationReport) -> None:
    """RFC 12711 §3's three rules."""
    strategies = unit.data.get("strategies") or {}
    loader = unit.data.get("loader")

    # `local: mirror` is rejected by the JSON Schema enum; this catches the
    # second rule, which the schema cannot express.
    if strategies.get("local") == "ingest" and loader != "dlt":
        report.add(
            CHECK,
            Severity.ERROR,
            unit.key,
            "strategies.local is `ingest` but the unit is not dlt-backed",
            "Airbyte cannot run in k3d, so `local: ingest` on an Airbyte unit is false on its face (RFC 12711 §3).",
        )

    mirrors = MIRROR_STRATEGY in strategies.values()
    declared = "mirror_max_age_days" in unit.data
    if mirrors and not declared:
        report.add(
            CHECK,
            Severity.ERROR,
            unit.key,
            "a strategy is `mirror` but mirror_max_age_days is not set",
            "No default: a stale mirror has to fail against a number somebody chose.",
        )
    if declared and not mirrors:
        report.add(
            CHECK,
            Severity.ERROR,
            unit.key,
            "mirror_max_age_days is set but no strategy is `mirror`",
        )


def _check_loader_block(unit: Unit, report: ValidationReport) -> None:
    loader = unit.data.get("loader")
    for name in ("airbyte", "dlt"):
        present = name in unit.data
        expected = loader == name
        if present and not expected:
            report.add(
                CHECK,
                Severity.ERROR,
                unit.key,
                f"`{name}:` is present but loader is {loader!r}",
            )
        if expected and not present:
            report.add(
                CHECK,
                Severity.ERROR,
                unit.key,
                f"loader is {loader!r} but there is no `{name}:` block",
            )


def _check_tables(unit: Unit, report: ValidationReport) -> None:
    prefix = unit.data.get("table_prefix", "")
    for table in unit.tables:
        raw_table = table.get("raw_table", "")
        if prefix and not raw_table.startswith(prefix):
            report.add(
                CHECK,
                Severity.ERROR,
                unit.key,
                f"{raw_table} does not start with the unit's table_prefix {prefix!r}",
            )
        sync_mode = table.get("sync_mode", "")
        if sync_mode.startswith(INCREMENTAL_PREFIX) and not table.get("cursor_field"):
            report.add(
                CHECK,
                Severity.ERROR,
                unit.key,
                f"{raw_table} is {sync_mode} but declares no cursor_field",
                "An incremental stream with no cursor rides the source-defined one; "
                "for Postgres that is xmin, which dlt cannot reproduce.",
            )


def _check_connections(unit: Unit, report: ValidationReport) -> None:
    if unit.data.get("loader") != "airbyte":
        return
    dagster_visible = unit.data.get("dagster_visible", True)
    declared = {str(table.get("raw_table", "")) for table in unit.tables}
    prefix = unit.data.get("table_prefix", "")

    carried: dict[str, int] = {}
    for connection in unit.connections:
        name = connection.get("name", "")
        if dagster_visible and not name.lower().endswith(DAGSTER_SELECTOR_SUFFIX):
            report.add(
                CHECK,
                Severity.ERROR,
                unit.key,
                f"connection {name!r} does not end with {DAGSTER_SELECTOR_SUFFIX!r}",
                "Dagster's connection_selector_fn drops it, so its tables would "
                "never materialize. Set dagster_visible: false if that is intended.",
            )
        for stream in connection.get("streams") or []:
            carried[f"{prefix}{stream}"] = carried.get(f"{prefix}{stream}", 0) + 1

    for raw_table in sorted(declared - set(carried)):
        report.add(
            CHECK,
            Severity.ERROR,
            unit.key,
            f"{raw_table} is declared but no connection carries its stream",
        )
    for raw_table in sorted(set(carried) - declared):
        report.add(
            CHECK,
            Severity.ERROR,
            unit.key,
            f"a connection carries {raw_table} but the unit does not declare it",
        )
    for raw_table, count in sorted(carried.items()):
        if count > 1:
            report.add(
                CHECK,
                Severity.ERROR,
                unit.key,
                f"{raw_table} is carried by {count} connections in the same unit",
                "Two connections loading one table race each other into the same raw table.",
            )


def _check_cross_unit(units: list[Unit], report: ValidationReport) -> None:
    """Check the invariants that make prefix → unit a function (§3.3 rules 4 and 8)."""
    prefixes: list[tuple[str, Unit]] = [
        (unit.data["table_prefix"], unit) for unit in units if unit.data.get("table_prefix")
    ]
    for prefix, unit in prefixes:
        for other_prefix, other in prefixes:
            if unit.path == other.path:
                continue
            if prefix.startswith(other_prefix):
                report.add(
                    CHECK,
                    Severity.ERROR,
                    unit.key,
                    f"table_prefix {prefix!r} overlaps {other_prefix!r} from {other.key}",
                    "Prefixes must be pairwise non-overlapping, or a raw table maps to two units.",
                )

    seen_keys: dict[str, Path] = {}
    seen_tables: dict[str, str] = {}
    for unit in units:
        if unit.key in seen_keys:
            report.add(
                CHECK,
                Severity.ERROR,
                unit.key,
                f"(deployment, layer) is already defined by {seen_keys[unit.key].name}",
            )
        seen_keys[unit.key] = unit.path
        for table in unit.tables:
            raw_table = table.get("raw_table", "")
            if raw_table in seen_tables:
                report.add(
                    CHECK,
                    Severity.ERROR,
                    unit.key,
                    f"{raw_table} is also declared by {seen_tables[raw_table]}",
                )
            seen_tables[raw_table] = unit.key


def validate_inventory(inventory_dir: Path, report: ValidationReport) -> list[Unit]:
    """Run every rule over the inventory, adding findings to `report`."""
    vocabulary = load_vocabulary(inventory_dir)
    validator = _load_schema(inventory_dir)
    units = load_units(inventory_dir)

    well_formed = []
    for unit in units:
        if not _check_shape(unit, validator, report):
            # Later rules index into fields the schema just proved absent.
            continue
        _check_vocabulary(unit, vocabulary, report)
        _check_strategies(unit, report)
        _check_loader_block(unit, report)
        _check_tables(unit, report)
        _check_connections(unit, report)
        well_formed.append(unit)

    # Only well-formed units: two units missing `deployment` both key as `?/?`,
    # and reporting that as a duplicate key blames the wrong thing.
    _check_cross_unit(well_formed, report)
    return units
