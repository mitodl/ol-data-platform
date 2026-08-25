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
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator

from ol_dbt_cli.lib import git_utils
from ol_dbt_cli.lib.validation import Severity, ValidationReport

CHECK = "inventory"
REMOVAL_CHECK = "inventory_removal"
RECONCILE_CHECK = "inventory_reconcile"

DEFAULT_INVENTORY_DIR = Path("ingestion/inventory")
UNITS_SUBDIR = "units"
SCHEMA_PATH = Path("schema") / "unit.schema.json"
RETIRED_SCHEMA_PATH = Path("schema") / "retired.schema.json"
VOCABULARY_FILENAME = "vocabulary.yml"
RETIRED_FILENAME = "retired.yml"

DAGSTER_SELECTOR_SUFFIX = "s3 data lake"
INCREMENTAL_PREFIX = "incremental"
XMIN_REPLICATION = "xmin"
MIRROR_STRATEGY = "mirror"
AIRBYTE_LOADER = "airbyte"


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
    # Paths mix property names and array indices, so stringify before sorting:
    # comparing an int to a str raises rather than ordering.
    errors = sorted(
        validator.iter_errors(unit.data),
        key=lambda e: [str(part) for part in e.absolute_path],
    )
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
    # second rule, which the schema cannot express. dlt is the only supported
    # local ingest target, so this stays keyed on `loader != dlt` rather than
    # on Airbyte alone: adding `dagster` (rule 7) did not add a second way to
    # ingest locally, and reading the rule as "bans Airbyte" would let a unit
    # declare a local path that nothing supports.
    if strategies.get("local") == "ingest" and loader != "dlt":
        report.add(
            CHECK,
            Severity.ERROR,
            unit.key,
            "strategies.local is `ingest` but the unit is not dlt-backed",
            "dlt is the only supported local ingest target: Airbyte cannot run in k3d, "
            "and no other loader has a local path (RFC 12711 §3).",
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
    airbyte = unit.data.get("airbyte")
    rides_xmin = isinstance(airbyte, dict) and airbyte.get("replication_method") == XMIN_REPLICATION
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
        if sync_mode.startswith(INCREMENTAL_PREFIX) and not table.get("cursor_field") and not rides_xmin:
            report.add(
                CHECK,
                Severity.ERROR,
                unit.key,
                f"{raw_table} is {sync_mode} but declares no cursor_field",
                "An incremental stream with no cursor rides the source-defined one, "
                "and nothing on this unit says what that is.",
            )

    if rides_xmin and (riders := [t for t in unit.tables if _is_cursorless_incremental(t)]):
        # Reported once per unit, not once per table. `replication_method: xmin`
        # already says these ride the source-defined cursor (§3.4), so a
        # per-table error would be the same fact 514 times over — and writing a
        # cursor_field to silence it would invent config Airbyte does not hold,
        # which is exactly what breaks step 5's empty-preview import.
        report.add(
            CHECK,
            Severity.WARNING,
            unit.key,
            f"{len(riders)} incremental stream(s) ride xmin, which dlt cannot reproduce",
            "Each needs a replacement cursor column chosen before dlt can take "
            "over this unit, and source-postgres 3.8+ refuses xmin outright on any "
            "database that has ever wrapped around. See "
            "tk-determine-per-source-incremental-cursor-viabilit-51f299.",
        )


def _is_cursorless_incremental(table: dict[str, Any]) -> bool:
    return str(table.get("sync_mode", "")).startswith(INCREMENTAL_PREFIX) and not table.get("cursor_field")


def _check_connections(unit: Unit, report: ValidationReport) -> None:
    if unit.data.get("loader") != AIRBYTE_LOADER:
        return
    dagster_visible = unit.data.get("dagster_visible", True)
    # Joined on the stream NAME, not on `table_prefix + name`: `raw_table` is
    # declared per table and need not be the concatenation, so rebuilding it
    # here would invent disagreements that are not the author's error.
    declared = {str(table.get("name", "")) for table in unit.tables}

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
            carried[str(stream)] = carried.get(str(stream), 0) + 1

    for stream in sorted(declared - set(carried)):
        report.add(
            CHECK,
            Severity.ERROR,
            unit.key,
            f"table {stream!r} is declared but no connection carries that stream",
        )
    for stream in sorted(set(carried) - declared):
        report.add(
            CHECK,
            Severity.ERROR,
            unit.key,
            f"a connection carries stream {stream!r} but the unit declares no such table",
        )
    for stream, count in sorted(carried.items()):
        if count > 1:
            report.add(
                CHECK,
                Severity.ERROR,
                unit.key,
                f"stream {stream!r} is carried {count} times within this unit",
                "Whether that is two connections or one connection listing the "
                "stream twice, both writes land in the same raw table.",
            )

    # The renderer joins a connection's stream name to its table entry to find
    # the stream's `namespace` (§6.2), so the name has to identify one table.
    # Two same-named streams in different source namespaces cannot be expressed
    # here — and they could not be loaded either: `raw_table` is prefix + name,
    # so Airbyte would write both into one destination table.
    names = Counter(str(table.get("name", "")) for table in unit.tables)
    for name, count in sorted(names.items()):
        if count > 1:
            report.add(
                CHECK,
                Severity.ERROR,
                unit.key,
                f"stream name {name!r} is declared by {count} tables in this unit",
                "Stream name must identify one table, since that is how a "
                "connection's `streams` entry resolves to its namespace.",
            )


def _check_cross_unit(units: list[Unit], report: ValidationReport) -> None:
    """Check the invariants that give every raw table exactly one owner (§3.3 rule 8)."""
    # Nested prefixes are allowed. The rule that used to forbid them was a proxy
    # for "a raw table maps to two units", which `seen_tables` below enforces
    # directly — and §1.1 fixed that raw tables are declared, never parsed, so a
    # prefix documents a unit rather than routing to it. The proxy was strictly
    # stronger than the invariant and made real deployments unexpressible:
    # edxorg's raw namespace nests three loaders, with dlt's
    # `raw__edxorg__s3__tables__` sitting inside Airbyte's `raw__edxorg__s3__`.
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
        # setdefault, not assignment: with three units sharing a key, the third
        # should point at the original, not at the second duplicate.
        seen_keys.setdefault(unit.key, unit.path)
        for table in unit.tables:
            raw_table = table.get("raw_table", "")
            if raw_table in seen_tables:
                report.add(
                    CHECK,
                    Severity.ERROR,
                    unit.key,
                    f"{raw_table} is also declared by {seen_tables[raw_table]}",
                )
            seen_tables.setdefault(raw_table, unit.key)


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
    _check_retired(inventory_dir, well_formed, report)
    return units


# ---------------------------------------------------------------------------
# §7.2 — the graveyard, and the removal/rename check
# ---------------------------------------------------------------------------


def unit_key(data: dict[str, Any]) -> str:
    """Spell the `deployment/layer` key exactly as `Unit.key` spells it."""
    return f"{data.get('deployment', '?')}/{data.get('layer', '?')}"


def load_retired(inventory_dir: Path) -> list[dict[str, Any]]:
    """Read `retired.yml`. A missing file is an empty graveyard, not an error.

    The file is optional so that a fresh checkout of the inventory — or a test
    fixture that only cares about units — does not have to carry one.
    """
    path = inventory_dir / RETIRED_FILENAME
    if not path.exists():
        return []
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        return []
    entries = raw.get("retired")
    return [entry for entry in entries if isinstance(entry, dict)] if isinstance(entries, list) else []


def retired_pairs(entries: list[dict[str, Any]]) -> set[tuple[str, str]]:
    """`(unit_key, raw_table)` for every table any graveyard entry accounts for."""
    pairs: set[tuple[str, str]] = set()
    for entry in entries:
        key = unit_key(entry)
        for raw_table in entry.get("raw_tables") or []:
            pairs.add((key, str(raw_table)))
    return pairs


def declared_pairs(units: list[Unit]) -> set[tuple[str, str]]:
    """`(unit_key, raw_table)` for every table the inventory currently declares."""
    return {(unit.key, str(table.get("raw_table", ""))) for unit in units for table in unit.tables}


def _check_retired(inventory_dir: Path, units: list[Unit], report: ValidationReport) -> None:
    """Validate `retired.yml`'s shape, and that it does not contradict the units."""
    path = inventory_dir / RETIRED_FILENAME
    if not path.exists():
        return
    schema_path = inventory_dir / RETIRED_SCHEMA_PATH
    raw = yaml.safe_load(path.read_text())
    validator = Draft202012Validator(json.loads(schema_path.read_text()))
    for error in sorted(
        validator.iter_errors(raw),
        key=lambda e: [str(part) for part in e.absolute_path],
    ):
        location = "/".join(str(part) for part in error.absolute_path) or "(root)"
        report.add(CHECK, Severity.ERROR, RETIRED_FILENAME, f"{location} {error.message}")

    live = declared_pairs(units)
    for key, raw_table in sorted(retired_pairs(load_retired(inventory_dir)) & live):
        report.add(
            CHECK,
            Severity.ERROR,
            key,
            f"{raw_table} is retired but the unit still declares it",
            "The graveyard and the units contradict each other. Either the table "
            "is still loaded — drop the retired.yml entry — or it is not, and the "
            "unit entry is what should go.",
        )


@dataclass
class Snapshot:
    """The whole inventory as of one git ref: the units plus the graveyard.

    They travel together because the §7.2 check reads both on both sides of the
    diff — a table may disappear from `units/` in the same commit that adds its
    `retired.yml` entry, and a deleted graveyard entry is itself a finding.
    """

    units: list[Unit] = field(default_factory=list)
    retired: list[dict[str, Any]] = field(default_factory=list)


def load_snapshot(inventory_dir: Path) -> Snapshot:
    return Snapshot(units=load_units(inventory_dir), retired=load_retired(inventory_dir))


def load_snapshot_at_ref(inventory_dir: Path, ref: str, repo_root: Path | None = None) -> Snapshot:
    """Read the inventory as it stood at git *ref*, without touching the worktree.

    Units the branch deleted only exist in the tree, so the file list comes from
    `git ls-tree` rather than from disk. A unit that is unparseable at the base
    ref is skipped rather than raising: the check's job is to report what this
    change removed, and it should not be blocked by a mess somebody else merged.
    """
    units: list[Unit] = []
    for path in sorted(git_utils.list_files_at_ref(inventory_dir / UNITS_SUBDIR, ref, repo_root)):
        if path.suffix != ".yml":
            continue
        content = git_utils.get_file_at_ref(path, ref, repo_root)
        if content is None:
            continue
        try:
            parsed = yaml.safe_load(content)
        except yaml.YAMLError:
            continue
        units.append(Unit(path=path, data=parsed if isinstance(parsed, dict) else {}))

    retired: list[dict[str, Any]] = []
    content = git_utils.get_file_at_ref(inventory_dir / RETIRED_FILENAME, ref, repo_root)
    if content:
        try:
            raw = yaml.safe_load(content)
        except yaml.YAMLError:
            raw = None
        if isinstance(raw, dict) and isinstance(raw.get("retired"), list):
            retired = [entry for entry in raw["retired"] if isinstance(entry, dict)]
    return Snapshot(units=units, retired=retired)


def check_removals(
    previous: Snapshot,
    current: Snapshot,
    report: ValidationReport,
) -> None:
    """Fail on any `(unit, raw_table)` that disappeared without being acknowledged.

    The failure this catches is silent: a dropped entry means the loader simply
    stops loading, with no error anywhere and a dbt model that quietly goes
    stale. A rename is the subtle half — it looks like a delete plus an add, so
    an add-only check waves it through while every downstream model is orphaned.

    Acknowledgement is either a `retired.yml` entry (dated and reasoned) or
    `renamed_from:` on another table in the same unit. Findings here are ERRORs
    and are deliberately not baselineable: unlike a warehouse-shaped finding,
    this one is always fixable by editing text in the same pull request.
    """
    before = declared_pairs(previous.units)
    after = declared_pairs(current.units)

    renames: dict[tuple[str, str], str] = {}
    for unit in current.units:
        for table in unit.tables:
            old = table.get("renamed_from")
            if old:
                renames[unit.key, str(old)] = str(table.get("raw_table", ""))

    graveyard = retired_pairs(current.retired)

    for key, raw_table in sorted(before - after):
        if (key, raw_table) in graveyard:
            continue
        if (key, raw_table) in renames:
            report.add(
                REMOVAL_CHECK,
                Severity.INFO,
                key,
                f"{raw_table} was renamed to {renames[key, raw_table]}",
            )
            continue
        report.add(
            REMOVAL_CHECK,
            Severity.ERROR,
            key,
            f"{raw_table} disappeared from the inventory without acknowledgement",
            "Add it to ingestion/inventory/retired.yml with a date and a reason, "
            "or set `renamed_from: " + raw_table + "` on the entry that replaced it.",
        )

    # A `renamed_from` pointing at a table that did not disappear is either a
    # typo or a leftover from an earlier PR. Left alone it is inert, but it also
    # silently pre-authorises a future removal of the name it holds.
    for (key, old), new in sorted(renames.items()):
        if (key, old) not in before - after:
            report.add(
                REMOVAL_CHECK,
                Severity.WARNING,
                key,
                f"{new} claims `renamed_from: {old}`, but {old} did not disappear in this change",
            )

    # Deleting a graveyard entry is how the record of what we used to load gets
    # lost, so the ratchet runs on retired.yml too.
    for key, raw_table in sorted(retired_pairs(previous.retired) - graveyard):
        report.add(
            REMOVAL_CHECK,
            Severity.ERROR,
            key,
            f"the retired.yml entry for {raw_table} was deleted",
            "Graveyard entries are never removed — the record of what we used to load is the point of the file.",
        )


# ---------------------------------------------------------------------------
# §5 — what the inventory generates
# ---------------------------------------------------------------------------

RENDER_SCHEMA_VERSION = 1


def dagster_group_name(connection_name: str) -> str:
    """Reproduce `OLAirbyteTranslator.get_asset_spec`'s group-name derivation exactly.

    `dg_projects/lakehouse/lakehouse/definitions.py` collapses dashes and
    whitespace to underscores, drops everything else — including Airbyte's
    U+2192 arrow — strips, and lowercases. This has to match character for
    character: the derived name is the key of the sync-interval map, and a miss
    there falls back to 24 hours in silence (§1.3).
    """
    return re.sub(r"[^A-Za-z0-9_]", "", re.sub(r"[-\s]+", "_", connection_name)).strip("_").lower()


class RenderError(Exception):
    """The inventory cannot be rendered, and rendering it anyway would be a lie."""


def _tables_by_stream(unit: Unit) -> dict[str, dict[str, Any]]:
    """Index a unit's tables by stream name.

    `validate` guarantees this is a bijection with the streams the unit's
    connections carry — that is what the connection rules exist for — but
    `render` deliberately does not validate first, so it cannot assume it.
    """
    return {str(table.get("name", "")): table for table in unit.tables}


def _resolve_stream(unit: Unit, by_stream: dict[str, dict[str, Any]], stream: str) -> dict[str, Any]:
    """Find the table a connection's stream refers to, or refuse to render.

    Skipping the stream would be worse than failing. This render is applied to
    production Airbyte: a dropped stream is a connection Pulumi reconfigures to
    stop carrying a table, which is precisely the silent-omission failure the
    inventory exists to end — and it would be silent in a committed JSON file
    that a human reviewed. Better to name the missing declaration and stop.
    """
    table = by_stream.get(stream)
    if table is None:
        msg = (
            f"{unit.path.name}: connection stream {stream!r} in unit {unit.key} matches no "
            f"table in that unit, so there is nothing to render it from. "
            f"Run `ol-dbt inventory validate` — it reports this and everything else wrong with the file."
        )
        raise RenderError(msg)
    return table


def _render_stream(table: dict[str, Any]) -> dict[str, Any]:
    stream: dict[str, Any] = {
        "name": table.get("name"),
        "sync_mode": table.get("sync_mode"),
    }
    if table.get("namespace"):
        stream["namespace"] = table["namespace"]
    if table.get("cursor_field"):
        stream["cursor_field"] = list(table["cursor_field"])
    if table.get("primary_key"):
        # The inventory stores this exactly as `configurations.streams[].primaryKey`
        # does — a list of paths, each path a list of segments — so it round-trips
        # without ambiguity, which is what makes the rendered config comparable
        # with the imported one (§6.4).
        stream["primary_key"] = [list(path) for path in table["primary_key"]]
    if table.get("excluded_columns"):
        # Emitted as the exclusion, not as `selected_fields`. Airbyte wants the
        # complement, and computing it needs the source's discovered schema —
        # which the inventory deliberately does not hold (§2). The consumer
        # complements it against the catalog it already reads.
        stream["excluded_columns"] = list(table["excluded_columns"])
    return stream


def render_airbyte(units: list[Unit]) -> dict[str, Any]:
    """Render the narrow, stable JSON that crosses the boundary into ol-infrastructure.

    Only the Airbyte-relevant fields, and only `loader: airbyte` units: as each
    source migrates to dlt its unit flips `loader`, the renderer stops emitting
    it, and Pulumi removes the connection (§6.5). `schema_version` is what lets
    the YAML keep growing dlt-shaped fields without breaking the consumer (§4).
    """
    rendered = []
    for unit in sorted(units, key=lambda u: u.key):
        if unit.data.get("loader") != AIRBYTE_LOADER:
            continue
        airbyte = unit.data.get("airbyte") or {}
        by_stream = _tables_by_stream(unit)
        entry: dict[str, Any] = {
            "deployment": unit.data.get("deployment"),
            "layer": unit.data.get("layer"),
            "table_prefix": unit.data.get("table_prefix"),
            "source_kind": airbyte.get("source_kind"),
            "connections": [
                {
                    "name": connection.get("name"),
                    "status": connection.get("status"),
                    "sync_interval_hours": connection.get("sync_interval_hours"),
                    "dagster_group_name": dagster_group_name(str(connection.get("name", ""))),
                    "streams": [
                        _render_stream(_resolve_stream(unit, by_stream, str(stream)))
                        for stream in connection.get("streams") or []
                    ],
                }
                for connection in unit.connections
            ],
        }
        if airbyte.get("replication_method"):
            entry["replication_method"] = airbyte["replication_method"]
        rendered.append(entry)
    return {
        "schema_version": RENDER_SCHEMA_VERSION,
        "generated_by": "ol-dbt inventory render airbyte",
        "source": "mitodl/ol-data-platform ingestion/inventory",
        "units": rendered,
    }


def render_dagster_intervals(units: list[Unit]) -> dict[str, int]:
    """`group_name -> sync interval in hours`, replacing the hand-maintained literal.

    Keyed on the derived Dagster group name rather than the connection name,
    because that is what `definitions.py` looks the interval up by. Units marked
    `dagster_visible: false` are excluded: Dagster's `connection_selector_fn`
    never sees their connections, so an entry for one is dead weight that reads
    like coverage.

    Paused connections are kept. Dagster builds its assets from the live
    workspace and selects on the connection name alone, so a paused connection
    still produces a group — and omitting its interval would silently hand it
    the 24-hour default the moment somebody re-enables it.
    """
    intervals: dict[str, int] = {}
    sources: dict[str, str] = {}
    for unit in sorted(units, key=lambda u: u.key):
        if not unit.data.get("dagster_visible", True):
            continue
        for connection in unit.connections:
            name = str(connection.get("name", ""))
            if not name.lower().endswith(DAGSTER_SELECTOR_SUFFIX):
                continue
            hours = connection.get("sync_interval_hours")
            if not isinstance(hours, int):
                continue
            group = dagster_group_name(name)
            if group in intervals and intervals[group] != hours:
                msg = (
                    f"connections {sources[group]!r} and {name!r} both derive the Dagster "
                    f"group {group!r} but disagree on sync_interval_hours "
                    f"({intervals[group]} vs {hours}) — rendering one would silently drop "
                    f"the other's cadence."
                )
                raise RenderError(msg)
            intervals[group] = hours
            sources[group] = name
    return dict(sorted(intervals.items()))


# ---------------------------------------------------------------------------
# Reconciliation (§5): the inventory against independent observations
# ---------------------------------------------------------------------------
#
# The warehouse and the dbt sources are observations, not authorities. Warehouse
# introspection used to *be* the source of truth for `ol-dbt generate sources`,
# and that is the defect this inventory exists to correct — so reconcile reports
# disagreement and never edits either side. Its worth is precisely that it is an
# independent observation: a check that agrees by construction checks nothing.


@dataclass
class Reconciliation:
    """One comparison of declared tables against observed ones, in three buckets."""

    both: set[str] = field(default_factory=set)
    declared_not_observed: set[str] = field(default_factory=set)
    observed_not_declared: set[str] = field(default_factory=set)


def reconcile_tables(declared: set[str], observed: set[str]) -> Reconciliation:
    return Reconciliation(
        both=declared & observed,
        declared_not_observed=declared - observed,
        observed_not_declared=observed - declared,
    )


def tables_by_raw_name(units: list[Unit]) -> dict[str, str]:
    """Map every declared raw table to the unit declaring it.

    A dict rather than a multimap because rule 8 forbids two units declaring the
    same raw table. Reconcile deliberately does not validate first, so a broken
    inventory can violate that — the last unit read then wins here, and
    `validate` is what names the collision.
    """
    return {str(table.get("raw_table", "")): unit.key for unit in units for table in unit.tables}


def modeled_tables(units: list[Unit]) -> set[str]:
    """Raw tables the inventory claims dbt declares as sources (§1.4)."""
    return {str(table.get("raw_table", "")) for unit in units for table in unit.tables if table.get("modeled")}


def units_for_table(units: list[Unit], raw_table: str) -> list[str]:
    """Return every unit whose `table_prefix` covers an undeclared raw table.

    Longest prefix first, but *all* of them: since rule 4 now permits nesting,
    more than one unit can cover a table and none of them owns it. Naming only
    the longest would assert an owner the inventory does not establish —
    `raw__edxorg__s3__course_` and `raw__edxorg__` both cover a stray
    `raw__edxorg__s3__course_*`, and they are different pipelines. Ownership
    comes from a declared `raw_table`, which by definition an undeclared table
    does not have.
    """
    matches = [
        unit for unit in units if (prefix := unit.data.get("table_prefix")) and raw_table.startswith(str(prefix))
    ]
    matches.sort(key=lambda unit: len(str(unit.data["table_prefix"])), reverse=True)
    return [unit.key for unit in matches]


def reconcile_warehouse(
    units: list[Unit],
    warehouse_tables: set[str],
    report: ValidationReport,
) -> Reconciliation:
    """Report the inventory against the tables the warehouse actually holds.

    Both directions are warnings rather than errors. A declared table the
    warehouse lacks is usually broken ingestion, but is also what a just-added
    entry looks like before its first sync; an undeclared table in the warehouse
    is usually drift, but is also what a table loaded for years and never
    declared looks like. Neither is fixable by editing the pull request in front
    of you, which is the line §7.2 draws for what may be an ERROR.
    """
    owners = tables_by_raw_name(units)
    result = reconcile_tables(set(owners), warehouse_tables)

    for raw_table in sorted(result.declared_not_observed):
        report.add(
            RECONCILE_CHECK,
            Severity.WARNING,
            owners[raw_table],
            f"{raw_table} is declared but the warehouse does not hold it",
            "Either the loader is failing for this table or it has never run. "
            "If we deliberately stopped loading it, retire it in retired.yml so "
            "the graveyard records when — dropping the entry instead is exactly "
            "the silent disappearance §7.2 exists to prevent.",
        )

    for raw_table in sorted(result.observed_not_declared):
        candidates = units_for_table(units, raw_table)
        if not candidates:
            model, detail = (
                "(unclaimed)",
                "No unit's table_prefix covers it, so it needs a new unit — or it is "
                "a leftover from an ingestion we have already stopped.",
            )
        elif len(candidates) == 1:
            model, detail = (
                candidates[0],
                f"It falls under {candidates[0]}'s table_prefix, so its entry belongs in that unit.",
            )
        else:
            model, detail = (
                "(ambiguous)",
                f"Prefixes from {len(candidates)} units cover it — {', '.join(candidates)} — "
                "so which one should declare it cannot be read off the name. Nesting is "
                "allowed, so the longest match is not the owner.",
            )
        report.add(
            RECONCILE_CHECK,
            Severity.WARNING,
            model,
            f"{raw_table} is in the warehouse but no unit declares it",
            detail,
        )

    return result


def reconcile_dbt(
    units: list[Unit],
    dbt_tables: set[str],
    inventory_dir: Path,
    report: ValidationReport,
) -> Reconciliation:
    """Report the inventory against the raw tables dbt declares as sources.

    dbt is a strict subset of what we load — 372 of ~2,090 tables (§1.4) — so a
    loaded table carrying no dbt source is normal and is not reported. The two
    directions that do mean something:

    * a dbt source table no unit loads: dbt reads something the inventory says
      does not arrive, so either the inventory is incomplete or the model is
      stale. This is step 3's acceptance criterion, and the one ERROR here.
    * a `modeled: true` table dbt does not declare: that flag is what step 7
      generates the sources YAML from, so a wrong one silently drops a table
      from the generation.
    """
    owners = tables_by_raw_name(units)
    result = reconcile_tables(set(owners), dbt_tables)
    retired = {raw_table for _, raw_table in retired_pairs(load_retired(inventory_dir))}

    for raw_table in sorted(result.observed_not_declared):
        if raw_table in retired:
            report.add(
                RECONCILE_CHECK,
                Severity.WARNING,
                "(dbt sources)",
                f"{raw_table} is a dbt source but the inventory retired it",
                "We stopped loading this table, so the model reading it is going "
                "stale on whatever it last held. That is the failure retired.yml "
                "makes findable — the fix belongs in the model, not the graveyard.",
            )
        else:
            report.add(
                RECONCILE_CHECK,
                Severity.ERROR,
                "(dbt sources)",
                f"{raw_table} is a dbt source but no unit declares it",
                "Every dbt-declared raw table has to map to exactly one unit. Add "
                "it to the unit whose table_prefix covers it — do not delete the "
                "dbt source to make this pass.",
            )

    modeled = modeled_tables(units)
    for raw_table in sorted(modeled - dbt_tables):
        report.add(
            RECONCILE_CHECK,
            Severity.WARNING,
            owners[raw_table],
            f"{raw_table} is marked modeled but dbt declares no source for it",
            "`modeled:` is what step 7 generates the sources YAML from, so the "
            "flag is either stale or the dbt source has gone missing.",
        )

    # The mirror image, and the more damaging direction. A table dbt declares
    # but the unit flags `modeled: false` lands in `result.both` and in neither
    # subtraction above, so it would go unreported — while step 7, generating
    # sources from `modeled:` alone, would silently drop a source dbt is
    # actually reading.
    for raw_table in sorted((dbt_tables & set(owners)) - modeled):
        report.add(
            RECONCILE_CHECK,
            Severity.WARNING,
            owners[raw_table],
            f"{raw_table} is marked modeled: false but dbt declares a source for it",
            "Step 7 generates the sources YAML from `modeled:`, so leaving this "
            "flag false would delete a source dbt is reading. Set it true, or "
            "remove the dbt source if it is the one that is wrong.",
        )

    return result
