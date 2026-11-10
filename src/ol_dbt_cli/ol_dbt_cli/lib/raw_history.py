"""Generate the raw history snapshots from the ingestion inventory.

Every table in history scope (``inventory.history_in_scope``) gets a dbt snapshot
of its raw table, one file of snapshots and one docs YAML per unit, written by
``ol-dbt inventory history --write``. The generator also writes the dbt sources
of the keyset tables and the per-table delete filter map the snapshot macro and
staging read. CI renders everything again and fails on any difference, so a
hand edit or an inventory change that was not regenerated cannot merge.

Scope and settings were decided on witan task
tk-decide-the-scope-of-raw-table-history-scd2-snaps-8430c2 (2026-10-09), and the
keyset consumption on tk-delete-capture-for-dlt-migrated-sources-reconcil-826b18
(2026-10-29). See INGESTION_INVENTORY_SPEC.md §11.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ol_dbt_cli.lib.inventory import (
    AIRBYTE_LOADER,
    DEFAULT_ENVIRONMENT,
    DLT_LOADER,
    KEYSET_SUFFIX,
    LOADER_METADATA_COLUMNS,
    RenderError,
    Unit,
    history_absence,
    history_in_scope,
    keyset_tables,
)

SNAPSHOT_DIR = Path("src/ol_dbt/snapshots/raw_history")
KEYSET_SOURCES_PATH = Path("src/ol_dbt/models/staging/_raw_keysets__sources.yml")
DELETE_FILTER_MACRO_PATH = Path("src/ol_dbt/macros/_raw_delete_filters.sql")

KEYSET_READ_STARTED_AT = "_keyset_read_started_at"
LATEST_LOAD_COLUMN = "_dlt_load_id"
# A keyset older than this many load intervals is logged as stale. It still
# filters the snapshot (raw_delete_filter says why), so deletes after its read
# wait for a fresh one.
STALE_KEYSET_INTERVALS = 2

GENERATED_BY = "ol-dbt inventory history --write"


@dataclass(frozen=True)
class DeleteFilter:
    """How a table's snapshot input is narrowed so that a deleted record leaves it.

    ``keyset``: keep rows whose key is in ``<raw_table>__keyset`` or that the
    loader wrote after the keyset was read. ``latest_load``: keep the rows of the
    newest dlt load, for a dlt merge that re-reads the whole source each load.
    """

    kind: str
    key: str | None = None
    loader_time_column: str | None = None
    max_age_hours: int | None = None


@dataclass(frozen=True)
class HistoryTable:
    """One snapshot the generator writes."""

    unit_key: str
    name: str
    raw_table: str
    keys: tuple[str, ...]
    absence: str
    live: bool
    exclude_columns: tuple[str, ...] = ()
    ignore_columns: tuple[str, ...] = ()
    delete_filter: DeleteFilter | None = None
    full_read: bool = False
    markers: tuple[dict[str, Any], ...] = field(default=())

    @property
    def snapshot_name(self) -> str:
        """Name the snapshot after its raw table, which is what the pilot did.

        The eight pilot snapshots exist in production under these names, and a
        rename would orphan them.
        """
        return f"snapshot_{self.raw_table}"


def _unit_interval_hours(unit: Unit) -> int:
    """Return the longest production load interval of the unit's active connections."""
    hours = [
        connection["sync_interval_hours"]
        for connection in unit.connections
        if connection.get("environment", DEFAULT_ENVIRONMENT) == "production"
        and connection.get("status") == "active"
        and isinstance(connection.get("sync_interval_hours"), int)
    ]
    if not hours:
        msg = (
            f"{unit.key} has keyset tables but no active production connection with a "
            "sync_interval_hours, so there is no load interval to judge a stale keyset by"
        )
        raise RenderError(msg)
    return max(hours)


def _delete_filter(unit: Unit, table: dict[str, Any], keyset_names: set[str], absence: str) -> DeleteFilter | None:
    if table["name"] in keyset_names:
        loader = str(unit.data["loader"])
        loader_time = LOADER_METADATA_COLUMNS.get(loader)
        if loader not in {AIRBYTE_LOADER, DLT_LOADER} or loader_time is None:
            msg = f"{unit.key} has keyset tables but its loader {loader!r} stamps no load time"
            raise RenderError(msg)
        return DeleteFilter(
            kind="keyset",
            key=str(table["primary_key"][0][0]),
            loader_time_column=loader_time,
            max_age_hours=STALE_KEYSET_INTERVALS * _unit_interval_hours(unit),
        )
    dlt = unit.data.get("dlt")
    merges = isinstance(dlt, dict) and dlt.get("write_disposition") == "merge"
    if (
        unit.data.get("loader") == DLT_LOADER
        and merges
        and table.get("sync_mode") == "full_refresh_overwrite_deduped"
        and absence == "deleted"
    ):
        return DeleteFilter(kind="latest_load", loader_time_column=LATEST_LOAD_COLUMN)
    return None


def history_tables(units: list[Unit]) -> dict[str, list[HistoryTable]]:
    """Map each unit key to the snapshots its tables get, in table order."""
    keysets = keyset_tables(units)
    selected: dict[str, list[HistoryTable]] = {}
    for unit in sorted(units, key=lambda u: u.key):
        tables = [table for table in unit.tables if history_in_scope(table)]
        if not tables:
            continue
        absence = history_absence(unit)
        if absence is None:
            msg = f"{unit.key} has tables in history scope but states no history.absence"
            raise RenderError(msg)
        unit_history = unit.data.get("history") or {}
        keyset_names = {row["table"] for row in keysets.get(unit.key, [])}
        rows = []
        for table in sorted(tables, key=lambda t: str(t["raw_table"])):
            settings = table.get("history") or {}
            markers = tuple(
                marker
                for marker in unit_history.get("markers", [])
                if table["name"] in marker.get("tables", [table["name"]])
            )
            delete_filter = _delete_filter(unit, table, keyset_names, absence)
            rows.append(
                HistoryTable(
                    unit_key=unit.key,
                    name=str(table["name"]),
                    raw_table=str(table["raw_table"]),
                    keys=tuple(str(path[0]) for path in table["primary_key"]),
                    absence=absence,
                    live=bool(settings.get("live", unit_history.get("live", False))),
                    exclude_columns=tuple(settings.get("exclude_columns", [])),
                    ignore_columns=tuple(settings.get("ignore_columns", [])),
                    delete_filter=delete_filter,
                    full_read=str(table["sync_mode"]).startswith("full_refresh"),
                    markers=markers,
                )
            )
        selected[unit.key] = rows
    return selected


def _jinja(value: object) -> str:
    """Spell a str, list or bool as a Jinja literal."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_jinja(item) for item in value) + "]"
    if value is None:
        return "none"
    if isinstance(value, int):
        return str(value)
    msg = f"cannot spell {value!r} as Jinja"
    raise TypeError(msg)


def _unit_basename(unit_key: str) -> str:
    return unit_key.replace("/", "__")


def _render_snapshot_sql(unit_key: str, tables: list[HistoryTable]) -> str:
    lines = [
        "{#-",
        "    GENERATED FILE - DO NOT EDIT.",
        "",
        f"    Regenerate with `{GENERATED_BY}`; CI fails if this file and",
        f"    ingestion/inventory/units/{_unit_basename(unit_key)}.yml disagree.",
        "-#}",
    ]
    for table in tables:
        unique_key = table.keys[0] if len(table.keys) == 1 else list(table.keys)
        arguments = [_jinja(table.raw_table), f"unique_key={_jinja(unique_key)}"]
        if table.absence != "deleted":
            arguments.append(f"absence={_jinja(table.absence)}")
        if table.exclude_columns:
            arguments.append(f"exclude_columns={_jinja(table.exclude_columns)}")
        if table.ignore_columns:
            arguments.append(f"ignore_columns={_jinja(table.ignore_columns)}")
        arguments.append(f"live={_jinja(table.live)}")
        lines += [
            "",
            f"{{% snapshot {table.snapshot_name} %}}",
            "{{ raw_history_snapshot(",
            *(
                f"    {argument}," if index < len(arguments) - 1 else f"    {argument}"
                for index, argument in enumerate(arguments)
            ),
            ") }}",
            "{% endsnapshot %}",
        ]
    return "\n".join(lines) + "\n"


_DELETE_TEXT = {
    "full_read": (
        "A record that leaves the source gets a final row with dbt_is_deleted 'True', since every load reads the "
        "whole table."
    ),
    "keyset": (
        "A record deleted at the source gets a final row with dbt_is_deleted 'True' once the unit's keyset "
        "({keyset}) has been read; until then, and while it is stale, deletes wait."
    ),
    "latest_load": (
        "A record missing from the newest load gets a final row with dbt_is_deleted 'True'; the snapshot reads only "
        "the rows of that load."
    ),
    "none": (
        "Deletes are not recorded: the load is incremental, so raw keeps every record it has seen and nothing tells "
        "this table which ones the source has deleted."
    ),
    "aged_out": (
        "A record that stops appearing in the source is not marked deleted (absence: aged_out), because the source "
        "only returns its recent records; the table has no dbt_is_deleted column."
    ),
}


def _delete_text(table: HistoryTable) -> str:
    if table.absence == "aged_out":
        return _DELETE_TEXT["aged_out"]
    if table.delete_filter is not None:
        return _DELETE_TEXT[table.delete_filter.kind].format(keyset=table.raw_table + KEYSET_SUFFIX)
    return _DELETE_TEXT["full_read" if table.full_read else "none"]


def _snapshot_columns(table: HistoryTable) -> list[dict[str, str]]:
    columns = [
        {"name": key, "description": f"primary key of the {table.unit_key} {table.name} record."} for key in table.keys
    ]
    columns += [
        {"name": "dbt_scd_id", "description": "key of this version (hash of the primary key and dbt_updated_at)."},
        {"name": "dbt_updated_at", "description": "time of the snapshot run that recorded this version."},
        {"name": "dbt_valid_from", "description": "time of the snapshot run that first saw this version."},
        {
            "name": "dbt_valid_to",
            "description": "time of the snapshot run that saw it replaced or deleted; null for the current version.",
        },
    ]
    if table.absence == "deleted":
        columns.append(
            {
                "name": "dbt_is_deleted",
                "description": (
                    "'True' on the row written when the record left the source, 'False' otherwise. The deleted row "
                    "also has a null dbt_valid_to."
                ),
            }
        )
    return columns


def _snapshot_meta(table: HistoryTable) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "unit": table.unit_key,
        "absence": table.absence,
        "delete_capture": "aged_out"
        if table.absence == "aged_out"
        else (
            table.delete_filter.kind
            if table.delete_filter is not None
            else ("full_read" if table.full_read else "none")
        ),
    }
    if table.markers:
        meta["markers"] = [dict(marker) for marker in table.markers]
    return meta


# A description longer than this would wrap anyway, so it is written folded.
FOLD_AFTER = 64


class _Folded(str):
    __slots__ = ()


class _DocsDumper(yaml.SafeDumper):
    """Write each description as a folded block, as the hand-written docs YAML does."""


def _represent_folded(dumper: yaml.SafeDumper, data: _Folded) -> yaml.ScalarNode:
    return dumper.represent_scalar("tag:yaml.org,2002:str", str(data), style=">")


_DocsDumper.add_representer(_Folded, _represent_folded)


def _fold_descriptions(node: object) -> object:
    if isinstance(node, dict):
        return {
            key: _Folded(value) if key == "description" and len(value) > FOLD_AFTER else _fold_descriptions(value)
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [_fold_descriptions(item) for item in node]
    return node


def _dump_yaml(document: dict[str, Any], header: list[str]) -> str:
    body = yaml.dump(
        _fold_descriptions(document),
        Dumper=_DocsDumper,
        sort_keys=False,
        width=80,
        indent=2,
        default_flow_style=False,
        allow_unicode=True,
    )
    return "\n".join(["---", *(f"# {line}" if line else "#" for line in header)]) + "\n" + body


def _render_snapshot_yaml(unit_key: str, tables: list[HistoryTable]) -> str:
    snapshots = [
        {
            "name": table.snapshot_name,
            "description": (
                f"History of {table.raw_table} ({table.unit_key}, table {table.name}): one row per version of each "
                "record, with every source column except the loader's, secret-pattern columns and any the inventory "
                f"excludes. Resolution is one snapshot run, and history starts at the first run. {_delete_text(table)}"
            ),
            "config": {"meta": {"raw_history": _snapshot_meta(table)}},
            "columns": _snapshot_columns(table),
        }
        for table in tables
    ]
    header = [
        "GENERATED FILE - DO NOT EDIT.",
        f"Regenerate with `{GENERATED_BY}`; CI fails if this file and",
        f"ingestion/inventory/units/{_unit_basename(unit_key)}.yml disagree.",
    ]
    return _dump_yaml({"version": 2, "snapshots": snapshots}, header)


def _render_keyset_sources(selected: dict[str, list[HistoryTable]]) -> str:
    tables = [
        {
            "name": table.raw_table + KEYSET_SUFFIX,
            "description": (
                f"Primary key of every current record of {table.raw_table}, read from the {table.unit_key} source "
                "database by ol_dlt.keyset after each load and replaced each time. A key in raw that is missing here "
                "was deleted at the source."
            ),
            "columns": [
                {"name": table.delete_filter.key, "description": "primary key of a current source record."},
                {
                    "name": KEYSET_READ_STARTED_AT,
                    "description": (
                        "source database clock read before the load's first keyset select, one value per load. A raw "
                        "row the loader wrote after this existed when the keyset was read."
                    ),
                },
            ],
        }
        for rows in selected.values()
        for table in rows
        if table.delete_filter is not None and table.delete_filter.kind == "keyset"
    ]
    source = {
        "name": "ol_warehouse_raw_data",
        "loader": "dlt",
        "database": "{{ target.database | default(none) }}",
        "schema": '{{ target.schema.replace(var("schema_suffix", ""), "").rstrip("_") }}_raw',
        "tables": tables,
    }
    header = [
        "GENERATED FILE - DO NOT EDIT.",
        f"Regenerate with `{GENERATED_BY}`; CI fails if this file and",
        "ingestion/inventory/ disagree. The keyset tables of the history tables",
        "whose deletes only a keys-only read of the source shows.",
    ]
    return _dump_yaml({"version": 2, "sources": [source]}, header)


def _render_delete_filter_macro(selected: dict[str, list[HistoryTable]]) -> str:
    lines = [
        "{#-",
        "    GENERATED FILE - DO NOT EDIT.",
        "",
        f"    Regenerate with `{GENERATED_BY}`; CI fails if this file and",
        "    ingestion/inventory/ disagree.",
        "",
        "    Maps each raw table whose deletes have to be filtered in (a keyset, or the",
        "    newest dlt load) to how. raw_delete_filter reads it, for the raw history",
        "    snapshots and for staging alike.",
        "-#}",
        "{% macro raw_delete_filter_map() %}",
        "    {% do return({",
    ]
    for rows in selected.values():
        for table in rows:
            spec = table.delete_filter
            if spec is None:
                continue
            entry: dict[str, object] = {"kind": spec.kind, "loader_time_column": spec.loader_time_column}
            if spec.kind == "keyset":
                entry |= {"key": spec.key, "max_age_hours": spec.max_age_hours}
            rendered = ", ".join(f"{_jinja(name)}: {_jinja(value)}" for name, value in entry.items())
            lines.append(f"        {_jinja(table.raw_table)}: {{{rendered}}},")
    lines += ["    }) %}", "{% endmacro %}", ""]
    return "\n".join(lines)


def render_history(units: list[Unit]) -> dict[Path, str]:
    """Render every generated file, keyed by its path relative to the repo root."""
    selected = history_tables(units)
    rendered: dict[Path, str] = {}
    for unit_key, tables in selected.items():
        basename = _unit_basename(unit_key)
        rendered[SNAPSHOT_DIR / f"{basename}.sql"] = _render_snapshot_sql(unit_key, tables)
        rendered[SNAPSHOT_DIR / f"_{basename}__snapshots.yml"] = _render_snapshot_yaml(unit_key, tables)
    rendered[KEYSET_SOURCES_PATH] = _render_keyset_sources(selected)
    rendered[DELETE_FILTER_MACRO_PATH] = _render_delete_filter_macro(selected)
    return rendered


def stale_history_files(repo_root: Path, rendered: dict[Path, str]) -> list[Path]:
    """Return the generated paths whose committed content differs, and leftovers to delete.

    Every file under SNAPSHOT_DIR is generated, so one the render does not
    produce is a unit that left scope and must go.
    """
    stale = [path for path, text in sorted(rendered.items()) if _read(repo_root / path) != text]
    snapshot_dir = repo_root / SNAPSHOT_DIR
    if snapshot_dir.exists():
        stale += sorted(
            path.relative_to(repo_root)
            for path in snapshot_dir.iterdir()
            if path.relative_to(repo_root) not in rendered
        )
    return stale


def write_history_files(repo_root: Path, rendered: dict[Path, str]) -> list[Path]:
    """Write the rendered files and delete leftovers; return what changed."""
    changed = stale_history_files(repo_root, rendered)
    for path in changed:
        target = repo_root / path
        if path in rendered:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(rendered[path])
        else:
            target.unlink()
    return changed


def _read(path: Path) -> str | None:
    return path.read_text() if path.exists() else None
