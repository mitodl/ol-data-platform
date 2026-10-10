"""Raw-table fixtures for the local lake.

A unit whose inventory entry says ``strategies.local: fixture`` has no local
loader, so its raw tables have to be created from something committed. This is
that something: one file per unit under ``ingestion/inventory/fixtures/``, named
as the unit file is, holding each table's columns with their types and a few
rows::

    schema_version: 1
    tables:
      raw__ocw__s3__course_content:
        columns:
          course_slug: string
          course_retrieved_at: timestamp
        rows:
        - course_slug: 18-01-fall-2020
          course_retrieved_at: 2026-01-01 00:00:00

The file carries the types because nothing else in the repository does: the dbt
source files name columns for some tables and types for almost none, and the
only other record is production Glue, which a contributor without AWS
credentials cannot read.

Rows are hand-written, never sampled. Raw application tables hold learner data
(``users_user``, ``auth_user``), and a sampling command would put the scrubbing
decision in code that runs against production. ``capture`` therefore takes the
shape of a table and nothing else.

The loader writes through StarRocks, not pyiceberg: the local catalog hands
every client the in-cluster object store endpoint, which does not resolve from
the host, and StarRocks is the engine that then reads the tables.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from ol_dbt_cli.lib.inventory import Unit

FIXTURES_SUBDIR = "fixtures"
SCHEMA_VERSION = 1
FIXTURE_STRATEGY = "fixture"

# Iceberg primitive names on the left, what StarRocks calls the column in a
# CREATE TABLE on an Iceberg catalog on the right. `timestamp` covers Iceberg's
# timestamptz as well: StarRocks reads both as DATETIME, and Glue reports both
# as `timestamp`, so a second name would record a difference nothing here can
# observe.
STARROCKS_TYPES: dict[str, str] = {
    "boolean": "BOOLEAN",
    "int": "INT",
    "long": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "date": "DATE",
    "timestamp": "DATETIME",
    "string": "STRING",
}
_DECIMAL = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")

# Hive type names as Glue reports them for an Iceberg table.
_GLUE_TYPES: dict[str, str] = {
    "boolean": "boolean",
    "tinyint": "int",
    "smallint": "int",
    "int": "int",
    "integer": "int",
    "bigint": "long",
    "float": "float",
    "double": "double",
    "date": "date",
    "timestamp": "timestamp",
    "string": "string",
}
_GLUE_SIZED_STRING = re.compile(r"^(var)?char\(\d+\)$")


@dataclass
class TableFixture:
    """One raw table: its columns in order, and the rows to load."""

    raw_table: str
    columns: dict[str, str]
    rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class UnitFixture:
    """One parsed fixture file."""

    path: Path
    tables: list[TableFixture]


def starrocks_type(fixture_type: str) -> str:
    """Return the StarRocks column type for a fixture type.

    :param fixture_type: A key of :data:`STARROCKS_TYPES`, or ``decimal(p,s)``.
    :returns: The type as StarRocks DDL spells it.
    :rtype: str
    :raises ValueError: For a type the fixture format does not have.
    """
    if fixture_type in STARROCKS_TYPES:
        return STARROCKS_TYPES[fixture_type]
    if match := _DECIMAL.match(fixture_type):
        return f"DECIMAL({match.group(1)}, {match.group(2)})"
    msg = f"unsupported fixture type {fixture_type!r}; use one of {', '.join(STARROCKS_TYPES)} or decimal(p,s)"
    raise ValueError(msg)


def fixture_type_from_glue(glue_type: str) -> str | None:
    """Return the fixture type for a Glue column type, or ``None`` if it has none.

    Nested types (``struct``, ``array``, ``map``) and ``binary`` have none: the
    loader passes rows as SQL parameters, which cannot carry them.
    """
    normalized = glue_type.strip().lower()
    if normalized in _GLUE_TYPES:
        return _GLUE_TYPES[normalized]
    if _GLUE_SIZED_STRING.match(normalized):
        return "string"
    if _DECIMAL.match(normalized):
        return normalized.replace(" ", "")
    return None


def fixture_path(inventory_dir: Path, unit: Unit) -> Path:
    """Return where the fixture for ``unit`` lives: named as the unit file is."""
    return inventory_dir / FIXTURES_SUBDIR / unit.path.name


def parse_fixture(path: Path, unit: Unit) -> UnitFixture:
    """Read a fixture file and check it against its unit.

    :param path: The fixture file.
    :param unit: The inventory unit of the same name.
    :returns: The parsed fixture.
    :rtype: UnitFixture
    :raises ValueError: If the unit is not a fixture unit, a table is not the
        unit's, a type is not in the format, or a row sets a column the table
        does not have.
    """
    data = yaml.safe_load(path.read_text())
    if data["schema_version"] != SCHEMA_VERSION:
        msg = f"{path}: schema_version {data['schema_version']!r}, expected {SCHEMA_VERSION}"
        raise ValueError(msg)
    local_strategy = unit.data["strategies"]["local"]
    if local_strategy != FIXTURE_STRATEGY:
        msg = f"{path}: unit {unit.key} has strategies.local: {local_strategy}, not {FIXTURE_STRATEGY}"
        raise ValueError(msg)
    declared = {table["raw_table"] for table in unit.tables}

    tables = []
    for raw_table, body in data["tables"].items():
        if raw_table not in declared:
            msg = f"{path}: {raw_table} is not a table of unit {unit.key}"
            raise ValueError(msg)
        columns: dict[str, str] = body["columns"]
        if not columns:
            msg = f"{path}: {raw_table} has no columns"
            raise ValueError(msg)
        for column, fixture_type in columns.items():
            try:
                starrocks_type(fixture_type)
            except ValueError as error:
                msg = f"{path}: {raw_table}.{column}: {error}"
                raise ValueError(msg) from error
        rows = body.get("rows") or []
        for number, row in enumerate(rows, start=1):
            if unknown := sorted(set(row) - set(columns)):
                msg = f"{path}: {raw_table} row {number} sets {', '.join(unknown)}, which the table does not have"
                raise ValueError(msg)
        tables.append(TableFixture(raw_table=raw_table, columns=dict(columns), rows=list(rows)))
    return UnitFixture(path=path, tables=tables)


def load_fixtures(inventory_dir: Path, units: Iterable[Unit], *, selected: bool = False) -> list[UnitFixture]:
    """Read the committed fixtures of ``units``.

    :param inventory_dir: Directory holding ``units/`` and ``fixtures/``.
    :param units: The inventory units to read fixtures for.
    :param selected: The units were named by the caller, so one with no fixture
        file is an error, not a unit nobody has written a fixture for yet.
    :raises ValueError: If a fixture is invalid, a selected unit has none, or a
        fixture file has no unit of the same name.
    """
    units = list(units)
    paths = {fixture_path(inventory_dir, unit): unit for unit in units}
    if selected:
        if missing := [str(path) for path in paths if not path.exists()]:
            msg = f"no fixture file: {', '.join(missing)}"
            raise ValueError(msg)
    else:
        committed = set((inventory_dir / FIXTURES_SUBDIR).glob("*.yml"))
        if orphans := sorted(str(path) for path in committed - set(paths)):
            msg = f"fixture with no inventory unit of the same name: {', '.join(orphans)}"
            raise ValueError(msg)
    return [parse_fixture(path, unit) for path, unit in sorted(paths.items()) if path.exists()]


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def qualified_name(catalog: str, schema: str, table: str | None = None) -> str:
    """Return a backtick-quoted ``catalog.schema[.table]``."""
    parts = [catalog, schema] if table is None else [catalog, schema, table]
    return ".".join(_quote(part) for part in parts)


def create_table_sql(table: TableFixture, catalog: str, schema: str) -> str:
    """Return the CREATE TABLE for ``table`` in an Iceberg catalog."""
    columns = ", ".join(f"{_quote(name)} {starrocks_type(kind)}" for name, kind in table.columns.items())
    return f"CREATE TABLE {qualified_name(catalog, schema, table.raw_table)} ({columns})"


def insert_sql(table: TableFixture, catalog: str, schema: str) -> str:
    """Return a parameterized INSERT covering every column of ``table``."""
    columns = ", ".join(_quote(name) for name in table.columns)
    placeholders = ", ".join(["%s"] * len(table.columns))
    return f"INSERT INTO {qualified_name(catalog, schema, table.raw_table)} ({columns}) VALUES ({placeholders})"  # noqa: S608


def row_parameters(table: TableFixture) -> list[tuple[Any, ...]]:
    """Return each row as INSERT parameters, in column order.

    A column the row leaves out is NULL. A mapping or list is written as JSON
    text, so a JSON-in-a-string column (``data_json``, an Airbyte ``metadata``)
    can be written in the fixture as the structure it holds.
    """
    return [tuple(_parameter(row.get(column)) for column in table.columns) for row in table.rows]


def _parameter(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def merge_captured(existing: Mapping[str, Any] | None, captured: Mapping[str, Mapping[str, str]]) -> dict[str, Any]:
    """Fold freshly captured column maps into a fixture document.

    A table already in the document keeps its rows and takes the captured
    columns; a table not captured is left alone.

    :param existing: The parsed fixture file, or ``None`` when there is none.
    :param captured: ``{raw_table: {column: fixture type}}``.
    :returns: The document to write.
    :rtype: dict[str, Any]
    """
    document: dict[str, Any] = {"schema_version": SCHEMA_VERSION, "tables": {}}
    if existing:
        document["tables"] = {name: dict(body) for name, body in existing["tables"].items()}
    for raw_table, columns in captured.items():
        body = document["tables"].setdefault(raw_table, {"rows": []})
        document["tables"][raw_table] = {"columns": dict(columns), "rows": body.get("rows") or []}
    return document
