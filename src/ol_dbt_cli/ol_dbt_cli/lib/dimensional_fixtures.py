"""Dimensional-layer fixtures for the local lake.

The raw fixtures (:mod:`ol_dbt_cli.lib.raw_fixtures`) feed models that build
upward from staging. Some consumers read only the dimensional layer, and the
chain under it is long: the b2b materialized views read thirteen dimensional
tables, and their facts come from Open edX tracking logs and grades that have no
local source. A dimensional fixture writes those tables directly into
``ol_warehouse_local_dimensional``, where the views' sources resolve on the
local lake.

The file has the raw format's shape, plus parameters::

    schema_version: 1
    parameters:
      sso_organization_id:
        type: string
        description: The Keycloak organization's id.
    tables:
      dim_organization:
        columns:
          organization_pk: string
          sso_organization_id: string
        rows:
        - organization_pk: fixture-org
          sso_organization_id: $sso_organization_id

A row value that is exactly ``$name`` is replaced at load time with the value
given for that parameter. Parameters exist for ids another system assigns when
the local stack is set up (Keycloak picks organization ids itself, and MITx
Online numbers its contracts), which no committed file can know. A parameter's
type has to be the type of every column it fills, so a value that fits the
parameter fits the column.
"""

from __future__ import annotations

import re
import struct
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from ol_dbt_cli.lib.raw_fixtures import (
    SCHEMA_VERSION,
    TableFixture,
    check_columns,
    check_row,
    coerce_value,
    starrocks_type,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

DEFAULT_DIMENSIONAL_FIXTURES_DIR = Path("src/ol_dbt/fixtures/dimensional")
# The schema the `dimensional` dbt source resolves to under `--env dev`
# (`ol_warehouse_{DBT_DATA_LAKE_ENV}_dimensional`, with DBT_DATA_LAKE_ENV=local).
LOCAL_DIMENSIONAL_SCHEMA = "ol_warehouse_local_dimensional"
_PLACEHOLDER = re.compile(r"^\$([a-z_][a-z0-9_]*)$")


@dataclass(frozen=True)
class Parameter:
    """A value a dimensional fixture takes at load time."""

    name: str
    fixture_type: str
    description: str


@dataclass
class DimensionalFixture:
    """One parsed dimensional fixture file. Rows still hold their ``$name`` placeholders."""

    path: Path
    parameters: dict[str, Parameter]
    tables: list[TableFixture]


def _placeholder(value: Any) -> str | None:
    """Return the parameter a row value names, or ``None`` if it names none."""
    if isinstance(value, str) and (match := _PLACEHOLDER.match(value)):
        return match.group(1)
    return None


def dimensional_fixture_path(fixtures_dir: Path, name: str) -> Path:
    """Return the file of the dimensional fixture ``name``."""
    return fixtures_dir / f"{name}.yml"


def parse_dimensional_fixture(path: Path) -> DimensionalFixture:
    """Read a dimensional fixture file and check it.

    :param path: The fixture file.
    :returns: The parsed fixture.
    :rtype: DimensionalFixture
    :raises ValueError: If a type is not in the format, a row sets a column the
        table does not have or a value its column cannot hold, a placeholder
        names an undeclared parameter or fills a column of another type, or a
        declared parameter is never used.
    """
    data = yaml.safe_load(path.read_text())
    if data["schema_version"] != SCHEMA_VERSION:
        msg = f"{path}: schema_version {data['schema_version']!r}, expected {SCHEMA_VERSION}"
        raise ValueError(msg)

    parameters: dict[str, Parameter] = {}
    for name, body in (data.get("parameters") or {}).items():
        if not _PLACEHOLDER.match(f"${name}"):
            msg = f"{path}: parameter {name!r} is not a lowercase identifier"
            raise ValueError(msg)
        try:
            starrocks_type(body["type"])
        except ValueError as error:
            msg = f"{path}: parameter {name}: {error}"
            raise ValueError(msg) from error
        # A value comes from --set as text, and a boolean column takes only 0 or 1.
        if body["type"] == "boolean":
            msg = f"{path}: parameter {name} is boolean, which --set cannot supply; parameters are ids"
            raise ValueError(msg)
        parameters[name] = Parameter(name, body["type"], body["description"])

    used: set[str] = set()
    tables = []
    for table_name, body in data["tables"].items():
        columns: dict[str, str] = body["columns"]
        check_columns(columns, f"{path}: {table_name}")
        rows = body.get("rows") or []
        for number, row in enumerate(rows, start=1):
            label = f"{path}: {table_name} row {number}"
            literal = {}
            for column, value in row.items():
                name = _placeholder(value)
                if name is None:
                    literal[column] = value
                    continue
                if name not in parameters:
                    msg = f"{label} uses ${name}, which the file does not declare under parameters"
                    raise ValueError(msg)
                if column in columns and parameters[name].fixture_type != columns[column]:
                    msg = (
                        f"{label} puts ${name} ({parameters[name].fixture_type}) in column {column}, "
                        f"which is {columns[column]}"
                    )
                    raise ValueError(msg)
                used.add(name)
                literal[column] = None
            check_row(literal, columns, label)
        tables.append(TableFixture(raw_table=table_name, columns=dict(columns), rows=list(rows)))

    if unused := sorted(set(parameters) - used):
        msg = f"{path}: parameter(s) {', '.join(unused)} declared but used by no row"
        raise ValueError(msg)
    return DimensionalFixture(path=path, parameters=parameters, tables=tables)


def resolve_parameters(fixture: DimensionalFixture, values: Mapping[str, str]) -> list[TableFixture]:
    """Return the fixture's tables with every placeholder replaced by its value.

    :param fixture: A parsed dimensional fixture.
    :param values: ``{parameter: value}``, as given on the command line.
    :returns: The tables, ready to load.
    :rtype: list[TableFixture]
    :raises ValueError: If a declared parameter has no value, a value is given
        for a parameter the file does not declare, or a value does not fit its
        parameter's type.
    """
    if missing := sorted(set(fixture.parameters) - set(values)):
        msg = f"{fixture.path} needs a value for: {', '.join(missing)}"
        raise ValueError(msg)
    if unknown := sorted(set(values) - set(fixture.parameters)):
        msg = f"{fixture.path} declares no parameter {', '.join(unknown)}"
        raise ValueError(msg)
    typed: dict[str, Any] = {}
    for name, value in values.items():
        fixture_type = fixture.parameters[name].fixture_type
        try:
            typed[name] = coerce_value(value, fixture_type)
        except (ValueError, TypeError, ArithmeticError, struct.error) as error:
            msg = f"{name}={value!r} is not a valid {fixture_type}"
            raise ValueError(msg) from error

    def substituted(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            column: typed[name] if (name := _placeholder(value)) is not None else value for column, value in row.items()
        }

    return [replace(table, rows=[substituted(row) for row in table.rows]) for table in fixture.tables]
