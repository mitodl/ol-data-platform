"""Empty stand-in relations for dbt unit test inputs.

dbt reads a unit test input's column names and types from the input's relation in the
warehouse; it never uses YAML-declared types (dbt-core 1.12 ``parser/unit_tests.py`` passes
``column_name_to_data_types=None``). On a DuckDB target with none of the warehouse in it every
unit test therefore fails with "Not able to get columns ... because the relation doesn't exist".

This derives one empty table per input relation from ``manifest.json``. An input that documents
its columns gets exactly those, so dbt still rejects a fixture setting a column the input no
longer has; an input that documents none gets the columns its fixtures set. Types are the
documented ``data_type``, else inferred from the fixture values. Created in a scratch database,
that is enough for dbt to build the fixture CTEs, with no warehouse credentials.
"""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import duckdb

_REF = re.compile(r"""^ref\(\s*['"]([^'"]+)['"]\s*\)$""")
_SOURCE = re.compile(r"""^source\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)$""")

# Fallback for a column that is undocumented and only ever NULL in fixtures. A wrong guess
# usually fails loudly (DuckDB refuses to compare VARCHAR with a number without a cast), and
# the fix is to document the column's data_type rather than to guess better here.
DEFAULT_TYPE = "VARCHAR"


@dataclass
class StubRelation:
    """An input relation to create."""

    database: str
    schema: str
    identifier: str
    documented: dict[str, str] = field(default_factory=dict)
    observed: dict[str, set[str]] = field(default_factory=dict)

    def add_value(self, column: str, value: Any) -> None:
        types = self.observed.setdefault(column.lower(), set())
        value_type = _value_type(value)
        if value_type:
            types.add(value_type)

    def columns(self) -> dict[str, str]:
        """Return ``{column: duckdb type}``."""
        if self.documented:
            return {
                column: data_type or _merge_types(self.observed.get(column, set()))
                for column, data_type in self.documented.items()
            }
        return {column: _merge_types(types) for column, types in self.observed.items()}


def _value_type(value: Any) -> str | None:
    # bool is a subclass of int, so it has to be checked first.
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, str):
        return "VARCHAR"
    return None


def _merge_types(types: set[str]) -> str:
    if len(types) == 1:
        return next(iter(types))
    if types == {"BIGINT", "DOUBLE"}:
        return "DOUBLE"
    return DEFAULT_TYPE


def _csv_value(value: str) -> Any:
    """Read a CSV cell as dbt's csv fixtures would type it: numbers as numbers."""
    for parse in (int, float):
        try:
            return parse(value)
        except ValueError:
            continue
    return None if value == "" else value


def _fixture_rows(given: dict[str, Any]) -> list[dict[str, Any]]:
    """Rows of a ``dict`` or inline ``csv`` fixture. A ``sql`` fixture needs no relation."""
    rows = given.get("rows")
    fixture_format = given.get("format", "dict")
    if fixture_format == "dict":
        return list(rows or [])
    if fixture_format == "csv" and isinstance(rows, str):
        return [
            {column: _csv_value(value) for column, value in row.items()}
            for row in csv.DictReader(io.StringIO(rows.strip()))
        ]
    return []


def _resolve_input(input_expr: str, tested: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    expr = input_expr.strip()
    if expr == "this":
        return tested
    if match := _REF.match(expr):
        name = match.group(1)
        candidates = [
            node
            for node in manifest["nodes"].values()
            if node["name"] == name and node["resource_type"] in ("model", "seed", "snapshot")
        ]
    elif match := _SOURCE.match(expr):
        source_name, table_name = match.groups()
        candidates = [
            source
            for source in manifest["sources"].values()
            if source["source_name"] == source_name and source["name"] == table_name
        ]
    else:
        msg = f"unsupported input {input_expr!r}"
        raise ValueError(msg)
    if len(candidates) != 1:
        msg = f"input {input_expr!r} matches {len(candidates)} manifest nodes, expected 1"
        raise ValueError(msg)
    return candidates[0]


def stub_relations(manifest: dict[str, Any]) -> tuple[list[StubRelation], list[str]]:
    """Collect the relations every unit test in ``manifest`` reads from.

    An input that can't be resolved is reported rather than raised, so one bad unit test
    leaves dbt to fail that test alone.

    :param manifest: Parsed ``target/manifest.json``.
    :returns: One stub per distinct input relation, merged across all unit tests, and a
        message per input that could not be resolved.
    :rtype: tuple[list[StubRelation], list[str]]
    """
    stubs: dict[tuple[str, str, str], StubRelation] = {}
    problems: list[str] = []
    for unit_test in manifest.get("unit_tests", {}).values():
        tested = manifest["nodes"][unit_test["depends_on"]["nodes"][0]]
        for given in unit_test["given"]:
            if given.get("format") == "sql":
                continue
            try:
                node = _resolve_input(given["input"], tested, manifest)
            except ValueError as error:
                problems.append(f"{unit_test['name']}: no stand-in for {error}")
                continue
            key = (node["database"], node["schema"], node.get("alias") or node.get("identifier") or node["name"])
            stub = stubs.get(key)
            if stub is None:
                stub = StubRelation(*key)
                for column in node.get("columns", {}).values():
                    stub.documented[column["name"].lower()] = column.get("data_type") or ""
                stubs[key] = stub
            for row in _fixture_rows(given):
                for column, value in row.items():
                    stub.add_value(column, value)
    return list(stubs.values()), problems


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def create_stub_relations(
    conn: duckdb.DuckDBPyConnection, stubs: Iterable[StubRelation]
) -> tuple[list[str], list[str]]:
    """Create (or replace) an empty table for each stub.

    A stub DuckDB can't create (no known columns, a Trino-only documented type, a database
    other than the target's) is reported rather than raised, for the same reason as in
    :func:`stub_relations`.

    :param conn: Connection to the scratch database the dbt target points at.
    :param stubs: Relations from :func:`stub_relations`.
    :returns: The fully-qualified names created, and a message per stub that was not.
    :rtype: tuple[list[str], list[str]]
    """
    created: list[str] = []
    failed: list[str] = []
    for stub in stubs:
        schema = f"{_quote(stub.database)}.{_quote(stub.schema)}"
        relation = f"{schema}.{_quote(stub.identifier)}"
        columns = stub.columns()
        if not columns:
            failed.append(
                f"No columns known for {relation}: it documents none and its fixtures set none. "
                "Document its columns or give it a `format: sql` fixture."
            )
            continue
        column_sql = ", ".join(f"{_quote(name)} {data_type}" for name, data_type in columns.items())
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            conn.execute(f"CREATE OR REPLACE TABLE {relation} ({column_sql})")
        except duckdb.Error as error:
            failed.append(f"Could not create {relation}: {error}")
            continue
        created.append(relation)
    return created, failed
