"""Empty stand-in relations for dbt unit test inputs.

dbt reads a unit test input's column names and types from the input's relation in the
warehouse; it never uses YAML-declared types (dbt-core 1.12 ``parser/unit_tests.py`` passes
``column_name_to_data_types=None``). On a DuckDB target with none of the warehouse in it every
unit test therefore fails with "Not able to get columns ... because the relation doesn't exist".

This derives one empty table per input relation from ``manifest.json``: every column the input
node documents, plus every column a fixture sets for it in any unit test, typed by the
documented ``data_type`` or else by the fixture values. Created in a throwaway database, that is
enough for dbt to build the fixture CTEs, with no warehouse credentials.
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

# Fallback for a column that is undocumented and only ever NULL in fixtures. A wrong guess fails
# loudly (DuckDB refuses to compare VARCHAR with a number without a cast), so the fix is to
# document the column's data_type rather than to guess better here.
DEFAULT_TYPE = "VARCHAR"


@dataclass
class StubRelation:
    """An input relation to create, with its columns in first-seen order."""

    database: str
    schema: str
    identifier: str
    declared: dict[str, str] = field(default_factory=dict)
    observed: dict[str, set[str]] = field(default_factory=dict)

    def add_values(self, column: str, value: Any) -> None:
        types = self.observed.setdefault(column.lower(), set())
        value_type = _value_type(value)
        if value_type:
            types.add(value_type)

    def columns(self) -> dict[str, str]:
        """Return ``{column: duckdb type}``, documented columns first."""
        resolved = dict(self.declared)
        for column, types in self.observed.items():
            if column not in resolved:
                resolved[column] = _merge_types(types)
        return resolved


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


def _fixture_rows(given: dict[str, Any]) -> list[dict[str, Any]]:
    """Rows of a ``dict`` or inline ``csv`` fixture. A ``sql`` fixture needs no relation."""
    rows = given.get("rows")
    fixture_format = given.get("format", "dict")
    if fixture_format == "dict":
        return list(rows or [])
    if fixture_format == "csv" and isinstance(rows, str):
        return list(csv.DictReader(io.StringIO(rows.strip())))
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
        msg = f"Unsupported unit test input {input_expr!r}"
        raise ValueError(msg)
    if len(candidates) != 1:
        msg = f"Unit test input {input_expr!r} matches {len(candidates)} manifest nodes, expected 1"
        raise ValueError(msg)
    return candidates[0]


def stub_relations(manifest: dict[str, Any]) -> list[StubRelation]:
    """Collect the relations every unit test in ``manifest`` reads from.

    :param manifest: Parsed ``target/manifest.json``.
    :returns: One stub per distinct input relation, merged across all unit tests.
    :rtype: list[StubRelation]
    """
    stubs: dict[tuple[str, str, str], StubRelation] = {}
    for unit_test in manifest.get("unit_tests", {}).values():
        tested = manifest["nodes"][unit_test["depends_on"]["nodes"][0]]
        for given in unit_test["given"]:
            if given.get("format") == "sql":
                continue
            node = _resolve_input(given["input"], tested, manifest)
            key = (node["database"], node["schema"], node.get("alias") or node.get("identifier") or node["name"])
            stub = stubs.get(key)
            if stub is None:
                stub = StubRelation(*key)
                for column in node.get("columns", {}).values():
                    stub.declared[column["name"].lower()] = column.get("data_type") or ""
                stubs[key] = stub
            for row in _fixture_rows(given):
                for column, value in row.items():
                    stub.add_values(column, value)
    for stub in stubs.values():
        for column, data_type in list(stub.declared.items()):
            if not data_type:
                stub.declared[column] = _merge_types(stub.observed.get(column, set()))
    return list(stubs.values())


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def create_stub_relations(conn: duckdb.DuckDBPyConnection, stubs: Iterable[StubRelation]) -> list[str]:
    """Create (or replace) an empty table for each stub.

    :param conn: Connection to the throwaway database the dbt target points at.
    :param stubs: Relations from :func:`stub_relations`.
    :returns: The fully-qualified names created.
    :rtype: list[str]
    """
    created = []
    for stub in stubs:
        columns = stub.columns()
        if not columns:
            continue
        schema = f"{_quote(stub.database)}.{_quote(stub.schema)}"
        relation = f"{schema}.{_quote(stub.identifier)}"
        column_sql = ", ".join(f"{_quote(name)} {data_type}" for name, data_type in columns.items())
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"CREATE OR REPLACE TABLE {relation} ({column_sql})")
        created.append(relation)
    return created
