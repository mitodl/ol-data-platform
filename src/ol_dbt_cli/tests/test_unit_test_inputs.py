"""Tests for lib/unit_test_inputs.py — stand-in relations for dbt unit test inputs."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import duckdb
import pytest

from ol_dbt_cli.lib.unit_test_inputs import create_stub_relations, stub_relations


def _model(name: str, columns: dict[str, str] | None = None, schema: str = "main_staging") -> dict[str, Any]:
    return {
        "unique_id": f"model.open_learning.{name}",
        "name": name,
        "resource_type": "model",
        "database": "dev",
        "schema": schema,
        "alias": name,
        "columns": {column: {"name": column, "data_type": data_type} for column, data_type in (columns or {}).items()},
    }


def _manifest(
    *unit_tests: dict[str, Any], nodes: list[dict[str, Any]], sources: Sequence[dict[str, Any]] = ()
) -> dict[str, Any]:
    return {
        "nodes": {node["unique_id"]: node for node in nodes},
        "sources": {source["unique_id"]: source for source in sources},
        "unit_tests": {f"unit_test.open_learning.t{i}": test for i, test in enumerate(unit_tests)},
    }


def _unit_test(model: str, *given: dict[str, Any]) -> dict[str, Any]:
    return {"depends_on": {"nodes": [f"model.open_learning.{model}"]}, "given": list(given)}


def test_documented_columns_come_first_and_keep_their_type() -> None:
    upstream = _model("stg__a", {"id": "bigint", "name": ""})
    tested = _model("int__b", schema="main_intermediate")
    manifest = _manifest(
        _unit_test("int__b", {"input": "ref('stg__a')", "rows": [{"id": 1, "name": "x", "extra": 2.5}]}),
        nodes=[upstream, tested],
    )

    (stub,) = stub_relations(manifest)

    assert (stub.database, stub.schema, stub.identifier) == ("dev", "main_staging", "stg__a")
    assert stub.columns() == {"id": "bigint", "name": "VARCHAR", "extra": "DOUBLE"}


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([1, 2], "BIGINT"),
        ([1, 2.5], "DOUBLE"),
        ([True], "BOOLEAN"),
        ([1, "a"], "VARCHAR"),
        ([None], "VARCHAR"),
    ],
)
def test_undocumented_column_type_is_inferred_across_unit_tests(values: list[Any], expected: str) -> None:
    upstream = _model("stg__a")
    tested = _model("int__b")
    manifest = _manifest(
        *[_unit_test("int__b", {"input": "ref('stg__a')", "rows": [{"c": value}]}) for value in values],
        nodes=[upstream, tested],
    )

    (stub,) = stub_relations(manifest)

    assert stub.columns() == {"c": expected}


def test_this_resolves_to_the_tested_model_and_sources_by_name() -> None:
    tested = _model("tfact_x", {"x_pk": "varchar"}, schema="main_dimensional")
    source = {
        "unique_id": "source.open_learning.raw.raw__t",
        "source_name": "raw",
        "name": "raw__t",
        "resource_type": "source",
        "database": "dev",
        "schema": "main_raw",
        "identifier": "raw__t",
        "columns": {},
    }
    manifest = _manifest(
        _unit_test(
            "tfact_x",
            {"input": "this", "rows": []},
            {"input": "source('raw', 'raw__t')", "rows": [{"id": 1}]},
            {"input": "ref('ignored')", "format": "sql", "rows": "select 1 as a"},
        ),
        nodes=[tested],
        sources=[source],
    )

    stubs = {stub.identifier: stub for stub in stub_relations(manifest)}

    assert set(stubs) == {"tfact_x", "raw__t"}
    assert stubs["tfact_x"].schema == "main_dimensional"
    assert stubs["raw__t"].columns() == {"id": "BIGINT"}


def test_csv_fixture_columns_are_read_from_the_header() -> None:
    manifest = _manifest(
        _unit_test("int__b", {"input": "ref('stg__a')", "format": "csv", "rows": "id,name\n1,x\n"}),
        nodes=[_model("stg__a"), _model("int__b")],
    )

    (stub,) = stub_relations(manifest)

    assert stub.columns() == {"id": "VARCHAR", "name": "VARCHAR"}


def test_ambiguous_ref_is_refused() -> None:
    manifest = _manifest(
        _unit_test("int__b", {"input": "ref('missing')", "rows": []}),
        nodes=[_model("int__b")],
    )

    with pytest.raises(ValueError, match="matches 0 manifest nodes"):
        stub_relations(manifest)


def test_create_stub_relations_builds_empty_tables_and_skips_columnless_ones() -> None:
    manifest = _manifest(
        _unit_test(
            "int__b",
            {"input": "ref('stg__a')", "rows": [{"id": 1}]},
            {"input": "ref('stg__empty')", "rows": []},
        ),
        nodes=[_model("stg__a"), _model("stg__empty"), _model("int__b")],
    )
    conn = duckdb.connect(":memory:")
    (database,) = conn.execute("select current_database()").fetchall()[0]
    stubs = stub_relations(manifest)
    for stub in stubs:
        stub.database = database

    created = create_stub_relations(conn, stubs)
    # Idempotent: a second run replaces rather than fails.
    create_stub_relations(conn, stubs)

    assert created == [f'"{database}"."main_staging"."stg__a"']
    assert conn.execute('select count(*) from main_staging."stg__a"').fetchone() == (0,)
    assert conn.execute('describe main_staging."stg__a"').fetchall()[0][:2] == ("id", "BIGINT")
