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


def test_documented_input_gets_only_its_documented_columns() -> None:
    # An undocumented fixture column is left off so dbt still rejects it as stale.
    upstream = _model("stg__a", {"id": "bigint", "name": ""})
    tested = _model("int__b", schema="main_intermediate")
    manifest = _manifest(
        _unit_test("int__b", {"input": "ref('stg__a')", "rows": [{"id": 1, "name": "x", "extra": 2.5}]}),
        nodes=[upstream, tested],
    )

    (stub,), problems = stub_relations(manifest)

    assert problems == []
    assert (stub.database, stub.schema, stub.identifier) == ("dev", "main_staging", "stg__a")
    assert stub.columns() == {"id": "bigint", "name": "VARCHAR"}


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

    (stub,), _ = stub_relations(manifest)

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

    stubs = {stub.identifier: stub for stub in stub_relations(manifest)[0]}

    assert set(stubs) == {"tfact_x", "raw__t"}
    assert stubs["tfact_x"].schema == "main_dimensional"
    assert stubs["raw__t"].columns() == {"id": "BIGINT"}


def test_csv_fixture_columns_are_read_from_the_header_and_typed() -> None:
    manifest = _manifest(
        _unit_test("int__b", {"input": "ref('stg__a')", "format": "csv", "rows": "id,amt,name\n1,2.5,x\n10,3,\n"}),
        nodes=[_model("stg__a"), _model("int__b")],
    )

    (stub,), _ = stub_relations(manifest)

    assert stub.columns() == {"id": "BIGINT", "amt": "DOUBLE", "name": "VARCHAR"}


def test_unresolvable_inputs_are_reported_without_blocking_others() -> None:
    test = _unit_test(
        "int__b",
        {"input": "ref('missing')", "rows": []},
        {"input": "ref('stg__a', v=2)", "rows": []},
        {"input": "ref('stg__a')", "rows": [{"id": 1}]},
    )
    test["name"] = "t"
    manifest = _manifest(test, nodes=[_model("stg__a"), _model("int__b")])

    stubs, problems = stub_relations(manifest)

    assert [stub.identifier for stub in stubs] == ["stg__a"]
    assert len(problems) == 2
    assert "matches 0 manifest nodes" in problems[0]
    assert "unsupported input" in problems[1]


def test_create_stub_relations_builds_empty_tables_and_reports_failures() -> None:
    manifest = _manifest(
        _unit_test(
            "int__b",
            {"input": "ref('stg__a')", "rows": [{"id": 1}]},
            {"input": "ref('stg__empty')", "rows": []},
            {"input": "ref('stg__trino_type')", "rows": []},
        ),
        nodes=[
            _model("stg__a"),
            _model("stg__empty"),
            _model("stg__trino_type", {"created_on": "timestamp(6) with time zone"}),
            _model("int__b"),
        ],
    )
    conn = duckdb.connect(":memory:")
    (database,) = conn.execute("select current_database()").fetchall()[0]
    stubs, _ = stub_relations(manifest)
    for stub in stubs:
        stub.database = database

    created, failed = create_stub_relations(conn, stubs)
    # Idempotent: a second run replaces rather than fails.
    create_stub_relations(conn, stubs)

    assert created == [f'"{database}"."main_staging"."stg__a"']
    assert len(failed) == 2
    assert failed[0].startswith("No columns known")
    assert failed[1].startswith("Could not create")
    assert conn.execute('select count(*) from main_staging."stg__a"').fetchone() == (0,)
    assert conn.execute('describe main_staging."stg__a"').fetchall()[0][:2] == ("id", "BIGINT")
