"""Tests for the local lake's dimensional fixtures (`ol-dbt fixtures load-dimensional`).

As with the raw fixtures, the loader's SQL is checked as text here and against a
real StarRocks only by hand. These pin the parameter rules and keep the
committed b2b fixture in step with the views it exists for.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import duckdb
import jinja2
import pytest
import sqlglot
import yaml

from ol_dbt_cli.commands import fixtures as fixtures_command
from ol_dbt_cli.lib.cohort_policy import column_meta
from ol_dbt_cli.lib.dimensional_fixtures import (
    DEFAULT_DIMENSIONAL_FIXTURES_DIR,
    LOCAL_DIMENSIONAL_SCHEMA,
    parse_dimensional_fixture,
    resolve_parameters,
)
from ol_dbt_cli.lib.raw_fixtures import TableFixture

REPO_ROOT = Path(__file__).resolve().parents[3]
B2B_FIXTURE = REPO_ROOT / DEFAULT_DIMENSIONAL_FIXTURES_DIR / "b2b_organization.yml"
B2B_MODEL_DIRS = [REPO_ROOT / "src/ol_dbt/models/b2b_analytics", REPO_ROOT / "src/ol_dbt/models/b2b_learner_records"]
B2B_MACROS = REPO_ROOT / "src/ol_dbt/macros/b2b_engagement.sql"
_DIMENSIONAL_SOURCE = re.compile(r"source\(\s*'dimensional'\s*,\s*'(\w+)'\s*\)")
_B2B_MACRO_NAMES = ("b2b_contracts", "b2b_learner_courserun_months", "b2b_learner_courserun_engagement")
B2B_VALUES = {"sso_organization_id": "4f0c2b8e-1111-4c4c-9a9a-0123456789ab", "contract_id": "7"}
# ol-analytics-api's b2b_dashboard anonymization_floor: a row whose primary cohort is under it is withheld.
ANONYMIZATION_FLOOR = 5
_DUCKDB_TYPES = {
    "string": "VARCHAR",
    "long": "BIGINT",
    "int": "INTEGER",
    "boolean": "BOOLEAN",
    "double": "DOUBLE",
    "date": "DATE",
}


def _write(tmp_path: Path, document: dict[str, Any]) -> Path:
    path = tmp_path / "demo.yml"
    path.write_text(yaml.safe_dump({"schema_version": 1, **document}, sort_keys=False))
    return path


def _document(
    rows: list[dict[str, Any]], parameters: dict[str, str] | None = None, column_type: str = "string"
) -> dict[str, Any]:
    return {
        "parameters": {
            name: {"type": kind, "description": f"The {name}."} for name, kind in (parameters or {}).items()
        },
        "tables": {"dim_thing": {"columns": {"thing_pk": "string", "external_id": column_type}, "rows": rows}},
    }


def test_placeholders_are_replaced_with_typed_values(tmp_path: Path) -> None:
    path = _write(
        tmp_path, _document([{"thing_pk": "a", "external_id": "$external_id"}], {"external_id": "long"}, "long")
    )

    (table,) = resolve_parameters(parse_dimensional_fixture(path), {"external_id": "42"})

    assert table.rows == [{"thing_pk": "a", "external_id": 42}]


@pytest.mark.parametrize(
    ("rows", "parameters", "column_type", "message"),
    [
        ([{"thing_pk": "a", "external_id": "$nope"}], {}, "string", "does not declare under parameters"),
        ([{"thing_pk": "a", "external_id": "$external_id"}], {"external_id": "long"}, "string", "which is string"),
        ([{"thing_pk": "a"}], {"external_id": "string"}, "string", "used by no row"),
        ([{"thing_pk": "a", "other": "$external_id"}], {"external_id": "string"}, "string", "does not have"),
        ([{"thing_pk": "a", "external_id": "x"}], {}, "long", "not a valid long"),
        ([{"thing_pk": "a", "external_id": "$external_id"}], {"external_id": "boolean"}, "boolean", "--set cannot"),
    ],
)
def test_parse_rejects(
    tmp_path: Path, rows: list[dict[str, Any]], parameters: dict[str, str], column_type: str, message: str
) -> None:
    path = _write(tmp_path, _document(rows, parameters, column_type))

    with pytest.raises(ValueError, match=message):
        parse_dimensional_fixture(path)


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({}, "needs a value for: external_id"),
        ({"external_id": "1", "extra": "2"}, "declares no parameter extra"),
        ({"external_id": "one"}, "is not a valid long"),
    ],
)
def test_resolve_rejects(tmp_path: Path, values: dict[str, str], message: str) -> None:
    path = _write(
        tmp_path, _document([{"thing_pk": "a", "external_id": "$external_id"}], {"external_id": "long"}, "long")
    )

    with pytest.raises(ValueError, match=message):
        resolve_parameters(parse_dimensional_fixture(path), values)


def test_the_b2b_fixture_covers_every_dimensional_table_the_b2b_views_read() -> None:
    sql = [path.read_text() for directory in B2B_MODEL_DIRS for path in directory.glob("*.sql")]
    read = {name for text in [*sql, B2B_MACROS.read_text()] for name in _DIMENSIONAL_SOURCE.findall(text)}

    fixture = parse_dimensional_fixture(B2B_FIXTURE)

    assert read
    assert {table.raw_table for table in fixture.tables} == read


def test_the_b2b_fixture_resolves() -> None:
    tables = resolve_parameters(parse_dimensional_fixture(B2B_FIXTURE), B2B_VALUES)

    contract = next(table for table in tables if table.raw_table == "dim_contract")
    organization = next(table for table in tables if table.raw_table == "dim_organization")
    assert contract.rows[0]["contract_id"] == 7
    assert organization.rows[0]["sso_organization_id"] == B2B_VALUES["sso_organization_id"]


def _duckdb_with(tables: list[TableFixture]) -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB holding ``tables`` under the local lake's three-part names."""
    db = duckdb.connect()
    db.execute("ATTACH ':memory:' AS ol_data_lake_local")
    db.execute(f"CREATE SCHEMA ol_data_lake_local.{LOCAL_DIMENSIONAL_SCHEMA}")
    for table in tables:
        name = f"ol_data_lake_local.{LOCAL_DIMENSIONAL_SCHEMA}.{table.raw_table}"
        columns = ", ".join(f"{column} {_DUCKDB_TYPES[kind]}" for column, kind in table.columns.items())
        db.execute(f"CREATE TABLE {name} ({columns})")
        db.executemany(
            f"INSERT INTO {name} VALUES ({', '.join('?' for _ in table.columns)})",  # noqa: S608
            [[row.get(column) for column in table.columns] for row in table.rows],
        )
    return db


def _rendered_b2b_views() -> dict[str, str]:
    """Return each b2b view's SQL as DuckDB runs it.

    Renders only what the views use: source(), config() and the b2b_engagement
    macros. Anything else is undefined and fails the render, so a view that
    starts using another macro shows up here rather than passing untested.
    """
    env = jinja2.Environment(undefined=jinja2.StrictUndefined, autoescape=False)  # noqa: S701
    env.globals["source"] = lambda _, table: f"ol_data_lake_local.{LOCAL_DIMENSIONAL_SCHEMA}.{table}"
    env.globals["config"] = lambda **_: ""
    macros = env.from_string(B2B_MACROS.read_text()).module
    env.globals.update({name: getattr(macros, name) for name in _B2B_MACRO_NAMES})
    views = {}
    for directory in B2B_MODEL_DIRS:
        for path in sorted(directory.glob("*.sql")):
            (sql,) = sqlglot.transpile(env.from_string(path.read_text()).render(), read="starrocks", write="duckdb")
            views[path.stem] = sql
    return views


def _primary_cohorts() -> dict[str, str]:
    """Return each aggregate view's primary cohort column, from its ``config.meta.cohort``."""
    primaries = {}
    for directory in B2B_MODEL_DIRS:
        for path in directory.glob("_*__models.yml"):
            for model in yaml.safe_load(path.read_text())["models"]:
                for column in model.get("columns", []):
                    cohort = column_meta(column, "cohort") or {}
                    if cohort.get("role") == "primary":
                        primaries[model["name"]] = column["name"]
    return primaries


def test_every_b2b_view_shows_rows_from_the_fixture() -> None:
    """Each view returns rows, and an aggregate view has a row the API's floor lets through.

    A view that suppresses to nothing would leave its dashboard section empty
    locally, which looks the same as a broken view.
    """
    db = _duckdb_with(resolve_parameters(parse_dimensional_fixture(B2B_FIXTURE), B2B_VALUES))
    primaries = _primary_cohorts()
    assert primaries

    for view, sql in _rendered_b2b_views().items():
        result = db.execute(sql)
        names = [description[0] for description in result.description]
        rows = [dict(zip(names, row, strict=True)) for row in result.fetchall()]
        assert rows, f"{view} returns no rows"
        if view in primaries:
            cohorts = [row[primaries[view]] for row in rows]
            assert min(cohorts) >= ANONYMIZATION_FLOOR, f"{view}: a {primaries[view]} in {cohorts} is suppressed"
        assert all(row["sso_organization_id"] == B2B_VALUES["sso_organization_id"] for row in rows), (
            f"{view} has rows for another organization"
        )


class _Cursor:
    def __init__(self, statements: list[tuple[str, Any]]) -> None:
        self.statements = statements

    def execute(self, sql: str) -> None:
        self.statements.append((sql, None))

    def executemany(self, sql: str, parameters: list[tuple[Any, ...]]) -> None:
        self.statements.append((sql, parameters))


class _Connection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self.statements)

    def close(self) -> None:
        pass


def test_load_dimensional_writes_the_dimensional_schema(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write(tmp_path, _document([{"thing_pk": "a", "external_id": "$external_id"}], {"external_id": "string"}))
    connection = _Connection()
    monkeypatch.setattr(fixtures_command, "connect_local_lake", lambda *_: connection)

    fixtures_command.load_dimensional("demo", set_=["external_id=x-1"], fixtures_dir=tmp_path)

    table = f"`ol_data_lake_local`.`{LOCAL_DIMENSIONAL_SCHEMA}`.`dim_thing`"
    assert [sql for sql, _ in connection.statements] == [
        f"CREATE DATABASE IF NOT EXISTS `ol_data_lake_local`.`{LOCAL_DIMENSIONAL_SCHEMA}`",
        f"DROP TABLE IF EXISTS {table} FORCE",
        f"CREATE TABLE {table} (`thing_pk` STRING, `external_id` STRING)",
        f"INSERT INTO {table} (`thing_pk`, `external_id`) VALUES (%s, %s)",  # noqa: S608
    ]
    assert connection.statements[-1][1] == [("a", "x-1")]


@pytest.mark.parametrize(
    "assignments",
    [
        [],
        # What `--set external_id=$UNSET_VARIABLE` reaches the command as.
        ["external_id="],
        ["external_id=a", "external_id=b"],
    ],
)
def test_load_dimensional_with_a_bad_set_connects_to_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, assignments: list[str]
) -> None:
    _write(tmp_path, _document([{"thing_pk": "a", "external_id": "$external_id"}], {"external_id": "string"}))

    def refuse(*_: Any) -> None:
        pytest.fail("connected before the parameters were checked")

    monkeypatch.setattr(fixtures_command, "connect_local_lake", refuse)

    with pytest.raises(SystemExit):
        fixtures_command.load_dimensional("demo", set_=assignments, fixtures_dir=tmp_path)
