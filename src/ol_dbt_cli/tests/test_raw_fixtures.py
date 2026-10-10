"""Tests for the local lake's raw fixtures (`ol-dbt fixtures`).

The loader's SQL is checked as text here and against a real StarRocks only by
hand, so these pin what the text depends on: the type map, the column order
shared by the DDL and the INSERT, and the checks that keep a fixture file from
drifting away from the inventory unit it belongs to.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.commands import fixtures as fixtures_command
from ol_dbt_cli.lib.inventory import DEFAULT_INVENTORY_DIR, load_units
from ol_dbt_cli.lib.raw_fixtures import (
    TableFixture,
    create_table_sql,
    fixture_type_from_glue,
    insert_sql,
    load_fixtures,
    merge_captured,
    row_parameters,
    starrocks_type,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
RAW_TABLE = "raw__demo__s3__things"


def _inventory(tmp_path: Path, fixture: dict[str, Any] | None, *, local: str = "fixture") -> Path:
    unit = {
        "schema_version": 1,
        "deployment": "demo",
        "layer": "s3",
        "scope": "singleton",
        "strategies": {"qa": "omit", "local": local},
        "loader": "dlt",
        "table_prefix": "raw__demo__s3__",
        "tables": [
            {"name": "things", "raw_table": RAW_TABLE, "sync_mode": "full_refresh_overwrite", "modeled": True},
            {"name": "unread", "raw_table": "raw__demo__s3__unread", "sync_mode": "append", "modeled": False},
        ],
    }
    (tmp_path / "units").mkdir()
    (tmp_path / "units" / "demo__s3.yml").write_text(yaml.safe_dump(unit))
    if fixture is not None:
        (tmp_path / "fixtures").mkdir()
        (tmp_path / "fixtures" / "demo__s3.yml").write_text(yaml.safe_dump(fixture))
    return tmp_path


def _fixture(columns: dict[str, str], rows: list[dict[str, Any]], table: str = RAW_TABLE) -> dict[str, Any]:
    return {"schema_version": 1, "tables": {table: {"columns": columns, "rows": rows}}}


def test_committed_fixtures_are_valid() -> None:
    inventory_dir = REPO_ROOT / DEFAULT_INVENTORY_DIR
    fixtures = load_fixtures(inventory_dir, load_units(inventory_dir))
    assert fixtures
    for fixture in fixtures:
        for table in fixture.tables:
            create_table_sql(table, "c", "s")
            assert all(len(row) == len(table.columns) for row in row_parameters(table))


@pytest.mark.parametrize(
    ("fixture_type", "expected"),
    [("long", "BIGINT"), ("timestamp", "DATETIME"), ("string", "STRING"), ("decimal(38,9)", "DECIMAL(38, 9)")],
)
def test_starrocks_type(fixture_type: str, expected: str) -> None:
    assert starrocks_type(fixture_type) == expected


def test_starrocks_type_rejects_what_the_format_lacks() -> None:
    with pytest.raises(ValueError, match="unsupported fixture type 'varchar'"):
        starrocks_type("varchar")


@pytest.mark.parametrize(
    ("glue_type", "expected"),
    [
        ("bigint", "long"),
        ("varchar(255)", "string"),
        ("TIMESTAMP", "timestamp"),
        ("decimal(38, 9)", "decimal(38,9)"),
        ("struct<a:int>", None),
        ("array<string>", None),
        ("binary", None),
    ],
)
def test_fixture_type_from_glue(glue_type: str, expected: str | None) -> None:
    assert fixture_type_from_glue(glue_type) == expected


def test_ddl_and_insert_share_the_column_order() -> None:
    table = TableFixture(
        raw_table=RAW_TABLE,
        columns={"id": "long", "data_json": "string", "seen_at": "timestamp"},
        rows=[{"seen_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC), "id": 1, "data_json": {"a": [1]}}, {}],
    )

    assert create_table_sql(table, "cat", "raw") == (
        f"CREATE TABLE `cat`.`raw`.`{RAW_TABLE}` (`id` BIGINT, `data_json` STRING, `seen_at` DATETIME)"
    )
    assert insert_sql(table, "cat", "raw") == (
        f"INSERT INTO `cat`.`raw`.`{RAW_TABLE}` (`id`, `data_json`, `seen_at`) VALUES (%s, %s, %s)"  # noqa: S608
    )
    first, second = row_parameters(table)
    assert first == (1, json.dumps({"a": [1]}), datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC))
    assert second == (None, None, None)


def test_load_fixtures_reads_a_valid_file(tmp_path: Path) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long"}, [{"id": 1}]))

    (fixture,) = load_fixtures(inventory_dir, load_units(inventory_dir))

    assert [(table.raw_table, table.columns, table.rows) for table in fixture.tables] == [
        (RAW_TABLE, {"id": "long"}, [{"id": 1}])
    ]


@pytest.mark.parametrize(
    ("fixture", "local", "message"),
    [
        (_fixture({"id": "long"}, [{"id": 1, "nmae": "x"}]), "fixture", "row 1 sets nmae"),
        (_fixture({"id": "bigint"}, []), "fixture", "things.id: unsupported fixture type 'bigint'"),
        (_fixture({"id": "long"}, [], table="raw__other__s3__things"), "fixture", "is not a table of unit demo/s3"),
        (_fixture({}, []), "fixture", "has no columns"),
        (_fixture({"id": "long"}, []), "ingest", "strategies.local: ingest, not fixture"),
        ({"schema_version": 2, "tables": {}}, "fixture", "schema_version 2"),
    ],
)
def test_load_fixtures_rejects(tmp_path: Path, fixture: dict[str, Any], local: str, message: str) -> None:
    inventory_dir = _inventory(tmp_path, fixture, local=local)

    with pytest.raises(ValueError, match=message):
        load_fixtures(inventory_dir, load_units(inventory_dir))


def test_selected_unit_without_a_fixture_is_an_error(tmp_path: Path) -> None:
    inventory_dir = _inventory(tmp_path, None)
    units = load_units(inventory_dir)

    assert load_fixtures(inventory_dir, units) == []
    with pytest.raises(ValueError, match="no fixture file"):
        load_fixtures(inventory_dir, units, selected=True)


def test_fixture_without_a_unit_is_an_error(tmp_path: Path) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long"}, []))
    (inventory_dir / "fixtures" / "gone__s3.yml").write_text("schema_version: 1\ntables: {}\n")

    with pytest.raises(ValueError, match="gone__s3.yml"):
        load_fixtures(inventory_dir, load_units(inventory_dir))


def test_merge_captured_keeps_rows_and_other_tables() -> None:
    existing = {
        "schema_version": 1,
        "tables": {
            RAW_TABLE: {"columns": {"id": "long"}, "rows": [{"id": 1}]},
            "raw__demo__s3__kept": {"columns": {"k": "string"}, "rows": []},
        },
    }

    merged = merge_captured(existing, {RAW_TABLE: {"id": "long", "name": "string"}, "raw__demo__s3__new": {"n": "int"}})

    assert merged["tables"] == {
        RAW_TABLE: {"columns": {"id": "long", "name": "string"}, "rows": [{"id": 1}]},
        "raw__demo__s3__kept": {"columns": {"k": "string"}, "rows": []},
        "raw__demo__s3__new": {"columns": {"n": "int"}, "rows": []},
    }


def test_capture_writes_modeled_tables_and_leaves_out_nested_columns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long"}, [{"id": 1}]))
    requested: dict[str, Any] = {}

    def landed(database: str, *, prefixes: list[str], region: str) -> dict[str, dict[str, str]]:
        requested.update(database=database, prefixes=prefixes, region=region)
        return {
            RAW_TABLE: {"id": "bigint", "tags": "array<string>", "seen_at": "timestamp"},
            "raw__demo__s3__unread": {"id": "bigint"},
        }

    monkeypatch.setattr(fixtures_command, "column_types_by_table", landed)

    fixtures_command.capture(unit=["demo/s3"], inventory_dir=inventory_dir)

    assert requested["prefixes"] == ["raw__demo__s3__"]
    written = yaml.safe_load((inventory_dir / "fixtures" / "demo__s3.yml").read_text())
    assert written == _fixture({"id": "long", "seen_at": "timestamp"}, [{"id": 1}])
    (fixture,) = load_fixtures(inventory_dir, load_units(inventory_dir))
    assert fixture.tables[0].rows == [{"id": 1}]


def test_capture_rejects_a_unit_that_is_not_a_fixture_unit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    inventory_dir = _inventory(tmp_path, None, local="ingest")
    monkeypatch.setattr(fixtures_command, "column_types_by_table", lambda *_, **__: {RAW_TABLE: {"id": "bigint"}})

    with pytest.raises(SystemExit):
        fixtures_command.capture(unit=["demo/s3"], inventory_dir=inventory_dir)
    assert not (inventory_dir / "fixtures").exists()


def test_capture_rejects_a_table_the_units_do_not_declare(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    inventory_dir = _inventory(tmp_path, None)
    monkeypatch.setattr(fixtures_command, "column_types_by_table", lambda *_, **__: {})

    with pytest.raises(SystemExit):
        fixtures_command.capture(unit=["demo/s3"], inventory_dir=inventory_dir, table=["raw__demo__s3__nope"])
