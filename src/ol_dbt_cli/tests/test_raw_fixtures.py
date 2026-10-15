"""Tests for the local lake's raw fixtures (`ol-dbt fixtures`).

The loader's SQL is checked as text here and against a real StarRocks only by
hand, so these pin what the text depends on: the type map, the column order
shared by the DDL and the INSERT, and the checks that keep a fixture file from
drifting away from the inventory unit it belongs to.
"""

from __future__ import annotations

import datetime
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
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
    unit_factories,
    validate_generated,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
RAW_TABLE = "raw__demo__s3__things"
FACTORY_PACKAGES = Path(__file__).parent / "fixtures"


def _inventory(
    tmp_path: Path,
    fixture: dict[str, Any] | None,
    *,
    local: str = "fixture",
    primary_key: tuple[str, ...] = ("id",),
) -> Path:
    things: dict[str, Any] = {"name": "things", "raw_table": RAW_TABLE, "sync_mode": "full_refresh_overwrite"}
    if primary_key:
        things["primary_key"] = [[column] for column in primary_key]
    unit = {
        "schema_version": 1,
        "deployment": "demo",
        "layer": "s3",
        "scope": "singleton",
        "strategies": {"qa": "omit", "local": local},
        "loader": "dlt",
        "table_prefix": "raw__demo__s3__",
        "tables": [
            {**things, "modeled": True},
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


def test_committed_hand_written_keys_are_what_their_columns_hold() -> None:
    inventory_dir = REPO_ROOT / DEFAULT_INVENTORY_DIR
    units = {unit.path.name: unit for unit in load_units(inventory_dir)}
    for fixture in load_fixtures(inventory_dir, units.values()):
        validate_generated(fixture, units[fixture.path.name], {table.raw_table: [] for table in fixture.tables})


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
        (_fixture({"id": "long"}, []), "omit", "strategies.local: omit, which takes no fixture"),
        ({"schema_version": 2, "tables": {}}, "fixture", "schema_version 2"),
        (
            _fixture({"id": "long", "n": "long"}, [{"id": 1}, {"id": 2, "n": "one"}]),
            "fixture",
            "things row 2 has 'one' in column n, which is not a valid long",
        ),
        (
            _fixture({"id": "long", "n": "int"}, [{"id": 1, "n": 2**31}]),
            "fixture",
            "row 1 has 2147483648 in column n, which is not a valid int",
        ),
        (
            _fixture({"id": "long", "n": "decimal(5,2)"}, [{"id": 1, "n": 1.234}]),
            "fixture",
            r"row 1 has 1.234 in column n, which is not a valid decimal\(5,2\)",
        ),
        (
            _fixture({"id": "long", "at": "timestamp"}, [{"id": 1, "at": "yesterday"}]),
            "fixture",
            "row 1 has 'yesterday' in column at, which is not a valid timestamp",
        ),
        (
            _fixture({"id": "long"}, [{"id": 1}, {"id": 2}, {"id": "1"}]),
            "fixture",
            "things row 3 repeats the primary key of row 1, id='1'",
        ),
    ],
)
def test_load_fixtures_rejects(tmp_path: Path, fixture: dict[str, Any], local: str, message: str) -> None:
    inventory_dir = _inventory(tmp_path, fixture, local=local)

    with pytest.raises(ValueError, match=message):
        load_fixtures(inventory_dir, load_units(inventory_dir))


@pytest.mark.parametrize(
    ("rows", "table"),
    [
        # A row that leaves its key out is NULL there, as in any other column.
        ([{"name": "a"}, {"name": "a"}, {"id": None, "name": "a"}], {}),
        ([{"id": 1}, {"id": 1}], {"primary_key": []}),
        ([{"id": 1, "name": "a"}, {"id": 1, "name": "b"}], {"primary_key": [["id"], ["name"]]}),
        ([{"id": 1}, {"id": 1, "name": "a"}], {"primary_key": [["id"], ["name"]]}),
        # An append table holds every version of a row.
        ([{"id": 1, "name": "a"}, {"id": 1, "name": "b"}], {"sync_mode": "incremental_append"}),
        # Keys a fixture row cannot hold, which `validate_generated` reports.
        ([{"id": 1}, {"id": 1}], {"primary_key": [["id", "nested"]]}),
        ([{"id": 1}, {"id": 1}], {"primary_key": [["id"], ["undeclared"]]}),
    ],
)
def test_hand_written_rows_that_are_not_compared_by_key(
    tmp_path: Path, rows: list[dict[str, Any]], table: dict[str, Any]
) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long", "name": "string"}, rows))
    (unit,) = load_units(inventory_dir)
    unit.tables[0].update(table)

    (loaded,) = load_fixtures(inventory_dir, [unit])

    assert loaded.tables[0].rows == rows


def test_a_hand_written_compound_key_is_compared_whole(tmp_path: Path) -> None:
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "a"}, {"id": 1, "name": "a"}]
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long", "name": "string"}, rows), primary_key=("id", "name"))

    with pytest.raises(ValueError, match="row 3 repeats the primary key of row 1, id=1, name='a'"):
        load_fixtures(inventory_dir, load_units(inventory_dir))


def test_a_unit_without_a_factory_package_has_none(tmp_path: Path) -> None:
    (unit,) = load_units(_inventory(tmp_path, None))

    assert unit_factories(unit) is None


def test_a_factory_package_that_fails_to_import_is_an_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (unit,) = load_units(_inventory(tmp_path, None))
    package = tmp_path / "broken_factories" / "demo__s3"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("import no_such_dependency\n")
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(ModuleNotFoundError, match="no_such_dependency"):
        unit_factories(unit, package="broken_factories")


def test_a_factory_package_without_the_contract_is_an_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A directory with only `_generated.py` imports as a namespace package."""
    (unit,) = load_units(_inventory(tmp_path, None))
    package = tmp_path / "scaffolded_factories" / "demo__s3"
    package.mkdir(parents=True)
    (package / "_generated.py").write_text("")
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(ValueError, match="scaffolded_factories.demo__s3 has no SEED or generate"):
        unit_factories(unit, package="scaffolded_factories")


def test_generated_rows_pass_validation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.syspath_prepend(str(FACTORY_PACKAGES))
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long", "name": "string"}, [{"id": 1, "name": "by hand"}]))
    (unit,) = load_units(inventory_dir)
    (fixture,) = load_fixtures(inventory_dir, [unit])

    factories = unit_factories(unit, package="fixture_factories")
    assert factories is not None
    generated = factories.generate({table.raw_table: table.rows for table in fixture.tables}, scale=2)

    validate_generated(fixture, unit, generated)
    assert factories.SEED == 20261013  # noqa: PLR2004
    assert generated == {
        RAW_TABLE: [
            {"id": 2},
            {"id": 3, "name": "thing 3"},
            {"id": 4},
            {"id": 5, "name": "thing 5"},
            {"id": 6},
            {"id": 7, "name": "thing 7"},
        ]
    }


@pytest.mark.parametrize(
    ("generated", "message"),
    [
        ({"raw__demo__s3__unread": [{"id": 1}]}, "demo/s3 factories: raw__demo__s3__unread is not a table of"),
        ({RAW_TABLE: [{"id": 2}, {"id": 3, "nmae": "x"}]}, "things generated row 2 sets nmae"),
        ({RAW_TABLE: [{"name": "x"}]}, "things generated row 1 has no value for primary key column id"),
        ({RAW_TABLE: [{"id": None}]}, "things generated row 1 has no value for primary key column id"),
        ({RAW_TABLE: [{"id": 1}]}, "things generated row 1 repeats primary key id=1"),
        ({RAW_TABLE: [{"id": 2}, {"id": 2}]}, "things generated row 2 repeats primary key id=2"),
        ({RAW_TABLE: [{"id": "1"}]}, "things generated row 1 repeats primary key id='1'"),
        (
            {RAW_TABLE: [{"id": "one"}]},
            "things generated row 1 has 'one' in primary key column id, which is not a valid long",
        ),
    ],
)
def test_validate_generated_rejects(tmp_path: Path, generated: dict[str, list[dict[str, Any]]], message: str) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long", "name": "string"}, [{"id": 1}]))
    (unit,) = load_units(inventory_dir)
    (fixture,) = load_fixtures(inventory_dir, [unit])

    with pytest.raises(ValueError, match=message):
        validate_generated(fixture, unit, generated)


def test_validate_generated_compares_every_column_of_a_compound_key(tmp_path: Path) -> None:
    fixture_file = _fixture({"id": "long", "name": "string"}, [{"id": 1, "name": "a"}])
    inventory_dir = _inventory(tmp_path, fixture_file, primary_key=("id", "name"))
    (unit,) = load_units(inventory_dir)
    (fixture,) = load_fixtures(inventory_dir, [unit])

    validate_generated(fixture, unit, {RAW_TABLE: [{"id": 1, "name": "b"}, {"id": 2, "name": "a"}]})
    with pytest.raises(ValueError, match="generated row 2 repeats primary key id=1, name='a'"):
        validate_generated(fixture, unit, {RAW_TABLE: [{"id": 2, "name": "a"}, {"id": 1, "name": "a"}]})
    with pytest.raises(ValueError, match="generated row 1 has no value for primary key column name"):
        validate_generated(fixture, unit, {RAW_TABLE: [{"id": 2}]})


@pytest.mark.parametrize(
    ("fixture_type", "hand_written", "generated"),
    [
        ("string", "7", 7),
        ("double", 1, "1.0"),
        ("decimal(10,2)", "1.50", 1.5),
        ("date", datetime.date(2026, 1, 1), "2026-01-01"),
        ("timestamp", datetime.datetime(2026, 1, 1, 12), "2026-01-01 12:00:00"),  # noqa: DTZ001
        ("timestamp", datetime.datetime(2026, 1, 1, 12), datetime.datetime(2026, 1, 1, 12, tzinfo=datetime.UTC)),  # noqa: DTZ001
        ("timestamp", datetime.datetime(2026, 1, 1), datetime.date(2026, 1, 1)),  # noqa: DTZ001
        ("date", datetime.date(2026, 1, 1), datetime.datetime(2026, 1, 1, 12, tzinfo=datetime.UTC)),
        ("float", 0.1, 0.10000000001),
        ("boolean", True, 1),
    ],
)
def test_validate_generated_compares_keys_as_the_column_holds_them(
    tmp_path: Path, fixture_type: str, hand_written: Any, generated: Any
) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": fixture_type}, [{"id": hand_written}]))
    (unit,) = load_units(inventory_dir)
    (fixture,) = load_fixtures(inventory_dir, [unit])

    with pytest.raises(ValueError, match="generated row 1 repeats primary key"):
        validate_generated(fixture, unit, {RAW_TABLE: [{"id": generated}]})


@pytest.mark.parametrize(
    ("fixture_type", "value"),
    [
        ("int", 2**31),
        ("long", -(2**63) - 1),
        ("long", 1.5),
        ("long", Decimal("2.7")),
        ("long", "2.7"),
        ("float", 1e39),
        ("decimal(5,2)", "1.234"),
        ("decimal(5,2)", 1000),
        ("decimal(5,2)", "NaN"),
        ("decimal(38,9)", "1" + "0" * 29),
        ("decimal(3,3)", 1),
        ("boolean", "true"),
        ("boolean", 2),
        ("date", "yesterday"),
        ("date", 20260101),
        ("date", [2026, 1, 1]),
        ("timestamp", 1767225600),
    ],
)
def test_validate_generated_rejects_a_key_its_column_cannot_hold(tmp_path: Path, fixture_type: str, value: Any) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": fixture_type}, []))
    (unit,) = load_units(inventory_dir)
    (fixture,) = load_fixtures(inventory_dir, [unit])

    with pytest.raises(ValueError, match="generated row 1 has .* in primary key column id, which is not a valid"):
        validate_generated(fixture, unit, {RAW_TABLE: [{"id": value}]})


@pytest.mark.parametrize(
    ("fixture_type", "value"),
    [
        ("int", 2**31 - 1),
        ("long", -(2**63)),
        ("long", Decimal("3.0")),
        ("long", 4.0),
        ("decimal(5,2)", "999.99"),
        ("decimal(5,2)", -0.5),
        ("decimal(5,2)", "1.500"),
        ("decimal(38,9)", "9" * 29 + ".123456789"),
        ("decimal(3,3)", 0),
        ("boolean", False),
    ],
)
def test_validate_generated_accepts_a_key_at_the_edge_of_its_column(
    tmp_path: Path, fixture_type: str, value: Any
) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": fixture_type}, []))
    (unit,) = load_units(inventory_dir)
    (fixture,) = load_fixtures(inventory_dir, [unit])

    validate_generated(fixture, unit, {RAW_TABLE: [{"id": value}]})


def test_validate_generated_rejects_a_nested_primary_key_path(tmp_path: Path) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long"}, []))
    (unit,) = load_units(inventory_dir)
    (fixture,) = load_fixtures(inventory_dir, [unit])
    unit.tables[0]["primary_key"] = [["data", "id"]]

    with pytest.raises(ValueError, match=r"things has nested primary key path data\.id"):
        validate_generated(fixture, unit, {RAW_TABLE: [{"id": 1}]})


def test_validate_generated_without_a_primary_key_checks_tables_and_columns_only(tmp_path: Path) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long"}, [{"id": 1}]), primary_key=())
    (unit,) = load_units(inventory_dir)
    (fixture,) = load_fixtures(inventory_dir, [unit])

    validate_generated(fixture, unit, {RAW_TABLE: [{"id": 1}, {}]})
    with pytest.raises(ValueError, match="generated row 1 sets nmae"):
        validate_generated(fixture, unit, {RAW_TABLE: [{"nmae": "x"}]})


def test_validate_generated_rejects_a_key_column_the_fixture_does_not_declare(tmp_path: Path) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"name": "string"}, []))
    (unit,) = load_units(inventory_dir)
    (fixture,) = load_fixtures(inventory_dir, [unit])

    with pytest.raises(ValueError, match="things has primary key column id, which .* does not declare"):
        validate_generated(fixture, unit, {RAW_TABLE: [{"name": "x"}]})


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


def test_an_ingest_unit_keeps_its_fixture(tmp_path: Path) -> None:
    """The fixture fills the unit's tables when the app its source reads is not running."""
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long"}, [{"id": 1}]), local="ingest")

    (fixture,) = load_fixtures(inventory_dir, load_units(inventory_dir))

    assert fixture.tables[0].rows == [{"id": 1}]


def test_capture_rejects_a_unit_that_takes_no_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    inventory_dir = _inventory(tmp_path, None, local="omit")
    monkeypatch.setattr(fixtures_command, "column_types_by_table", lambda *_, **__: {RAW_TABLE: {"id": "bigint"}})

    with pytest.raises(SystemExit):
        fixtures_command.capture(unit=["demo/s3"], inventory_dir=inventory_dir)
    assert not (inventory_dir / "fixtures").exists()


def test_capture_rejects_a_table_the_units_do_not_declare(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    inventory_dir = _inventory(tmp_path, None)
    monkeypatch.setattr(fixtures_command, "column_types_by_table", lambda *_, **__: {})

    with pytest.raises(SystemExit):
        fixtures_command.capture(unit=["demo/s3"], inventory_dir=inventory_dir, table=["raw__demo__s3__nope"])


def test_capture_keeps_comments_and_writes_nothing_when_glue_has_no_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inventory_dir = _inventory(tmp_path, None)
    path = inventory_dir / "fixtures" / "demo__s3.yml"
    monkeypatch.setattr(fixtures_command, "column_types_by_table", lambda *_, **__: {})

    fixtures_command.capture(unit=["demo/s3"], inventory_dir=inventory_dir)
    assert not path.exists()

    path.parent.mkdir()
    path.write_text(
        "schema_version: 1\ntables:\n"
        f"  {RAW_TABLE}:\n    columns:\n      id: long\n    rows:\n"
        "    # the row the model must drop\n    - id: 1\n"
    )
    monkeypatch.setattr(
        fixtures_command, "column_types_by_table", lambda *_, **__: {RAW_TABLE: {"id": "bigint", "name": "string"}}
    )

    fixtures_command.capture(unit=["demo/s3"], inventory_dir=inventory_dir)

    assert "# the row the model must drop" in path.read_text()
    (fixture,) = load_fixtures(inventory_dir, load_units(inventory_dir))
    assert fixture.tables[0].columns == {"id": "long", "name": "string"}
    assert fixture.tables[0].rows == [{"id": 1}]


class _Recorder:
    """Stands in for the StarRocks connection and its cursor."""

    def __init__(self) -> None:
        self.connect_kwargs: dict[str, Any] = {}
        self.statements: list[tuple[str, Any]] = []

    def connect(self, **kwargs: Any) -> _Recorder:
        self.connect_kwargs = kwargs
        return self

    def cursor(self) -> _Recorder:
        return self

    def execute(self, statement: str) -> None:
        self.statements.append((statement, None))

    def executemany(self, statement: str, parameters: list[tuple[Any, ...]]) -> None:
        self.statements.append((statement, parameters))

    def close(self) -> None:
        pass


def test_load_recreates_each_table_on_the_local_catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import mysql.connector  # noqa: PLC0415

    fixture = _fixture({"id": "long"}, [{"id": 1}])
    fixture["tables"]["raw__demo__s3__unread"] = {"columns": {"id": "long"}, "rows": []}
    inventory_dir = _inventory(tmp_path, fixture)
    recorder = _Recorder()
    monkeypatch.setattr(mysql.connector, "connect", recorder.connect)
    # A shell set up for the QA profile must not move the root login off the local lake.
    monkeypatch.setenv("DBT_STARROCKS_HOST", "lakehouse.qa.example")

    fixtures_command.load(inventory_dir=inventory_dir)

    assert recorder.connect_kwargs == {
        "host": "127.0.0.1",
        "port": 9030,
        "user": "root",
        "password": "",
        "autocommit": True,
    }
    raw = "`ol_data_lake_local`.`ol_warehouse_local_raw`"
    assert recorder.statements == [
        (f"CREATE DATABASE IF NOT EXISTS {raw}", None),
        (f"DROP TABLE IF EXISTS {raw}.`{RAW_TABLE}` FORCE", None),
        (f"CREATE TABLE {raw}.`{RAW_TABLE}` (`id` BIGINT)", None),
        (f"INSERT INTO {raw}.`{RAW_TABLE}` (`id`) VALUES (%s)", [(1,)]),  # noqa: S608
        (f"DROP TABLE IF EXISTS {raw}.`raw__demo__s3__unread` FORCE", None),
        (f"CREATE TABLE {raw}.`raw__demo__s3__unread` (`id` BIGINT)", None),
    ]


def test_load_without_fixtures_exits(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        fixtures_command.load(inventory_dir=_inventory(tmp_path, None))


@pytest.fixture
def demo_factories(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give the `demo/s3` unit the test factory package, which continues its ids."""
    monkeypatch.syspath_prepend(str(FACTORY_PACKAGES))
    monkeypatch.setattr(
        fixtures_command, "unit_factories", lambda unit: unit_factories(unit, package="fixture_factories")
    )


def _recorded_load(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **options: Any) -> _Recorder:
    import mysql.connector  # noqa: PLC0415

    inventory_dir = _inventory(tmp_path, _fixture({"id": "long", "name": "string"}, [{"id": 1, "name": "by hand"}]))
    recorder = _Recorder()
    monkeypatch.setattr(mysql.connector, "connect", recorder.connect)
    fixtures_command.load(inventory_dir=inventory_dir, **options)
    return recorder


def _inserted(recorder: _Recorder) -> list[tuple[Any, ...]]:
    (parameters,) = [parameters for statement, parameters in recorder.statements if statement.startswith("INSERT")]
    return parameters


@pytest.mark.usefixtures("demo_factories")
def test_load_inserts_the_hand_written_rows_then_the_generated_ones(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    recorder = _recorded_load(tmp_path, monkeypatch)

    assert _inserted(recorder) == [(1, "by hand"), (2, None), (3, "thing 3"), (4, None)]
    assert f"ol_warehouse_local_raw.{RAW_TABLE}: 1 hand-written + 3 generated row(s)" in capsys.readouterr().out


@pytest.mark.usefixtures("demo_factories")
def test_load_scales_the_generated_rows_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = _recorded_load(tmp_path, monkeypatch, scale=2)

    assert [row[0] for row in _inserted(recorder)] == [1, 2, 3, 4, 5, 6, 7]


@pytest.mark.usefixtures("demo_factories")
def test_load_with_no_generate_inserts_the_file_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    recorder = _recorded_load(tmp_path, monkeypatch, generate=False)

    assert _inserted(recorder) == [(1, "by hand")]
    assert f"ol_warehouse_local_raw.{RAW_TABLE}: 1 row(s)" in capsys.readouterr().out


def test_load_runs_every_factory_package_before_it_drops_a_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(hand_written: Any, scale: float) -> dict[str, list[dict[str, Any]]]:
        return {RAW_TABLE: [{"id": 1}]}

    monkeypatch.setattr(fixtures_command, "unit_factories", lambda unit: SimpleNamespace(SEED=1, generate=broken))

    with pytest.raises(ValueError, match="generated row 1 repeats primary key id=1"):
        _recorded_load(tmp_path, monkeypatch)


@pytest.mark.usefixtures("demo_factories")
def test_generate_writes_what_load_would_insert_without_connecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import mysql.connector  # noqa: PLC0415

    fixture = _fixture({"id": "long", "name": "string", "seen": "date"}, [{"id": 1, "seen": datetime.date(2026, 1, 2)}])
    inventory_dir = _inventory(tmp_path, fixture)

    def no_connection(**_: Any) -> None:
        raise AssertionError

    monkeypatch.setattr(mysql.connector, "connect", no_connection)

    fixtures_command.generate_command(unit=["demo/s3"], inventory_dir=inventory_dir)

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines == [
        {"raw_table": RAW_TABLE, "row": {"id": 1, "seen": "2026-01-02"}},
        {"raw_table": RAW_TABLE, "row": {"id": 2}},
        {"raw_table": RAW_TABLE, "row": {"id": 3, "name": "thing 3"}},
        {"raw_table": RAW_TABLE, "row": {"id": 4}},
    ]


@pytest.mark.usefixtures("demo_factories")
def test_generate_writes_a_file_per_table(tmp_path: Path) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long", "name": "string"}, []))
    out = tmp_path / "rows"

    fixtures_command.generate_command(unit=["demo/s3"], inventory_dir=inventory_dir, table=[RAW_TABLE], out=out)

    assert [json.loads(line) for line in (out / f"{RAW_TABLE}.jsonl").read_text().splitlines()] == [
        {"id": 1, "name": "thing 1"},
        {"id": 2},
        {"id": 3, "name": "thing 3"},
    ]


def test_generate_refuses_a_table_the_units_do_not_have(tmp_path: Path) -> None:
    inventory_dir = _inventory(tmp_path, _fixture({"id": "long"}, []))

    with pytest.raises(SystemExit):
        fixtures_command.generate_command(unit=["demo/s3"], inventory_dir=inventory_dir, table=["raw__demo__s3__nope"])
