"""Tests for the keys-only keyset reads."""

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn

import dlt
import pytest
import yaml
from sqlalchemy.engine import URL

from ol_dlt import config, database, keyset
from ol_dlt.database import DatabaseSourceSpec
from ol_dlt.keyset import KeysetTable
from ol_dlt.keyset.specs import KEYSET_SPECS

# src/ol_dlt/tests/ -> repo root.
INVENTORY_UNITS = Path(__file__).parents[3] / "ingestion" / "inventory" / "units"

_SPEC = DatabaseSourceSpec(
    name="example",
    raw_table_prefix="raw__example__app__postgres__",
    database="example",
    vault_mount="postgres-example",
    db_schema=None,  # SQLite has no named schema
)
_TABLES = (
    KeysetTable(name="widget", key="id"),
    KeysetTable(name="page", key="page_ptr_id"),
)


def _inventory_raw_tables(unit: str) -> dict[str, str]:
    path = INVENTORY_UNITS / f"{unit.replace('/', '__')}.yml"
    data = yaml.safe_load(path.read_text())
    return {table["name"]: table["raw_table"] for table in data["tables"]}


def test_every_rendered_unit_has_a_spec() -> None:
    assert set(keyset.inventory_keyset_tables()) == set(KEYSET_SPECS)


@pytest.mark.parametrize("unit", sorted(KEYSET_SPECS))
def test_keyset_names_follow_the_inventory_raw_tables(unit: str) -> None:
    """Each keyset is named for the raw table the inventory declares, plus the suffix.

    The staging filter and the history snapshot find a table's keyset by that
    name, so a spec prefix that drifts from the unit's would leave every one of
    its tables unfiltered.
    """
    spec = KEYSET_SPECS[unit]
    raw_tables = _inventory_raw_tables(unit)
    for table in keyset.inventory_keyset_tables()[unit]:
        assert keyset.keyset_table_name(spec, table) == (
            f"{raw_tables[table.name]}__keyset"
        )


@pytest.mark.parametrize("unit", sorted(KEYSET_SPECS))
def test_keyset_pipeline_is_apart_from_the_loader(unit: str) -> None:
    pipeline = keyset.unit_keyset_pipeline(unit)
    assert pipeline.pipeline_name == f"{KEYSET_SPECS[unit].name}_keyset"
    assert (
        pipeline.pipeline_name
        != database.pipeline_for(KEYSET_SPECS[unit]).pipeline_name
    )


def test_keyset_resources_replace_and_carry_the_clock_column() -> None:
    source = keyset.build_keyset_source(_SPEC, _TABLES, profile="dev")
    assert set(source.resources) == {
        "raw__example__app__postgres__widget__keyset",
        "raw__example__app__postgres__page__keyset",
    }
    for resource in source.resources.values():
        schema = resource.compute_table_schema()
        assert schema["write_disposition"] == "replace"
        assert keyset.READ_STARTED_AT_COLUMN in schema["columns"]


def test_an_empty_table_list_is_refused() -> None:
    with pytest.raises(ValueError, match="no keyset tables"):
        keyset.build_keyset_source(_SPEC, (), profile="dev")


def test_deployed_profile_never_reads_vault_at_definition_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EXAMPLE_DB_HOST", "example.rds.amazonaws.com")

    def _fail(*_args: object, **_kwargs: object) -> NoReturn:
        msg = "Vault was read while the source was being defined"
        raise AssertionError(msg)

    monkeypatch.setattr(database.vault, "read_database_credentials", _fail)
    keyset.build_keyset_source(_SPEC, _TABLES, profile="qa")


def test_ocw_studio_mount_is_named_per_environment() -> None:
    spec = KEYSET_SPECS["ocw/app_postgres"]
    assert spec.vault_mount_for("qa") == "postgres-ocw-studio-applications-qa"
    assert (
        spec.vault_mount_for("production")
        == "postgres-ocw-studio-applications-production"
    )


@pytest.mark.parametrize("unit", ["mitx/mysql", "xpro/mysql", "mitxonline/mysql"])
def test_edxapp_specs_connect_over_pymysql_with_tls(
    unit: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = KEYSET_SPECS[unit]
    monkeypatch.setenv(f"{spec.env_prefix}_HOST", "edxapp-db.rds.amazonaws.com")
    url = database._connection_url(spec, "qa")  # noqa: SLF001
    assert (url.drivername, url.port, url.database) == (
        "mysql+pymysql",
        3306,
        "edxapp",
    )
    assert dict(url.query) == {"ssl_check_hostname": "false"}
    assert spec.vault_mount_for("qa") == f"mariadb-{unit.split('/')[0]}"


@pytest.mark.parametrize(
    ("table", "schema"),
    [
        ("coursewarehistoryextended_studentmodulehistoryextended", "edxapp_csmh"),
        # The connection's own database.
        ("auth_user", None),
    ],
)
def test_a_keyset_reads_the_table_s_own_schema(
    table: str, schema: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """edxapp's csmh table lives in edxapp_csmh, beside the spec's edxapp."""
    selected: dict[str, object] = {}

    def _sql_table(**kwargs: object) -> object:
        selected.update(kwargs)
        return dlt.resource([], name=str(kwargs["table"]))

    monkeypatch.setattr(keyset, "sql_table", _sql_table)
    keyset.build_keyset_resource(
        KEYSET_SPECS["mitx/mysql"], KeysetTable(name=table, key="id"), profile="dev"
    )
    assert (selected["schema"], selected["table"]) == (schema, table)


def _sqlite_source(monkeypatch: pytest.MonkeyPatch, db_path: Path) -> None:
    monkeypatch.setattr(
        database,
        "_connection_url",
        lambda *_a, **_k: URL.create("sqlite", database=str(db_path)),
    )


@pytest.mark.integration
def test_materializes_keys_only_with_one_clock_per_load(
    test_profile: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A keyset holds the key and the load's clock, and a re-read drops deleted keys."""
    db_path = tmp_path / "example.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT, secret TEXT)"
        )
        connection.execute("CREATE TABLE page (page_ptr_id INTEGER PRIMARY KEY)")
        connection.executemany(
            "INSERT INTO widget VALUES (?, ?, ?)",
            [(1, "first", "a"), (2, "second", "b"), (3, "third", "c")],
        )
    _sqlite_source(monkeypatch, db_path)
    pipeline = keyset.keyset_pipeline_for(_SPEC, profile="test")
    widget = "raw__example__app__postgres__widget__keyset"
    page = "raw__example__app__postgres__page__keyset"

    before = datetime.now(UTC).replace(microsecond=0)
    info = pipeline.run(keyset.build_keyset_source(_SPEC, _TABLES, profile="test"))
    assert not info.has_failed_jobs

    columns = set(pipeline.default_schema.get_table_columns(widget))
    assert columns == {"id", keyset.READ_STARTED_AT_COLUMN, "_dlt_load_id"}
    rows = pipeline.dataset()[widget].arrow().sort_by("id").to_pylist()
    assert [row["id"] for row in rows] == [1, 2, 3]
    # An empty source table still lands, with its columns.
    assert set(pipeline.dataset()[page].arrow().column_names) >= {
        "page_ptr_id",
        keyset.READ_STARTED_AT_COLUMN,
    }
    stamps = {row[keyset.READ_STARTED_AT_COLUMN] for row in rows}
    assert len(stamps) == 1
    first_stamp = stamps.pop()
    assert first_stamp >= before

    with sqlite3.connect(db_path) as connection:
        connection.execute("DELETE FROM widget WHERE id = 2")
        connection.execute("INSERT INTO page VALUES (7)")
    info = pipeline.run(keyset.build_keyset_source(_SPEC, _TABLES, profile="test"))
    assert not info.has_failed_jobs
    rows = pipeline.dataset()[widget].arrow().sort_by("id").to_pylist()
    assert [row["id"] for row in rows] == [1, 3]
    page_rows = pipeline.dataset()[page].arrow().to_pylist()
    assert [row["page_ptr_id"] for row in page_rows] == [7]
    second_stamps = {row[keyset.READ_STARTED_AT_COLUMN] for row in rows + page_rows}
    assert len(second_stamps) == 1
    assert second_stamps.pop() >= first_stamp


@pytest.mark.integration
def test_the_clock_is_read_before_any_table_is_selected(
    test_profile: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The stamp must precede every table's read, or a consumer writes false deletes.

    A row created after a table's select is missing from its keyset; the
    consumer keeps it only because its load time is later than the stamp. A
    stamp taken after that select would drop it.
    """
    db_path = tmp_path / "example.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE widget (id INTEGER PRIMARY KEY)")
        connection.execute("CREATE TABLE page (page_ptr_id INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO widget VALUES (1)")
        connection.execute("INSERT INTO page VALUES (1)")
    _sqlite_source(monkeypatch, db_path)

    events: list[str] = []
    read_clock = keyset._read_source_clock  # noqa: SLF001
    load_rows = keyset.EmptyTableLoader.load_rows

    def _clock(connection: object) -> datetime:
        events.append("clock")
        return read_clock(connection)

    def _select(self: keyset.EmptyTableLoader, *args: object) -> object:
        events.append(f"select {self.table.name}")
        return load_rows(self, *args)

    monkeypatch.setattr(keyset, "_read_source_clock", _clock)
    monkeypatch.setattr(keyset.EmptyTableLoader, "load_rows", _select)
    pipeline = keyset.keyset_pipeline_for(_SPEC, profile="test")
    assert not pipeline.run(
        keyset.build_keyset_source(_SPEC, _TABLES, profile="test")
    ).has_failed_jobs
    assert events[0] == "clock"
    assert events.count("clock") == 1
    assert sorted(events[1:]) == ["select page", "select widget"]


def test_the_load_id_column_is_nullable() -> None:
    """Same reason as every other source: see config.DLT_LOAD_ID_COLUMN."""
    resource = keyset.build_keyset_resource(_SPEC, _TABLES[0], profile="dev")
    columns = resource.compute_table_schema()["columns"]
    assert columns["_dlt_load_id"]["nullable"] is True


@pytest.mark.integration
def test_a_keyset_replaces_its_iceberg_table(
    sqlite_iceberg_lake: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Deployed profiles write Iceberg; a deleted key must leave the table there too."""
    db_path = tmp_path / "example.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT)")
        connection.executemany(
            "INSERT INTO widget VALUES (?, ?)", [(1, "first"), (2, "second")]
        )
    _sqlite_source(monkeypatch, db_path)
    monkeypatch.setattr(config, "active_table_format", lambda *_a, **_k: "iceberg")
    pipeline = dlt.pipeline(
        pipeline_name="keyset_iceberg_test",
        destination=dlt.destinations.filesystem(
            bucket_url=sqlite_iceberg_lake.as_uri()
        ),
        dataset_name="raw",
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    table = "raw__example__app__postgres__widget__keyset"
    tables = (KeysetTable(name="widget", key="id"),)

    assert not pipeline.run(
        keyset.build_keyset_source(_SPEC, tables, profile="test")
    ).has_failed_jobs
    with sqlite3.connect(db_path) as connection:
        connection.execute("DELETE FROM widget WHERE id = 1")
    assert not pipeline.run(
        keyset.build_keyset_source(_SPEC, tables, profile="test")
    ).has_failed_jobs

    arrow = pipeline.dataset()[table].arrow()
    assert arrow.column("id").to_pylist() == [2]
    assert arrow.schema.field(keyset.READ_STARTED_AT_COLUMN).type.tz == "UTC"
