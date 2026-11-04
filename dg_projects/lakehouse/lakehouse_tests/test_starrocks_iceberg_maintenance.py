"""Tests for the statements StarRocksIcebergMaintenanceResource sends.

The rewrite mode is the part that matters: a threshold rewrite leaves the
position deletes of every data file over the threshold in place, so a table
holding delete files has to be rewritten whole.
"""

from collections.abc import Callable
from typing import Any, Self

import pytest
from dagster import build_init_resource_context
from lakehouse.resources import starrocks_iceberg_maintenance as module
from lakehouse.resources.starrocks import StarRocksResource
from lakehouse.resources.starrocks_iceberg_maintenance import (
    StarRocksIcebergMaintenanceResource,
)
from pymysql.err import InterfaceError, OperationalError, ProgrammingError

CANT_CONNECT = 2003
SERVER_LOST = 2013
SYNTAX_ERROR = 1064
UNKNOWN_TABLE = 5502
SCHEMA = "ol_warehouse_production_intermediate"

Responder = Callable[[str, Any], list[dict[str, Any]]]


class FakeCursor:
    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn
        self.rows: list[dict[str, Any]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, sql: str, params: Any) -> None:
        self.conn.statements.append((sql, params))
        self.rows = self.conn.respond(sql, params)

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows


class FakeConnection:
    def __init__(self, respond: Responder) -> None:
        self.respond = respond
        self.statements: list[tuple[str, Any]] = []
        self.open = True

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        return None

    def close(self) -> None:
        self.open = False


class FakeStarRocks:
    """Stands in for StarRocksResource.connect; counts the logins it hands out."""

    def __init__(self, respond: Responder, connect_failures: int = 0) -> None:
        self.respond = respond
        self.connect_failures = connect_failures
        self.connections: list[FakeConnection] = []

    def connect(self) -> FakeConnection:
        if self.connect_failures:
            self.connect_failures -= 1
            raise OperationalError(CANT_CONNECT, "Can't connect to StarRocks")
        conn = FakeConnection(self.respond)
        self.connections.append(conn)
        return conn

    @property
    def statements(self) -> list[str]:
        return [sql for conn in self.connections for sql, _ in conn.statements]


def _engine(
    monkeypatch: pytest.MonkeyPatch,
    respond: Responder,
    connect_failures: int = 0,
    catalog: str = "ol_data_lake_production",
) -> tuple[StarRocksIcebergMaintenanceResource, FakeStarRocks]:
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    fake = FakeStarRocks(respond, connect_failures)
    monkeypatch.setattr(
        StarRocksResource, "connect", lambda _self: fake.connect(), raising=True
    )
    starrocks = StarRocksResource.model_construct(
        vault_mount_point="database-starrocks",
        host="starrocks",
        database="b2b_analytics",
    )
    engine = StarRocksIcebergMaintenanceResource.model_construct(
        starrocks=starrocks, catalog=catalog
    )
    return engine, fake


def _with_delete_files(count: int) -> Responder:
    def respond(sql: str, _params: Any) -> list[dict[str, Any]]:
        if "$files" in sql:
            return [{"delete_files": count}]
        return [{"status": "success"}]

    return respond


def test_a_table_with_delete_files_is_rewritten_whole(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, fake = _engine(monkeypatch, _with_delete_files(5))
    engine.optimize(SCHEMA, "int__learn__courses")
    table = f"`ol_data_lake_production`.`{SCHEMA}`.`int__learn__courses"
    count_sql = f"SELECT count(*) AS delete_files FROM {table}$files`"  # noqa: S608
    assert fake.statements == [
        f"{count_sql} WHERE `content` <> 0",
        f"ALTER TABLE {table}` EXECUTE rewrite_data_files(rewrite_all => true)",
    ]


def test_a_table_without_delete_files_rewrites_only_small_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, fake = _engine(monkeypatch, _with_delete_files(0))
    engine.optimize(SCHEMA, "int__learn__courses")
    assert fake.statements[-1] == (
        f"ALTER TABLE `ol_data_lake_production`.`{SCHEMA}`.`int__learn__courses` "
        "EXECUTE rewrite_data_files(min_file_size_bytes => 536870912)"
    )


def test_the_threshold_follows_file_size_mb(monkeypatch: pytest.MonkeyPatch) -> None:
    engine, fake = _engine(monkeypatch, _with_delete_files(0))
    engine.optimize(SCHEMA, "t", file_size_mb=128)
    assert fake.statements[-1].endswith("(min_file_size_bytes => 134217728)")


def test_backticks_in_identifiers_are_doubled(monkeypatch: pytest.MonkeyPatch) -> None:
    engine, fake = _engine(monkeypatch, _with_delete_files(0), catalog="cat`x")
    engine.optimize("s`1", "t`2")
    assert fake.statements[-1].startswith("ALTER TABLE `cat``x`.`s``1`.`t``2` ")


def _analyze_result(msg_type: str, msg_text: str) -> Responder:
    def respond(_sql: str, _params: Any) -> list[dict[str, Any]]:
        return [
            {"Table": "t", "Op": "analyze", "Msg_type": msg_type, "Msg_text": msg_text}
        ]

    return respond


def test_an_analyze_that_reports_an_error_row_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed synchronous ANALYZE answers with a row, not a MySQL error."""
    engine, _ = _engine(monkeypatch, _analyze_result("error", "collect timed out"))
    with pytest.raises(RuntimeError, match="collect timed out"):
        engine.analyze(SCHEMA, "t")


def test_a_successful_analyze_returns(monkeypatch: pytest.MonkeyPatch) -> None:
    engine, fake = _engine(monkeypatch, _analyze_result("status", "OK"))
    engine.analyze(SCHEMA, "t")
    assert fake.statements == [
        f"ANALYZE TABLE `ol_data_lake_production`.`{SCHEMA}`.`t`"
    ]


def test_an_empty_table_analyze_returns(monkeypatch: pytest.MonkeyPatch) -> None:
    # StarRocks sends no result set for ANALYZE of a table with no rows.
    engine, _ = _engine(monkeypatch, lambda _sql, _params: [])
    engine.analyze(SCHEMA, "t")


def _catalog_with(*tables: str) -> Responder:
    """Answer as a schema listing *tables*, where a name with "view" is a view."""

    def respond(sql: str, _params: Any) -> list[dict[str, Any]]:
        if "information_schema" in sql:
            return [{"table_schema": SCHEMA, "table_name": name} for name in tables]
        if "gone" in sql:
            raise OperationalError(UNKNOWN_TABLE, "Unknown table")
        if "unparseable" in sql:
            raise ProgrammingError(SYNTAX_ERROR, "Getting syntax error")
        if "view" in sql:
            return [{"View": "v", "Create View": "CREATE VIEW ..."}]
        return [{"Table": "t", "Create Table": "CREATE TABLE ..."}]

    return respond


def test_views_listed_as_base_tables_are_excluded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The external catalog's information_schema calls an Iceberg view a table."""
    engine, fake = _engine(monkeypatch, _catalog_with("int__a_table", "int__a_view"))
    candidates = {
        (SCHEMA, "int__a_table"),
        (SCHEMA, "int__a_view"),
        ("ol_warehouse_production_mart", "absent"),
    }
    assert engine.base_tables_among(candidates) == {(SCHEMA, "int__a_table")}
    listing_sql, listing_params = fake.connections[0].statements[0]
    assert "`ol_data_lake_production`.information_schema.tables" in listing_sql
    assert listing_params == (SCHEMA, "ol_warehouse_production_mart")


def test_only_candidates_are_loaded(monkeypatch: pytest.MonkeyPatch) -> None:
    """__dbt_tmp and backup relations share the schema but no config names them."""
    engine, fake = _engine(
        monkeypatch,
        _catalog_with("int__a_table", "int__a_table__dbt_tmp", "int__old_backup"),
    )
    assert engine.base_tables_among({(SCHEMA, "int__a_table")}) == {
        (SCHEMA, "int__a_table")
    }
    shows = [sql for sql in fake.statements if sql.startswith("SHOW CREATE")]
    assert shows == [
        f"SHOW CREATE TABLE `ol_data_lake_production`.`{SCHEMA}`.`int__a_table`"
    ]


def test_a_table_that_cannot_be_loaded_is_skipped_not_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One bad relation must not stop maintenance for every other table."""
    engine, fake = _engine(
        monkeypatch, _catalog_with("int__a_table", "int__gone", "int__unparseable")
    )
    candidates = {
        (SCHEMA, "int__a_table"),
        (SCHEMA, "int__gone"),
        (SCHEMA, "int__unparseable"),
    }
    assert engine.base_tables_among(candidates) == {(SCHEMA, "int__a_table")}
    # Both are errors the server reported about a statement, so the
    # connection is still good.
    assert len(fake.connections) == 1


def test_no_candidates_sends_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    engine, fake = _engine(monkeypatch, _with_delete_files(0))
    assert engine.base_tables_among(set()) == set()
    assert fake.connections == []


def test_one_login_serves_every_statement(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each StarRocksResource.execute would have Vault create a database user."""
    engine, fake = _engine(monkeypatch, _with_delete_files(0))
    for table in ("a", "b", "c"):
        engine.optimize(SCHEMA, table)
    assert len(fake.connections) == 1
    assert len(fake.statements) == 6


def test_teardown_closes_the_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    engine, fake = _engine(monkeypatch, _with_delete_files(0))
    engine.delete_file_count(SCHEMA, "t")
    engine.teardown_after_execution(build_init_resource_context())
    assert not fake.connections[0].open
    # A second teardown, or one with no connection, is a no-op.
    engine.teardown_after_execution(build_init_resource_context())


def test_a_lost_connection_reconnects_and_resends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lost = {"once": False}

    def respond(_sql: str, _params: Any) -> list[dict[str, Any]]:
        if not lost["once"]:
            lost["once"] = True
            raise OperationalError(SERVER_LOST, "Lost connection to server")
        return [{"delete_files": 0}]

    engine, fake = _engine(monkeypatch, respond)
    assert engine.delete_file_count(SCHEMA, "t") == 0
    assert len(fake.connections) == 2


def test_a_rewrite_is_not_resent_after_a_lost_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The FE may still be running the first rewrite."""

    def respond(sql: str, _params: Any) -> list[dict[str, Any]]:
        if sql.startswith("ALTER TABLE"):
            raise OperationalError(SERVER_LOST, "Lost connection to server")
        return [{"delete_files": 0}]

    engine, fake = _engine(monkeypatch, respond)
    with pytest.raises(OperationalError):
        engine.optimize(SCHEMA, "t")
    alter = (
        f"ALTER TABLE `ol_data_lake_production`.`{SCHEMA}`.`t` "
        "EXECUTE rewrite_data_files(min_file_size_bytes => 536870912)"
    )
    assert [sql for sql in fake.statements if sql.startswith("ALTER")] == [alter]
    # The dead connection is not handed to the next table.
    engine.delete_file_count(SCHEMA, "u")
    assert len(fake.connections) == 2


def test_a_rewrite_is_sent_once_connecting_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed connect never reached the server, so the ALTER has not run."""
    engine, fake = _engine(monkeypatch, _with_delete_files(0), connect_failures=1)
    engine.delete_file_count(SCHEMA, "t")
    engine._drop_connection()
    fake.connect_failures = 1
    engine.optimize(SCHEMA, "t")
    assert sum(sql.startswith("ALTER") for sql in fake.statements) == 1


def test_a_statement_error_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    # pymysql raises ProgrammingError for 1064, not OperationalError.
    def respond(_sql: str, _params: Any) -> list[dict[str, Any]]:
        raise ProgrammingError(SYNTAX_ERROR, "Getting syntax error")

    engine, fake = _engine(monkeypatch, respond)
    with pytest.raises(ProgrammingError):
        engine.optimize(SCHEMA, "v")
    assert len(fake.statements) == 1
    assert fake.connections[0].open


def test_a_broken_socket_is_not_reused(monkeypatch: pytest.MonkeyPatch) -> None:
    """InterfaceError is not retriable, but the connection is dead after it."""
    broken = {"once": False}

    def respond(_sql: str, _params: Any) -> list[dict[str, Any]]:
        if not broken["once"]:
            broken["once"] = True
            raise InterfaceError(0, "")
        return [{"delete_files": 0}]

    engine, fake = _engine(monkeypatch, respond)
    with pytest.raises(InterfaceError):
        engine.delete_file_count(SCHEMA, "t")
    assert engine.delete_file_count(SCHEMA, "u") == 0
    assert len(fake.connections) == 2
