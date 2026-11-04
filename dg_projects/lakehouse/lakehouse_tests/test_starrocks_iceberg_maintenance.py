"""Tests for the statements StarRocksIcebergMaintenanceResource sends.

The rewrite mode is the part that matters: a threshold rewrite leaves the
position deletes of every data file over the threshold in place, so a table
holding delete files has to be rewritten whole.
"""

from collections.abc import Callable
from typing import Any, Self

import pytest
from lakehouse.resources import starrocks_iceberg_maintenance as module
from lakehouse.resources.starrocks import StarRocksResource
from lakehouse.resources.starrocks_iceberg_maintenance import (
    StarRocksIcebergMaintenanceResource,
)
from pymysql.err import OperationalError

SERVER_LOST = 2013
SYNTAX_ERROR = 1064
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

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        return None


class FakeStarRocks:
    """Stands in for StarRocksResource.connect; counts the logins it hands out."""

    def __init__(self, respond: Responder) -> None:
        self.respond = respond
        self.connections: list[FakeConnection] = []

    def connect(self) -> FakeConnection:
        conn = FakeConnection(self.respond)
        self.connections.append(conn)
        return conn

    @property
    def statements(self) -> list[str]:
        return [sql for conn in self.connections for sql, _ in conn.statements]


def _engine(
    monkeypatch: pytest.MonkeyPatch, respond: Responder
) -> tuple[StarRocksIcebergMaintenanceResource, FakeStarRocks]:
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    fake = FakeStarRocks(respond)
    monkeypatch.setattr(
        StarRocksResource, "connect", lambda _self: fake.connect(), raising=True
    )
    starrocks = StarRocksResource.model_construct(
        vault_mount_point="database-starrocks",
        host="starrocks",
        database="b2b_analytics",
    )
    engine = StarRocksIcebergMaintenanceResource.model_construct(
        starrocks=starrocks, catalog="ol_data_lake_production"
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
    assert fake.statements == [
        f"SELECT count(*) AS delete_files FROM {table}$files` WHERE `content` <> 0",  # noqa: S608
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


def test_views_listed_as_base_tables_are_excluded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The external catalog's information_schema calls an Iceberg view a table."""

    def respond(sql: str, _params: Any) -> list[dict[str, Any]]:
        if "information_schema" in sql:
            return [
                {"table_schema": SCHEMA, "table_name": "int__a_table"},
                {"table_schema": SCHEMA, "table_name": "int__a_view"},
            ]
        if "int__a_view" in sql:
            return [{"View": "int__a_view", "Create View": "CREATE VIEW ..."}]
        return [{"Table": "int__a_table", "Create Table": "CREATE TABLE ..."}]

    engine, fake = _engine(monkeypatch, respond)
    assert engine.base_tables_in_schemas({SCHEMA, "ol_warehouse_production_mart"}) == {
        (SCHEMA, "int__a_table")
    }
    listing_sql, listing_params = fake.connections[0].statements[0]
    assert "`ol_data_lake_production`.information_schema.tables" in listing_sql
    assert listing_params == (SCHEMA, "ol_warehouse_production_mart")


def test_no_schemas_sends_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    engine, fake = _engine(monkeypatch, _with_delete_files(0))
    assert engine.base_tables_in_schemas(set()) == set()
    assert fake.connections == []


def test_one_login_serves_every_statement(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each StarRocksResource.execute would have Vault create a database user."""
    engine, fake = _engine(monkeypatch, _with_delete_files(0))
    for table in ("a", "b", "c"):
        engine.optimize(SCHEMA, table)
    assert len(fake.connections) == 1
    assert len(fake.statements) == 6


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


def test_a_statement_error_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    def respond(_sql: str, _params: Any) -> list[dict[str, Any]]:
        raise OperationalError(SYNTAX_ERROR, "Table of ICEBERG_VIEW doesn't support")

    engine, fake = _engine(monkeypatch, respond)
    with pytest.raises(OperationalError):
        engine.optimize(SCHEMA, "v")
    assert len(fake.statements) == 1
