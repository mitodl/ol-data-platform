"""Tests for when StarRocksResource retries a statement.

A CREATE TABLE AS SELECT that dies mid-statement can leave its table created,
so retrying it fails on "already exists" and hides the real error. Those
statements are only retried when the connection itself failed.
"""

from typing import Any, Self

import pytest
from lakehouse.resources import starrocks as starrocks_module
from lakehouse.resources.starrocks import StarRocksResource
from pymysql.err import OperationalError

SERVER_LOST = 2013


class FakeCursor:
    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, sql: str, _params: Any) -> None:
        self.conn.statements.append(sql)
        raise OperationalError(SERVER_LOST, "Lost connection to server during query")

    def fetchall(self) -> list[dict[str, Any]]:
        return []


class FakeConnection:
    def __init__(self, statements: list[str]) -> None:
        self.statements = statements

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        return None

    def close(self) -> None:
        return None


@pytest.fixture
def resource(monkeypatch: pytest.MonkeyPatch) -> StarRocksResource:
    monkeypatch.setattr(starrocks_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        StarRocksResource, "generate_credentials", lambda _self: ("user", "pass")
    )
    return StarRocksResource.model_construct(
        vault_mount_point="database-starrocks",
        vault_role="admin",
        host="starrocks",
        port=9030,
        database="b2b_analytics",
    )


def _connect_with(
    monkeypatch: pytest.MonkeyPatch, statements: list[str], failures: int = 0
) -> None:
    attempts = {"n": 0}

    def fake_connect(**_: Any) -> FakeConnection:
        attempts["n"] += 1
        if attempts["n"] <= failures:
            raise OperationalError(2003, "Can't connect to StarRocks")
        return FakeConnection(statements)

    monkeypatch.setattr(starrocks_module, "connect", fake_connect)


def test_an_idempotent_statement_is_retried(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    statements: list[str] = []
    _connect_with(monkeypatch, statements)
    with pytest.raises(OperationalError):
        resource.execute("REFRESH MATERIALIZED VIEW mv")
    assert len(statements) == 3


def test_a_non_idempotent_statement_is_sent_once(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    statements: list[str] = []
    _connect_with(monkeypatch, statements)
    with pytest.raises(OperationalError):
        resource.execute("CREATE TABLE t AS SELECT 1", idempotent=False)
    assert statements == ["CREATE TABLE t AS SELECT 1"]


def test_a_failed_connection_is_still_retried_for_a_non_idempotent_statement(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    # The statement never reached the server, so sending it now is its first run.
    statements: list[str] = []
    _connect_with(monkeypatch, statements, failures=1)
    with pytest.raises(OperationalError):
        resource.execute("CREATE TABLE t AS SELECT 1", idempotent=False)
    assert statements == ["CREATE TABLE t AS SELECT 1"]


def test_a_fetch_with_no_params_binds_nothing(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    # pymysql's Cursor.execute runs `query % args` for any args that is not
    # None, so an empty tuple binds nothing and still makes a literal `%` in
    # the SQL raise "not enough arguments for format string". The QA mirror
    # sends a `where` an operator wrote through fetch, and a LIKE pattern or a
    # date_format mask there is exactly that.
    sent: list[tuple[str, Any]] = []

    class RecordingCursor(FakeCursor):
        def execute(self, sql: str, _params: Any) -> None:
            sent.append((sql, _params))

    class RecordingConnection(FakeConnection):
        def cursor(self) -> RecordingCursor:
            return RecordingCursor(self)

    monkeypatch.setattr(
        starrocks_module, "connect", lambda **_: RecordingConnection([])
    )
    sql = "EXPLAIN SELECT a FROM t WHERE b LIKE '%sandbox%'"
    assert resource.fetch(sql) == []
    assert sent == [(sql, None)]
