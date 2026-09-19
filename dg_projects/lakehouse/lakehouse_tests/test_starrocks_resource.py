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
