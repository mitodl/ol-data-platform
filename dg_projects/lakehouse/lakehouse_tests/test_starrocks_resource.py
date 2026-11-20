"""Tests for when StarRocksResource retries a statement.

A CREATE TABLE AS SELECT that dies mid-statement can leave its table created,
so retrying it fails on "already exists" and hides the real error. Those
statements are only retried when the connection itself failed.
"""

import ssl
import sys
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


class QuietCursor(FakeCursor):
    def execute(self, sql: str, _params: Any) -> None:
        self.conn.statements.append(sql)


class QuietConnection(FakeConnection):
    def cursor(self) -> QuietCursor:
        return QuietCursor(self)


def _record_connect_kwargs(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def fake_connect(**kwargs: Any) -> QuietConnection:
        calls.append(kwargs)
        return QuietConnection([])

    monkeypatch.setattr(starrocks_module, "connect", fake_connect)
    return calls


def test_a_static_login_never_asks_vault(monkeypatch: pytest.MonkeyPatch) -> None:
    # model_construct leaves `vault` unset, so reaching for it raises.
    local = StarRocksResource.model_construct(
        vault_mount_point="database-starrocks",
        vault_role="admin",
        host="127.0.0.1",
        port=9030,
        database="b2b_analytics",
        static_username="root",
        static_password="",
    )
    calls = _record_connect_kwargs(monkeypatch)

    local.execute("SELECT 1")

    assert local.generate_credentials() == ("root", "")
    assert [(c["user"], c["password"], c["ssl"]) for c in calls] == [("root", "", None)]


def test_a_vault_login_requires_tls(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    calls = _record_connect_kwargs(monkeypatch)

    resource.execute("SELECT 1")

    assert calls[0]["user"] == "user"
    assert isinstance(calls[0]["ssl"], ssl.SSLContext)


FES = [
    {"IP": "fe-0.fe-search", "QueryPort": "9030", "Alive": "true"},
    {"IP": "fe-1.fe-search", "QueryPort": "9030", "Alive": "true"},
    {"IP": "fe-2.fe-search", "QueryPort": "9030", "Alive": "false"},
]


class FrontendsConnection(QuietConnection):
    def __init__(self, *, role_active: bool) -> None:
        super().__init__([])
        self.role_active = role_active

    def cursor(self) -> "FrontendsCursor":
        return FrontendsCursor(self)


class FrontendsCursor(QuietCursor):
    conn: FrontendsConnection

    def execute(self, sql: str, _params: Any = None) -> None:
        self.sql = sql

    def fetchall(self) -> list[dict[str, Any]]:
        if self.sql == "SHOW FRONTENDS":
            return FES
        if self.sql.startswith("SELECT is_role_in_session("):
            return [{"active": int(self.conn.role_active)}]
        return []


def _frontends_refusing_until(
    monkeypatch: pytest.MonkeyPatch,
    refusals: dict[str, int],
    code: int = 1045,
    calls: list[dict[str, Any]] | None = None,
    inactive_role: dict[str, int] | None = None,
) -> list[str]:
    """Each FE refuses the login *refusals[host]* times, then accepts it.

    With *inactive_role*, an FE that accepts the login reports its role as not
    yet active *inactive_role[host]* times first.
    """
    hosts: list[str] = []
    inactive = inactive_role or {}

    def fake_connect(**kwargs: Any) -> QuietConnection:
        host = kwargs["host"]
        hosts.append(host)
        if calls is not None:
            calls.append(kwargs)
        if refusals.get(host, 0) > 0:
            refusals[host] -= 1
            raise OperationalError(code, f"Access denied for user {kwargs['user']}")
        if inactive.get(host, 0) > 0:
            inactive[host] -= 1
            return FrontendsConnection(role_active=False)
        return FrontendsConnection(role_active=True)

    monkeypatch.setattr(starrocks_module, "connect", fake_connect)
    return hosts


def test_wait_for_login_checks_every_live_fe(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    hosts = _frontends_refusing_until(monkeypatch, {"fe-1.fe-search": 2})

    resource.wait_for_login("v-user", "pw")

    # Each poll lists the FEs again; fe-1 is polled until it accepts; fe-2 is down.
    assert hosts == ["starrocks", "fe-0.fe-search", "fe-1.fe-search"] * 3


def test_wait_for_login_retries_the_service_itself(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    hosts = _frontends_refusing_until(monkeypatch, {"starrocks": 1})

    resource.wait_for_login("v-user", "pw")

    assert hosts[:2] == ["starrocks", "starrocks"]


def test_wait_for_login_names_the_fe_that_never_accepts(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    _frontends_refusing_until(monkeypatch, {"fe-0.fe-search": 10_000})
    # Time passes only while the wait sleeps between polls.
    now = [0.0]
    monkeypatch.setattr(starrocks_module.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(
        starrocks_module.time, "sleep", lambda s: now.__setitem__(0, now[0] + s)
    )

    with pytest.raises(starrocks_module.LoginNotPropagatedError) as raised:
        resource.wait_for_login("v-user", "pw")

    assert "Access denied" in raised.value.refusing["fe-0.fe-search:9030"]


def test_wait_for_login_raises_an_unrelated_error(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    _frontends_refusing_until(monkeypatch, {"fe-0.fe-search": 1}, code=1064)

    with pytest.raises(OperationalError):
        resource.wait_for_login("v-user", "pw")


def test_wait_for_login_skips_a_static_login(monkeypatch: pytest.MonkeyPatch) -> None:
    local = StarRocksResource.model_construct(
        vault_mount_point="database-starrocks",
        vault_role="admin",
        host="127.0.0.1",
        port=9030,
        database="b2b_analytics",
        static_username="root",
        static_password="",
    )
    calls = _record_connect_kwargs(monkeypatch)

    local.wait_for_login("root", "")

    assert calls == []


def test_wait_for_login_skips_an_fe_that_is_down(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    """A rolling restart must not hold the wait open: dbt can't land there."""
    _frontends_refusing_until(monkeypatch, {"fe-0.fe-search": 10_000}, code=2003)

    resource.wait_for_login("v-user", "pw")


def test_wait_for_login_does_not_pass_on_an_empty_fe_list(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    _frontends_refusing_until(monkeypatch, {})
    monkeypatch.setattr(sys.modules[__name__], "FES", [])
    clock = iter(range(0, 10_000, 30))
    monkeypatch.setattr(starrocks_module.time, "monotonic", lambda: next(clock))

    with pytest.raises(starrocks_module.LoginNotPropagatedError):
        resource.wait_for_login("v-user", "pw")


def test_wait_for_login_connects_with_tls_and_no_database(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    calls: list[dict[str, Any]] = []
    _frontends_refusing_until(monkeypatch, {}, calls=calls)

    resource.wait_for_login("v-user", "pw")

    assert {c["database"] for c in calls} == {None}
    assert all(isinstance(c["ssl"], ssl.SSLContext) for c in calls)


def test_wait_for_login_waits_for_the_role_on_each_fe(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    """A follower can accept the password before it replays the GRANT."""
    hosts = _frontends_refusing_until(
        monkeypatch, {}, inactive_role={"fe-1.fe-search": 1, "starrocks": 1}
    )

    resource.wait_for_login("v-user", "pw")

    # The service's FE lacks the role on the first poll, so no FE list yet.
    assert hosts == [
        "starrocks",
        "starrocks",
        "fe-0.fe-search",
        "fe-1.fe-search",
        "starrocks",
        "fe-0.fe-search",
        "fe-1.fe-search",
    ]


def test_wait_for_login_reports_fes_left_unchecked_at_the_deadline(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    _frontends_refusing_until(monkeypatch, {"fe-0.fe-search": 10_000})
    # The deadline is set at 0 and fe-0 is probed at 0; by fe-1 it has passed.
    clock = iter([0, 0, *range(200, 10_000)])
    monkeypatch.setattr(starrocks_module.time, "monotonic", lambda: next(clock))

    with pytest.raises(starrocks_module.LoginNotPropagatedError) as raised:
        resource.wait_for_login("v-user", "pw")

    assert raised.value.refusing["fe-1.fe-search:9030"] == (
        "not checked before the deadline"
    )


def test_probes_time_out_and_ordinary_connections_do_not(
    monkeypatch: pytest.MonkeyPatch, resource: StarRocksResource
) -> None:
    calls: list[dict[str, Any]] = []
    _frontends_refusing_until(monkeypatch, {}, calls=calls)

    resource.wait_for_login("v-user", "pw")
    resource.execute("SELECT 1")

    *probes, ordinary = calls
    assert {(c["read_timeout"], c["write_timeout"]) for c in probes} == {(10, 10)}
    assert (ordinary["read_timeout"], ordinary["write_timeout"]) == (None, None)
