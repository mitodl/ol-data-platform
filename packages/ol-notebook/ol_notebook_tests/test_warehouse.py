import base64
import json
import stat
import sys
import types
from pathlib import Path
from typing import Any

import mysql.connector
import ol_notebook.warehouse as warehouse
import pytest
import sqlalchemy


def _jwt(claims: dict[str, Any]) -> str:
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=")
    return f"e30.{payload.decode()}.c2ln"


@pytest.fixture
def fake_marimo(monkeypatch: pytest.MonkeyPatch) -> types.SimpleNamespace:
    """Stand in for marimo, whose app_meta() only has a request inside a kernel."""
    meta = types.SimpleNamespace(request=None)
    module = types.ModuleType("marimo")
    module.app_meta = lambda: meta  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "marimo", module)
    return meta


@pytest.fixture
def captured_connect(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Record mysql.connector.connect calls, with the token file as it was then."""
    calls: list[dict[str, Any]] = []

    def connect(**kwargs: Any) -> str:
        token_path = Path(kwargs["openid_token_file"])
        calls.append(
            {
                **kwargs,
                "token": token_path.read_text(),
                "token_mode": stat.S_IMODE(token_path.stat().st_mode),
            }
        )
        return "connection"

    monkeypatch.setattr(mysql.connector, "connect", connect)
    monkeypatch.setenv("STARROCKS_HOST", "lakehouse.qa.starrocks.ol.mit.edu")
    monkeypatch.delenv("STARROCKS_ID_TOKEN_FILE", raising=False)
    monkeypatch.delenv("STARROCKS_PORT", raising=False)
    monkeypatch.delenv("STARROCKS_TLS_VERIFY_IDENTITY", raising=False)
    return calls


def test_login_name_reads_the_starrocks_username_claim() -> None:
    token = _jwt({"starrocks_username": "jdoe", "preferred_username": "jdoe@mit.edu"})
    assert warehouse.login_name(token) == "jdoe"


@pytest.mark.parametrize(
    "token",
    ["not-a-jwt", "a.!!!.c", _jwt({"preferred_username": "jdoe@mit.edu"})],
)
def test_login_name_refuses_tokens_without_the_claim(token: str) -> None:
    with pytest.raises(warehouse.WarehouseAccessError):
        warehouse.login_name(token)


@pytest.mark.usefixtures("fake_marimo")
def test_a_cell_without_a_request_is_refused() -> None:
    with pytest.raises(warehouse.WarehouseAccessError, match="without a browser"):
        warehouse.viewer_access_token()


def test_a_request_without_a_token_is_refused(
    fake_marimo: types.SimpleNamespace,
) -> None:
    fake_marimo.request = types.SimpleNamespace(headers={"cookie": "x"})
    with pytest.raises(warehouse.WarehouseAccessError, match="x-access-token"):
        warehouse.viewer_access_token()


def test_connect_logs_in_as_the_viewer_over_verified_tls(
    fake_marimo: types.SimpleNamespace, captured_connect: list[dict[str, Any]]
) -> None:
    token = _jwt({"starrocks_username": "jdoe"})
    fake_marimo.request = types.SimpleNamespace(headers={"x-access-token": token})

    assert warehouse.connect() == "connection"

    (call,) = captured_connect
    assert call["user"] == "jdoe"
    assert call["token"] == token
    assert call["token_mode"] == 0o600
    assert not Path(call["openid_token_file"]).exists()
    assert call["host"] == "lakehouse.qa.starrocks.ol.mit.edu"
    assert call["port"] == warehouse.DEFAULT_MYSQL_PORT
    assert call["auth_plugin"] == "authentication_openid_connect_client"
    assert call["ssl_verify_cert"] is True
    assert call["ssl_verify_identity"] is True
    assert call["use_pure"] is True


def test_each_connection_uses_the_current_request(
    fake_marimo: types.SimpleNamespace, captured_connect: list[dict[str, Any]]
) -> None:
    for name in ("first", "second"):
        fake_marimo.request = types.SimpleNamespace(
            headers={"x-access-token": _jwt({"starrocks_username": name})}
        )
        warehouse.connect()
    assert [call["user"] for call in captured_connect] == ["first", "second"]


def test_connect_prefers_the_starrocks_auth_token_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    captured_connect: list[dict[str, Any]],
) -> None:
    token_file = tmp_path / "qa.id_token"
    token_file.write_text("id-token")
    token_file.chmod(0o600)
    monkeypatch.setenv("STARROCKS_ID_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("STARROCKS_USER", "jdoe")
    monkeypatch.setenv("STARROCKS_HOST", "127.0.0.1")
    monkeypatch.setenv("STARROCKS_PORT", "19030")
    monkeypatch.setenv("STARROCKS_TLS_VERIFY_IDENTITY", "false")

    warehouse.connect()

    (call,) = captured_connect
    assert call["user"] == "jdoe"
    assert call["openid_token_file"] == str(token_file)
    assert token_file.exists()
    assert call["port"] == 19030
    assert call["ssl_verify_identity"] is False


def test_engine_uses_the_starrocks_dialect_over_mysql_connector() -> None:
    from ol_notebook.dialect import StarRocksMySQLConnectorDialect  # noqa: PLC0415

    dialect = warehouse.engine().dialect
    assert isinstance(dialect, StarRocksMySQLConnectorDialect)
    # marimo's data sources panel lists catalogs only for this name.
    assert dialect.name == "starrocks"
    assert dialect.driver == "mysqlconnector"


def test_percent_signs_reach_starrocks_unescaped() -> None:
    query = "SELECT DATE_FORMAT(dt, '%Y-%m'), 7 % 3 FROM t WHERE n LIKE 'a%'"
    compiled = sqlalchemy.text(query).compile(dialect=warehouse.engine().dialect)
    assert str(compiled) == query
    modulo = sqlalchemy.literal_column("a") % sqlalchemy.literal_column("b")
    assert str(modulo.compile(dialect=warehouse.engine().dialect)) == "a % b"
