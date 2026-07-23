"""Unit + materialization tests for the reusable database source."""

import sqlite3
from pathlib import Path
from typing import NoReturn

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from ol_dlt import config, database
from ol_dlt.database import DatabaseSourceSpec, DatabaseTable

_SPEC = DatabaseSourceSpec(
    name="example",
    raw_table_prefix="raw__example__app__postgres__",
    database="example",
    vault_mount="postgres-example",
    tables=(
        DatabaseTable(name="widget", primary_key="id"),
        DatabaseTable(name="gadget", primary_key="id", excluded_columns=("secret",)),
    ),
)


def test_cursor_column_requires_primary_key() -> None:
    with pytest.raises(ValueError, match="primary_key is required"):
        DatabaseTable(name="thing", cursor_column="updated_at")


def test_local_profile_url_uses_local_dev_defaults() -> None:
    url = database._connection_url(_SPEC, "dev")  # noqa: SLF001
    assert url.host == database.LOCAL_DEV_HOST
    assert url.username == database.LOCAL_DEV_USERNAME
    assert url.password == database.LOCAL_DEV_PASSWORD
    assert url.database == "example"
    assert "sslmode" not in url.query


def test_local_profile_url_honors_env_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EXAMPLE_DB_HOST", "db.internal")
    monkeypatch.setenv("EXAMPLE_DB_PORT", "6543")
    monkeypatch.setenv("EXAMPLE_DB_USERNAME", "reader")
    url = database._connection_url(_SPEC, "dev")  # noqa: SLF001
    assert (url.host, url.port, url.username) == ("db.internal", 6543, "reader")


def test_deployed_profile_url_omits_credentials_and_requires_tls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EXAMPLE_DB_HOST", "example.rds.amazonaws.com")
    url = database._connection_url(_SPEC, "production")  # noqa: SLF001
    # Credentials are injected per connection from Vault, never in the URL.
    assert url.username is None
    assert url.password is None
    assert url.query["sslmode"] == "require"


def test_deployed_profile_requires_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EXAMPLE_DB_HOST", raising=False)
    with pytest.raises(ValueError, match="EXAMPLE_DB_HOST is not set"):
        database._connection_url(_SPEC, "production")  # noqa: SLF001


def test_deployed_profile_never_reads_vault_at_definition_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Building the source must not open a Vault lease.

    Dagster instantiates every source when the code location is imported; a
    lease taken there would be long expired by the time a scheduled run fires.
    """
    monkeypatch.setenv("EXAMPLE_DB_HOST", "example.rds.amazonaws.com")

    def _fail(*_args: object, **_kwargs: object) -> NoReturn:
        msg = "Vault was read while the source was being defined"
        raise AssertionError(msg)

    monkeypatch.setattr(database.vault, "read_database_credentials", _fail)
    source = database.build_database_source(_SPEC, profile="production")
    assert set(source.resources) == {
        "raw__example__app__postgres__widget",
        "raw__example__app__postgres__gadget",
    }


def test_vault_credentials_are_injected_at_connection_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        database.vault,
        "read_database_credentials",
        lambda mount, role: (f"v-{mount}", f"p-{role}"),
    )
    engine = create_engine("postgresql+psycopg2://example.rds.amazonaws.com/example")
    database._vault_credential_injector(_SPEC)(engine)  # noqa: SLF001

    connect_params: dict[str, str] = {}
    engine.dialect.dispatch.do_connect(engine.dialect, None, [], connect_params)
    assert connect_params == {
        "user": "v-postgres-example",
        "password": "p-readonly",  # pragma: allowlist secret
    }


def test_table_format_follows_the_explicit_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit profile must drive the table format, not ``DLT_PROFILE``.

    Everything else in ``build_table_resource`` keys off the resolved profile,
    so letting the table-format hint fall back to the ambient env var would
    write plain parquet hints onto a run that is otherwise configured for a
    deployed (Iceberg) destination, and vice versa.
    """
    monkeypatch.setenv("DLT_PROFILE", "test")
    monkeypatch.setenv("EXAMPLE_DB_HOST", "example.rds.amazonaws.com")
    monkeypatch.setattr(
        database.vault, "read_database_credentials", lambda *_a: ("u", "p")
    )
    deployed = database.build_database_source(_SPEC, profile="production")
    local = database.build_database_source(_SPEC, profile="dev")
    assert (
        deployed.resources["raw__example__app__postgres__widget"]
        .compute_table_schema()
        .get("table_format")
        == "iceberg"
    )
    assert (
        local.resources["raw__example__app__postgres__widget"]
        .compute_table_schema()
        .get("table_format")
        == "native"
    )


def test_build_database_source_selects_a_subset() -> None:
    source = database.build_database_source(_SPEC, tables=["widget"], profile="dev")
    assert set(source.resources) == {"raw__example__app__postgres__widget"}


def test_build_database_source_rejects_an_empty_selection() -> None:
    with pytest.raises(ValueError, match="no tables selected"):
        database.build_database_source(_SPEC, tables=["nope"], profile="dev")


@pytest.mark.integration
def test_materializes_a_database_to_the_test_profile(
    test_profile: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end: reflect, extract and write a real relational source.

    SQLite stands in for Postgres so the test stays hermetic; everything under
    test (deferred reflection, resource renaming, column exclusion, the raw
    table layout) is driver-independent.
    """
    db_path = tmp_path / "example.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute(
            "CREATE TABLE gadget (id INTEGER PRIMARY KEY, name TEXT, secret TEXT)"
        )
        connection.executemany(
            "INSERT INTO widget VALUES (?, ?)", [(1, "first"), (2, "second")]
        )
        connection.execute("INSERT INTO gadget VALUES (1, 'gizmo', 'hunter2')")

    monkeypatch.setattr(
        database,
        "_connection_url",
        lambda *_a, **_k: URL.create("sqlite", database=str(db_path)),
    )
    spec = DatabaseSourceSpec(
        name="example",
        raw_table_prefix="raw__example__app__postgres__",
        database="example",
        vault_mount="postgres-example",
        db_schema=None,  # SQLite has no named schema
        tables=_SPEC.tables,
    )
    pipeline = config.pipeline_for("example")
    info = pipeline.run(database.build_database_source(spec, profile="test"))
    assert not info.has_failed_jobs

    tables = pipeline.default_schema.tables
    assert "raw__example__app__postgres__widget" in tables
    gadget_columns = tables["raw__example__app__postgres__gadget"]["columns"]
    assert "name" in gadget_columns
    # excluded_columns must keep credential material out of the warehouse.
    assert "secret" not in gadget_columns
