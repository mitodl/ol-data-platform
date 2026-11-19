"""Tests for the choice of raw tables a unit's dlt cutover archives."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from ol_dbt_cli.lib.airbyte_archive import ArchiveTable, active_production_connections, archive_tables
from ol_dbt_cli.lib.inventory import RenderError, Unit, load_units

REAL_INVENTORY = Path(__file__).resolve().parents[3] / "ingestion" / "inventory"


def _connection(environment: str, streams: list[str], status: str = "active") -> dict[str, Any]:
    return {
        "environment": environment,
        "name": f"{environment} connection",
        "status": status,
        "sync_interval_hours": 6 if environment == "production" else None,
        "streams": streams,
    }


def _table(name: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "raw_table": f"raw__app__{name}", "sync_mode": "incremental_deduped_history", **extra}


def _unit(connections: list[dict[str, Any]], tables: list[dict[str, Any]], **extra: Any) -> Unit:
    data = {
        "deployment": "app",
        "layer": "app_postgres",
        "airbyte": {"source_kind": "source-postgres", "connections": connections},
        "tables": tables,
        **extra,
    }
    return Unit(path=Path("app__app_postgres.yml"), data=data)


def test_every_production_stream_is_archived_modeled_or_not() -> None:
    unit = _unit(
        [_connection("production", ["b", "a"]), _connection("production", ["c"], status="inactive")],
        [_table("a", modeled=True), _table("b", modeled=False), _table("c"), _table("qa_only")],
        dlt={"source_module": "ol_dlt.sources.app", "write_disposition": "replace"},
    )
    assert [table.raw_table for table in archive_tables(unit)] == [
        "raw__app__a",
        "raw__app__b",
        "raw__app__c",
    ]


def test_qa_connections_are_not_archived() -> None:
    unit = _unit(
        [_connection("production", ["a"]), _connection("qa", ["b"])],
        [_table("a"), _table("b")],
    )
    assert [table.raw_table for table in archive_tables(unit)] == ["raw__app__a"]


def test_history_excludes_carry_over_and_name_the_archive() -> None:
    unit = _unit(
        [_connection("production", ["a"])],
        [_table("a", history={"exclude_columns": ["extra_data"]})],
    )
    assert archive_tables(unit) == [ArchiveTable(raw_table="raw__app__a", exclude_columns=("extra_data",))]
    assert archive_tables(unit)[0].archive_table == "airbyte_archive__raw__app__a"
    assert archive_tables(unit)[0].as_arg() == {"raw_table": "raw__app__a", "exclude_columns": ["extra_data"]}


def test_a_dlt_source_appending_to_the_same_tables_needs_no_archive() -> None:
    unit = _unit(
        [_connection("production", ["a"])],
        [_table("a")],
        dlt={"source_module": "ol_dlt.sources.app", "write_disposition": "append"},
    )
    assert archive_tables(unit) == []


def test_active_production_connections_are_named() -> None:
    unit = _unit(
        [
            _connection("production", ["a"]),
            _connection("production", ["b"], status="inactive"),
            _connection("qa", ["a"]),
        ],
        [_table("a"), _table("b")],
    )
    assert active_production_connections(unit) == ["production connection"]


def test_no_production_connection_fails() -> None:
    unit = _unit([_connection("qa", ["a"])], [_table("a")])
    with pytest.raises(RenderError, match="no production Airbyte connection"):
        archive_tables(unit)


def test_a_stream_with_no_table_entry_fails() -> None:
    unit = _unit([_connection("production", ["a", "missing"])], [_table("a")])
    with pytest.raises(RenderError, match="missing"):
        archive_tables(unit)


def test_first_cutover_unit_archives_every_production_stream() -> None:
    units = {unit.key: unit for unit in load_units(REAL_INVENTORY)}
    unit = units["mitxonline/app_postgres"]
    streams = {
        stream
        for connection in unit.connections
        if connection["environment"] == "production"
        for stream in connection["streams"]
    }
    archived = archive_tables(unit)
    assert len(archived) == len(streams)
    assert all(table.raw_table.startswith("raw__mitxonline__app__postgres__") for table in archived)
