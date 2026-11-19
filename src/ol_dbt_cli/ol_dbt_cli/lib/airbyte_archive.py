"""Which raw tables a unit's production cutover copies into the history layer.

A unit's first production dlt load replaces the tables Airbyte loaded, and raw
snapshot expiry then removes their older Iceberg snapshots. What Airbyte
accrued (keys deleted at the source, earlier versions in append tables) is in
no other table, so each table is copied into the history schema first
(INGESTION_INVENTORY_SPEC.md §10.4). The copy is made by the dbt macro
``archive_airbyte_raw_tables``; this module only decides what it copies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ol_dbt_cli.lib.inventory import RenderError, Unit, _tables_by_stream

ARCHIVE_PREFIX = "airbyte_archive__"


def active_connections(unit: Unit, environment: str) -> list[str]:
    """Return the names of the unit's connections in one environment still marked active.

    An archive taken while one of them still syncs can miss the rows of a sync
    that lands after its row-count check, so an archive waits until the unit's
    file records every connection loading that environment's lake as paused.

    :param unit: The inventory unit being archived.
    :param environment: The connection environment, ``production`` or ``qa``.
    :returns: The active connections' names.
    :rtype: list[str]
    """
    return [
        str(connection["name"])
        for connection in unit.connections
        if connection["environment"] == environment and connection["status"] == "active"
    ]


@dataclass(frozen=True)
class ArchiveTable:
    """One raw table to copy, and the columns the copy leaves out."""

    raw_table: str
    exclude_columns: tuple[str, ...] = ()

    @property
    def archive_table(self) -> str:
        return f"{ARCHIVE_PREFIX}{self.raw_table}"

    def as_arg(self) -> dict[str, Any]:
        """Return the table as the macro's ``tables`` argument expects it."""
        return {"raw_table": self.raw_table, "exclude_columns": list(self.exclude_columns)}


def archive_tables(unit: Unit) -> list[ArchiveTable]:
    """Return the raw tables a unit's production Airbyte connections load.

    Every stream of every production connection counts, active or not and
    modeled or not: snapshots cover modeled tables only, so for the rest the
    copy is the only record. A unit whose dlt source appends to the same tables
    gets none, because those tables go on holding every row Airbyte wrote.

    :param unit: The inventory unit being cut over.
    :returns: The tables to copy, ordered by raw table name.
    :rtype: list[ArchiveTable]
    :raises RenderError: When the unit has no production connection, or a
        stream has no table entry.
    """
    dlt = unit.data.get("dlt") or {}
    if dlt.get("write_disposition") == "append":
        return []
    streams = {
        stream
        for connection in unit.connections
        if connection["environment"] == "production"
        for stream in connection["streams"]
    }
    if not streams:
        msg = f"{unit.key} has no production Airbyte connection, so there is nothing to archive"
        raise RenderError(msg)
    tables = _tables_by_stream(unit)
    missing = sorted(streams - tables.keys())
    if missing:
        msg = f"{unit.key}: production streams with no table entry: {', '.join(missing)}"
        raise RenderError(msg)
    return sorted(
        (
            ArchiveTable(
                raw_table=str(tables[stream]["raw_table"]),
                exclude_columns=tuple((tables[stream].get("history") or {}).get("exclude_columns", [])),
            )
            for stream in streams
        ),
        key=lambda table: table.raw_table,
    )
