"""Keys-only reads of a database's tables, for detecting hard deletes.

A table that Airbyte loads incrementally by xmin or cursor never loses a row in
raw when the source deletes it. Pulling just the primary key of every current
row, and nothing else, gives the full live key set; a key in raw that is absent
from it was deleted at the source. The raw history snapshots and staging read
these to filter deleted keys (decided 2026-10-29 on witan task
tk-delete-capture-for-dlt-migrated-sources-reconcil-826b18).

Each source table becomes one ``<raw_table>__keyset`` table, replaced on every
load, holding the key in its source type plus ``_keyset_read_started_at``: the
source database's clock read before the first table of the load was selected,
one value for the whole load. A consumer keeps a raw row whose key is in the
keyset, or whose loader time is later than that clock less a margin, since a
row extracted after the keyset read started existed then. The clock therefore
has to precede every table's read, never follow one. dlt extracts every table
before it loads any, so a failed read leaves the previous keysets whole.

The pipeline is named ``<spec.name>_keyset`` and writes under its own bucket
prefix, so its state and schema never touch the loader's for the same database.

Which tables get a keyset is not decided here. ``tables.json`` beside this
module is rendered from the ingestion inventory by
``ol-dbt inventory keysets --write``, and CI fails when the two disagree.
"""

import json
import threading
from collections.abc import Generator, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from typing import Any

import dlt
import pyarrow as pa
from dlt.sources.sql_database import remove_nullability_adapter, sql_table
from sqlalchemy import func, select
from sqlalchemy.engine import Connection, Engine

from ol_dlt import config
from ol_dlt.database import (
    DEFAULT_CHUNK_SIZE,
    DatabaseSourceSpec,
    EmptyTableLoader,
    _credentials_for,
    _uses_vault,
    _vault_credential_injector,
)
from ol_dlt.keyset.specs import KEYSET_SPECS

KEYSET_SUFFIX = "__keyset"
READ_STARTED_AT_COLUMN = "_keyset_read_started_at"

# Where the load's clock reading is kept. The load package state is one dict per
# load, shared by every resource extracted into it, which is exactly the scope
# "one value per load" needs.
_LOAD_STATE_KEY = "keyset_read_started_at"

# Guards the check-then-read of the load's clock. Extraction runs one worker
# (.dlt/config.toml) and nothing races for it today, but the cost of two tables
# reading two different clocks is a stamp later than one of the reads.
_CLOCK_LOCK = threading.Lock()

_READ_STARTED_AT_HINT: dict[str, dict[str, Any]] = {
    READ_STARTED_AT_COLUMN: {
        "data_type": "timestamp",
        "timezone": True,
        "precision": 6,
        "nullable": True,
    },
}


@dataclass(frozen=True)
class KeysetTable:
    """One source table and its single-column primary key."""

    name: str
    key: str


def keyset_table_name(spec: DatabaseSourceSpec, table: KeysetTable) -> str:
    """Return the raw warehouse name of ``table``'s keyset."""
    return f"{spec.raw_table_prefix}{table.name}{KEYSET_SUFFIX}"


def _read_source_clock(connection: Connection) -> datetime:
    """Return the source database's current time, in UTC.

    MySQL's ``CURRENT_TIMESTAMP`` is in the session time zone and carries no
    zone, so MySQL reads ``UTC_TIMESTAMP()``. Postgres returns a zoned value,
    and SQLite (tests) returns a UTC string.
    """
    clock = (
        func.utc_timestamp()
        if connection.dialect.name == "mysql"
        else func.current_timestamp()
    )
    value = connection.execute(select(clock)).scalar_one()
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _load_read_started_at(engine: Engine) -> datetime:
    """Return this load's clock reading, reading the source on the first call."""
    state = dlt.current.load_package_state()["state"]
    with _CLOCK_LOCK:
        if _LOAD_STATE_KEY not in state:
            with engine.connect() as connection:
                state[_LOAD_STATE_KEY] = _read_source_clock(connection).isoformat()
        return datetime.fromisoformat(state[_LOAD_STATE_KEY])


class KeysetTableLoader(EmptyTableLoader):
    """Table loader that stamps every chunk with the load's source clock.

    The clock is read on this table's own connection before its select runs,
    or reused from an earlier table of the same load, so it never follows the
    read it stamps.
    """

    def load_rows(self, backend_kwargs: dict[str, Any] | None = None) -> Iterator[Any]:
        """Read the load's clock, then yield the key column with the stamp."""
        self._read_started_at = _load_read_started_at(self.engine)
        yield from super().load_rows(backend_kwargs)

    def _convert_result(
        self,
        result: Any,  # noqa: ANN401
        backend_kwargs: dict[str, Any],
    ) -> Iterator[Any]:
        stamp_type = pa.timestamp("us", tz="UTC")
        for item in super()._convert_result(result, backend_kwargs):
            yield item.append_column(
                pa.field(READ_STARTED_AT_COLUMN, stamp_type),
                pa.array([self._read_started_at] * item.num_rows, type=stamp_type),
            )


def build_keyset_resource(
    spec: DatabaseSourceSpec,
    table: KeysetTable,
    *,
    profile: str | None = None,
) -> Any:  # noqa: ANN401
    """Return the keys-only dlt resource for one table of ``spec``."""
    resolved_profile = profile or config.active_profile()
    name = keyset_table_name(spec, table)
    resource = sql_table(
        credentials=_credentials_for(spec, resolved_profile),
        table=table.name,
        schema=spec.db_schema,
        included_columns=[table.key],
        chunk_size=DEFAULT_CHUNK_SIZE,
        backend="pyarrow",
        table_loader_class=KeysetTableLoader,
        reflection_level="full",
        # No connection, and no Vault lease, until extraction.
        defer_table_reflect=True,
        table_adapter_callback=remove_nullability_adapter,
        write_disposition="replace",
        engine_adapter_callback=(
            _vault_credential_injector(spec, resolved_profile)
            if _uses_vault(resolved_profile)
            else None
        ),
    )
    return resource.with_name(name).apply_hints(
        table_name=name,
        table_format=config.active_table_format(resolved_profile),
        columns={**config.DLT_LOAD_ID_COLUMN, **_READ_STARTED_AT_HINT},
    )


def build_keyset_source(
    spec: DatabaseSourceSpec,
    tables: Sequence[KeysetTable],
    *,
    profile: str | None = None,
) -> Any:  # noqa: ANN401
    """Return the ``@dlt.source`` reading the keysets of ``tables`` from ``spec``."""
    if not tables:
        msg = f"{spec.name}: no keyset tables given"
        raise ValueError(msg)

    @dlt.source(name=f"{spec.name}_keyset")
    def _source() -> Generator[Any]:
        for table in tables:
            yield build_keyset_resource(spec, table, profile=profile)

    return _source()


def keyset_pipeline_for(
    spec: DatabaseSourceSpec, *, profile: str | None = None
) -> dlt.Pipeline:
    """Return the keyset pipeline for ``spec``, apart from the loader's."""
    return config.pipeline_for(f"{spec.name}_keyset", profile=profile)


def inventory_keyset_tables() -> Mapping[str, tuple[KeysetTable, ...]]:
    """Return the rendered keyset tables, by inventory unit (``deployment/layer``)."""
    rendered = json.loads(
        resources.files(__package__).joinpath("tables.json").read_text()
    )
    return {
        unit: tuple(KeysetTable(name=row["table"], key=row["key"]) for row in rows)
        for unit, rows in rendered["units"].items()
    }


def build_unit_keyset_source(unit: str, *, profile: str | None = None) -> Any:  # noqa: ANN401
    """Return the keyset source for inventory unit ``unit``."""
    return build_keyset_source(
        KEYSET_SPECS[unit], inventory_keyset_tables()[unit], profile=profile
    )


def unit_keyset_pipeline(unit: str) -> dlt.Pipeline:
    """Return the keyset pipeline for inventory unit ``unit``."""
    return keyset_pipeline_for(KEYSET_SPECS[unit])
