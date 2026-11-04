"""Compaction and statistics for dbt-built Iceberg tables, run on StarRocks.

The StarRocks counterpart of ``TrinoMaintenanceResource``, for when Galaxy is
gone and StarRocks writes the dbt layers into the Glue-backed Iceberg catalog.

Compaction is ``ALTER TABLE ... EXECUTE rewrite_data_files``. With
``min_file_size_bytes`` it rewrites only the data files under that size, and
drops a position delete only when the one data file it names was among them. A
delete against a file over the threshold is left in place for good, which is
every delete+insert model whose compacted file outgrows the threshold.
A table that holds any delete file is therefore rewritten whole
(``rewrite_all``), the one mode that drops every position and equality delete
the rewrite made dangling.

Statistics are ``ANALYZE TABLE``. StarRocks keeps them in its own
``_statistics_`` database rather than in the table, and also collects them on
its own when a query reads a column without any. The scheduled run collects
every column of a table ahead of the first query that needs them.

Every statement goes over one connection, opened on first use: ``execute`` and
``fetch`` on ``StarRocksResource`` open a connection, and so a new Vault
database user, per statement, and a nightly run sends several statements for
each of several hundred tables.
"""

import logging
import time
from typing import Any

from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field as PydanticField
from pydantic import PrivateAttr
from pymysql import Connection
from pymysql.cursors import DictCursor
from pymysql.err import MySQLError, OperationalError

from lakehouse.resources.starrocks import (
    _MAX_ATTEMPTS,
    _RETRIABLE_ERRORS,
    _RETRY_BASE_DELAY,
    StarRocksResource,
)

log = logging.getLogger(__name__)

BYTES_PER_MB = 1024 * 1024

# MySQL client-side error codes (CR_*). An error in this range was raised by
# pymysql about the connection, not reported by the server about a statement.
_CLIENT_ERRNOS = range(2000, 3000)
# The server numbers its errors from here; pymysql raises its own socket and
# protocol failures with 0 (InterfaceError) or a message and no number
# (InternalError "Packet sequence number wrong").
_FIRST_SERVER_ERRNO = 1000


def _errno(exc: MySQLError) -> int | None:
    return exc.args[0] if exc.args and isinstance(exc.args[0], int) else None


def _connection_survives(exc: MySQLError) -> bool:
    """Whether the connection *exc* was raised on can run the next statement.

    True only for an error the server reported about the statement (a
    ProgrammingError for 1064, an OperationalError for StarRocks' 5502, ...).
    Anything pymysql raised about the socket or the protocol leaves a
    connection that fails every statement after it.
    """
    errno = _errno(exc)
    return (
        errno is not None
        and errno >= _FIRST_SERVER_ERRNO
        and errno not in _CLIENT_ERRNOS
        and errno not in _RETRIABLE_ERRORS
    )


def _quote(identifier: str) -> str:
    return "`{}`".format(identifier.replace("`", "``"))


class StarRocksIcebergMaintenanceResource(
    ConfigurableResource["StarRocksIcebergMaintenanceResource"]
):
    """Runs Iceberg compaction and ANALYZE through a StarRocks external catalog."""

    starrocks: StarRocksResource
    catalog: str = PydanticField(
        description=(
            "StarRocks external Iceberg catalog holding the warehouse schemas, "
            "e.g. 'ol_data_lake_production'."
        )
    )

    # Quoted: pymysql.Connection is generic only to type checkers.
    _connection: "Connection[DictCursor] | None" = PrivateAttr(default=None)

    def teardown_after_execution(self, context: InitResourceContext) -> None:  # noqa: ARG002
        self._drop_connection()

    def _drop_connection(self) -> None:
        # pymysql's close() raises on a connection it already closed, and a
        # socket error leaves `open` False without marking it closed.
        if self._connection is not None and self._connection.open:
            self._connection.close()
        self._connection = None

    def _qualified(self, schema: str, table: str) -> str:
        return f"{_quote(self.catalog)}.{_quote(schema)}.{_quote(table)}"

    def _fetch(
        self,
        sql: str,
        params: tuple[str, ...] | None = None,
        *,
        retry_statement: bool = True,
    ) -> list[dict[str, Any]]:
        """Run *sql* on the shared connection and return its rows.

        A transient error drops the connection and retries on a new one with
        fresh credentials, as `StarRocksResource.execute` does. Pass
        ``retry_statement=False`` for a statement that must not be sent twice:
        it is then retried only when opening the connection failed, so it never
        reached the server.
        """
        last_exc: MySQLError | None = None
        for attempt in range(_MAX_ATTEMPTS):
            if attempt:
                delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
                log.warning(
                    "StarRocks error %s (attempt %d/%d) -- reconnecting in %ds",
                    last_exc,
                    attempt,
                    _MAX_ATTEMPTS,
                    delay,
                )
                time.sleep(delay)
            if self._connection is None:
                try:
                    self._connection = self.starrocks.connect()
                except OperationalError as exc:
                    if exc.args[0] not in _RETRIABLE_ERRORS:
                        raise
                    last_exc = exc
                    continue
            try:
                with self._connection.cursor() as cursor:
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                self._connection.commit()
            except MySQLError as exc:
                if not _connection_survives(exc):
                    self._drop_connection()
                if not retry_statement or _errno(exc) not in _RETRIABLE_ERRORS:
                    raise
                last_exc = exc
                continue
            return list(rows)
        raise last_exc  # type: ignore[misc]  # loop always sets last_exc before falling through

    def delete_file_count(self, schema: str, table: str) -> int:
        """Return how many position and equality delete files the table holds."""
        files = self._qualified(schema, f"{table}$files")
        rows = self._fetch(
            # Identifiers are backquoted; StarRocks binds no identifiers.
            f"SELECT count(*) AS delete_files FROM {files} WHERE `content` <> 0"  # noqa: S608
        )
        return int(rows[0]["delete_files"])

    def optimize(self, schema: str, table: str, file_size_mb: int = 512) -> None:
        """Compact the table's small data files and apply its delete files.

        Issues one of::

            ALTER TABLE <catalog>.<schema>.<table>
            EXECUTE rewrite_data_files(rewrite_all => true)

            ALTER TABLE <catalog>.<schema>.<table>
            EXECUTE rewrite_data_files(min_file_size_bytes => <file_size_mb MiB>)

        the first when the table holds any delete file (see the module
        docstring), the second otherwise.

        The ALTER is not re-sent after a lost connection. The FE may still be
        running the first rewrite, and a second one started beside it rewrites
        the same files concurrently.
        """
        # rewrite_all rewrites the whole table, however few rows were deleted.
        # StarRocks main after 4.1.7 (unreleased as of 2026-11-04) adds
        # delete_file_threshold and delete_ratio_threshold
        # (IcebergRewriteFileSelector), which rewrite only the data files the
        # deletes apply to. Switch to those after the cluster upgrade.
        if self.delete_file_count(schema, table):
            args = "rewrite_all => true"
        else:
            args = f"min_file_size_bytes => {file_size_mb * BYTES_PER_MB}"
        rows = self._fetch(
            f"ALTER TABLE {self._qualified(schema, table)} "
            f"EXECUTE rewrite_data_files({args})",
            retry_statement=False,
        )
        log.info("rewrite_data_files(%s) complete: %s.%s %s", args, schema, table, rows)

    def analyze(self, schema: str, table: str) -> None:
        """Collect StarRocks optimizer statistics for every column of the table.

        A synchronous ANALYZE that fails still returns a result row, with
        ``Msg_type`` ``error``, rather than raising.
        """
        rows = self._fetch(f"ANALYZE TABLE {self._qualified(schema, table)}")
        errors = [row["Msg_text"] for row in rows if row["Msg_type"] == "error"]
        if errors:
            msg = f"ANALYZE TABLE {schema}.{table} failed: {'; '.join(errors)}"
            raise RuntimeError(msg)
        log.info("ANALYZE complete: %s.%s", schema, table)

    def _is_view(self, schema: str, table: str) -> bool:
        rows = self._fetch(f"SHOW CREATE TABLE {self._qualified(schema, table)}")
        return "Create View" in rows[0]

    def base_tables_among(
        self, candidates: set[tuple[str, str]]
    ) -> set[tuple[str, str]]:
        """Return the *candidates* that exist as Iceberg tables.

        StarRocks lists an Iceberg view in an external catalog's
        ``information_schema.tables`` as a ``BASE TABLE`` too. Each listed
        candidate is told apart by ``SHOW CREATE TABLE``, which answers a view
        with a ``Create View`` column; ``rewrite_data_files`` on a view fails
        with "Table of ICEBERG_VIEW catalog doesn't support [ALTER]".

        Only candidates get a ``SHOW CREATE TABLE``, so ``__dbt_tmp`` and other
        relations no config names are never loaded. A candidate whose
        ``SHOW CREATE TABLE`` fails (dropped since the listing, or a catalog
        entry StarRocks cannot load) is left out with a warning, and the asset
        reports it with the other configs that have no table.
        """
        if not candidates:
            return set()
        schemas = tuple(sorted({schema for schema, _ in candidates}))
        placeholders = ", ".join("%s" for _ in schemas)
        rows = self._fetch(
            # Only the %s placeholders are interpolated; the values are bound.
            "SELECT table_schema, table_name "  # noqa: S608
            f"FROM {_quote(self.catalog)}.information_schema.tables "
            f"WHERE table_type = 'BASE TABLE' AND table_schema IN ({placeholders})",
            schemas,
        )
        listed = {(row["table_schema"], row["table_name"]) for row in rows}
        tables: set[tuple[str, str]] = set()
        for schema, table in sorted(listed & candidates):
            try:
                if not self._is_view(schema, table):
                    tables.add((schema, table))
            except MySQLError as exc:
                log.warning(
                    "Cannot load %s.%s, not maintaining it: %s", schema, table, exc
                )
        return tables
