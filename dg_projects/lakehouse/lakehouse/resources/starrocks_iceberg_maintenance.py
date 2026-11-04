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

from dagster import ConfigurableResource
from pydantic import Field as PydanticField
from pydantic import PrivateAttr
from pymysql import Connection
from pymysql.cursors import DictCursor
from pymysql.err import OperationalError

from lakehouse.resources.starrocks import (
    _MAX_ATTEMPTS,
    _RETRIABLE_ERRORS,
    _RETRY_BASE_DELAY,
    StarRocksResource,
)

log = logging.getLogger(__name__)

BYTES_PER_MB = 1024 * 1024


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

    def _qualified(self, schema: str, table: str) -> str:
        return f"`{self.catalog}`.`{schema}`.`{table}`"

    def _fetch(
        self, sql: str, params: tuple[str, ...] | None = None
    ) -> list[dict[str, Any]]:
        """Run *sql* on the shared connection and return its rows.

        A transient error drops the connection and retries on a new one with
        fresh credentials, as `StarRocksResource.execute` does. Re-sending a
        rewrite that died partway is safe: an Iceberg commit is atomic, so the
        first attempt either replaced the files or changed nothing.
        """
        last_exc: OperationalError | None = None
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
            try:
                if self._connection is None:
                    self._connection = self.starrocks.connect()
                with self._connection.cursor() as cursor:
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                self._connection.commit()
            except OperationalError as exc:
                if exc.args[0] not in _RETRIABLE_ERRORS:
                    raise
                last_exc = exc
                self._connection = None
                continue
            return list(rows)
        raise last_exc  # type: ignore[misc]  # loop always sets last_exc before falling through

    def delete_file_count(self, schema: str, table: str) -> int:
        """Return how many position and equality delete files the table holds."""
        files = f"`{self.catalog}`.`{schema}`.`{table}$files`"
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
        """
        if self.delete_file_count(schema, table):
            args = "rewrite_all => true"
        else:
            args = f"min_file_size_bytes => {file_size_mb * BYTES_PER_MB}"
        rows = self._fetch(
            f"ALTER TABLE {self._qualified(schema, table)} "
            f"EXECUTE rewrite_data_files({args})"
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

    def base_tables_in_schemas(self, schemas: set[str]) -> set[tuple[str, str]]:
        """Return the ``(schema, table)`` pairs that exist as Iceberg tables.

        Same contract as ``TrinoMaintenanceResource.base_tables_in_schemas``,
        but StarRocks lists an Iceberg view in an external catalog's
        ``information_schema.tables`` as a ``BASE TABLE`` too. Each listed
        relation is told apart by ``SHOW CREATE TABLE``, which answers a view
        with a ``Create View`` column; ``rewrite_data_files`` on a view fails
        with "Table of ICEBERG_VIEW catalog doesn't support [ALTER]".
        """
        if not schemas:
            return set()
        ordered = tuple(sorted(schemas))
        placeholders = ", ".join("%s" for _ in ordered)
        rows = self._fetch(
            # Only the %s placeholders are interpolated; the values are bound.
            "SELECT table_schema, table_name "  # noqa: S608
            f"FROM `{self.catalog}`.information_schema.tables "
            f"WHERE table_type = 'BASE TABLE' AND table_schema IN ({placeholders})",
            ordered,
        )
        listed = {(row["table_schema"], row["table_name"]) for row in rows}
        return {
            (schema, table)
            for schema, table in listed
            if "Create View"
            not in self._fetch(f"SHOW CREATE TABLE {self._qualified(schema, table)}")[0]
        }
