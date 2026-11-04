"""Reads of the warehouse tables the ml assets consume and write."""

import os
from collections.abc import Mapping, Sequence
from functools import reduce

import polars as pl
from dagster import Failure
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.failures import permanent_failure
from ol_orchestrate.lib.glue_helper import (
    FIXTURE_DIR_VARIABLE,
    LOCAL_LAKE_VARIABLE,
    get_dbt_model_as_dataframe,
    load_dbt_model_table,
)
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, EqualTo, In
from pyiceberg.io.pyarrow import ArrowScan


def warehouse_database(layer: str) -> str:
    """Return the Glue database the ml assets read and write for a layer.

    In dev, this is the production catalog under a personal schema suffix.

    :param layer: e.g. intermediate or dimensional
    :returns: the Glue database name
    :rtype: str
    """
    if DAGSTER_ENV == "dev":
        # docker-compose passes the variable through empty when it is unset,
        # which would name the shared ol_warehouse_production__<layer> database.
        suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
        if not suffix:
            msg = "DBT_SCHEMA_SUFFIX is not set; set it to your dev schema suffix"
            raise ValueError(msg)
        return f"ol_warehouse_production_{suffix}_{layer}"
    return f"ol_warehouse_{DAGSTER_ENV}_{layer}"


def _refuse_dev_redirects() -> None:
    # These redirect the read to fixtures or the local lake, while the assets that
    # use this always write to Glue. Each reads its own earlier output to know what
    # is stored, so a redirected read finds nothing and pays for the whole corpus
    # again.
    redirects = [
        variable
        for variable in (FIXTURE_DIR_VARIABLE, LOCAL_LAKE_VARIABLE)
        if DAGSTER_ENV == "dev" and os.environ.get(variable)
    ]
    if redirects:
        msg = (
            f"Unset {' and '.join(redirects)}: these assets write to Glue and must "
            "read what they stored from there."
        )
        raise permanent_failure(msg)


def read_table(layer: str, table: str) -> pl.LazyFrame | None:
    """Scan a warehouse table, or return None when it does not exist yet.

    :param layer: the warehouse layer, e.g. intermediate
    :param table: the table name
    :returns: the table, lazily
    :rtype: pl.LazyFrame | None
    :raises PermanentFailure: when a dev read redirect is set
    """
    _refuse_dev_redirects()
    try:
        return get_dbt_model_as_dataframe(
            database_name=warehouse_database(layer), table_name=table
        )
    except NoSuchTableError:
        return None


def require_table(layer: str, table: str) -> pl.LazyFrame:
    """Scan an upstream table this asset cannot run without.

    :raises Failure: when the table has not been built yet
    """
    rows = read_table(layer, table)
    if rows is None:
        msg = f"{warehouse_database(layer)}.{table} has not been built yet"
        raise Failure(msg)
    return rows


def load_matching_rows(
    database_name: str,
    table_name: str,
    columns: Sequence[str],
    equals: Mapping[str, str],
) -> pl.DataFrame:
    """Read the rows of an Iceberg table whose columns equal the given values.

    :param database_name: the Glue database
    :param table_name: the table
    :param columns: the columns to read
    :param equals: column values every returned row has, at least one
    :returns: the rows, with ``columns`` in the order given
    :rtype: pl.DataFrame
    :raises ValueError: when ``equals`` is empty
    """
    if not equals:
        msg = f"reading {database_name}.{table_name} needs at least one column value"
        raise ValueError(msg)
    scan = load_dbt_model_table(database_name, table_name).scan(
        row_filter=reduce(
            And, (EqualTo(column, value) for column, value in equals.items())
        ),
        selected_fields=tuple(columns),
    )
    # The scan returns the selected columns in the table's order.
    return pl.DataFrame(scan.to_arrow_batch_reader().read_all()).select(columns)


def load_rows_in(
    database_name: str,
    table_name: str,
    columns: Sequence[str],
    column: str,
    values: Sequence[str],
) -> pl.DataFrame:
    """Read the rows of an Iceberg table whose column is one of the values.

    pyiceberg reads every data file of a scan at the same time and filters each
    file only after reading it. When the values are spread across every file, as
    feedback_conversation_pk is, that holds most of the table in memory: reading
    the rated conversations' vectors of feedback_embeddings that way ran the
    sentiment assets out of memory. This reads the files one at a time, so memory
    holds one file and the rows kept so far.

    In dev with a fixture or local lake redirect set, it filters that table instead.

    :param database_name: the Glue database
    :param table_name: the table
    :param columns: the columns to read
    :param column: the column to match
    :param values: the values to keep
    :returns: the matching rows, with ``columns`` in the order given
    :rtype: pl.DataFrame
    """
    if DAGSTER_ENV == "dev" and (
        os.environ.get(FIXTURE_DIR_VARIABLE) or os.environ.get(LOCAL_LAKE_VARIABLE)
    ):
        return (
            get_dbt_model_as_dataframe(
                database_name=database_name, table_name=table_name
            )
            .filter(pl.col(column).is_in(list(values)))
            .select(columns)
            .collect()
        )
    table = load_dbt_model_table(database_name, table_name)
    row_filter = In(column, set(values))
    scan = table.scan(row_filter=row_filter, selected_fields=tuple(columns))
    reader = ArrowScan(table.metadata, table.io, scan.projection(), row_filter)
    frames = [
        pl.DataFrame(reader.to_table([task])).select(columns)
        for task in scan.plan_files()
    ]
    if not frames:
        return pl.DataFrame(scan.to_arrow()).select(columns)
    return pl.concat(frames)


def read_rows(
    layer: str, table: str, columns: Sequence[str], **equals: str
) -> pl.DataFrame | None:
    """Read a table's rows whose columns equal the given values, if it exists.

    Every equality goes into the Iceberg scan, which keeps only the matching
    rows and prunes the files of other partitions. polars does not do that for a
    filter on :func:`read_table`'s scan: of ``etl_source == "mitxonline" &
    run_readable_id == <run>`` it handed pyiceberg only the ``etl_source`` term,
    so the scan returned all 2.6M mitxonline rows of
    integrations__learn__content_files with their text, and reading one course
    run (2,279 rows) took 18.7 GB.

    :param layer: the warehouse layer
    :param table: the table name
    :param columns: the columns to read
    :param equals: column values every returned row has, at least one
    :returns: the rows, or None when the table does not exist yet
    :rtype: pl.DataFrame | None
    :raises PermanentFailure: when a dev read redirect is set
    """
    _refuse_dev_redirects()
    try:
        return load_matching_rows(warehouse_database(layer), table, columns, equals)
    except NoSuchTableError:
        return None


def require_rows(
    layer: str, table: str, columns: Sequence[str], **equals: str
) -> pl.DataFrame:
    """Read the rows of an upstream table this asset cannot run without.

    :param layer: the warehouse layer
    :param table: the table name
    :param columns: the columns to read
    :param equals: column values every returned row has, at least one
    :rtype: pl.DataFrame
    :raises Failure: when the table has not been built yet
    """
    rows = read_rows(layer, table, columns, **equals)
    if rows is None:
        msg = f"{warehouse_database(layer)}.{table} has not been built yet"
        raise Failure(msg)
    return rows
