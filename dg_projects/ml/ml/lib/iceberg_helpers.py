"""Small shared helpers for the feedback assets' Iceberg tables."""

import warnings

import polars as pl
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table.upsert_util import create_match_filter


def table_exists(catalog: Catalog, table_identifier: str) -> bool:
    """Whether table_identifier (e.g. '<database>.<table>') exists in the catalog."""
    try:
        catalog.load_table(table_identifier)
    except NoSuchTableError:
        return False
    return True


def upsert_chunk(
    catalog: Catalog,
    table_identifier: str,
    chunk_df: pl.DataFrame,
    join_cols: list[str],
) -> pl.DataFrame:
    """Write one checkpoint chunk to a table, replacing the rows with its keys.

    A crash after this call keeps everything written so far, so the next run's
    incremental filter sees it as done, with no separate recovery step.

    The table is created if it does not exist yet: a brand-new deployment (or a
    dropped dev table) has none, and a chunk, not the io_manager's write of the
    asset's return, is the first write of a run. A column chunk_df has and the
    table lacks is added, null on existing rows.

    The rows are replaced with a delete of the chunk's keys and an append, in one
    transaction, rather than Table.upsert. Adding a column commits a new schema
    but no snapshot, and pyiceberg 0.12's upsert reads the stored rows it matches
    at the main branch's snapshot (DataScan.use_ref), whose projection is that
    snapshot's older schema. get_rows_to_update then casts the chunk to those
    rows' schema and raises, so every chunk holding a stored key would fail until
    an all-new chunk happened to write a snapshot. The delete rewrites files
    projected to the current schema, so it works across a column addition.

    :param catalog: the Iceberg catalog.
    :param table_identifier: "database.table".
    :param chunk_df: the rows, at most one per key.
    :param join_cols: the columns that identify a row.
    :returns: chunk_df in the table's column order, as written. The io_manager's
        upsert of an asset's return casts by position, so a returned frame must
        be in this order.
    :rtype: pl.DataFrame
    :raises ValueError: if two rows share a key. Table.upsert refused those before
        writing anything; a delete and append would store both.
    """
    if chunk_df.height == 0:
        return chunk_df
    if chunk_df.select(join_cols).is_duplicated().any():
        msg = f"Chunk for {table_identifier} has duplicate rows for key {join_cols}"
        raise ValueError(msg)
    table = catalog.create_table_if_not_exists(
        table_identifier, schema=chunk_df.to_arrow().schema
    )
    with table.update_schema() as update:
        update.union_by_name(chunk_df.to_arrow().schema)
    # union_by_name appends new columns at the table's end regardless of chunk_df's
    # order, and the write casts by position. table.schema().fields gives the
    # top-level field names only; column_names also lists nested ones.
    ordered_df = chunk_df.select([field.name for field in table.schema().fields])
    rows = ordered_df.to_arrow()
    with table.transaction() as transaction, warnings.catch_warnings():
        # A chunk of new keys deletes nothing, which pyiceberg warns about.
        warnings.filterwarnings(
            "ignore", message="Delete operation did not match any records"
        )
        transaction.delete(create_match_filter(rows, join_cols))
        transaction.append(rows)
    return ordered_df
