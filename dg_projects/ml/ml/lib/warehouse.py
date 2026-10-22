"""Reads of the warehouse tables the ml assets consume and write."""

import os

import polars as pl
from dagster import Failure
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.failures import permanent_failure
from ol_orchestrate.lib.glue_helper import (
    FIXTURE_DIR_VARIABLE,
    LOCAL_LAKE_VARIABLE,
    get_dbt_model_as_dataframe,
)
from pyiceberg.exceptions import NoSuchTableError


def warehouse_database(layer: str) -> str:
    """Return the Glue database the ml assets read and write for a layer.

    Mirrors feedback_redacted.py and the courseware_intermediate dbt source: dev
    writes to the production catalog under a personal schema suffix.

    :param layer: e.g. intermediate or dimensional
    :returns: the Glue database name
    :rtype: str
    """
    if DAGSTER_ENV == "dev":
        # docker-compose passes the variable through empty when it is unset,
        # which would name the shared ol_warehouse_production__<layer> database.
        suffix = os.environ["DBT_SCHEMA_SUFFIX"]
        if not suffix:
            msg = "DBT_SCHEMA_SUFFIX is empty; set it to your dev schema suffix"
            raise ValueError(msg)
        return f"ol_warehouse_production_{suffix}_{layer}"
    return f"ol_warehouse_{DAGSTER_ENV}_{layer}"


def read_table(layer: str, table: str) -> pl.LazyFrame | None:
    """Scan a warehouse table, or return None when it does not exist yet.

    :param layer: the warehouse layer, e.g. intermediate
    :param table: the table name
    :returns: the table, lazily
    :rtype: pl.LazyFrame | None
    :raises PermanentFailure: when a dev read redirect is set (see below)
    """
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
