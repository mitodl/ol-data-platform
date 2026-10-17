"""Warehouse reads, partitions and model clients shared by the courseware assets."""

import os

import polars as pl
from anthropic import Anthropic, AnthropicBedrock
from dagster import (
    DynamicPartitionsDefinition,
    Failure,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)
from ml.resources.llm import LLMClientFactory
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from pyiceberg.exceptions import NoSuchTableError

# The platforms int__courseware__extraction_sources reads.
COURSEWARE_PLATFORMS = ("mitxonline", "mitxpro", "residential")
COURSERUN_PARTITIONS = DynamicPartitionsDefinition(name="courseware_courserun")
# Platform is static and the run is dynamic, so the key space is their cross
# product. A combination with no source rows is never observed, which keeps it
# from being requested: extraction requires its extraction_text partition.
EXTRACTION_PARTITIONS = MultiPartitionsDefinition(
    {
        "platform": StaticPartitionsDefinition(list(COURSEWARE_PLATFORMS)),
        "courserun": COURSERUN_PARTITIONS,
    }
)
# The partition dimensions as columns, for the Iceberg IO manager's partition_expr.
EXTRACTION_PARTITION_EXPR = {
    "platform": "platform",
    "courserun": "courserun_readable_id",
}


def warehouse_database(layer: str) -> str:
    """Return the Glue database the courseware assets read and write for a layer.

    Mirrors feedback_redacted.py and the courseware_intermediate dbt source: dev
    writes to the production catalog under a personal schema suffix.

    :param layer: e.g. intermediate or dimensional
    :returns: the Glue database name
    :rtype: str
    """
    if DAGSTER_ENV == "dev":
        return f"ol_warehouse_production_{os.environ['DBT_SCHEMA_SUFFIX']}_{layer}"
    return f"ol_warehouse_{DAGSTER_ENV}_{layer}"


def read_table(layer: str, table: str) -> pl.LazyFrame | None:
    """Scan a warehouse table, or return None when it does not exist yet.

    :param layer: the warehouse layer, e.g. intermediate
    :param table: the table name
    :returns: the table, lazily
    :rtype: pl.LazyFrame | None
    """
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


def latest_run_rows(
    layer: str, table: str, run_column: str, run_id: str | None
) -> pl.DataFrame | None:
    """Return one run's rows of a per-run snapshot table, or None for no run."""
    rows = read_table(layer, table)
    if rows is None or run_id is None:
        return None
    return rows.filter(pl.col(run_column) == run_id).collect()


def partition_courserun(key: MultiPartitionKey) -> tuple[str, str]:
    """Return (platform, courserun_readable_id) for an extraction partition."""
    dimensions = key.keys_by_dimension
    return dimensions["platform"], dimensions["courserun"]


def anthropic_client(
    llm: LLMClientFactory,
    model_id: str | None,
    *,
    default_model: str,
    default_bedrock_model: str,
) -> tuple[Anthropic | AnthropicBedrock, str]:
    """Return the llm resource's client and the model id to send it.

    The courseware library calls messages.stream with tool use and output_config,
    which only the Anthropic and Bedrock clients take; any other client_class fails
    here instead of on the first call.

    :param llm: the llm resource
    :param model_id: the run's override, in the client's own id namespace
    :param default_model: the first-party API default
    :param default_bedrock_model: the Bedrock inference profile default
    :returns: the client and the model id
    :rtype: tuple[Anthropic | AnthropicBedrock, str]
    """
    client = llm.get_client()
    if isinstance(client, AnthropicBedrock):
        return client, model_id or default_bedrock_model
    if isinstance(client, Anthropic):
        return client, model_id or default_model
    msg = (
        f"The courseware assets need an Anthropic or Bedrock client, but the llm "
        f"resource has client_class={llm.client_class!r}."
    )
    raise TypeError(msg)
