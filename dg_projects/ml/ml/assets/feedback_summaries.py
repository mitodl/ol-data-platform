import contextlib
import os

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    Failure,
    MetadataValue,
    asset,
)
from ml.lib.summarize import (
    JOIN_COLS,
    build_summary_client,
    filter_unsummarized,
    summarize_and_checkpoint,
)
from ml.resources.llm import LLMClientFactory
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import (
    get_dbt_model_as_dataframe,
)
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from pydantic import Field
from pyiceberg.exceptions import NoSuchTableError

if DAGSTER_ENV == "dev":
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    database_name = f"ol_warehouse_production_{_schema_suffix}_intermediate"
else:
    database_name = "ol_warehouse_production_intermediate"


class FeedbackSummariesConfig(Config):
    full_refresh: bool = Field(
        default=False,
        description="Re-summarize every eligible conversation, not only new ones.",
    )
    sample_limit: int | None = Field(
        default=None,
        description=(
            "Cap the number of conversations summarized, for local tests. Applied "
            "after the incremental filter, so repeated runs keep finding new "
            "candidates instead of re-hitting already-summarized rows."
        ),
    )


@asset(
    code_version="feedback_summaries_v1",
    group_name="feedback",
    key=AssetKey(["intermediate", "feedback_summaries"]),
    deps=[AssetKey(["intermediate", "int__feedback__conversation"])],
    automation_condition=upstream_or_code_changes(),
    io_manager_key="io_manager",
    pool="feedback_summaries",
    metadata={
        "schema": database_name,
        "write_mode": "upsert",
        "upsert_options": {"join_cols": JOIN_COLS},
    },
)
def feedback_summaries(
    context: AssetExecutionContext,
    config: FeedbackSummariesConfig,
    llm: LLMClientFactory,
) -> pl.DataFrame:
    """
    Summarize multi-turn conversations via LLM; skip single-turn/short ones (§A.1).

    The one per-record LLM call in the design - see feedback_ml_approach.md §A.1 for
    the skip threshold and measured cost.
    """
    source_df = get_dbt_model_as_dataframe(
        database_name=database_name,
        table_name="int__feedback__conversation",
    ).collect()

    already_summarized_df = pl.DataFrame(
        schema={
            **dict.fromkeys(JOIN_COLS, pl.String),
            "turn_count": pl.Int64,
            "summary_model_version": pl.String,
        }
    )
    if not config.full_refresh:
        with contextlib.suppress(NoSuchTableError):
            already_summarized_df = (
                get_dbt_model_as_dataframe(
                    database_name=database_name,
                    table_name="feedback_summaries",
                )
                .select([*JOIN_COLS, "turn_count", "summary_model_version"])
                .collect()
            )

    # Built before filtering: filter_unsummarized needs the model actually in use
    # to re-submit a conversation whose stored summary_model_version has since
    # gone stale (a model/prompt change), not just a turn_count change.
    client = build_summary_client(llm)
    unsummarized_df = filter_unsummarized(
        source_df, already_summarized_df, current_model_version=client.model_version
    )
    if config.sample_limit is not None:
        unsummarized_df = unsummarized_df.head(config.sample_limit)

    errors: list[str] = []
    catalog = get_glue_catalog()
    table_identifier = f"{database_name}.feedback_summaries"
    summaries_df = summarize_and_checkpoint(
        unsummarized_df,
        client,
        (catalog, table_identifier),
        errors=errors,
    )

    llm_call_count = summaries_df.filter(
        pl.col("summary_model_version").is_not_null()
    ).height
    # A failed conversation is dropped from summaries_df entirely (unlike a
    # skipped-by-length-rule row, which is kept with a null summary), so this
    # difference is exactly the failure count -- including rows never attempted
    # because of an early abort.
    failed_count = unsummarized_df.height - summaries_df.height
    # len(errors) rather than failed_count: on an early abort, failed_count also
    # counts never-attempted rows, overstating how many calls actually ran.
    attempted_count = llm_call_count + len(errors)

    context.log.info(
        "Processed %d conversations (%d LLM calls, %d skipped by the length rule, "
        "%d failed, %d already summarized, %d total upstream)",
        summaries_df.height,
        llm_call_count,
        summaries_df.height - llm_call_count,
        failed_count,
        already_summarized_df.height,
        source_df.height,
    )

    # 100% failure would otherwise look identical to "nothing new to summarize".
    if attempted_count > 0 and llm_call_count == 0:
        sample_errors = "; ".join(errors[:3])
        msg = (
            f"All {attempted_count} attempted LLM calls failed; the summary "
            f"client/credential is likely misconfigured. Sample errors: {sample_errors}"
        )
        raise Failure(msg)

    if failed_count:
        context.log.warning(
            "%d conversation(s) failed to summarize this run; will retry next run",
            failed_count,
        )

    context.add_output_metadata(
        {
            "llm_call_count": MetadataValue.int(llm_call_count),
            "failed_count": MetadataValue.int(failed_count),
            "already_summarized_count": MetadataValue.int(already_summarized_df.height),
            "total_upstream_count": MetadataValue.int(source_df.height),
        }
    )

    return summaries_df
