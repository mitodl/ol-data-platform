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
from ml.lib.cluster import (
    DEFAULT_FEEDBACK_SINCE,
    DEFAULT_MIN_CONVERSATION_CHARS_BY_SOURCE,
    DEFAULT_PLATFORMS,
    drop_short_conversations,
)
from ml.lib.summarize import (
    JOIN_COLS,
    SUMMARIZE_CHECKPOINT_BATCH_SIZE,
    SUMMARIZE_MAX_CONCURRENCY,
    SUMMARY_MODEL,
    Summarizer,
    fetch_summary_prompt,
    filter_unsummarized,
    summarize_and_checkpoint,
    summary_use_case,
)
from ol_llm import BatchFailed
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


# Summaries only: clusters see just what was summarized, so they keep the shared default
DEFAULT_SUMMARIES_FEEDBACK_SINCE = DEFAULT_FEEDBACK_SINCE or "2025-01-01"


class FeedbackSummariesConfig(Config):
    full_refresh: bool = Field(
        default=False,
        description="Re-summarize every eligible conversation, not only new ones.",
    )
    sample_limit: int | None = Field(
        default=None,
        description="Cap the number of upstream rows read, for fast local testing.",
    )
    feedback_since: str | None = Field(
        default=DEFAULT_SUMMARIES_FEEDBACK_SINCE,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description=(
            "Only summarize conversations opened on or after this date (YYYY-MM-DD). "
            "Rows already in feedback_summaries from earlier runs are kept. Defaults "
            "to the FEEDBACK_SINCE env var, else 2025-01-01. Null reads the full "
            "history."
        ),
    )
    platforms: list[str] | None = Field(
        default=DEFAULT_PLATFORMS,
        description=(
            "Only summarize conversations whose platform is in this list, e.g. "
            "['mitlearn']. Embeddings and clusters follow, because they read only "
            "what feedback_summaries holds. Set to null to include every platform, "
            "and conversations with no platform."
        ),
    )
    source_slugs: list[str] | None = Field(
        default=None,
        description=(
            "Only summarize conversations from these sources, e.g. ['zendesk']. "
            "Unset or null includes every source."
        ),
    )
    min_conversation_chars_by_source: dict[str, int] | None = Field(
        default=DEFAULT_MIN_CONVERSATION_CHARS_BY_SOURCE,
        description=(
            "Skip conversations shorter than this many characters, per source. The "
            "default drops tutor chats that are only a suggested-question button, "
            "such as 'What is this course about?'. Sources not listed have no "
            "minimum. Null skips none. Cluster steps always apply the default "
            "(DEFAULT_MIN_CONVERSATION_CHARS_BY_SOURCE), so change it there to change "
            "both."
        ),
    )
    model: str = Field(
        default=SUMMARY_MODEL,
        pattern=r"^[a-z0-9-]+:.+$",
        description=(
            "The model in ol-llm's provider:model form, e.g. "
            "'bedrock:us.anthropic.claude-haiku-5-5' or "
            "'anthropic:claude-sonnet-5', to try a different model's cost/quality "
            "without a code change. A different model re-summarizes every "
            "conversation summarized since the idempotency key, which includes the "
            "model; one summarized before it is redone only when its turns change. "
            "Unset uses SUMMARY_MODEL (ml.lib.summarize)."
        ),
    )
    max_concurrency: int = Field(
        default=SUMMARIZE_MAX_CONCURRENCY,
        ge=1,
        description=(
            "How many conversations are summarized at once -- each is an "
            "independent blocking request, so this is the lever for wall-clock "
            "time at scale. Unset uses SUMMARIZE_MAX_CONCURRENCY (ml.lib.summarize)."
        ),
    )
    batch_size: int = Field(
        default=SUMMARIZE_CHECKPOINT_BATCH_SIZE,
        ge=1,
        description=(
            "How many rows are summarized and checkpointed together. A larger "
            "value means fewer, cheaper checkpoint commits, at the cost of "
            "redoing more LLM calls on a mid-chunk crash. Unset uses "
            "SUMMARIZE_CHECKPOINT_BATCH_SIZE (ml.lib.summarize)."
        ),
    )


@asset(
    code_version="feedback_summaries_v7",
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
        "schema_update_mode": "update",
    },
)
def feedback_summaries(
    context: AssetExecutionContext,
    config: FeedbackSummariesConfig,
) -> pl.DataFrame:
    """
    Summarize every in-scope conversation that has text via LLM.

    The one per-record LLM call in the design. Scope comes from the config filters,
    including min_conversation_chars_by_source for sources with low-signal rows.
    """
    source_lazy = get_dbt_model_as_dataframe(
        database_name=database_name,
        table_name="int__feedback__conversation",
    )
    if config.feedback_since is not None:
        # conversation_opened_at is an ISO8601 string, so a YYYY-MM-DD prefix
        # compares correctly as text
        source_lazy = source_lazy.filter(
            pl.col("conversation_opened_at") >= config.feedback_since
        )
    if config.platforms is not None:
        source_lazy = source_lazy.filter(pl.col("platform").is_in(config.platforms))
    if config.source_slugs is not None:
        source_lazy = source_lazy.filter(
            pl.col("source_slug").is_in(config.source_slugs)
        )
    source_lazy = drop_short_conversations(
        source_lazy, config.min_conversation_chars_by_source
    )
    if config.sample_limit is not None:
        source_lazy = source_lazy.limit(config.sample_limit)
    source_df = source_lazy.collect()

    already_summarized_df = pl.DataFrame(
        schema={
            **dict.fromkeys(JOIN_COLS, pl.String),
            "turn_count": pl.Int64,
            "summary_model_version": pl.String,
        }
    )
    if not config.full_refresh:
        with contextlib.suppress(NoSuchTableError):
            already_summarized_lazy = get_dbt_model_as_dataframe(
                database_name=database_name,
                table_name="feedback_summaries",
            )
            # idempotency_key is a newer column -- a table upserted before it
            # existed won't have it until upsert_chunk's schema evolution
            # next runs; select only what's actually there.
            select_cols = [
                col
                for col in [
                    *JOIN_COLS,
                    "turn_count",
                    "summary_model_version",
                    "idempotency_key",
                ]
                if col in already_summarized_lazy.collect_schema().names()
            ]
            already_summarized_df = already_summarized_lazy.select(
                select_cols
            ).collect()

    # Fetched once: the whole run uses one prompt version, and its template hash
    # is part of every row's key.
    summarizer = Summarizer(
        use_case=summary_use_case(config.model),
        prompt=fetch_summary_prompt(),
        run_id=context.run_id,
    )
    unsummarized_df = filter_unsummarized(
        source_df, already_summarized_df, summarizer.keys
    )
    context.log.info(
        "Summarizing with %s and prompt %s %s (%s source)",
        config.model,
        summarizer.prompt.name,
        summarizer.prompt.version,
        summarizer.prompt.source,
    )

    table_identifier = f"{database_name}.feedback_summaries"
    try:
        summaries_df, report = summarize_and_checkpoint(
            unsummarized_df,
            summarizer,
            (get_glue_catalog(), table_identifier),
            batch_size=config.batch_size,
            max_concurrency=config.max_concurrency,
            log=context.log,
        )
    except BatchFailed as exc:
        # Every chunk before the stop is already upserted into the table.
        msg = f"feedback_summaries stopped with model {config.model!r}: {exc}"
        raise Failure(msg) from exc

    llm_call_count = report.succeeded
    failed_count = len(report.failures)
    context.log.info(
        "Processed %d conversations (%d LLM calls, %d skipped for no text, "
        "%d failed, %d already summarized, %d total upstream)",
        summaries_df.height,
        llm_call_count,
        summaries_df.height - llm_call_count,
        failed_count,
        already_summarized_df.height,
        source_df.height,
    )
    if failed_count:
        context.log.warning(
            "%d conversation(s) failed to summarize this run; will retry next run. %s",
            failed_count,
            report.describe(),
        )

    context.add_output_metadata(
        {
            "model": MetadataValue.text(config.model),
            "prompt_version": MetadataValue.text(str(summarizer.prompt.version)),
            "prompt_source": MetadataValue.text(summarizer.prompt.source),
            "llm_call_count": MetadataValue.int(llm_call_count),
            "failed_count": MetadataValue.int(failed_count),
            "already_summarized_count": MetadataValue.int(already_summarized_df.height),
            "total_upstream_count": MetadataValue.int(source_df.height),
        }
    )

    return summaries_df
