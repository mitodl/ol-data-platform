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
    SUMMARY_PROMPT,
    SUMMARY_PROMPT_NAME,
    build_summary_client,
    filter_unsummarized,
    summarize_and_checkpoint,
)
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import get_prompt_version
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
        description="Cap the number of upstream rows read, for fast local testing.",
    )
    feedback_since: str | None = Field(
        default=DEFAULT_FEEDBACK_SINCE,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description=(
            "Only summarize conversations opened on or after this date (YYYY-MM-DD). "
            "Rows already in feedback_summaries from earlier runs are kept. Defaults "
            "to feedback_clusters' feedback_since, so both cover the same range. "
            "Unset or null reads the full history."
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
    model_version: str | None = Field(
        default=None,
        description=(
            "Override the model id sent to the anthropic/openai/openai_compatible/"
            "azure_openai client classes, e.g. to try a different model's cost/"
            "quality without a code change. Unset uses SUMMARY_MODEL_VERSION "
            "(ml.lib.summarize). Ignored when the llm resource's client_class is "
            "'bedrock' -- see bedrock_model_version."
        ),
    )
    bedrock_model_version: str | None = Field(
        default=None,
        description=(
            "Same as model_version, but for client_class='bedrock' -- Bedrock has "
            "its own model/inference-profile id namespace (e.g. "
            "'global.anthropic.claude-haiku-4-5-20251001-v1:0'), never a plain "
            "Anthropic API id. Unset uses BEDROCK_SUMMARY_MODEL_VERSION."
        ),
    )
    max_concurrency: int = Field(
        default=SUMMARIZE_MAX_CONCURRENCY,
        ge=1,
        description=(
            "How many summarize() calls run at once -- each is an independent "
            "blocking network request, so this is the lever for wall-clock time "
            "at scale. Unset uses SUMMARIZE_MAX_CONCURRENCY (ml.lib.summarize)."
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
    code_version="feedback_summaries_v3",
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
    llm: LLMClientFactory,
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
            "prompt_version": pl.String,
        }
    )
    if not config.full_refresh:
        with contextlib.suppress(NoSuchTableError):
            already_summarized_lazy = get_dbt_model_as_dataframe(
                database_name=database_name,
                table_name="feedback_summaries",
            )
            # prompt_version is a newer column -- a table upserted before it
            # existed won't have it until the asset's own schema-evolution step
            # (checkpoint_chunk) next runs; select only what's actually there.
            select_cols = [*JOIN_COLS, "turn_count", "summary_model_version"]
            if "prompt_version" in already_summarized_lazy.collect_schema().names():
                select_cols.append("prompt_version")
            already_summarized_df = already_summarized_lazy.select(
                select_cols
            ).collect()

    # Built before filtering: filter_unsummarized needs the model/prompt actually
    # in use to re-submit a conversation whose stored summary_model_version or
    # prompt_version has since gone stale, not just a turn_count change.
    client = build_summary_client(
        llm, config.model_version, config.bedrock_model_version
    )
    unsummarized_df = filter_unsummarized(
        source_df,
        already_summarized_df,
        current_model_version=client.model_version,
        current_prompt_version=get_prompt_version(SUMMARY_PROMPT_NAME, SUMMARY_PROMPT),
    )

    errors: list[str] = []
    catalog = get_glue_catalog()
    table_identifier = f"{database_name}.feedback_summaries"
    summaries_df = summarize_and_checkpoint(
        unsummarized_df,
        client,
        (catalog, table_identifier),
        batch_size=config.batch_size,
        errors=errors,
        max_concurrency=config.max_concurrency,
        context=context,
    )

    llm_call_count = summaries_df.filter(
        pl.col("summary_model_version").is_not_null()
    ).height
    # A failed conversation is dropped from summaries_df entirely (unlike a
    # row with no text, which is kept with a null summary), so this
    # difference is exactly the failure count -- including rows never attempted
    # because of an early abort.
    failed_count = unsummarized_df.height - summaries_df.height
    # len(errors) rather than failed_count: on an early abort, failed_count also
    # counts never-attempted rows, overstating how many calls actually ran.
    attempted_count = llm_call_count + len(errors)

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

    # 100% failure would otherwise look identical to "nothing new to summarize".
    if attempted_count > 0 and llm_call_count == 0:
        sample_errors = "; ".join(errors[:3])
        msg = (
            f"All {attempted_count} attempted LLM calls failed via "
            f"client_class={llm.client_class!r}, "
            f"model_version={client.model_version!r}, base_url={llm.base_url!r} -- "
            f"check these resolved to what you intended (a stale/mismatched "
            f"client_class is a common cause). Sample errors: {sample_errors}"
        )
        raise Failure(msg)

    if failed_count:
        context.log.warning(
            "%d conversation(s) failed to summarize this run; will retry next run",
            failed_count,
        )

    context.add_output_metadata(
        {
            "model_version": MetadataValue.text(client.model_version),
            "llm_call_count": MetadataValue.int(llm_call_count),
            "failed_count": MetadataValue.int(failed_count),
            "already_summarized_count": MetadataValue.int(already_summarized_df.height),
            "total_upstream_count": MetadataValue.int(source_df.height),
        }
    )

    return summaries_df
