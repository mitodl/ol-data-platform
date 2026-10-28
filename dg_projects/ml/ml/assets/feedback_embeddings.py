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
from ml.lib.embed import (
    EMBEDDING_BATCH_SIZE,
    EMBEDDING_MAX_CONCURRENCY,
    JOIN_COLS,
    UPSERT_JOIN_COLS,
    embed_and_checkpoint,
    filter_unembedded,
    has_embeddable_text,
    resolve_embedding_model,
    resolve_embedding_text,
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


class FeedbackEmbeddingsConfig(Config):
    full_refresh: bool = Field(
        default=False,
        description="Re-embed every eligible conversation, not only new ones.",
    )
    sample_limit: int | None = Field(
        default=None,
        description=(
            "Cap the number of upstream rows read, for fast local testing -- "
            "matches feedback_summaries' sample_limit semantics. Applied "
            "before the incremental filter, so a repeated run may re-select "
            "already-embedded rows if they're within this cap."
        ),
    )
    embedding_model: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9-]+:.+$",
        description=(
            "The embedding model in ol-llm's provider:model form, e.g. "
            "'bedrock:amazon.titan-embed-text-v2:0', 'openai:text-embedding-3-large' "
            "or 'google-cloud:gemini-embedding-001'. Its vectors are stored under "
            "the id without the provider, next to other models' rows. Unset uses "
            "EMBEDDING_MODEL (ml.lib.embed). Readers of feedback_embeddings take "
            "the same value."
        ),
    )
    embedding_dim: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Override the embedding vector dimension for this run. Unset uses "
            "EMBEDDING_DIM (ml.lib.embed)."
        ),
    )
    batch_size: int = Field(
        default=EMBEDDING_BATCH_SIZE,
        ge=1,
        description=(
            "About how many conversations are embedded and checkpointed together. "
            "A larger value means fewer, cheaper checkpoint commits. Unset uses "
            "EMBEDDING_BATCH_SIZE (ml.lib.embed)."
        ),
    )
    max_concurrency: int = Field(
        default=EMBEDDING_MAX_CONCURRENCY,
        ge=1,
        description=(
            "How many embedding requests run at once -- the lever for wall-clock "
            "time with a model like Bedrock Titan that takes one text per request. "
            "Unset uses EMBEDDING_MAX_CONCURRENCY (ml.lib.embed)."
        ),
    )


@asset(
    code_version="feedback_embeddings_v5",
    group_name="feedback",
    key=AssetKey(["intermediate", "feedback_embeddings"]),
    deps=[
        AssetKey(["intermediate", "feedback_summaries"]),
        AssetKey(["intermediate", "int__feedback__conversation"]),
    ],
    automation_condition=upstream_or_code_changes(),
    io_manager_key="io_manager",
    pool="feedback_embeddings",
    metadata={
        "schema": database_name,
        "write_mode": "upsert",
        "upsert_options": {"join_cols": UPSERT_JOIN_COLS},
        "schema_update_mode": "update",
    },
)
def feedback_embeddings(
    context: AssetExecutionContext,
    config: FeedbackEmbeddingsConfig,
) -> pl.DataFrame:
    """
    Embed each conversation once, per its feedback_summaries embedding_input arm.

    One shared vector per conversation (feedback_ml_approach.md §B) -- computed off
    the LLM summary where feedback_summaries produced one, off the redacted
    concatenated turns otherwise. Runs after feedback_summaries so the arm decision
    is already made; a conversation feedback_summaries hasn't reached yet (upstream
    still processing) is simply absent here and picked up next run. A conversation
    whose text changed since it was embedded is embedded again.
    """
    summaries_lazy = get_dbt_model_as_dataframe(
        database_name=database_name,
        table_name="feedback_summaries",
    )
    if config.sample_limit is not None:
        summaries_lazy = summaries_lazy.limit(config.sample_limit)
    summaries_df = summaries_lazy.collect()

    conversation_lazy = get_dbt_model_as_dataframe(
        database_name=database_name,
        table_name="int__feedback__conversation",
    )
    if config.sample_limit is not None:
        # Restricted to the (already-limited) summaries_df's own pks, not
        # independently limited -- resolve_embedding_text's join needs both
        # sides to cover the same conversations, or a sample run resolves
        # almost nothing.
        conversation_lazy = conversation_lazy.filter(
            pl.col("feedback_conversation_pk").is_in(
                summaries_df["feedback_conversation_pk"]
            )
        )
    conversation_df = conversation_lazy.collect()
    resolved_df = resolve_embedding_text(summaries_df, conversation_df)

    model = resolve_embedding_model(config.embedding_model, config.embedding_dim)

    already_embedded_df = pl.DataFrame(
        schema={
            **dict.fromkeys(JOIN_COLS, pl.String),
            "turn_count": pl.Int64,
            "embedding_input": pl.String,
            "idempotency_key": pl.String,
        }
    )
    if not config.full_refresh:
        with contextlib.suppress(NoSuchTableError):
            embedded_lazy = get_dbt_model_as_dataframe(
                database_name=database_name,
                table_name="feedback_embeddings",
            )
            # idempotency_key is a newer column -- a table upserted before it
            # existed won't have it until upsert_chunk's schema evolution next
            # runs, and filter_unembedded keeps main's rule for those rows.
            stored_key = (
                pl.col("idempotency_key")
                if "idempotency_key" in embedded_lazy.collect_schema().names()
                else pl.lit(None, dtype=pl.String).alias("idempotency_key")
            )
            # Scoped to this model/dim, so a conversation with more than one
            # model's vector doesn't fan out the pk-only join in filter_unembedded.
            already_embedded_df = (
                embedded_lazy.filter(model.matches())
                .select(*JOIN_COLS, "turn_count", "embedding_input", stored_key)
                .collect()
            )

    unembedded_df = filter_unembedded(resolved_df, already_embedded_df, model)
    no_text_count = sum(
        not has_embeddable_text(text) for text in unembedded_df["resolved_text"]
    )
    context.log.info(
        "Embedding %d conversations with %s at %d dimensions",
        unembedded_df.height - no_text_count,
        model.model,
        model.dim,
    )

    table_identifier = f"{database_name}.feedback_embeddings"
    try:
        embeddings_df, report = embed_and_checkpoint(
            unembedded_df,
            model,
            model.embedder(run_id=context.run.run_id),
            (get_glue_catalog(), table_identifier),
            batch_size=config.batch_size,
            max_concurrency=config.max_concurrency,
            log=context.log,
        )
    except BatchFailed as exc:
        # Every chunk before the stop is already upserted into the table.
        msg = f"feedback_embeddings stopped with model {model.model!r}: {exc}"
        raise Failure(msg) from exc

    failed_count = len(report.failures)
    context.log.info(
        "Embedded %d conversations (%d already embedded, %d total upstream, "
        "%d failed, %d with no text yet, %d input tokens)",
        embeddings_df.height,
        already_embedded_df.height,
        resolved_df.height,
        failed_count,
        no_text_count,
        report.input_tokens,
    )
    if failed_count:
        context.log.warning(
            "%d conversation(s) failed to embed this run; will retry next run. %s",
            failed_count,
            report.batch.describe(),
        )

    context.add_output_metadata(
        {
            "embedding_model": MetadataValue.text(model.model),
            "embedding_model_version": MetadataValue.text(model.model_version),
            "embedding_dim": MetadataValue.int(model.dim),
            "embedded_count": MetadataValue.int(embeddings_df.height),
            "failed_count": MetadataValue.int(failed_count),
            "no_text_count": MetadataValue.int(no_text_count),
            "input_tokens": MetadataValue.int(report.input_tokens),
            "already_embedded_count": MetadataValue.int(already_embedded_df.height),
            "total_upstream_count": MetadataValue.int(resolved_df.height),
        }
    )

    return embeddings_df
