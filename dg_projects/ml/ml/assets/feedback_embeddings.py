import contextlib
import os

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MetadataValue,
    asset,
)
from ml.lib.embed import (
    JOIN_COLS,
    build_embedding_client,
    embed_conversations,
    filter_unembedded,
    resolve_embedding_text,
)
from ml.resources.llm import LLMClientFactory
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import (
    get_dbt_model_as_dataframe,
)
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
            "Cap the number of conversations embedded, for local tests. Applied "
            "after the incremental filter, so repeated runs keep finding new "
            "candidates instead of re-hitting already-embedded rows."
        ),
    )


@asset(
    code_version="feedback_embeddings_v1",
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
        "upsert_options": {"join_cols": JOIN_COLS},
    },
)
def feedback_embeddings(
    context: AssetExecutionContext,
    config: FeedbackEmbeddingsConfig,
    embedding_llm: LLMClientFactory,
) -> pl.DataFrame:
    """
    Embed each conversation once, per its feedback_summaries embedding_input arm.

    One shared vector per conversation (feedback_ml_approach.md §B) -- computed off
    the LLM summary where feedback_summaries produced one, off the redacted
    concatenated turns otherwise. Runs after feedback_summaries so the arm decision
    is already made; a conversation feedback_summaries hasn't reached yet (upstream
    still processing) is simply absent here and picked up next run.
    """
    summaries_df = get_dbt_model_as_dataframe(
        database_name=database_name,
        table_name="feedback_summaries",
    ).collect()
    conversation_df = get_dbt_model_as_dataframe(
        database_name=database_name,
        table_name="int__feedback__conversation",
    ).collect()
    resolved_df = resolve_embedding_text(summaries_df, conversation_df)

    already_embedded_df = pl.DataFrame(
        schema={
            **dict.fromkeys(JOIN_COLS, pl.String),
            "turn_count": pl.Int64,
            "embedding_input": pl.String,
            "embedding_model_version": pl.String,
            "embedding_dim": pl.Int64,
        }
    )
    if not config.full_refresh:
        with contextlib.suppress(NoSuchTableError):
            already_embedded_df = (
                get_dbt_model_as_dataframe(
                    database_name=database_name,
                    table_name="feedback_embeddings",
                )
                .select(
                    [
                        *JOIN_COLS,
                        "turn_count",
                        "embedding_input",
                        "embedding_model_version",
                        "embedding_dim",
                    ]
                )
                .collect()
            )

    # Built before filtering: filter_unembedded needs the model/dim actually in use
    # to re-submit a conversation whose stored embedding_model_version or
    # embedding_dim has since gone stale (a model change or dimension sweep), not
    # just a turn_count or embedding_input change.
    client = build_embedding_client(embedding_llm)
    unembedded_df = filter_unembedded(
        resolved_df,
        already_embedded_df,
        current_model_version=client.model_version,
        current_dim=client.dim,
    )
    if config.sample_limit is not None:
        unembedded_df = unembedded_df.head(config.sample_limit)
    embeddings_df = embed_conversations(unembedded_df, client)

    # A null resolved_text or a failed API call inside embed_conversations silently
    # drops that row rather than raising, so the run itself still reports success --
    # this is the visible signal for a partial run, surfaced as metadata (queryable
    # from the asset catalog, not just buried in a log line) rather than failing the
    # whole materialization over what may be a single transient row.
    dropped_count = unembedded_df.height - embeddings_df.height

    context.log.info(
        "Embedded %d conversations (%d already embedded, %d total upstream, "
        "%d dropped)",
        embeddings_df.height,
        already_embedded_df.height,
        resolved_df.height,
        dropped_count,
    )
    if dropped_count:
        context.log.warning(
            "%d conversation(s) were not embedded this run; will be retried on the "
            "next upstream/code-triggered run",
            dropped_count,
        )

    context.add_output_metadata(
        {
            "embedded_count": MetadataValue.int(embeddings_df.height),
            "dropped_count": MetadataValue.int(dropped_count),
            "already_embedded_count": MetadataValue.int(already_embedded_df.height),
            "total_upstream_count": MetadataValue.int(resolved_df.height),
        }
    )

    return embeddings_df
