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
    JOIN_COLS,
    build_embedding_client,
    embed_and_checkpoint,
    filter_unembedded,
    resolve_embedding_text,
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
    embedding_model_version: str | None = Field(
        default=None,
        description=(
            "Override the embedding model id for this run. Unset uses "
            "EMBEDDING_MODEL_VERSION (ml.lib.embed)."
        ),
    )
    embedding_dim: int | None = Field(
        default=None,
        description=(
            "Override the embedding vector dimension for this run. Unset uses "
            "EMBEDDING_DIM (ml.lib.embed)."
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
    client = build_embedding_client(
        embedding_llm, config.embedding_model_version, config.embedding_dim
    )
    unembedded_df = filter_unembedded(
        resolved_df,
        already_embedded_df,
        current_model_version=client.model_version,
        current_dim=client.dim,
    )
    if config.sample_limit is not None:
        unembedded_df = unembedded_df.head(config.sample_limit)

    errors: list[str] = []
    catalog = get_glue_catalog()
    table_identifier = f"{database_name}.feedback_embeddings"
    embeddings_df = embed_and_checkpoint(
        unembedded_df,
        client,
        (catalog, table_identifier),
        errors=errors,
    )

    # A null resolved_text or a failed API call is dropped from embeddings_df
    # entirely, so this difference is exactly the failure count -- including rows
    # never attempted because of an early abort.
    dropped_count = unembedded_df.height - embeddings_df.height
    # len(errors) rather than dropped_count: a null resolved_text is dropped
    # without an API call or an error message, so dropped_count alone would
    # overstate how many embed calls actually ran.
    attempted_count = embeddings_df.height + len(errors)

    context.log.info(
        "Embedded %d conversations (%d already embedded, %d total upstream, "
        "%d dropped)",
        embeddings_df.height,
        already_embedded_df.height,
        resolved_df.height,
        dropped_count,
    )

    # 100% failure would otherwise look identical to "nothing new to embed".
    if attempted_count > 0 and embeddings_df.height == 0:
        sample_errors = "; ".join(errors[:3])
        msg = (
            f"All {attempted_count} attempted embedding calls failed; the "
            f"embedding client/credential is likely misconfigured. Sample "
            f"errors: {sample_errors}"
        )
        raise Failure(msg)

    if dropped_count:
        context.log.warning(
            "%d conversation(s) were not embedded this run; will be retried on the "
            "next upstream/code-triggered run",
            dropped_count,
        )

    context.add_output_metadata(
        {
            "embedding_model_version": MetadataValue.text(client.model_version),
            "embedding_dim": MetadataValue.int(client.dim),
            "embedded_count": MetadataValue.int(embeddings_df.height),
            "dropped_count": MetadataValue.int(dropped_count),
            "already_embedded_count": MetadataValue.int(already_embedded_df.height),
            "total_upstream_count": MetadataValue.int(resolved_df.height),
        }
    )

    return embeddings_df
