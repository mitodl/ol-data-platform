"""Embedding of assembled/summarized feedback conversations."""

import logging
import os
from typing import Any, Protocol

import polars as pl
from ml.resources.llm import LLMClientFactory
from openai import OpenAI

JOIN_COLS = ["source_slug", "conversation_ref"]

# Safe baseline from feedback_ml_approach.md §B, pending the model bake-off. Anthropic
# has no embeddings API, so this must always resolve to an OpenAI-compatible model id,
# unlike SUMMARY_MODEL_VERSION which follows LLMClientFactory's own default provider.
EMBEDDING_MODEL_VERSION = os.environ.get(
    "EMBEDDING_MODEL_VERSION", "text-embedding-3-large"
)

# Matryoshka truncation via the API's own `dimensions` param (§B sweeps 256/512/1024).
# 1024 keeps clustering quality while cutting the 3072-dim default's storage 3x.
EMBEDDING_DIM = int(os.environ.get("EMBEDDING_DIM", "1024"))

logger = logging.getLogger(__name__)


class EmbeddingClient(Protocol):
    model_version: str
    dim: int

    def embed(self, text: str) -> list[float]: ...


class OpenAIEmbeddingClient:
    """Adapts an OpenAI-compatible client to the EmbeddingClient protocol."""

    def __init__(self, client: OpenAI) -> None:
        self._client = client
        self.model_version = EMBEDDING_MODEL_VERSION
        self.dim = EMBEDDING_DIM

    def embed(self, text: str) -> list[float]:
        response = self._client.embeddings.create(
            model=self.model_version,
            input=text,
            dimensions=self.dim,
        )
        return response.data[0].embedding


def build_embedding_client(llm: LLMClientFactory) -> OpenAIEmbeddingClient:
    client = llm.get_client()
    if not isinstance(client, OpenAI):
        # Anthropic has no embeddings endpoint at all -- unlike the summary asset,
        # there is no adapter to fall back to. The embedding_llm resource must be
        # configured with client_class='openai' or 'openai_compatible'.
        msg = (
            f"{type(client).__name__} has no embeddings API. Configure the "
            "embedding_llm resource's client_class as 'openai' or "
            "'openai_compatible'."
        )
        raise TypeError(msg)
    return OpenAIEmbeddingClient(client)


def resolve_embedding_text(
    summaries_df: pl.DataFrame, conversation_df: pl.DataFrame
) -> pl.DataFrame:
    """Pick each conversation's embedding input text per its embedding_input arm.

    Args:
        summaries_df: feedback_summaries output -- source_slug, conversation_ref,
            turn_count, conversation_summary, embedding_input.
        conversation_df: int__feedback__conversation -- source_slug, conversation_ref,
            conversation_text.

    Returns:
        pl.DataFrame: source_slug, conversation_ref, turn_count, embedding_input,
            resolved_text (conversation_summary where embedding_input == 'summary',
            else conversation_text). A conversation with no feedback_summaries row
            (skipped as short/single-turn, or not yet summarized) isn't emitted here.
    """
    joined = summaries_df.join(
        conversation_df.select([*JOIN_COLS, "conversation_text"]),
        on=JOIN_COLS,
        how="left",
    )
    return joined.with_columns(
        pl.when(pl.col("embedding_input") == "summary")
        .then(pl.col("conversation_summary"))
        .otherwise(pl.col("conversation_text"))
        .alias("resolved_text")
    ).select([*JOIN_COLS, "turn_count", "embedding_input", "resolved_text"])


def filter_unembedded(
    source_df: pl.DataFrame,
    already_embedded_df: pl.DataFrame,
    current_model_version: str | None = None,
) -> pl.DataFrame:
    """Drop conversations already embedded with their current input arm and model.

    Re-embeds a conversation whose embedding_input arm changed (e.g. a summary
    landed after the conversation was first embedded off concatenated_turns) or
    whose stored embedding_model_version is stale (a model change). Mirrors
    filter_unsummarized's turn_count check via embedding_input instead, since the
    embedding call operates on resolved_text, not turn_count directly.
    """
    already_embedded_cols = [*JOIN_COLS, "embedding_input"]
    has_model_version_col = "embedding_model_version" in already_embedded_df.columns
    check_model_version = current_model_version is not None and has_model_version_col
    if check_model_version:
        already_embedded_cols.append("embedding_model_version")

    already_embedded_selected = already_embedded_df.select(already_embedded_cols)
    if check_model_version:
        already_embedded_selected = already_embedded_selected.rename(
            {"embedding_model_version": "embedding_model_version_embedded"}
        )

    joined = source_df.join(
        already_embedded_selected,
        on=JOIN_COLS,
        how="left",
        suffix="_embedded",
    )
    is_new_or_changed = pl.col("embedding_input_embedded").is_null() | (
        pl.col("embedding_input") != pl.col("embedding_input_embedded")
    )
    if check_model_version:
        is_new_or_changed = is_new_or_changed | (
            pl.col("embedding_model_version_embedded").is_not_null()
            & (pl.col("embedding_model_version_embedded") != current_model_version)
        )
    return joined.filter(is_new_or_changed).select(source_df.columns)


def embed_conversations(df: pl.DataFrame, client: EmbeddingClient) -> pl.DataFrame:
    """Embed each conversation's resolved_text.

    Args:
        df: a frame with (at least) source_slug, conversation_ref, embedding_input,
            resolved_text columns, e.g. resolve_embedding_text's output.
        client: an object with an `embed(text: str) -> list[float]` method, e.g.
            an OpenAIEmbeddingClient wrapping LLMClientFactory.

    Returns:
        pl.DataFrame: source_slug, conversation_ref, embedding_vector, embedding_dim,
            embedding_model_version, embedding_input - keyed the same way
            feedback_conversation_pk is minted, for afact_feedback_conversation to
            left-join. A null resolved_text (upstream summary/redaction not ready
            yet) is skipped and retried next run, same as a failed API call
            (#2542's checkpointing precedent).
    """
    rows: list[dict[str, Any]] = df.to_dicts()
    source_slugs: list[str] = []
    conversation_refs: list[str] = []
    embedding_inputs: list[str] = []
    vectors: list[list[float]] = []
    for row in rows:
        text = row["resolved_text"]
        if text is None:
            continue
        try:
            vector = client.embed(text)
        except Exception:
            logger.warning(
                "Failed to embed conversation %s/%s; will retry next run",
                row["source_slug"],
                row["conversation_ref"],
                exc_info=True,
            )
            continue
        source_slugs.append(row["source_slug"])
        conversation_refs.append(row["conversation_ref"])
        embedding_inputs.append(row["embedding_input"])
        vectors.append(vector)

    return pl.DataFrame(
        {
            "source_slug": pl.Series(source_slugs, dtype=pl.String),
            "conversation_ref": pl.Series(conversation_refs, dtype=pl.String),
            "embedding_input": pl.Series(embedding_inputs, dtype=pl.String),
        }
    ).with_columns(
        pl.Series("embedding_vector", vectors, dtype=pl.List(pl.Float64)),
        pl.lit(client.dim, dtype=pl.Int64).alias("embedding_dim"),
        pl.lit(client.model_version, dtype=pl.String).alias("embedding_model_version"),
    )
