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

# Bounds each API call to this many conversations rather than one call per
# conversation
EMBEDDING_BATCH_SIZE = int(os.environ.get("EMBEDDING_BATCH_SIZE", "100"))

logger = logging.getLogger(__name__)


class EmbeddingClient(Protocol):
    model_version: str
    dim: int

    def embed_batch(self, texts: list[str]) -> list[list[float]]: ...


class OpenAIEmbeddingClient:
    """Adapts an OpenAI-compatible client to the EmbeddingClient protocol."""

    def __init__(self, client: OpenAI) -> None:
        self._client = client
        self.model_version = EMBEDDING_MODEL_VERSION
        self.dim = EMBEDDING_DIM

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        response = self._client.embeddings.create(
            model=self.model_version,
            input=texts,
            dimensions=self.dim,
        )
        # The API documents response order as matching input order, but sorting by
        # the returned index costs nothing and removes the risk of a silently
        # mismatched embedding-to-conversation pairing if that ever isn't true.
        ordered = sorted(response.data, key=lambda item: item.index)
        return [item.embedding for item in ordered]


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
    current_dim: int | None = None,
) -> pl.DataFrame:
    """Drop conversations already embedded with their current content, model, and dim.

    Re-embeds on a turn_count change (embedding_input's arm usually stays the same
    even though resolved_text changed, so that alone can't catch it), an
    embedding_input arm change, a stale embedding_model_version, or a stale
    embedding_dim (a dimension sweep on the same model).
    """
    already_embedded_cols = [*JOIN_COLS, "turn_count", "embedding_input"]
    has_model_version_col = "embedding_model_version" in already_embedded_df.columns
    check_model_version = current_model_version is not None and has_model_version_col
    if check_model_version:
        already_embedded_cols.append("embedding_model_version")
    has_dim_col = "embedding_dim" in already_embedded_df.columns
    check_dim = current_dim is not None and has_dim_col
    if check_dim:
        already_embedded_cols.append("embedding_dim")

    rename_map = {
        "turn_count": "turn_count_embedded",
        "embedding_input": "embedding_input_embedded",
    }
    if check_model_version:
        rename_map["embedding_model_version"] = "embedding_model_version_embedded"
    if check_dim:
        rename_map["embedding_dim"] = "embedding_dim_embedded"
    already_embedded_selected = already_embedded_df.select(
        already_embedded_cols
    ).rename(rename_map)

    joined = source_df.join(already_embedded_selected, on=JOIN_COLS, how="left")
    is_new_or_changed = pl.col("turn_count_embedded").is_null() | (
        pl.col("turn_count") != pl.col("turn_count_embedded")
    )
    is_new_or_changed = is_new_or_changed | (
        pl.col("embedding_input") != pl.col("embedding_input_embedded")
    )
    if check_model_version:
        is_new_or_changed = is_new_or_changed | (
            pl.col("embedding_model_version_embedded").is_not_null()
            & (pl.col("embedding_model_version_embedded") != current_model_version)
        )
    if check_dim:
        is_new_or_changed = is_new_or_changed | (
            pl.col("embedding_dim_embedded").is_not_null()
            & (pl.col("embedding_dim_embedded") != current_dim)
        )
    return joined.filter(is_new_or_changed).select(source_df.columns)


def _embed_chunk(
    chunk: list[dict[str, Any]], client: EmbeddingClient
) -> list[tuple[dict[str, Any], list[float]]]:
    """Embed one chunk via a single batched API call, falling back row-by-row.

    A single bad row (e.g. a length/encoding issue the API rejects) fails the whole
    batch call -- retrying one at a time isolates it rather than dropping every
    otherwise-fine row in the chunk along with it.
    """
    try:
        vectors = client.embed_batch([row["resolved_text"] for row in chunk])
    except Exception:
        logger.warning(
            "Batch embed failed for %d conversations; retrying individually",
            len(chunk),
            exc_info=True,
        )
        results = []
        for row in chunk:
            try:
                vector = client.embed_batch([row["resolved_text"]])[0]
            except Exception:
                logger.warning(
                    "Failed to embed conversation %s/%s; will retry next run",
                    row["source_slug"],
                    row["conversation_ref"],
                    exc_info=True,
                )
                continue
            results.append((row, vector))
        return results
    return list(zip(chunk, vectors, strict=True))


def embed_conversations(df: pl.DataFrame, client: EmbeddingClient) -> pl.DataFrame:
    """Embed each conversation's resolved_text, in bounded batches.

    Args:
        df: a frame with (at least) source_slug, conversation_ref, turn_count,
            embedding_input, resolved_text columns, e.g. resolve_embedding_text's
            output.
        client: an object with an `embed_batch(texts: list[str]) -> list[list[float]]`
            method, e.g. an OpenAIEmbeddingClient wrapping LLMClientFactory.

    Returns:
        pl.DataFrame: source_slug, conversation_ref, turn_count, embedding_vector,
            embedding_dim, embedding_model_version, embedding_input - keyed the same
            way feedback_conversation_pk is minted, for afact_feedback_conversation to
            left-join. turn_count is carried through so a later run's filter_unembedded
            can detect a conversation that gained a turn. A null resolved_text
            (upstream summary/redaction not ready yet) is skipped and retried next
            run, same as a failed API call (#2542's checkpointing precedent).
    """
    rows = [row for row in df.to_dicts() if row["resolved_text"] is not None]

    source_slugs: list[str] = []
    conversation_refs: list[str] = []
    turn_counts: list[int] = []
    embedding_inputs: list[str] = []
    vectors: list[list[float]] = []
    for chunk_start in range(0, len(rows), EMBEDDING_BATCH_SIZE):
        chunk = rows[chunk_start : chunk_start + EMBEDDING_BATCH_SIZE]
        for row, vector in _embed_chunk(chunk, client):
            source_slugs.append(row["source_slug"])
            conversation_refs.append(row["conversation_ref"])
            turn_counts.append(row["turn_count"])
            embedding_inputs.append(row["embedding_input"])
            vectors.append(vector)

    return pl.DataFrame(
        {
            "source_slug": pl.Series(source_slugs, dtype=pl.String),
            "conversation_ref": pl.Series(conversation_refs, dtype=pl.String),
            "turn_count": pl.Series(turn_counts, dtype=pl.Int64),
            "embedding_input": pl.Series(embedding_inputs, dtype=pl.String),
        }
    ).with_columns(
        # Float32, not the Series default of Float64: issue #2543 and
        # afact_feedback_conversation's embedding_vector column both specify a
        # single-precision Iceberg ARRAY<float> -- Float64 would double storage and
        # commit a different physical type than requested.
        pl.Series("embedding_vector", vectors, dtype=pl.List(pl.Float32)),
        pl.lit(client.dim, dtype=pl.Int64).alias("embedding_dim"),
        pl.lit(client.model_version, dtype=pl.String).alias("embedding_model_version"),
    )
