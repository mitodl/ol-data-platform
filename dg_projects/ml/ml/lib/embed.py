"""Embedding of assembled/summarized feedback conversations."""

import logging
import os
from typing import Any, Protocol

import openai
import polars as pl
from ml.resources.llm import LLMClientFactory
from openai import OpenAI
from pyiceberg.catalog import Catalog

JOIN_COLS = ["feedback_conversation_pk"]

EMBEDDING_CHECKPOINT_SCHEMA = {
    **dict.fromkeys([*JOIN_COLS, "source_slug", "conversation_ref"], pl.String),
    "turn_count": pl.Int64,
    "embedding_input": pl.String,
    "embedding_vector": pl.List(pl.Float32),
    "embedding_dim": pl.Int64,
    "embedding_model_version": pl.String,
}

# Abort after this many whole chunks in a row come back with zero successful
# embeddings, same rationale as summarize.py's MAX_CONSECUTIVE_FAILED_CHUNKS: a
# systemic error (bad credential) isn't going to start succeeding on the next
# chunk either.
EMBEDDING_MAX_CONSECUTIVE_FAILED_CHUNKS = int(
    os.environ.get("EMBEDDING_MAX_CONSECUTIVE_FAILED_CHUNKS", "1")
)

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

    def __init__(self, client: OpenAI, model_version: str, dim: int) -> None:
        self._client = client
        self.model_version = model_version
        self.dim = dim

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


def build_embedding_client(
    llm: LLMClientFactory,
    model_version: str | None = None,
    dim: int | None = None,
) -> OpenAIEmbeddingClient:
    """Build the client whose model_version/dim come from run config, else a default.

    model_version/dim are the feedback_embeddings asset's own per-run Config
    fields (FeedbackEmbeddingsConfig) -- None means the run didn't override them,
    so EMBEDDING_MODEL_VERSION/EMBEDDING_DIM apply.
    """
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
    return OpenAIEmbeddingClient(
        client,
        model_version or EMBEDDING_MODEL_VERSION,
        dim or EMBEDDING_DIM,
    )


def resolve_embedding_text(
    summaries_df: pl.DataFrame, conversation_df: pl.DataFrame
) -> pl.DataFrame:
    """Pick each conversation's embedding input text per its embedding_input arm.

    Args:
        summaries_df: feedback_summaries output -- feedback_conversation_pk,
            source_slug, conversation_ref, turn_count, conversation_summary,
            embedding_input.
        conversation_df: int__feedback__conversation -- feedback_conversation_pk,
            source_slug, conversation_ref, conversation_text.

    Returns:
        pl.DataFrame: feedback_conversation_pk, source_slug, conversation_ref,
            turn_count, embedding_input, resolved_text (conversation_summary where
            embedding_input == 'summary', else conversation_text). A conversation
            with no feedback_summaries row (skipped as short/single-turn, or not
            yet summarized) isn't emitted here.
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
    ).select(
        [
            *JOIN_COLS,
            "source_slug",
            "conversation_ref",
            "turn_count",
            "embedding_input",
            "resolved_text",
        ]
    )


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
    chunk: list[dict[str, Any]],
    client: EmbeddingClient,
    errors: list[str] | None = None,
) -> list[tuple[dict[str, Any], list[float]]]:
    """Embed one chunk via a single batched API call, falling back row-by-row.

    A single bad row (e.g. a length/encoding issue the API rejects) fails the whole
    batch call as openai.BadRequestError -- retrying one at a time isolates it
    rather than dropping every otherwise-fine row in the chunk along with it.

    Any other exception (rate limit, auth, connection, 5xx) is systemic: retrying
    row-by-row would just multiply the same failure by len(chunk) rather than fix
    anything -- e.g. 100 extra calls at an endpoint that already asked us to back
    off (the OpenAI SDK's own retry/backoff is exhausted by the time an error
    surfaces here at all). So it's recorded as a single whole-chunk failure instead,
    letting the caller's consecutive-failed-chunks counter decide whether to abort.

    errors, if given, collects each failure's message -- lets a caller surface
    *why* calls failed (e.g. in a Failure message) without changing this
    function's return type.
    """
    try:
        vectors = client.embed_batch([row["resolved_text"] for row in chunk])
    except openai.BadRequestError:
        logger.warning(
            "Batch embed failed for %d conversations; retrying individually",
            len(chunk),
            exc_info=True,
        )
        results = []
        for row in chunk:
            try:
                vector = client.embed_batch([row["resolved_text"]])[0]
            except Exception as e:
                logger.warning(
                    "Failed to embed conversation %s/%s; will retry next run",
                    row["source_slug"],
                    row["conversation_ref"],
                    exc_info=True,
                )
                if errors is not None:
                    errors.append(
                        f"{row['source_slug']}/{row['conversation_ref']}: "
                        f"{type(e).__name__}: {e}"
                    )
                continue
            results.append((row, vector))
        return results
    except Exception as e:
        logger.warning(
            "Batch embed failed for %d conversations with a systemic error; "
            "not retrying individually",
            len(chunk),
            exc_info=True,
        )
        if errors is not None:
            errors.append(f"chunk of {len(chunk)}: {type(e).__name__}: {e}")
        return []
    return list(zip(chunk, vectors, strict=True))


def _results_to_df(
    results: list[tuple[dict[str, Any], list[float]]], client: EmbeddingClient
) -> pl.DataFrame:
    if not results:
        return pl.DataFrame(schema=EMBEDDING_CHECKPOINT_SCHEMA)
    feedback_conversation_pks = [row["feedback_conversation_pk"] for row, _ in results]
    source_slugs = [row["source_slug"] for row, _ in results]
    conversation_refs = [row["conversation_ref"] for row, _ in results]
    turn_counts = [row["turn_count"] for row, _ in results]
    embedding_inputs = [row["embedding_input"] for row, _ in results]
    vectors = [vector for _, vector in results]

    return pl.DataFrame(
        {
            "feedback_conversation_pk": pl.Series(
                feedback_conversation_pks, dtype=pl.String
            ),
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


def checkpoint_embedding_chunk(
    catalog: Catalog, table_identifier: str, chunk_df: pl.DataFrame
) -> None:
    """Upsert one chunk directly into the real feedback_embeddings table.

    Mirrors summarize.checkpoint_chunk: a crash after this call keeps everything
    upserted so far, so the next run's ordinary filter_unembedded pass sees it as
    already embedded, no separate recovery step needed. table_identifier is
    "database.table", e.g. "ol_warehouse_production_intermediate.feedback_embeddings".

    create_table_if_not_exists rather than load_table: a brand-new deployment (or a
    dropped dev table) has no feedback_embeddings table yet, and this call -- not
    the io_manager's write of the asset's final return -- is the first write of any
    run, so it must be able to bootstrap the table itself.
    """
    if chunk_df.height == 0:
        return
    table = catalog.create_table_if_not_exists(
        table_identifier, schema=chunk_df.to_arrow().schema
    )
    table.upsert(
        df=chunk_df.to_arrow(),
        join_cols=JOIN_COLS,
        when_matched_update_all=True,
        when_not_matched_insert_all=True,
    )


def embed_and_checkpoint(
    df: pl.DataFrame,
    client: EmbeddingClient,
    checkpoint_target: tuple[Catalog, str],
    batch_size: int = EMBEDDING_BATCH_SIZE,
    errors: list[str] | None = None,
) -> pl.DataFrame:
    """Embed df in chunks, upserting each into feedback_embeddings as it completes.

    Args:
        df: a frame with (at least) feedback_conversation_pk, source_slug,
            conversation_ref, turn_count, embedding_input, resolved_text columns,
            e.g. resolve_embedding_text's output. A null resolved_text (upstream
            summary/redaction not ready yet) is skipped and retried next run, same
            as a failed API call (#2542's checkpointing precedent).
        client: an object with an `embed_batch(texts: list[str]) -> list[list[float]]`
            method, e.g. an OpenAIEmbeddingClient wrapping LLMClientFactory.
        checkpoint_target: (catalog, table_identifier) passed through to
            checkpoint_embedding_chunk.
        batch_size: rows per embed_batch call and per checkpoint upsert.
        errors: if given, collects every failure's message (see _embed_chunk) so a
            caller can surface *why* calls failed, e.g. in a Failure message.

    Returns:
        pl.DataFrame: feedback_conversation_pk, source_slug, conversation_ref,
            turn_count, embedding_vector, embedding_dim, embedding_model_version,
            embedding_input - keyed by feedback_conversation_pk, for
            afact_feedback_conversation to left-join. turn_count is carried through
            so a later run's filter_unembedded can detect a conversation that
            gained a turn.

    Mirrors summarize.summarize_and_checkpoint.

    Stops the whole loop (not just the current chunk) after
    EMBEDDING_MAX_CONSECUTIVE_FAILED_CHUNKS chunks in a row come back with zero
    successful embeddings -- a systemic error (bad credential) isn't going to start
    succeeding on the next chunk either. Everything embedded before the abort is
    already upserted into the real table.

    Returns the full concatenated output for the caller's normal return/metadata
    handling -- the caller's own write of this (e.g. via the io_manager) upserts
    the same rows again, which is a harmless no-op since they're already there.
    """
    catalog, table_identifier = checkpoint_target
    rows = [row for row in df.to_dicts() if row["resolved_text"] is not None]

    consecutive_failed_chunks = 0
    chunk_dfs: list[pl.DataFrame] = []
    for chunk_start in range(0, len(rows), batch_size):
        chunk = rows[chunk_start : chunk_start + batch_size]
        results = _embed_chunk(chunk, client, errors=errors)
        chunk_df = _results_to_df(results, client)
        chunk_dfs.append(chunk_df)
        checkpoint_embedding_chunk(catalog, table_identifier, chunk_df)

        if len(chunk) > 0 and len(results) == 0:
            consecutive_failed_chunks += 1
            if consecutive_failed_chunks >= EMBEDDING_MAX_CONSECUTIVE_FAILED_CHUNKS:
                break
        else:
            consecutive_failed_chunks = 0

    if chunk_dfs:
        return pl.concat(chunk_dfs)
    return pl.DataFrame(schema=EMBEDDING_CHECKPOINT_SCHEMA)
