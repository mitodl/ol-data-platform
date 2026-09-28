"""Embedding of assembled/summarized feedback conversations."""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any, Protocol

import openai
import polars as pl
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from dagster import AssetExecutionContext
from google import genai
from google.genai import types as genai_types
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import attach_llm_usage, attach_span_metadata, traced
from ol_orchestrate.lib.constants import DAGSTER_ENV
from openai import OpenAI
from pyiceberg.catalog import Catalog

JOIN_COLS = ["feedback_conversation_pk"]

# Storage upsert key: includes model/dim so a different model/dim override
# adds a row instead of overwriting the existing vector.
UPSERT_JOIN_COLS = [*JOIN_COLS, "embedding_model_version", "embedding_dim"]

EMBEDDING_CHECKPOINT_SCHEMA = {
    **dict.fromkeys([*JOIN_COLS, "source_slug", "conversation_ref"], pl.String),
    "turn_count": pl.Int64,
    "embedding_input": pl.String,
    "embedding_vector": pl.List(pl.Float32),
    "embedding_dim": pl.Int64,
    "embedding_model_version": pl.String,
    "embedded_at": pl.Datetime(time_zone="UTC"),
}

# Abort after this many whole chunks in a row come back with zero successful
# embeddings, same rationale as summarize.py's MAX_CONSECUTIVE_FAILED_CHUNKS: a
# systemic error (bad credential) isn't going to start succeeding on the next
# chunk either.
EMBEDDING_MAX_CONSECUTIVE_FAILED_CHUNKS = int(
    os.environ.get("EMBEDDING_MAX_CONSECUTIVE_FAILED_CHUNKS", "1")
)

# Safe baseline from feedback_ml_approach.md §B, pending the model bake-off. A model
# id is only valid for one vendor's API, so client_class='gemini'/'bedrock_embeddings'
# needs a matching override -- there is no one id valid everywhere (same reasoning as
# SUMMARY_MODEL_VERSION/BEDROCK_SUMMARY_MODEL_VERSION in ml.lib.summarize).
# FeedbackEmbeddingsConfig.embedding_model_version overrides this; the env var/default
# here is only the fallback when a run doesn't set it.
EMBEDDING_MODEL_VERSION = os.environ.get(
    "EMBEDDING_MODEL_VERSION", "text-embedding-3-large"
)

# Bedrock's own model id namespace, never an OpenAI/Gemini id -- client_class=
# 'bedrock_embeddings' needs this override instead. Titan Embed Text v2 supports
# Matryoshka truncation the same way OpenAI's does (dimensions param), unlike Cohere
# Embed, which has a fixed output size per model variant.
# FeedbackEmbeddingsConfig.bedrock_model_version overrides this the same way.
BEDROCK_EMBEDDING_MODEL_VERSION = os.environ.get(
    "BEDROCK_EMBEDDING_MODEL_VERSION", "amazon.titan-embed-text-v2:0"
)

# Matryoshka truncation via the API's own `dimensions` param (§B sweeps 256/512/1024).
# 1024 keeps clustering quality while cutting the 3072-dim default's storage 3x.
EMBEDDING_DIM = int(os.environ.get("EMBEDDING_DIM", "1024"))

# Bounds each API call to this many conversations rather than one call per
# conversation. Also the checkpoint commit size: each chunk is one Iceberg upsert,
# and more chunks means more commits, which is what actually dominates wall-clock
# time at scale.
EMBEDDING_BATCH_SIZE = int(os.environ.get("EMBEDDING_BATCH_SIZE", "500"))

# How many embed_batch sub-batch calls run at once.
EMBEDDING_MAX_CONCURRENCY = int(os.environ.get("EMBEDDING_MAX_CONCURRENCY", "20"))

logger = logging.getLogger(__name__)


class EmbeddingClient(Protocol):
    model_version: str
    dim: int
    # The largest number of texts this provider accepts in one embed_batch call --
    # independent of EMBEDDING_BATCH_SIZE/config.batch_size, which only controls
    # how many rows are checkpointed together. _embed_chunk splits a checkpoint
    # chunk into sub-batches of this size before calling embed_batch.
    max_request_batch_size: int

    def embed_batch(
        self, texts: list[str], *, trace_metadata: dict[str, Any] | None = None
    ) -> list[list[float]]: ...


class OpenAIEmbeddingClient:
    """Adapts an OpenAI-compatible client to the EmbeddingClient protocol."""

    # OpenAI's embeddings API accepts up to 2048 inputs per request.
    max_request_batch_size = 2048

    def __init__(self, client: OpenAI, model_version: str, dim: int) -> None:
        self._client = client
        self.model_version = model_version
        self.dim = dim

    @traced(
        "feedback_embed_openai",
        tags=["feedback_embedding"],
        ignore_arguments=["trace_metadata"],
    )
    def embed_batch(
        self, texts: list[str], *, trace_metadata: dict[str, Any] | None = None
    ) -> list[list[float]]:
        if trace_metadata is not None:
            attach_span_metadata(trace_metadata)
        response = self._client.embeddings.create(
            model=self.model_version,
            input=texts,
            dimensions=self.dim,
        )
        if response.usage is not None:
            attach_llm_usage(
                usage={
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": 0,
                    "total_tokens": response.usage.total_tokens,
                },
                model=self.model_version,
                provider="openai",
            )
        # The API documents response order as matching input order, but sorting by
        # the returned index costs nothing and removes the risk of a silently
        # mismatched embedding-to-conversation pairing if that ever isn't true.
        ordered = sorted(response.data, key=lambda item: item.index)
        return [item.embedding for item in ordered]


class GeminiEmbeddingClient:
    """Adapts a google-genai client to the EmbeddingClient protocol."""

    # Gemini's embed_content accepts up to 250 texts per request.
    max_request_batch_size = 250

    def __init__(self, client: genai.Client, model_version: str, dim: int) -> None:
        self._client = client
        self.model_version = model_version
        self.dim = dim

    @traced(
        "feedback_embed_gemini",
        tags=["feedback_embedding"],
        ignore_arguments=["trace_metadata"],
    )
    def embed_batch(
        self, texts: list[str], *, trace_metadata: dict[str, Any] | None = None
    ) -> list[list[float]]:
        if trace_metadata is not None:
            attach_span_metadata(trace_metadata)
        # Order is the API's own contract (response.embeddings lines up with the
        # input contents list), unlike OpenAI's which documents an index field --
        # nothing to sort by here.
        response = self._client.models.embed_content(
            model=self.model_version,
            contents=texts,
            config=genai_types.EmbedContentConfig(output_dimensionality=self.dim),
        )
        # No response-level usage field -- token_count is per-embedding, so summed.
        token_count = sum(
            embedding.statistics.token_count
            for embedding in response.embeddings
            if embedding.statistics is not None
            and embedding.statistics.token_count is not None
        )
        if token_count:
            attach_llm_usage(
                usage={
                    "prompt_tokens": token_count,
                    "completion_tokens": 0,
                    "total_tokens": token_count,
                },
                model=self.model_version,
                provider="google_ai",
            )
        return [embedding.values for embedding in response.embeddings]


class BedrockEmbeddingClient:
    """Adapts AWS Bedrock's native embedding models (Titan, Cohere) to
    EmbeddingClient.

    Titan and Cohere embedding models have different request/response shapes on
    Bedrock's shared invoke_model API -- unlike OpenAI/Gemini, there's no single
    contract to adapt to, so this dispatches on model_version's prefix.
    """

    # Cohere's Bedrock invoke_model accepts up to 96 texts per request. Titan has
    # no batch endpoint at all -- _embed_titan loops one invoke_model call per
    # text regardless of sub-batch size, so its sub-batch size must be 1, or
    # _embed_chunk's concurrency (one future per sub-batch) never actually
    # parallelizes Titan's real API calls.
    def __init__(self, client: BaseClient, model_version: str, dim: int) -> None:
        self._client = client
        self.model_version = model_version
        self.dim = dim
        self.max_request_batch_size = (
            1 if model_version.startswith("amazon.titan-embed") else 96
        )

    @traced(
        "feedback_embed_bedrock",
        tags=["feedback_embedding"],
        ignore_arguments=["trace_metadata"],
    )
    def embed_batch(
        self, texts: list[str], *, trace_metadata: dict[str, Any] | None = None
    ) -> list[list[float]]:
        if trace_metadata is not None:
            attach_span_metadata(trace_metadata)
        if self.model_version.startswith("amazon.titan-embed"):
            return self._embed_titan(texts)
        if self.model_version.startswith("cohere.embed"):
            return self._embed_cohere(texts)
        msg = (
            f"No Bedrock embedding adapter for model_version={self.model_version!r}. "
            "Supported prefixes: 'amazon.titan-embed', 'cohere.embed'."
        )
        raise ValueError(msg)

    def _embed_titan(self, texts: list[str]) -> list[list[float]]:
        # Titan's invoke_model embeds one inputText per call -- no batch endpoint,
        # unlike Cohere's below.
        embeddings = []
        for text in texts:
            body = json.dumps(
                {"inputText": text, "dimensions": self.dim, "normalize": True}
            )
            response = self._client.invoke_model(modelId=self.model_version, body=body)
            payload = json.loads(response["body"].read())
            embeddings.append(payload["embedding"])
        return embeddings

    def _embed_cohere(self, texts: list[str]) -> list[list[float]]:
        # v4 speaks Cohere's newer v2-style embed contract: embedding_types is
        # required in the request, and the response nests vectors under
        # embeddings.float instead of returning a flat embeddings list -- v3's
        # (legacy v1-style) shape, which this dispatches on.
        is_v4 = self.model_version.startswith("cohere.embed-v4")
        request_body: dict[str, Any] = {
            "texts": texts,
            "input_type": "search_document",
        }
        if is_v4:
            request_body["embedding_types"] = ["float"]
            # v4's Matryoshka truncation param -- unlike v3, which has no
            # dimension override at all. Unverified against a live response;
            # confirm the returned vector length actually matches self.dim.
            request_body["output_dimension"] = self.dim
        response = self._client.invoke_model(
            modelId=self.model_version, body=json.dumps(request_body)
        )
        payload = json.loads(response["body"].read())
        return payload["embeddings"]["float"] if is_v4 else payload["embeddings"]


def build_embedding_client(
    llm: LLMClientFactory,
    model_version: str | None = None,
    dim: int | None = None,
    bedrock_model_version: str | None = None,
) -> OpenAIEmbeddingClient | GeminiEmbeddingClient | BedrockEmbeddingClient:
    """Build the client whose model_version/dim come from run config, else a default.

    model_version/dim/bedrock_model_version are the feedback_embeddings asset's
    own per-run Config fields (FeedbackEmbeddingsConfig) -- None means the run
    didn't override them, so EMBEDDING_MODEL_VERSION/EMBEDDING_DIM/
    BEDROCK_EMBEDDING_MODEL_VERSION apply. bedrock_model_version is ignored
    unless the resolved client is BedrockEmbeddingClient -- same split as
    ml.lib.summarize.build_summary_client's model_version/bedrock_model_version.
    """
    client = llm.get_client()
    resolved_dim = dim or EMBEDDING_DIM
    if isinstance(client, OpenAI):
        return OpenAIEmbeddingClient(
            client, model_version or EMBEDDING_MODEL_VERSION, resolved_dim
        )
    if isinstance(client, genai.Client):
        return GeminiEmbeddingClient(
            client, model_version or EMBEDDING_MODEL_VERSION, resolved_dim
        )
    if isinstance(client, BaseClient):
        return BedrockEmbeddingClient(
            client,
            bedrock_model_version or BEDROCK_EMBEDDING_MODEL_VERSION,
            resolved_dim,
        )
    # Anthropic (chat-only, no embeddings API at all) lands here too -- unlike
    # the summary asset, there is no adapter to fall back to.
    msg = (
        f"{type(client).__name__} has no embeddings adapter. Configure the "
        "embedding_llm resource's client_class as one of 'openai', "
        "'openai_compatible', 'azure_openai', 'gemini', or 'bedrock_embeddings'."
    )
    raise TypeError(msg)


def default_embedding_model_version() -> str:
    """Return the model_version a default feedback_embeddings run writes, so a
    reader can filter on what was actually written instead of assuming
    EMBEDDING_MODEL_VERSION (#2689). Mirrors definitions.py's embedding_llm
    resource default.
    """
    provider = os.environ.get(
        "EMBEDDING_PROVIDER", "openai" if DAGSTER_ENV == "dev" else "bedrock_embeddings"
    )
    if provider == "bedrock_embeddings":
        return BEDROCK_EMBEDDING_MODEL_VERSION
    return EMBEDDING_MODEL_VERSION


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


def _is_isolatable_error(error: Exception) -> bool:
    """Whether error is about one bad row's content, not the whole request.

    openai.BadRequestError is always this (OpenAI has no other 4xx that reaches
    here). Bedrock's ValidationException is overloaded, though -- it also covers
    a bad model id, an unsupported dimension, or a malformed request schema, none
    of which a per-row retry can fix (that just turns one systemic failure into
    up to len(chunk) failed calls). Cohere's per-input length cap (e.g.
    embed-english-v3: 2048 chars) is the one ValidationException shape that is
    actually isolatable, and it's identifiable: Bedrock reports it as
    "#/texts/<index>: expected maxLength..." -- a JSON-pointer at a specific
    array element, unlike a request-level problem, which never names one.
    """
    if isinstance(error, openai.BadRequestError):
        return True
    if isinstance(error, ClientError):
        if error.response.get("Error", {}).get("Code") != "ValidationException":
            return False
        message = error.response.get("Error", {}).get("Message", "")
        return "#/texts/" in message
    return False


def _embed_request_batch(
    chunk: list[dict[str, Any]],
    client: EmbeddingClient,
    errors: list[str] | None = None,
) -> list[tuple[dict[str, Any], list[float]]]:
    """Embed one request-sized batch via a single API call, falling back row-by-row.

    A single bad row (e.g. a length/encoding issue the API rejects) fails the whole
    batch call as an isolatable error (see _is_isolatable_error) -- retrying one at
    a time isolates it rather than dropping every otherwise-fine row in the chunk
    along with it.

    Any other exception (rate limit, auth, connection, 5xx) is systemic: retrying
    row-by-row would just multiply the same failure by len(chunk) rather than fix
    anything -- e.g. 100 extra calls at an endpoint that already asked us to back
    off (the provider SDK's own retry/backoff is exhausted by the time an error
    surfaces here at all). So it's recorded as a single whole-chunk failure instead,
    letting the caller's consecutive-failed-chunks counter decide whether to abort.

    errors, if given, collects each failure's message -- lets a caller surface
    *why* calls failed (e.g. in a Failure message) without changing this
    function's return type.
    """
    try:
        vectors = client.embed_batch(
            [row["resolved_text"] for row in chunk],
            trace_metadata={
                "feedback_conversation_pks": [
                    row["feedback_conversation_pk"] for row in chunk
                ],
                "conversation_refs": [row["conversation_ref"] for row in chunk],
                "embedding_inputs": [row["embedding_input"] for row in chunk],
            },
        )
    except Exception as e:
        if not _is_isolatable_error(e):
            logger.warning(
                "Batch embed failed for %d conversations with a systemic error; "
                "not retrying individually",
                len(chunk),
                exc_info=True,
            )
            if errors is not None:
                errors.append(f"chunk of {len(chunk)}: {type(e).__name__}: {e}")
            return []
        logger.warning(
            "Batch embed failed for %d conversations; retrying individually",
            len(chunk),
            exc_info=True,
        )
        results = []
        for row in chunk:
            try:
                vector = client.embed_batch(
                    [row["resolved_text"]],
                    trace_metadata={
                        "feedback_conversation_pks": [row["feedback_conversation_pk"]],
                        "conversation_refs": [row["conversation_ref"]],
                        "embedding_inputs": [row["embedding_input"]],
                    },
                )[0]
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
    return list(zip(chunk, vectors, strict=True))


def _embed_chunk(
    chunk: list[dict[str, Any]],
    client: EmbeddingClient,
    errors: list[str] | None = None,
    max_concurrency: int = EMBEDDING_MAX_CONCURRENCY,
) -> list[tuple[dict[str, Any], list[float]]]:
    """Embed one checkpoint chunk, split into client.max_request_batch_size-sized
    API calls run concurrently.
    """
    request_batches = [
        chunk[start : start + client.max_request_batch_size]
        for start in range(0, len(chunk), client.max_request_batch_size)
    ]
    results: list[tuple[dict[str, Any], list[float]]] = []
    with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
        futures = [
            executor.submit(_embed_request_batch, request_batch, client, errors=errors)
            for request_batch in request_batches
        ]
        for future in futures:
            results.extend(future.result())
    return results


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
        pl.lit(datetime.now(tz=UTC), dtype=pl.Datetime(time_zone="UTC")).alias(
            "embedded_at"
        ),
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
    # A table from before embedded_at existed has an older schema than chunk_df --
    # union_by_name adds the new column (nulled on existing rows) instead of failing
    # the upsert; a no-op once the table already has it.
    with table.update_schema() as update:
        update.union_by_name(chunk_df.to_arrow().schema)
    # union_by_name appends new columns at the table's end regardless of chunk_df's
    # order, and upsert's pyarrow cast is positional -- so it must be reordered
    # to match the table, not chunk_df. table.schema().fields (== .columns) gives
    # the top-level field names only.
    ordered_chunk_df = chunk_df.select([field.name for field in table.schema().fields])
    table.upsert(
        df=ordered_chunk_df.to_arrow(),
        join_cols=UPSERT_JOIN_COLS,
        when_matched_update_all=True,
        when_not_matched_insert_all=True,
    )


def embed_and_checkpoint(  # noqa: PLR0913 -- each is an independent tuning knob
    df: pl.DataFrame,
    client: EmbeddingClient,
    checkpoint_target: tuple[Catalog, str],
    batch_size: int = EMBEDDING_BATCH_SIZE,
    errors: list[str] | None = None,
    max_concurrency: int = EMBEDDING_MAX_CONCURRENCY,
    context: AssetExecutionContext | None = None,
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
        batch_size: rows per checkpoint upsert. _embed_chunk splits this into
            smaller client.max_request_batch_size-sized embed_batch calls.
        errors: if given, collects every failure's message (see _embed_chunk) so a
            caller can surface *why* calls failed, e.g. in a Failure message.
        max_concurrency: how many of those embed_batch calls run at once.
        context: if given, logs per-chunk progress via context.log.info instead
            of the plain module logger.

    Returns:
        pl.DataFrame: feedback_conversation_pk, source_slug, conversation_ref,
            turn_count, embedding_vector, embedding_dim, embedding_model_version,
            embedding_input, embedded_at - keyed by UPSERT_JOIN_COLS, not
            feedback_conversation_pk alone; see int__feedback__embedding for the
            one-row-per-conversation view afact_feedback_conversation reads.
            turn_count is carried through so a later run's filter_unembedded can
            detect a conversation that gained a turn.

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
    log = context.log if context is not None else logger
    catalog, table_identifier = checkpoint_target
    rows = [row for row in df.to_dicts() if row["resolved_text"] is not None]

    consecutive_failed_chunks = 0
    chunk_dfs: list[pl.DataFrame] = []
    chunk_starts = range(0, len(rows), batch_size)
    total_chunks = len(chunk_starts)
    for chunk_index, chunk_start in enumerate(chunk_starts, start=1):
        chunk = rows[chunk_start : chunk_start + batch_size]
        results = _embed_chunk(
            chunk, client, errors=errors, max_concurrency=max_concurrency
        )
        chunk_df = _results_to_df(results, client)
        chunk_dfs.append(chunk_df)
        checkpoint_embedding_chunk(catalog, table_identifier, chunk_df)
        log.info(
            "Upserted chunk %d/%d (%d rows) into %s",
            chunk_index,
            total_chunks,
            chunk_df.height,
            table_identifier,
        )

        if len(chunk) > 0 and len(results) == 0:
            consecutive_failed_chunks += 1
            if consecutive_failed_chunks >= EMBEDDING_MAX_CONSECUTIVE_FAILED_CHUNKS:
                break
        else:
            consecutive_failed_chunks = 0

    if chunk_dfs:
        return pl.concat(chunk_dfs)
    return pl.DataFrame(schema=EMBEDDING_CHECKPOINT_SCHEMA)
