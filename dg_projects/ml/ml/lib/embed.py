"""Embedding of assembled/summarized feedback conversations, through ol-llm.

Every request goes through ol-llm's embed_sync (by way of ol_orchestrate's
OlLlmEmbedder), which gives it the batch profile's retries, ol-llm's error classes,
the vector count and length checks, and one Opik root trace per request.
resolve_embedding_model is the one place a run's model config becomes an ol-llm
model id, a stored embedding_model_version, and request limits; feedback_embeddings
and every reader of its table call it.
"""

import hashlib
import json
import logging
import os
import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from math import ceil
from typing import Any

import polars as pl
from ml.lib.iceberg_helpers import upsert_chunk
from ml.resources.opik_auth import OPIK_PROJECT_NAME
from ol_llm import (
    Attribution,
    BatchReport,
    Chunk,
    LLMError,
    LLMRejected,
    RowFailure,
    UseCase,
    process_batch,
)
from ol_llm.batch import rejects_the_row
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.embeddings.chunking import WholeDocumentChunker
from ol_orchestrate.lib.embeddings.documents import Document
from ol_orchestrate.lib.embeddings.models import (
    Embedder,
    EmbeddingModelSpec,
    OlLlmEmbedder,
)
from pyiceberg.catalog import Catalog

JOIN_COLS = ["feedback_conversation_pk"]

# Storage upsert key: includes model/dim so a different model/dim override
# adds a row instead of overwriting the existing vector.
UPSERT_JOIN_COLS = [*JOIN_COLS, "embedding_model_version", "embedding_dim"]

# Columns added after the table first existed go last, in the order they were
# added: upsert_chunk's union_by_name appends them to an existing table.
EMBEDDING_CHECKPOINT_SCHEMA = {
    **dict.fromkeys([*JOIN_COLS, "source_slug", "conversation_ref"], pl.String),
    "turn_count": pl.Int64,
    "embedding_input": pl.String,
    "embedding_vector": pl.List(pl.Float32),
    "embedding_dim": pl.Int64,
    "embedding_model_version": pl.String,
    "embedded_at": pl.Datetime(time_zone="UTC"),
    "idempotency_key": pl.String,
}

# A model id in ol-llm's provider:model form. Dev has no Bedrock IAM role, so it
# calls OpenAI with OPENAI_API_KEY; every deployed environment uses Bedrock's Titan
# Embed Text v2, which shortens its vectors to a requested length the way OpenAI's
# text-embedding-3 models do. The per-run embedding_model config overrides this.
EMBEDDING_MODEL = os.environ.get(
    "EMBEDDING_MODEL",
    "openai:text-embedding-3-large"
    if DAGSTER_ENV == "dev"
    else "bedrock:amazon.titan-embed-text-v2:0",
)

# Matryoshka truncation via the API's own `dimensions` param (§B sweeps 256/512/1024).
# 1024 keeps clustering quality while cutting the 3072-dim default's storage 3x. Set
# on every request: ol-llm 0.0.4's catalog lists the OpenAI and Azure models and
# Bedrock's Titan Embed Text v2 but no Cohere or Google model, so for those it has
# no default length to check the vectors against.
EMBEDDING_DIM = int(os.environ.get("EMBEDDING_DIM", "1024"))

# About how many conversations are checkpointed together: each chunk is one Iceberg
# upsert, and more chunks means more commits, which is what actually dominates
# wall-clock time at scale. A chunk holds whole requests, so it rounds up to a
# multiple of the model's request size.
EMBEDDING_BATCH_SIZE = int(os.environ.get("EMBEDDING_BATCH_SIZE", "500"))

# How many requests run at once, each on its own thread.
EMBEDDING_MAX_CONCURRENCY = int(os.environ.get("EMBEDDING_MAX_CONCURRENCY", "20"))

EMBEDDING_USE_CASE_NAME = "feedback-embedding"
FEEDBACK_DOC_KIND = "feedback_conversation"

# text-embedding-3's per-input limit is 8,192 tokens of cl100k_base, the encoding
# EmbeddingModelSpec counts with, so this cut is exact for OpenAI and Azure.
OPENAI_MAX_INPUT_TOKENS = 8191
# Not a documented Gemini API cap; it's what this has run with.
GEMINI_API_BATCH_SIZE = 250
# Vertex documents :predict as at most 20,000 tokens per request and 2,048 per text,
# and five texts per request for the text-embedding models
# (https://docs.cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings).
# Every other Vertex model embeds one text per request. Google counts with its own
# tokenizer, so the cl100k_base cut only keeps a text near the limit; Vertex
# truncates what is left over.
VERTEX_MAX_REQUEST_TOKENS = 20_000
VERTEX_TEXT_EMBEDDING_BATCH_SIZE = 5
# gemini-embedding-001 and the text-embedding models.
GOOGLE_MAX_INPUT_TOKENS = 2048
# gemini-embedding-2 takes 8,192 tokens per text on both the Gemini API
# (https://ai.google.dev/gemini-api/docs/embeddings) and Vertex.
GEMINI_EMBEDDING_2_MAX_INPUT_TOKENS = 8192
# Titan has no batch endpoint, so a request is one text. Titan v2 takes at most
# 50,000 characters or 8,192 tokens of its own tokenizer, which tiktoken cannot
# count, so a text over the token limit is shortened and sent again instead.
TITAN_MAX_INPUT_CHARS = 50_000
TITAN_MAX_SHORTEN_ATTEMPTS = 3
_TITAN_TOKEN_COUNTS = re.compile(
    r"Max input tokens: (\d+), request input token count: (\d+)"
)
# Cohere's Bedrock invoke_model accepts up to 96 texts per request, and its v3
# models 2,048 characters per text.
COHERE_BATCH_SIZE = 96
COHERE_V3_MAX_INPUT_CHARS = 2048
COHERE_V4_MAX_INPUT_TOKENS = 128_000
# Bedrock's ValidationException also covers a bad dimension or request schema,
# which a text sent alone fails the same way. Only Cohere's per-text limit names
# the text, as a JSON pointer such as "#/texts/3: expected maxLength".
BEDROCK_TEXT_REJECTION_MARKER = "#/texts/"

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class FeedbackEmbeddingModel:
    """An embedding model at a vector length, and how its requests are sized.

    :param spec: the model, its vector length, and its request limits.
    :param max_input_chars: the longest text sent, in characters.
    :param max_input_tokens: the longest text sent, in ``spec.token_encoding``
        tokens. Kept at or under ``spec.max_request_tokens``, which a text alone
        may not exceed.
    :param shortens_on_token_limit: shorten a text the model rejects for its
        token count and send it again (Titan).
    :param text_rejection_marker: text a rejection's message has only when it is
        about one text; unset when every row rejection is.
    """

    spec: EmbeddingModelSpec
    max_input_chars: int | None = None
    max_input_tokens: int | None = None
    shortens_on_token_limit: bool = False
    text_rejection_marker: str | None = None

    @property
    def model(self) -> str:
        """The ol-llm model id, ``provider:model``.

        :rtype: str
        """
        return self.spec.model

    @property
    def dim(self) -> int:
        """The vector length every request asks for.

        :rtype: int
        """
        return self.spec.vector_length

    @property
    def model_version(self) -> str:
        """The embedding_model_version stored and filtered on: the id without its
        provider, as the vendor SDK clients before ol-llm wrote it.

        :rtype: str
        """
        return self.model.partition(":")[2]

    def matches(self) -> pl.Expr:
        """Select feedback_embeddings-shaped rows with this model and length.

        :rtype: pl.Expr
        """
        return (pl.col("embedding_model_version") == self.model_version) & (
            pl.col("embedding_dim") == self.dim
        )

    def key(self, text: str, embedding_input: str) -> str:
        """Return the idempotency key of a conversation's embedding.

        :param text: the conversation's resolved_text.
        :param embedding_input: the arm resolved_text came from. A summary can
            equal the concatenated turns, and the stored arm decides which
            clusters the conversation is in.
        :returns: a hex SHA-256 that changes when the text, the arm, the model,
            or the vector length does.
        :rtype: str
        """
        parts = [text, embedding_input, self.model, self.dim]
        return hashlib.sha256(json.dumps(parts).encode()).hexdigest()

    def input_text(self, text: str) -> str:
        """Cut a text to what the model accepts.

        Long tickets embed from their first part rather than failing every run.

        :param text: the conversation's resolved_text.
        :rtype: str
        """
        text = text[: self.max_input_chars]
        if self.max_input_tokens is None:
            return text
        chunker = WholeDocumentChunker(
            encoding=self.spec.token_encoding, max_tokens=self.max_input_tokens
        )
        document = Document(doc_key="", doc_kind=FEEDBACK_DOC_KIND, text=text)
        [chunk] = chunker.chunk(document)
        return chunk.text

    def rejects_one_text(self, error: LLMError) -> bool:
        """Whether a request's error is about one of its texts, not the request.

        :param error: what the request raised.
        :rtype: bool
        """
        return rejects_the_row(error) and (
            self.text_rejection_marker is None
            or self.text_rejection_marker in str(error)
        )

    def use_case(self) -> UseCase:
        """Return the feedback-embedding use case calling this model.

        :rtype: UseCase
        """
        return UseCase(
            app=OPIK_PROJECT_NAME,
            name=EMBEDDING_USE_CASE_NAME,
            profile="batch",
            model=self.model,
        )

    def embedder(self, run_id: str | None = None) -> OlLlmEmbedder:
        """Return the embedder that calls this model through ol-llm.

        :param run_id: the Dagster run, for the traces.
        :rtype: OlLlmEmbedder
        """
        return OlLlmEmbedder(self.spec, self.use_case(), Attribution(run_id=run_id))


def resolve_embedding_model(
    model: str | None = None, dim: int | None = None
) -> FeedbackEmbeddingModel:
    """Resolve a run's model config, or the defaults, to the model it embeds with.

    The writer and every reader of feedback_embeddings call this, so they agree on
    the stored embedding_model_version for the same config.

    :param model: the ol-llm model id; ``EMBEDDING_MODEL`` when unset.
    :param dim: the vector length; ``EMBEDDING_DIM`` when unset.
    :returns: the model.
    :rtype: FeedbackEmbeddingModel
    :raises ValueError: for a provider or model family nothing here sizes
        requests for.
    """
    model = model or EMBEDDING_MODEL
    dim = dim or EMBEDDING_DIM
    provider, _, name = model.partition(":")
    google_max_input_tokens = (
        GEMINI_EMBEDDING_2_MAX_INPUT_TOKENS
        if name.startswith("gemini-embedding-2")
        else GOOGLE_MAX_INPUT_TOKENS
    )
    if provider in {"openai", "azure"}:
        return FeedbackEmbeddingModel(
            spec=EmbeddingModelSpec(model=model, dimensions=dim),
            max_input_tokens=OPENAI_MAX_INPUT_TOKENS,
        )
    if provider == "google":
        return FeedbackEmbeddingModel(
            spec=EmbeddingModelSpec(
                model=model, dimensions=dim, batch_size=GEMINI_API_BATCH_SIZE
            ),
            max_input_tokens=google_max_input_tokens,
        )
    if provider == "google-cloud":
        batch_size = (
            VERTEX_TEXT_EMBEDDING_BATCH_SIZE
            if name.startswith(("text-embedding-", "text-multilingual-embedding-"))
            else 1
        )
        return FeedbackEmbeddingModel(
            spec=EmbeddingModelSpec(
                model=model,
                dimensions=dim,
                batch_size=batch_size,
                max_request_tokens=VERTEX_MAX_REQUEST_TOKENS,
            ),
            max_input_tokens=google_max_input_tokens,
        )
    if provider == "bedrock" and name.startswith("amazon.titan-embed"):
        return FeedbackEmbeddingModel(
            spec=EmbeddingModelSpec(model=model, dimensions=dim, batch_size=1),
            max_input_chars=TITAN_MAX_INPUT_CHARS,
            shortens_on_token_limit=True,
        )
    if provider == "bedrock" and name.startswith(
        ("cohere.embed-english-v3", "cohere.embed-multilingual-v3")
    ):
        return FeedbackEmbeddingModel(
            spec=EmbeddingModelSpec(
                model=model, dimensions=dim, batch_size=COHERE_BATCH_SIZE
            ),
            max_input_chars=COHERE_V3_MAX_INPUT_CHARS,
            text_rejection_marker=BEDROCK_TEXT_REJECTION_MARKER,
        )
    if provider == "bedrock" and name.startswith("cohere.embed-v4"):
        return FeedbackEmbeddingModel(
            spec=EmbeddingModelSpec(
                model=model, dimensions=dim, batch_size=COHERE_BATCH_SIZE
            ),
            max_input_tokens=COHERE_V4_MAX_INPUT_TOKENS,
            text_rejection_marker=BEDROCK_TEXT_REJECTION_MARKER,
        )
    msg = (
        f"No request sizing for embedding model {model!r}. Supported: openai:, "
        "azure:, google:, google-cloud:, bedrock:amazon.titan-embed-*, and "
        "bedrock:cohere.embed-*."
    )
    raise ValueError(msg)


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


def has_embeddable_text(text: str | None) -> bool:
    """Whether embed_and_checkpoint sends this resolved_text to the model at all.

    A blank text is skipped along with a null one: Vertex's :predict drops an empty
    text from the request, so the embedding count stops matching and the whole
    request fails. Neither ol-llm nor PydanticAI drops one.
    """
    return text is not None and bool(text.strip())


def filter_unembedded(
    source_df: pl.DataFrame,
    already_embedded_df: pl.DataFrame,
    model: FeedbackEmbeddingModel,
) -> pl.DataFrame:
    """Drop conversations whose stored vector came from this text, model, and length.

    The idempotency key covers the resolved text, so a new turn, a changed summary,
    or a switch of embedding_input arm re-embeds a conversation, and a turn_count
    change whose summary has not been redone yet does not.

    A conversation not stored yet is always kept, with or without text, so the run
    counts one without text. A stored one without text is dropped, which leaves
    its vector in place.

    A row embedded before the key existed keeps the rule it was written under: it
    is re-embedded when its turn_count or embedding_input arm changed. Re-embedding
    every one of them would give each a new embedded_at, and feedback_cluster_assignment
    would re-place every conversation. The row gets a key the next time it is
    embedded.

    :param source_df: resolve_embedding_text's output.
    :param already_embedded_df: feedback_embeddings' rows for this model and
        length: feedback_conversation_pk, turn_count, embedding_input, and
        idempotency_key (null for a row written before the column existed).
    :param model: the run's model.
    :returns: the rows of source_df to embed or count as having no text.
    :rtype: pl.DataFrame
    """
    stored = already_embedded_df.select(
        *JOIN_COLS,
        pl.col("idempotency_key").alias("_stored_key"),
        pl.col("turn_count").alias("_stored_turn_count"),
        pl.col("embedding_input").alias("_stored_embedding_input"),
        pl.lit(True).alias("_stored"),  # noqa: FBT003
    )
    current_key = pl.Series(
        "_current_key",
        [
            model.key(text, embedding_input) if has_embeddable_text(text) else None
            for text, embedding_input in zip(
                source_df["resolved_text"], source_df["embedding_input"], strict=True
            )
        ],
        dtype=pl.String,
    )
    keyless_changed = pl.col("turn_count").ne_missing(
        pl.col("_stored_turn_count")
    ) | pl.col("embedding_input").ne_missing(pl.col("_stored_embedding_input"))
    return (
        source_df.with_columns(current_key)
        .join(stored, on=JOIN_COLS, how="left")
        .filter(
            pl.col("_stored").is_null()
            | (
                pl.col("_current_key").is_not_null()
                & pl.when(pl.col("_stored_key").is_null())
                .then(keyless_changed)
                .otherwise(pl.col("_current_key") != pl.col("_stored_key"))
            )
        )
        .select(source_df.columns)
    )


@dataclass(frozen=True)
class RequestOutcome:
    """What one request did: the conversations embedded and those rejected.

    :param embedded: each conversation with its vector.
    :param rejected: conversations whose own text the provider rejected, found by
        embedding a rejected request's texts one at a time.
    :param input_tokens: tokens the provider reported for the request.
    """

    embedded: list[tuple[dict[str, Any], list[float]]]
    rejected: list[RowFailure[dict[str, Any]]] = field(default_factory=list)
    input_tokens: int = 0


@dataclass(frozen=True, kw_only=True)
class EmbedReport:
    """What embed_and_checkpoint did, counted in conversations.

    :param failures: each conversation that was sent and not embedded.
    :param input_tokens: tokens the provider reported, over every request.
    :param batch: process_batch's report, which counts requests.
    """

    failures: list[RowFailure[dict[str, Any]]]
    input_tokens: int
    batch: BatchReport[list[dict[str, Any]]]


@dataclass(frozen=True)
class _RequestEmbedder:
    model: FeedbackEmbeddingModel
    embedder: Embedder
    log: logging.Logger

    def __call__(self, request: list[dict[str, Any]]) -> RequestOutcome:
        """Embed one request, isolating a conversation the provider rejects.

        A rejection of one text's input (ol-llm's rejects_the_row: a 400, 413, or
        422, narrowed by the model's rejects_one_text) fails the whole multi-text
        request, so its texts are sent again one at a time to keep the rest of
        the request. Any other failure (a rate limit or outage that outlasted
        ol-llm's retries, a rejected credential) is the request's, and raises so
        process_batch's circuit breaker counts it.
        """
        try:
            vectors, input_tokens = self._embed([row["input_text"] for row in request])
        except LLMError as error:
            if len(request) == 1 or not self.model.rejects_one_text(error):
                raise
            self.log.warning(
                "A request of %d conversations was rejected (%s); embedding them "
                "one at a time",
                len(request),
                error,
            )
            return self._one_at_a_time(request, error)
        return RequestOutcome(
            list(zip(request, vectors, strict=True)), [], input_tokens
        )

    def _one_at_a_time(
        self, request: list[dict[str, Any]], error: LLMError
    ) -> RequestOutcome:
        embedded: list[tuple[dict[str, Any], list[float]]] = []
        rejected: list[RowFailure[dict[str, Any]]] = []
        input_tokens = 0
        for row in request:
            try:
                [vector], tokens = self._embed([row["input_text"]])
            except LLMError as row_error:
                if not self.model.rejects_one_text(row_error):
                    raise
                rejected.append(RowFailure(row, row_error))
                continue
            embedded.append((row, vector))
            input_tokens += tokens
        # Every text rejected alone means the request was wrong (e.g. a dimension
        # the model does not take), so it goes to the circuit breaker.
        if not embedded:
            raise error
        return RequestOutcome(embedded, rejected, input_tokens)

    def _embed(self, texts: list[str]) -> tuple[list[list[float]], int]:
        if not self.model.shortens_on_token_limit:
            batch = self.embedder.embed(texts, input_type="document")
            return batch.vectors, batch.input_tokens
        [text] = texts
        for _ in range(TITAN_MAX_SHORTEN_ATTEMPTS):
            try:
                batch = self.embedder.embed([text], input_type="document")
            except LLMRejected as error:
                # PydanticAI raises Titan's 400 as ModelHTTPError with the response
                # body, which ol-llm keeps in LLMRejected's message.
                counts = _TITAN_TOKEN_COUNTS.search(str(error))
                if counts is None:
                    raise
                max_tokens, actual_tokens = (int(n) for n in counts.groups())
                text = text[: int(len(text) * max_tokens / actual_tokens * 0.9)]
            else:
                return batch.vectors, batch.input_tokens
        batch = self.embedder.embed([text], input_type="document")
        return batch.vectors, batch.input_tokens


def _requests(
    rows: list[dict[str, Any]], spec: EmbeddingModelSpec
) -> Iterator[list[dict[str, Any]]]:
    """Group rows, in order, into requests within the spec's text and token limits."""
    start = 0
    for texts in spec.request_batches([row["input_text"] for row in rows]):
        yield rows[start : start + len(texts)]
        start += len(texts)


def _embedded_frame(
    model: FeedbackEmbeddingModel,
    embedded: Sequence[tuple[dict[str, Any], list[float]]],
) -> pl.DataFrame:
    embedded_at = datetime.now(tz=UTC)
    return pl.DataFrame(
        [
            {
                "feedback_conversation_pk": row["feedback_conversation_pk"],
                "source_slug": row["source_slug"],
                "conversation_ref": row["conversation_ref"],
                "turn_count": row["turn_count"],
                "embedding_input": row["embedding_input"],
                # Float32: issue #2543 and afact_feedback_conversation's
                # embedding_vector column both specify a single-precision Iceberg
                # ARRAY<float>.
                "embedding_vector": vector,
                "embedding_dim": model.dim,
                "embedding_model_version": model.model_version,
                "embedded_at": embedded_at,
                "idempotency_key": row["idempotency_key"],
            }
            for row, vector in embedded
        ],
        schema=EMBEDDING_CHECKPOINT_SCHEMA,
    )


def embed_and_checkpoint(  # noqa: PLR0913 -- each is an independent tuning knob
    df: pl.DataFrame,
    model: FeedbackEmbeddingModel,
    embedder: Embedder,
    checkpoint_target: tuple[Catalog, str],
    *,
    batch_size: int = EMBEDDING_BATCH_SIZE,
    max_concurrency: int = EMBEDDING_MAX_CONCURRENCY,
    log: logging.Logger = logger,
) -> tuple[pl.DataFrame, EmbedReport]:
    """Embed df in chunks, upserting each into feedback_embeddings as it completes.

    A null, empty, or whitespace-only resolved_text (upstream summary/redaction not
    ready yet) is skipped and retried next run. The rest are grouped into requests
    by the model's limits, and process_batch runs up to max_concurrency of them at
    once on its threads. Threads do not inherit the caller's Opik context, and the
    asset has no trace open, so each request is its own root trace.

    A failed request leaves its conversations out of the table, so the next run
    sends them again. process_batch stops the run when its circuit breaker opens
    (an outage, a rate limit that outlasts the retries, or a rejected credential);
    everything upserted before that stays.

    :param df: filter_unembedded's output.
    :param model: the run's model.
    :param embedder: calls the model, e.g. ``model.embedder(run_id)``.
    :param checkpoint_target: the catalog and "database.table" to upsert into.
    :param batch_size: about how many conversations go in one upsert; a chunk
        holds ``ceil(batch_size / model.spec.batch_size)`` requests.
    :param max_concurrency: requests sent at once.
    :param log: where to log progress, e.g. Dagster's context.log.
    :returns: every row upserted, in the table's column order and keyed by
        UPSERT_JOIN_COLS, and what the run did.
    :rtype: tuple[pl.DataFrame, EmbedReport]
    :raises BatchFailed: if the circuit opened, or every attempted request failed.
    """
    catalog, table_identifier = checkpoint_target
    rows = [
        {
            **row,
            "input_text": model.input_text(row["resolved_text"]),
            "idempotency_key": model.key(row["resolved_text"], row["embedding_input"]),
        }
        for row in df.to_dicts()
        if has_embeddable_text(row["resolved_text"])
    ]
    upserted: list[pl.DataFrame] = []
    failures: list[RowFailure[dict[str, Any]]] = []
    input_tokens = 0

    def persist(chunk: Chunk[list[dict[str, Any]], RequestOutcome]) -> None:
        nonlocal input_tokens
        outcomes = [outcome for _, outcome in chunk.succeeded]
        for failed in chunk.failed:
            failures.extend(RowFailure(row, failed.error) for row in failed.row)
        for outcome in outcomes:
            failures.extend(outcome.rejected)
            input_tokens += outcome.input_tokens
        chunk_df = _embedded_frame(
            model, [pair for outcome in outcomes for pair in outcome.embedded]
        )
        if chunk_df.height:
            upserted.append(
                upsert_chunk(catalog, table_identifier, chunk_df, UPSERT_JOIN_COLS)
            )

    report = process_batch(
        _requests(rows, model.spec),
        _RequestEmbedder(model, embedder, log),
        persist,
        chunk_size=ceil(batch_size / model.spec.batch_size),
        max_workers=max_concurrency,
        logger=log,
    )
    embedded_df = (
        pl.concat(upserted)
        if upserted
        else pl.DataFrame(schema=EMBEDDING_CHECKPOINT_SCHEMA)
    )
    return embedded_df, EmbedReport(
        failures=failures, input_tokens=input_tokens, batch=report
    )
