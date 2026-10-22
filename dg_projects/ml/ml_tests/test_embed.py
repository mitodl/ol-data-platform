"""Tests for ml.lib.embed."""

import io
import json
import threading
import time
from collections.abc import Sequence
from typing import Any, Self

import boto3
import polars as pl
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber
from ml.lib import embed
from ol_llm import BatchFailed, LLMError, LLMRejected
from ol_llm.testing import override_embedding_models
from ol_orchestrate.lib.embeddings.models import EmbeddingBatch, EmbeddingModelSpec
from pydantic_ai.embeddings import TestEmbeddingModel
from pydantic_ai.embeddings.bedrock import BedrockEmbeddingModel
from pydantic_ai.embeddings.result import EmbedInputType
from pydantic_ai.models import override_allow_model_requests
from pydantic_ai.providers.bedrock import BedrockProvider

OPENAI = embed.resolve_embedding_model("openai:text-embedding-3-large", 3)
TITAN = embed.resolve_embedding_model("bedrock:amazon.titan-embed-text-v2:0", 3)
VERTEX_TEXT = embed.resolve_embedding_model("google-cloud:text-embedding-005", 3)

TITAN_TOO_LONG = (
    "400 Bad Request: Too many input tokens. Max input tokens: {max_tokens}, "
    "request input token count: {tokens} "
)


def _rejected(message: str, status_code: int = 400) -> LLMRejected:
    return LLMRejected(
        message, use_case="feedback-embedding", model="test", status_code=status_code
    )


class _FakeEmbedder:
    """An Embedder that returns a vector derived from each text's length.

    Raises a 400 row rejection for any request containing "boom", and a 401 for
    any request containing "denied".
    """

    def __init__(
        self, model: embed.FeedbackEmbeddingModel, *, delay: float = 0
    ) -> None:
        self._spec = model.spec
        self._delay = delay
        self.calls: list[list[str]] = []
        self._lock = threading.Lock()

    @property
    def spec(self) -> EmbeddingModelSpec:
        return self._spec

    def embed(
        self, texts: Sequence[str], *, input_type: EmbedInputType
    ) -> EmbeddingBatch:
        assert input_type == "document"
        with self._lock:
            self.calls.append(list(texts))
        time.sleep(self._delay)
        if "boom" in texts:
            msg = "status_code: 400, body: text 1 is malformed"
            raise _rejected(msg)
        if "denied" in texts:
            msg = "status_code: 401, body: bad credential"
            raise _rejected(msg, status_code=401)
        return EmbeddingBatch(
            vectors=[[float(len(text))] * self._spec.vector_length for text in texts],
            input_tokens=len(texts),
        )


class _TitanLimitEmbedder(_FakeEmbedder):
    """Rejects texts over max_chars the way Titan does, counting 2 chars per token."""

    def __init__(self, max_chars: int, message: str | None = None) -> None:
        super().__init__(TITAN)
        self._max_chars = max_chars
        self._message = message

    def embed(
        self, texts: Sequence[str], *, input_type: EmbedInputType
    ) -> EmbeddingBatch:
        assert input_type == "document"
        [text] = texts
        self.calls.append([text])
        if len(text) > self._max_chars:
            message = self._message or TITAN_TOO_LONG.format(
                max_tokens=self._max_chars // 2, tokens=len(text) // 2
            )
            raise _rejected(message)
        return EmbeddingBatch(vectors=[[0.1, 0.2, 0.3]], input_tokens=1)


class _FakeField:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeSchema:
    def __init__(self, top_level_names: list[str], nested_extra: list[str]) -> None:
        # Mirrors real pyiceberg: column_names flattens in nested field names
        # (e.g. a List column's "element" child) that fields/columns does not.
        self.column_names = top_level_names + nested_extra
        self.fields = [_FakeField(name) for name in top_level_names]


class _FakeSchemaUpdate:
    def __init__(self, table: "_FakeTable") -> None:
        self._table = table

    def union_by_name(self, schema: Any) -> None:
        # Mirrors real union_by_name: appends any not-yet-seen field at the end
        # of the table's column order, never inserting it where the incoming
        # schema happens to place it.
        for name in schema.names:
            if name not in self._table._column_names:
                self._table._column_names.append(name)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        pass


class _FakeTable:
    def __init__(self) -> None:
        self.upserts: list[dict[str, Any]] = []
        self._column_names: list[str] = []
        self._nested_extra_column_names: list[str] = []

    def update_schema(self) -> _FakeSchemaUpdate:
        return _FakeSchemaUpdate(self)

    def schema(self) -> _FakeSchema:
        return _FakeSchema(self._column_names, self._nested_extra_column_names)

    def upsert(self, **kwargs: Any) -> None:
        self.upserts.append(kwargs)


class _FakeCatalog:
    def __init__(self, table: _FakeTable) -> None:
        self._table = table
        self.create_calls: list[str] = []

    def create_table_if_not_exists(self, identifier: str, **kwargs: Any) -> _FakeTable:
        self.create_calls.append(identifier)
        if not self._table._column_names:
            schema = kwargs.get("schema")
            if schema is not None:
                self._table._column_names = list(schema.names)
        return self._table


def _target() -> tuple[_FakeCatalog, str]:
    return _FakeCatalog(_FakeTable()), "some_db.feedback_embeddings"


def _rows(*texts: str | None) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "feedback_conversation_pk": [f"pk-{i}" for i in range(len(texts))],
            "source_slug": ["zendesk"] * len(texts),
            "conversation_ref": [str(i) for i in range(len(texts))],
            "turn_count": [2] * len(texts),
            "embedding_input": ["summary"] * len(texts),
            "resolved_text": list(texts),
        },
        schema_overrides={"resolved_text": pl.String},
    )


def test_resolve_embedding_text_picks_summary_or_concatenated_turns() -> None:
    summaries_df = pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-1", "pk-2"],
            "source_slug": ["zendesk", "zendesk"],
            "conversation_ref": ["1", "2"],
            "turn_count": [3, 1],
            "conversation_summary": ["a short summary", None],
            "embedding_input": ["summary", "concatenated_turns"],
        }
    )
    conversation_df = pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-1", "pk-2"],
            "source_slug": ["zendesk", "zendesk"],
            "conversation_ref": ["1", "2"],
            "conversation_text": ["full turn 1\n---\nfull turn 2", "one turn"],
        }
    )

    resolved = embed.resolve_embedding_text(summaries_df, conversation_df)

    resolved_by_ref = {row["conversation_ref"]: row for row in resolved.to_dicts()}
    assert resolved_by_ref["1"]["resolved_text"] == "a short summary"
    assert resolved_by_ref["2"]["resolved_text"] == "one turn"


@pytest.mark.parametrize(
    ("model", "stored"),
    [
        ("openai:text-embedding-3-large", "text-embedding-3-large"),
        ("bedrock:amazon.titan-embed-text-v2:0", "amazon.titan-embed-text-v2:0"),
        ("google-cloud:gemini-embedding-001", "gemini-embedding-001"),
    ],
)
def test_resolve_embedding_model_stores_the_id_the_vendor_clients_wrote(
    model: str, stored: str
) -> None:
    resolved = embed.resolve_embedding_model(model, 512)

    assert resolved.model == model
    assert resolved.model_version == stored
    assert resolved.dim == 512


def test_resolve_embedding_model_defaults_to_the_module_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        embed, "EMBEDDING_MODEL", "bedrock:amazon.titan-embed-text-v2:0"
    )
    monkeypatch.setattr(embed, "EMBEDDING_DIM", 256)

    resolved = embed.resolve_embedding_model()

    assert resolved.model_version == "amazon.titan-embed-text-v2:0"
    assert resolved.dim == 256
    assert resolved.spec.dimensions == 256


@pytest.mark.parametrize(
    ("model", "batch_size", "max_request_tokens"),
    [
        ("google:gemini-embedding-001", 250, 250_000),
        ("google-cloud:gemini-embedding-001", 1, 20_000),
        ("google-cloud:text-embedding-005", 5, 20_000),
        ("google-cloud:text-multilingual-embedding-002", 5, 20_000),
        ("bedrock:amazon.titan-embed-text-v2:0", 1, 250_000),
        ("bedrock:cohere.embed-english-v3", 96, 250_000),
    ],
)
def test_resolve_embedding_model_sizes_requests_per_backend_and_model(
    model: str, batch_size: int, max_request_tokens: int
) -> None:
    spec = embed.resolve_embedding_model(model).spec

    assert spec.batch_size == batch_size
    assert spec.max_request_tokens == max_request_tokens


def test_resolve_embedding_model_rejects_a_model_it_cannot_size() -> None:
    with pytest.raises(ValueError, match="No request sizing"):
        embed.resolve_embedding_model(
            "bedrock:amazon.nova-2-multimodal-embeddings-v1:0"
        )


def test_matches_selects_the_model_version_and_dim() -> None:
    rows = pl.DataFrame(
        {
            "embedding_model_version": [
                "text-embedding-3-large",
                "text-embedding-3-large",
                "openai:text-embedding-3-large",
            ],
            "embedding_dim": [3, 1024, 3],
        }
    )

    assert rows.filter(OPENAI.matches()).height == 1


def test_input_text_cuts_titan_by_characters() -> None:
    assert TITAN.input_text("x" * 60_000) == "x" * 50_000


def test_input_text_cuts_openai_to_its_token_limit() -> None:
    text = "word " * 10_000

    cut = OPENAI.input_text(text)

    assert text.startswith(cut)
    assert len(cut) < len(text)
    assert OPENAI.input_text("short") == "short"


def _stored(turn_count: int, embedding_input: str = "summary") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-0"],
            "turn_count": [turn_count],
            "embedding_input": [embedding_input],
        }
    )


def test_filter_unembedded_drops_already_embedded_rows() -> None:
    source = _rows("same", "new")

    result = embed.filter_unembedded(source, _stored(2))

    assert result["conversation_ref"].to_list() == ["1"]
    assert result.columns == source.columns


def test_filter_unembedded_reembeds_on_turn_count_change() -> None:
    result = embed.filter_unembedded(_rows("an updated summary"), _stored(1))

    assert result["conversation_ref"].to_list() == ["0"]


def test_filter_unembedded_reembeds_on_input_arm_change() -> None:
    result = embed.filter_unembedded(
        _rows("a short summary"), _stored(2, "concatenated_turns")
    )

    assert result["conversation_ref"].to_list() == ["0"]


def test_embed_and_checkpoint_skips_null_and_blank_text() -> None:
    """Vertex's :predict sends no instance for an empty text, which would fail
    every other row in the same request.
    """
    embedder = _FakeEmbedder(OPENAI)

    result, report = embed.embed_and_checkpoint(
        _rows("hello", None, "", " \n "), OPENAI, embedder, _target()
    )

    assert result["conversation_ref"].to_list() == ["0"]
    assert embedder.calls == [["hello"]]
    assert report.failures == []


def test_embed_and_checkpoint_writes_the_stored_model_and_float32_vectors() -> None:
    result, report = embed.embed_and_checkpoint(
        _rows("hello"), OPENAI, _FakeEmbedder(OPENAI), _target()
    )

    [row] = result.to_dicts()
    assert row["embedding_model_version"] == "text-embedding-3-large"
    assert row["embedding_dim"] == 3
    assert row["embedding_vector"] == [5.0, 5.0, 5.0]
    assert row["turn_count"] == 2
    assert result["embedding_vector"].dtype == pl.List(pl.Float32)
    assert result.columns == list(embed.EMBEDDING_CHECKPOINT_SCHEMA)
    assert report.input_tokens == 1


def test_embed_and_checkpoint_returns_the_existing_tables_column_order() -> None:
    """The io_manager upserts the returned frame by position, so it must match
    the table's column order, not the frame's.
    """
    catalog, identifier = _target()
    table = catalog._table
    table._column_names = [
        "feedback_conversation_pk",
        "source_slug",
        "conversation_ref",
        "turn_count",
        "embedding_vector",
        "embedding_input",
        "embedding_dim",
        "embedding_model_version",
        "embedded_at",
    ]

    result, _ = embed.embed_and_checkpoint(
        _rows("hello"), OPENAI, _FakeEmbedder(OPENAI), (catalog, identifier)
    )

    assert result.columns == table._column_names
    assert table.upserts[0]["df"].schema.names == result.columns


def test_embed_and_checkpoint_sends_requests_within_the_models_limits() -> None:
    """Vertex's text-embedding models take five texts per request."""
    embedder = _FakeEmbedder(VERTEX_TEXT)
    texts = [f"text {i}" for i in range(7)]

    result, _ = embed.embed_and_checkpoint(
        _rows(*texts), VERTEX_TEXT, embedder, _target(), max_concurrency=1
    )

    assert result.height == 7
    assert embedder.calls == [texts[:5], texts[5:]]


def test_embed_and_checkpoint_upserts_about_batch_size_conversations_at_once() -> None:
    catalog, identifier = _target()
    embedder = _FakeEmbedder(VERTEX_TEXT)

    embed.embed_and_checkpoint(
        _rows(*(f"text {i}" for i in range(23))),
        VERTEX_TEXT,
        embedder,
        (catalog, identifier),
        batch_size=8,
    )

    # Two requests of five per chunk: 23 texts are five requests in three chunks.
    assert [upsert["df"].num_rows for upsert in catalog._table.upserts] == [10, 10, 3]
    assert catalog._table.upserts[0]["join_cols"] == embed.UPSERT_JOIN_COLS


def test_embed_and_checkpoint_runs_requests_concurrently() -> None:
    """Titan takes one text per request, so concurrency is its wall-clock lever."""
    call_delay = 0.2
    row_count = 8

    start = time.monotonic()
    result, _ = embed.embed_and_checkpoint(
        _rows(*(f"text {i}" for i in range(row_count))),
        TITAN,
        _FakeEmbedder(TITAN, delay=call_delay),
        _target(),
        max_concurrency=row_count,
    )
    elapsed = time.monotonic() - start

    assert result.height == row_count
    # Serial would take row_count * call_delay (1.6s).
    assert elapsed < row_count * call_delay / 2


def test_embed_and_checkpoint_isolates_a_rejected_text_in_a_request() -> None:
    embedder = _FakeEmbedder(OPENAI)

    result, report = embed.embed_and_checkpoint(
        _rows("hello", "boom", "world"), OPENAI, embedder, _target()
    )

    assert result["conversation_ref"].to_list() == ["0", "2"]
    assert embedder.calls == [
        ["hello", "boom", "world"],
        ["hello"],
        ["boom"],
        ["world"],
    ]
    [failure] = report.failures
    assert failure.row["conversation_ref"] == "1"
    assert failure.retry_next_run is False


def test_embed_and_checkpoint_stops_on_a_rejected_credential() -> None:
    """A 401 opens the circuit at once rather than failing every later request."""
    catalog, identifier = _target()
    embedder = _FakeEmbedder(TITAN)

    with pytest.raises(BatchFailed) as excinfo:
        embed.embed_and_checkpoint(
            _rows("denied", "hello", "world"),
            TITAN,
            embedder,
            (catalog, identifier),
            batch_size=1,
            max_concurrency=1,
        )

    assert embedder.calls == [["denied"]]
    assert excinfo.value.report.circuit_opened_by is not None
    # The failed chunk upserts nothing.
    assert catalog._table.upserts == []


def test_embed_and_checkpoint_keeps_earlier_chunks_when_a_later_one_stops() -> None:
    catalog, identifier = _target()

    with pytest.raises(BatchFailed):
        embed.embed_and_checkpoint(
            _rows("hello", "denied"),
            TITAN,
            _FakeEmbedder(TITAN),
            (catalog, identifier),
            batch_size=1,
            max_concurrency=1,
        )

    assert [upsert["df"].num_rows for upsert in catalog._table.upserts] == [1]


def test_titan_shortens_a_text_over_the_token_limit_and_retries() -> None:
    embedder = _TitanLimitEmbedder(max_chars=10_000)

    result, _ = embed.embed_and_checkpoint(
        _rows("x" * 40_000), TITAN, embedder, _target()
    )

    assert result.height == 1
    assert [len(text) for [text] in embedder.calls] == [40_000, 9_000]


def test_titan_fails_a_rejection_that_is_not_about_length() -> None:
    embedder = _TitanLimitEmbedder(
        max_chars=0, message="The provided model identifier is invalid."
    )

    with pytest.raises(BatchFailed, match="model identifier is invalid"):
        embed.embed_and_checkpoint(_rows("a"), TITAN, embedder, _target())

    assert len(embedder.calls) == 1


def _titan_response(vector: list[float]) -> dict[str, Any]:
    body = json.dumps({"embedding": vector}).encode()
    return {
        "body": StreamingBody(io.BytesIO(body), len(body)),
        "contentType": "application/json",
    }


def test_titan_shortens_through_ol_llm_and_pydantic_ai_bedrock() -> None:
    """Titan's real over-limit error, through PydanticAI's Bedrock model and
    ol-llm's policy wrapper, arrives as LLMRejected with the token counts in its
    message, which is what the shorten-and-retry reads.
    """
    client = boto3.client(
        "bedrock-runtime",
        region_name="us-east-1",
        aws_access_key_id="test",  # pragma: allowlist secret
        aws_secret_access_key="test",  # pragma: allowlist secret
    )
    stubber = Stubber(client)
    stubber.add_client_error(
        "invoke_model",
        service_error_code="ValidationException",
        service_message=TITAN_TOO_LONG.format(max_tokens=8192, tokens=16384),
        http_status_code=400,
    )
    stubber.add_response("invoke_model", _titan_response([0.1, 0.2, 0.3]))
    model = BedrockEmbeddingModel(
        "amazon.titan-embed-text-v2:0", provider=BedrockProvider(bedrock_client=client)
    )

    # One worker: override_embedding_models is a context variable, which
    # process_batch's threads would not see.
    with (
        stubber,
        override_embedding_models(model),
        override_allow_model_requests(allow_model_requests=True),
    ):
        result, _ = embed.embed_and_checkpoint(
            _rows("x" * 20_000),
            TITAN,
            TITAN.embedder(run_id="run"),
            _target(),
            max_concurrency=1,
        )
        stubber.assert_no_pending_responses()

    assert result["embedding_vector"].to_list() == [
        pytest.approx([0.1, 0.2, 0.3], rel=1e-6)
    ]


def test_embedder_calls_ol_llm_with_the_feedback_embedding_use_case() -> None:
    embedder = OPENAI.embedder(run_id="run")

    with override_embedding_models(TestEmbeddingModel()):
        result, report = embed.embed_and_checkpoint(
            _rows("hello", "world"), OPENAI, embedder, _target(), max_concurrency=1
        )

    assert result["embedding_vector"].to_list() == [[1.0] * 3, [1.0] * 3]
    assert report.batch.succeeded == 1
    assert OPENAI.use_case().name == "feedback-embedding"
    assert OPENAI.use_case().profile == "batch"


def test_embedder_raises_ol_llm_errors_for_a_failed_request() -> None:
    class _Unreadable(TestEmbeddingModel):
        async def embed(self, inputs, *, input_type, settings=None):
            result = await super().embed(
                inputs, input_type=input_type, settings=settings
            )
            return type(result)(
                embeddings=result.embeddings[:-1],
                inputs=result.inputs,
                input_type=result.input_type,
                usage=result.usage,
                model_name=result.model_name,
                provider_name=result.provider_name,
            )

    with (
        override_embedding_models(_Unreadable()),
        pytest.raises(LLMError, match="returned 1 vectors for 2 texts"),
    ):
        OPENAI.embedder().embed(["a", "b"], input_type="document")


def test_checkpoint_embedding_chunk_reorders_using_top_level_fields_only() -> None:
    """Pyiceberg's Schema.column_names includes nested fields (e.g. a List
    column's "element" child, surfaced as "embedding_vector.element") -- using
    it to reorder columns would try to select a name Polars doesn't have.
    """
    catalog, identifier = _target()
    table = catalog._table
    table._column_names = list(embed.EMBEDDING_CHECKPOINT_SCHEMA)
    table._nested_extra_column_names = ["embedding_vector.element"]
    chunk_df = embed._embedded_frame(OPENAI, [(_rows("a").to_dicts()[0], [0.1] * 3)])

    embed.checkpoint_embedding_chunk(catalog, identifier, chunk_df)

    assert len(table.upserts) == 1


def test_checkpoint_embedding_chunk_skips_empty_chunks_without_touching_catalog() -> (
    None
):
    catalog, identifier = _target()

    embed.checkpoint_embedding_chunk(
        catalog, identifier, pl.DataFrame(schema=embed.EMBEDDING_CHECKPOINT_SCHEMA)
    )

    assert catalog.create_calls == []
    assert catalog._table.upserts == []
