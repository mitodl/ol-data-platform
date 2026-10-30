"""Tests for ml.lib.embed."""

import io
import json
import threading
import time
from collections.abc import Sequence
from typing import Any

import boto3
import polars as pl
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber
from ml.lib import embed
from ml.lib.iceberg_helpers import upsert_chunk
from ol_llm import BatchFailed, LLMError, LLMRejected
from ol_llm.testing import override_embedding_models
from ol_orchestrate.lib.embeddings.models import EmbeddingBatch, EmbeddingModelSpec
from pydantic_ai.embeddings import TestEmbeddingModel
from pydantic_ai.embeddings.bedrock import BedrockEmbeddingModel
from pydantic_ai.embeddings.result import EmbedInputType
from pydantic_ai.models import override_allow_model_requests
from pydantic_ai.providers.bedrock import BedrockProvider
from pyiceberg.catalog.sql import SqlCatalog

OPENAI = embed.resolve_embedding_model("openai:text-embedding-3-large", 3)
TITAN = embed.resolve_embedding_model("bedrock:amazon.titan-embed-text-v2:0", 3)
VERTEX_TEXT = embed.resolve_embedding_model("google-cloud:text-embedding-005", 3)
COHERE = embed.resolve_embedding_model("bedrock:cohere.embed-english-v3", 3)

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


class _CohereEmbedder(_FakeEmbedder):
    """Rejects any request containing "boom" with Bedrock's ValidationException."""

    def __init__(self, message: str) -> None:
        super().__init__(COHERE)
        self._message = message

    def embed(
        self, texts: Sequence[str], *, input_type: EmbedInputType
    ) -> EmbeddingBatch:
        if "boom" in texts:
            self.calls.append(list(texts))
            raise _rejected(self._message.format(index=list(texts).index("boom")))
        return super().embed(texts, input_type=input_type)


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


TABLE = "some_db.feedback_embeddings"
Target = tuple[SqlCatalog, str]


@pytest.fixture
def target(iceberg_catalog: SqlCatalog) -> Target:
    return iceberg_catalog, TABLE


def _stored_rows(catalog: SqlCatalog) -> pl.DataFrame:
    return catalog.load_table(TABLE).scan().to_polars().sort("conversation_ref")


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


@pytest.mark.parametrize(
    ("model", "max_input_tokens"),
    [
        ("google:gemini-embedding-001", 2048),
        ("google:gemini-embedding-2", 8192),
        ("google:gemini-embedding-2-preview", 8192),
        ("google-cloud:gemini-embedding-001", 2048),
        ("google-cloud:gemini-embedding-2", 8192),
        ("google-cloud:text-embedding-005", 2048),
    ],
)
def test_resolve_embedding_model_caps_google_texts_per_model(
    model: str, max_input_tokens: int
) -> None:
    assert embed.resolve_embedding_model(model).max_input_tokens == max_input_tokens


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


def test_key_changes_with_the_text_the_arm_the_model_and_the_dim() -> None:
    keys = {
        OPENAI.key("a", "summary"),
        OPENAI.key("b", "summary"),
        OPENAI.key("a", "concatenated_turns"),
        embed.resolve_embedding_model("azure:text-embedding-3-large", 3).key(
            "a", "summary"
        ),
        embed.resolve_embedding_model("openai:text-embedding-3-large", 4).key(
            "a", "summary"
        ),
    }

    assert len(keys) == 5
    assert OPENAI.key("a", "summary") == OPENAI.key("a", "summary")


def test_input_text_cuts_titan_by_characters() -> None:
    assert TITAN.input_text("x" * 60_000) == "x" * 50_000


def test_input_text_cuts_openai_to_its_token_limit() -> None:
    text = "word " * 10_000

    cut = OPENAI.input_text(text)

    assert text.startswith(cut)
    assert len(cut) < len(text)
    assert OPENAI.input_text("short") == "short"


def _stored(
    keys: dict[str, str | None],
    *,
    turn_count: int = 2,
    embedding_input: str = "summary",
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "feedback_conversation_pk": list(keys),
            "turn_count": [turn_count] * len(keys),
            "embedding_input": [embedding_input] * len(keys),
            "idempotency_key": list(keys.values()),
        },
        schema={
            "feedback_conversation_pk": pl.String,
            "turn_count": pl.Int64,
            "embedding_input": pl.String,
            "idempotency_key": pl.String,
        },
    )


def _key(text: str) -> str:
    return OPENAI.key(text, "summary")


def test_filter_unembedded_drops_a_conversation_embedded_from_the_same_text() -> None:
    source = _rows("same", "new")

    result = embed.filter_unembedded(source, _stored({"pk-0": _key("same")}), OPENAI)

    assert result["conversation_ref"].to_list() == ["1"]
    assert result.columns == source.columns


def test_filter_unembedded_reembeds_a_changed_text() -> None:
    """A re-summarized conversation keeps its turn_count and arm, so only the text
    shows that its stored vector is stale.
    """
    result = embed.filter_unembedded(
        _rows("a new summary"), _stored({"pk-0": _key("the old summary")}), OPENAI
    )

    assert result["conversation_ref"].to_list() == ["0"]


def test_filter_unembedded_reembeds_an_arm_switch_with_the_same_text() -> None:
    """A summary equal to the concatenated turns must still update the stored arm,
    which decides the summary-scoped clusters.
    """
    stored = _stored({"pk-0": OPENAI.key("text", "concatenated_turns")})

    result = embed.filter_unembedded(_rows("text"), stored, OPENAI)

    assert result["conversation_ref"].to_list() == ["0"]


def test_filter_unembedded_keeps_a_keyless_row_with_the_same_turns_and_arm() -> None:
    """Rows embedded before the key existed are not all redone on deploy."""
    result = embed.filter_unembedded(_rows("text"), _stored({"pk-0": None}), OPENAI)

    assert result.height == 0


@pytest.mark.parametrize(
    ("turn_count", "embedding_input"), [(1, "summary"), (2, "concatenated_turns")]
)
def test_filter_unembedded_reembeds_a_keyless_row_whose_turns_or_arm_changed(
    turn_count: int, embedding_input: str
) -> None:
    stored = _stored(
        {"pk-0": None}, turn_count=turn_count, embedding_input=embedding_input
    )

    result = embed.filter_unembedded(_rows("text"), stored, OPENAI)

    assert result["conversation_ref"].to_list() == ["0"]


def test_filter_unembedded_keeps_new_rows_without_text_and_drops_stored_ones() -> None:
    result = embed.filter_unembedded(
        _rows(None, " "), _stored({"pk-0": _key("old")}), OPENAI
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_embed_and_checkpoint_skips_null_and_blank_text(target: Target) -> None:
    """Vertex's :predict sends no instance for an empty text, which would fail
    every other row in the same request.
    """
    embedder = _FakeEmbedder(OPENAI)

    result, report = embed.embed_and_checkpoint(
        _rows("hello", None, "", " \n "), OPENAI, embedder, target
    )

    assert result["conversation_ref"].to_list() == ["0"]
    assert embedder.calls == [["hello"]]
    assert report.failures == []


def test_embed_and_checkpoint_writes_the_stored_model_key_and_float32_vectors(
    target: Target,
) -> None:
    result, report = embed.embed_and_checkpoint(
        _rows("hello"), OPENAI, _FakeEmbedder(OPENAI), target
    )

    [row] = result.to_dicts()
    assert row["embedding_model_version"] == "text-embedding-3-large"
    assert row["embedding_dim"] == 3
    assert row["embedding_vector"] == [5.0, 5.0, 5.0]
    assert row["idempotency_key"] == _key("hello")
    assert row["turn_count"] == 2
    assert result["embedding_vector"].dtype == pl.List(pl.Float32)
    assert result.columns == list(embed.EMBEDDING_CHECKPOINT_SCHEMA)
    assert report.input_tokens == 1


def test_embed_and_checkpoint_updates_a_table_from_before_idempotency_key(
    target: Target,
) -> None:
    """A table created before idempotency_key existed gets it appended and its
    stored vectors replaced, and the returned frame is in the table's column
    order, which the io_manager's upsert casts by.
    """
    catalog, identifier = target
    old_columns = [
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
    existing = embed._embedded_frame(
        OPENAI,
        [
            ({**row, "idempotency_key": None}, [0.0] * 3)
            for row in _rows("old", "old").to_dicts()
        ],
    ).select(old_columns)
    catalog.create_table(identifier, schema=existing.to_arrow().schema).append(
        existing.to_arrow()
    )

    result, _ = embed.embed_and_checkpoint(
        _rows("hello", "new", "added").filter(pl.col("conversation_ref") != "1"),
        OPENAI,
        _FakeEmbedder(OPENAI),
        target,
    )

    table_columns = [
        field.name for field in catalog.load_table(identifier).schema().fields
    ]
    assert table_columns == [*old_columns, "idempotency_key"]
    assert result.columns == table_columns
    stored = _stored_rows(catalog)
    assert stored["conversation_ref"].to_list() == ["0", "1", "2"]
    assert stored["embedding_vector"].to_list() == [[5.0] * 3, [0.0] * 3, [5.0] * 3]
    assert stored["idempotency_key"].is_null().to_list() == [False, True, False]


def test_embed_and_checkpoint_sends_requests_within_the_models_limits(
    target: Target,
) -> None:
    """Vertex's text-embedding models take five texts per request."""
    embedder = _FakeEmbedder(VERTEX_TEXT)
    texts = [f"text {i}" for i in range(7)]

    result, _ = embed.embed_and_checkpoint(
        _rows(*texts), VERTEX_TEXT, embedder, target, max_concurrency=1
    )

    assert result.height == 7
    assert embedder.calls == [texts[:5], texts[5:]]


def test_embed_and_checkpoint_upserts_about_batch_size_conversations_at_once(
    target: Target, monkeypatch: pytest.MonkeyPatch
) -> None:
    written: list[tuple[int, list[str]]] = []

    def recording_upsert_chunk(*args: Any) -> pl.DataFrame:
        written.append((args[2].height, args[3]))
        return upsert_chunk(*args)

    monkeypatch.setattr(embed, "upsert_chunk", recording_upsert_chunk)

    embed.embed_and_checkpoint(
        _rows(*(f"text {i}" for i in range(23))),
        VERTEX_TEXT,
        _FakeEmbedder(VERTEX_TEXT),
        target,
        batch_size=8,
    )

    # Two requests of five per chunk: 23 texts are five requests in three chunks.
    assert written == [(10, embed.UPSERT_JOIN_COLS)] * 2 + [(3, embed.UPSERT_JOIN_COLS)]
    assert _stored_rows(target[0]).height == 23


def test_embed_and_checkpoint_runs_requests_concurrently(target: Target) -> None:
    """Titan takes one text per request, so concurrency is its wall-clock lever."""
    call_delay = 0.2
    row_count = 8

    start = time.monotonic()
    result, _ = embed.embed_and_checkpoint(
        _rows(*(f"text {i}" for i in range(row_count))),
        TITAN,
        _FakeEmbedder(TITAN, delay=call_delay),
        target,
        max_concurrency=row_count,
    )
    elapsed = time.monotonic() - start

    assert result.height == row_count
    # Serial would take row_count * call_delay (1.6s).
    assert elapsed < row_count * call_delay / 2


def test_embed_and_checkpoint_isolates_a_rejected_text_in_a_request(
    target: Target,
) -> None:
    embedder = _FakeEmbedder(OPENAI)

    result, report = embed.embed_and_checkpoint(
        _rows("hello", "boom", "world"), OPENAI, embedder, target
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


def test_cohere_isolates_a_text_bedrock_names_in_its_rejection(
    target: Target,
) -> None:
    embedder = _CohereEmbedder(
        "ValidationException: #/texts/{index}: expected maxLength"
    )

    result, report = embed.embed_and_checkpoint(
        _rows("hello", "boom", "world"), COHERE, embedder, target
    )

    assert result["conversation_ref"].to_list() == ["0", "2"]
    assert len(embedder.calls) == 4
    [failure] = report.failures
    assert failure.row["conversation_ref"] == "1"


def test_cohere_fails_a_request_level_rejection_without_splitting_it(
    target: Target,
) -> None:
    """A 400 that names no text (e.g. a dimension the model does not take) fails
    the same way for every text alone, so it is not retried 96 times.
    """
    embedder = _CohereEmbedder("ValidationException: Malformed input request")

    with pytest.raises(BatchFailed, match="Malformed input request"):
        embed.embed_and_checkpoint(
            _rows("hello", "boom", "world"), COHERE, embedder, target
        )

    assert embedder.calls == [["hello", "boom", "world"]]


def test_embed_and_checkpoint_stops_on_a_rejected_credential(target: Target) -> None:
    """A 401 opens the circuit at once rather than failing every later request."""
    catalog, identifier = target
    embedder = _FakeEmbedder(TITAN)

    with pytest.raises(BatchFailed) as excinfo:
        embed.embed_and_checkpoint(
            _rows("denied", "hello", "world"),
            TITAN,
            embedder,
            target,
            batch_size=1,
            max_concurrency=1,
        )

    assert embedder.calls == [["denied"]]
    assert excinfo.value.report.circuit_opened_by is not None
    # The failed chunk writes nothing.
    assert not catalog.table_exists(identifier)


def test_embed_and_checkpoint_keeps_earlier_chunks_when_a_later_one_stops(
    target: Target,
) -> None:
    with pytest.raises(BatchFailed):
        embed.embed_and_checkpoint(
            _rows("hello", "denied"),
            TITAN,
            _FakeEmbedder(TITAN),
            target,
            batch_size=1,
            max_concurrency=1,
        )

    assert _stored_rows(target[0])["conversation_ref"].to_list() == ["0"]


def test_titan_shortens_a_text_over_the_token_limit_and_retries(target: Target) -> None:
    embedder = _TitanLimitEmbedder(max_chars=10_000)

    result, _ = embed.embed_and_checkpoint(_rows("x" * 40_000), TITAN, embedder, target)

    assert result.height == 1
    assert [len(text) for [text] in embedder.calls] == [40_000, 9_000]


def test_titan_fails_a_rejection_that_is_not_about_length(target: Target) -> None:
    embedder = _TitanLimitEmbedder(
        max_chars=0, message="The provided model identifier is invalid."
    )

    with pytest.raises(BatchFailed, match="model identifier is invalid"):
        embed.embed_and_checkpoint(_rows("a"), TITAN, embedder, target)

    assert len(embedder.calls) == 1


def _bedrock_response(payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload).encode()
    return {
        "body": StreamingBody(io.BytesIO(body), len(body)),
        "contentType": "application/json",
    }


def _bedrock_client() -> Any:
    return boto3.client(
        "bedrock-runtime",
        region_name="us-east-1",
        aws_access_key_id="test",  # pragma: allowlist secret
        aws_secret_access_key="test",  # pragma: allowlist secret
    )


def test_titan_shortens_through_ol_llm_and_pydantic_ai_bedrock(target: Target) -> None:
    """Titan's real over-limit error, through PydanticAI's Bedrock model and
    ol-llm's policy wrapper, arrives as LLMRejected with the token counts in its
    message, which is what the shorten-and-retry reads.
    """
    client = _bedrock_client()
    stubber = Stubber(client)
    stubber.add_client_error(
        "invoke_model",
        service_error_code="ValidationException",
        service_message=TITAN_TOO_LONG.format(max_tokens=8192, tokens=16384),
        http_status_code=400,
    )
    stubber.add_response(
        "invoke_model", _bedrock_response({"embedding": [0.1, 0.2, 0.3]})
    )
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
            target,
            max_concurrency=1,
        )
        stubber.assert_no_pending_responses()

    assert result["embedding_vector"].to_list() == [
        pytest.approx([0.1, 0.2, 0.3], rel=1e-6)
    ]


def test_cohere_isolates_through_ol_llm_and_pydantic_ai_bedrock(
    target: Target,
) -> None:
    """Bedrock's per-text error, through PydanticAI's Bedrock model and ol-llm's
    policy wrapper, keeps the "#/texts/<index>" pointer in LLMRejected's message,
    which is what rejects_one_text reads.
    """
    client = _bedrock_client()
    stubber = Stubber(client)

    def reject_text(index: int) -> None:
        stubber.add_client_error(
            "invoke_model",
            service_error_code="ValidationException",
            service_message=(
                f"Malformed input request: #/texts/{index}: expected maxLength: "
                "2048, actual: 3000, please reformat your input and try again."
            ),
            http_status_code=400,
        )

    # The request, then "hello", "boom", and "world" one at a time.
    reject_text(1)
    stubber.add_response("invoke_model", _bedrock_response({"embeddings": [[0.1] * 3]}))
    reject_text(0)
    stubber.add_response("invoke_model", _bedrock_response({"embeddings": [[0.2] * 3]}))
    model = BedrockEmbeddingModel(
        "cohere.embed-english-v3", provider=BedrockProvider(bedrock_client=client)
    )

    with (
        stubber,
        override_embedding_models(model),
        override_allow_model_requests(allow_model_requests=True),
    ):
        result, report = embed.embed_and_checkpoint(
            _rows("hello", "boom", "world"),
            COHERE,
            COHERE.embedder(run_id="run"),
            target,
            max_concurrency=1,
        )
        stubber.assert_no_pending_responses()

    assert result["conversation_ref"].to_list() == ["0", "2"]
    [failure] = report.failures
    assert failure.row["conversation_ref"] == "1"


def test_embedder_calls_ol_llm_with_the_feedback_embedding_use_case(
    target: Target,
) -> None:
    embedder = OPENAI.embedder(run_id="run")

    with override_embedding_models(TestEmbeddingModel()):
        result, report = embed.embed_and_checkpoint(
            _rows("hello", "world"), OPENAI, embedder, target, max_concurrency=1
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
