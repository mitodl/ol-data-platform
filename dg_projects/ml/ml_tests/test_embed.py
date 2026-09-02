"""Tests for ml.lib.embed."""

import polars as pl
import pytest
from ml.lib import embed


class _FakeEmbeddingClient:
    """Returns a deterministic vector derived from each input text's length.

    Simulates a per-batch failure when any text in the batch is "boom", so
    embed_conversations' chunk-level retry fallback can be exercised.
    """

    def __init__(
        self, model_version: str = "text-embedding-3-large", dim: int = 3
    ) -> None:
        self.model_version = model_version
        self.dim = dim
        self.batch_calls: list[list[str]] = []

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        self.batch_calls.append(texts)
        if "boom" in texts:
            msg = "simulated API failure"
            raise RuntimeError(msg)
        return [[float(len(text))] * self.dim for text in texts]


def test_resolve_embedding_text_picks_summary_or_concatenated_turns() -> None:
    summaries_df = pl.DataFrame(
        {
            "source_slug": ["zendesk", "zendesk"],
            "conversation_ref": ["1", "2"],
            "turn_count": [3, 1],
            "conversation_summary": ["a short summary", None],
            "embedding_input": ["summary", "concatenated_turns"],
        }
    )
    conversation_df = pl.DataFrame(
        {
            "source_slug": ["zendesk", "zendesk"],
            "conversation_ref": ["1", "2"],
            "conversation_text": ["full turn 1\n---\nfull turn 2", "one turn"],
        }
    )

    resolved = embed.resolve_embedding_text(summaries_df, conversation_df)

    resolved_by_ref = {row["conversation_ref"]: row for row in resolved.to_dicts()}
    assert resolved_by_ref["1"]["resolved_text"] == "a short summary"
    assert resolved_by_ref["2"]["resolved_text"] == "one turn"


def test_filter_unembedded_drops_already_embedded_rows() -> None:
    source_df = pl.DataFrame(
        {
            "source_slug": ["zendesk", "zendesk"],
            "conversation_ref": ["1", "2"],
            "turn_count": [3, 1],
            "embedding_input": ["summary", "concatenated_turns"],
            "resolved_text": ["a", "b"],
        }
    )
    already_embedded_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [3],
            "embedding_input": ["summary"],
            "embedding_model_version": ["text-embedding-3-large"],
            "embedding_dim": [1024],
        }
    )

    result = embed.filter_unembedded(
        source_df,
        already_embedded_df,
        current_model_version="text-embedding-3-large",
        current_dim=1024,
    )

    assert result["conversation_ref"].to_list() == ["2"]


def test_filter_unembedded_reembeds_on_turn_count_change() -> None:
    """A conversation that gained a turn is reprocessed even if the arm is unchanged.

    embedding_input alone can't detect this: the arm (summary/concatenated_turns)
    usually stays the same across a turn_count change.
    """
    source_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [4],
            "embedding_input": ["summary"],
            "resolved_text": ["an updated summary"],
        }
    )
    already_embedded_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [3],
            "embedding_input": ["summary"],
            "embedding_model_version": ["text-embedding-3-large"],
            "embedding_dim": [1024],
        }
    )

    result = embed.filter_unembedded(
        source_df,
        already_embedded_df,
        current_model_version="text-embedding-3-large",
        current_dim=1024,
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unembedded_reembeds_on_input_arm_change() -> None:
    """A conversation embedded off concatenated_turns is reprocessed once summarized."""
    source_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [3],
            "embedding_input": ["summary"],
            "resolved_text": ["a short summary"],
        }
    )
    already_embedded_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [3],
            "embedding_input": ["concatenated_turns"],
            "embedding_model_version": ["text-embedding-3-large"],
            "embedding_dim": [1024],
        }
    )

    result = embed.filter_unembedded(
        source_df,
        already_embedded_df,
        current_model_version="text-embedding-3-large",
        current_dim=1024,
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unembedded_reembeds_on_stale_model_version() -> None:
    source_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [3],
            "embedding_input": ["summary"],
            "resolved_text": ["a short summary"],
        }
    )
    already_embedded_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [3],
            "embedding_input": ["summary"],
            "embedding_model_version": ["old-model"],
            "embedding_dim": [1024],
        }
    )

    result = embed.filter_unembedded(
        source_df,
        already_embedded_df,
        current_model_version="text-embedding-3-large",
        current_dim=1024,
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unembedded_reembeds_on_stale_dim() -> None:
    """A dimension sweep on the same model (e.g. 512 -> 1024) triggers re-embedding."""
    source_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [3],
            "embedding_input": ["summary"],
            "resolved_text": ["a short summary"],
        }
    )
    already_embedded_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [3],
            "embedding_input": ["summary"],
            "embedding_model_version": ["text-embedding-3-large"],
            "embedding_dim": [512],
        }
    )

    result = embed.filter_unembedded(
        source_df,
        already_embedded_df,
        current_model_version="text-embedding-3-large",
        current_dim=1024,
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_embed_conversations_skips_null_text() -> None:
    client = _FakeEmbeddingClient()
    df = pl.DataFrame(
        {
            "source_slug": ["zendesk", "zendesk"],
            "conversation_ref": ["1", "2"],
            "turn_count": [3, 1],
            "embedding_input": ["summary", "concatenated_turns"],
            "resolved_text": ["hello", None],
        }
    )

    result = embed.embed_conversations(df, client)

    assert result["conversation_ref"].to_list() == ["1"]
    assert result["turn_count"].to_list() == [3]
    assert result["embedding_dim"].to_list() == [3]
    assert result["embedding_model_version"].to_list() == ["text-embedding-3-large"]
    assert result["embedding_vector"].to_list() == [[5.0, 5.0, 5.0]]
    assert result["embedding_vector"].dtype == pl.List(pl.Float32)
    # the null-text row is filtered out before any batch is ever sent
    assert client.batch_calls == [["hello"]]


def test_embed_conversations_batches_calls() -> None:
    """Multiple rows within EMBEDDING_BATCH_SIZE go out in a single API call."""
    client = _FakeEmbeddingClient()
    df = pl.DataFrame(
        {
            "source_slug": ["zendesk", "zendesk", "zendesk"],
            "conversation_ref": ["1", "2", "3"],
            "turn_count": [1, 1, 1],
            "embedding_input": ["summary", "summary", "summary"],
            "resolved_text": ["hi", "hello", "hey"],
        }
    )

    result = embed.embed_conversations(df, client)

    assert sorted(result["conversation_ref"].to_list()) == ["1", "2", "3"]
    # one batch call carrying all three texts, not three separate calls
    assert client.batch_calls == [["hi", "hello", "hey"]]


def test_embed_conversations_retries_individually_on_batch_failure() -> None:
    """A bad row fails the batch call; the rest are recovered by retrying solo."""
    client = _FakeEmbeddingClient()
    df = pl.DataFrame(
        {
            "source_slug": ["zendesk", "zendesk", "zendesk"],
            "conversation_ref": ["1", "2", "3"],
            "turn_count": [1, 1, 1],
            "embedding_input": ["summary", "summary", "summary"],
            "resolved_text": ["hello", "boom", "world"],
        }
    )

    result = embed.embed_conversations(df, client)

    # "2" (the "boom" row) is dropped; "1" and "3" are recovered via solo retries
    assert sorted(result["conversation_ref"].to_list()) == ["1", "3"]
    assert client.batch_calls[0] == ["hello", "boom", "world"]
    # after the batch fails, each row is retried one at a time
    assert ["hello"] in client.batch_calls
    assert ["boom"] in client.batch_calls
    assert ["world"] in client.batch_calls


def test_build_embedding_client_rejects_non_openai_clients() -> None:
    class _FakeLLM:
        def get_client(self) -> object:
            return object()

    with pytest.raises(TypeError, match="embeddings API"):
        embed.build_embedding_client(_FakeLLM())


class _FakeTable:
    def __init__(self) -> None:
        self.upserts: list[dict[str, object]] = []

    def upsert(self, **kwargs: object) -> None:
        self.upserts.append(kwargs)


class _FakeCatalog:
    def __init__(self, table: _FakeTable) -> None:
        self._table = table
        self.load_calls: list[str] = []

    def load_table(self, identifier: str) -> _FakeTable:
        self.load_calls.append(identifier)
        return self._table


def _embedding_df(**overrides: object) -> pl.DataFrame:
    row = {
        "source_slug": "zendesk",
        "conversation_ref": "1",
        "turn_count": 1,
        "embedding_input": "summary",
        "resolved_text": "hello",
    }
    row.update(overrides)
    return pl.DataFrame([row])


def test_checkpoint_embedding_chunk_upserts_a_non_empty_chunk() -> None:
    table = _FakeTable()
    catalog = _FakeCatalog(table)
    chunk_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [1],
            "embedding_input": ["summary"],
            "embedding_vector": [[0.1, 0.2, 0.3]],
            "embedding_dim": [3],
            "embedding_model_version": ["text-embedding-3-large"],
        }
    )

    embed.checkpoint_embedding_chunk(catalog, "some_db.feedback_embeddings", chunk_df)

    assert catalog.load_calls == ["some_db.feedback_embeddings"]
    assert len(table.upserts) == 1
    assert table.upserts[0]["join_cols"] == embed.JOIN_COLS


def test_checkpoint_embedding_chunk_skips_empty_chunks_without_touching_catalog() -> (
    None
):
    table = _FakeTable()
    catalog = _FakeCatalog(table)
    empty_df = pl.DataFrame(schema=embed.EMBEDDING_CHECKPOINT_SCHEMA)

    embed.checkpoint_embedding_chunk(catalog, "some_db.feedback_embeddings", empty_df)

    assert catalog.load_calls == []
    assert table.upserts == []


def test_embed_and_checkpoint_upserts_each_chunk() -> None:
    table = _FakeTable()
    catalog = _FakeCatalog(table)
    client = _FakeEmbeddingClient()
    df = pl.concat(
        [_embedding_df(conversation_ref=str(i)) for i in range(5)],
        how="vertical_relaxed",
    )

    result = embed.embed_and_checkpoint(
        df,
        client,
        (catalog, "some_db.feedback_embeddings"),
        batch_size=2,
    )

    assert result.height == 5
    # 3 chunks of size 2, 2, 1 -- one upsert call per chunk
    assert len(table.upserts) == 3


def test_embed_and_checkpoint_aborts_early_on_a_systemic_failure() -> None:
    """A credential-type failure shouldn't burn through every remaining chunk with
    the same error -- a whole chunk with zero successes should abort the run
    (default EMBEDDING_MAX_CONSECUTIVE_FAILED_CHUNKS=1) instead of trying every
    chunk. Chunks already upserted before the abort stay in the real table -- no
    separate recovery step needed on the next run.
    """

    class _AlwaysFailingClient:
        model_version = "test-model"
        dim = 3

        def embed_batch(self, texts: list[str]) -> list[list[float]]:  # noqa: ARG002
            msg = "simulated auth failure"
            raise RuntimeError(msg)

    table = _FakeTable()
    catalog = _FakeCatalog(table)
    batch_size = 2
    df = pl.concat(
        [_embedding_df(conversation_ref=str(i)) for i in range(10)],
        how="vertical_relaxed",
    )
    errors: list[str] = []

    result = embed.embed_and_checkpoint(
        df,
        _AlwaysFailingClient(),
        (catalog, "some_db.feedback_embeddings"),
        batch_size=batch_size,
        errors=errors,
    )

    assert result.height == 0
    # Aborted after the first fully-failed chunk, not all 10 rows.
    assert len(errors) == batch_size
    assert len(errors) < df.height
