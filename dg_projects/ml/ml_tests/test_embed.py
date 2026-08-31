"""Tests for ml.lib.embed."""

import polars as pl
import pytest
from ml.lib import embed


class _FakeEmbeddingClient:
    """Returns a deterministic vector derived from the input text's length."""

    def __init__(
        self, model_version: str = "text-embedding-3-large", dim: int = 3
    ) -> None:
        self.model_version = model_version
        self.dim = dim
        self.calls: list[str] = []

    def embed(self, text: str) -> list[float]:
        self.calls.append(text)
        if text == "boom":
            msg = "simulated API failure"
            raise RuntimeError(msg)
        return [float(len(text))] * self.dim


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
            "embedding_input": ["summary", "concatenated_turns"],
            "resolved_text": ["a", "b"],
        }
    )
    already_embedded_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "embedding_input": ["summary"],
            "embedding_model_version": ["text-embedding-3-large"],
        }
    )

    result = embed.filter_unembedded(
        source_df,
        already_embedded_df,
        current_model_version="text-embedding-3-large",
    )

    assert result["conversation_ref"].to_list() == ["2"]


def test_filter_unembedded_reembeds_on_input_arm_change() -> None:
    """A conversation embedded off concatenated_turns is reprocessed once summarized."""
    source_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "embedding_input": ["summary"],
            "resolved_text": ["a short summary"],
        }
    )
    already_embedded_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "embedding_input": ["concatenated_turns"],
            "embedding_model_version": ["text-embedding-3-large"],
        }
    )

    result = embed.filter_unembedded(
        source_df,
        already_embedded_df,
        current_model_version="text-embedding-3-large",
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unembedded_reembeds_on_stale_model_version() -> None:
    source_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "embedding_input": ["summary"],
            "resolved_text": ["a short summary"],
        }
    )
    already_embedded_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "embedding_input": ["summary"],
            "embedding_model_version": ["old-model"],
        }
    )

    result = embed.filter_unembedded(
        source_df,
        already_embedded_df,
        current_model_version="text-embedding-3-large",
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_embed_conversations_skips_null_text_and_failed_calls() -> None:
    client = _FakeEmbeddingClient()
    df = pl.DataFrame(
        {
            "source_slug": ["zendesk", "zendesk", "zendesk"],
            "conversation_ref": ["1", "2", "3"],
            "embedding_input": ["summary", "concatenated_turns", "summary"],
            "resolved_text": ["hello", None, "boom"],
        }
    )

    result = embed.embed_conversations(df, client)

    assert result["conversation_ref"].to_list() == ["1"]
    assert result["embedding_dim"].to_list() == [3]
    assert result["embedding_model_version"].to_list() == ["text-embedding-3-large"]
    assert result["embedding_vector"].to_list() == [[5.0, 5.0, 5.0]]
    # the null-text row never reaches the client; the failed call is dropped from
    # the output but still shows up as an attempted call
    assert client.calls == ["hello", "boom"]


def test_build_embedding_client_rejects_non_openai_clients() -> None:
    class _FakeLLM:
        def get_client(self) -> object:
            return object()

    with pytest.raises(TypeError, match="embeddings API"):
        embed.build_embedding_client(_FakeLLM())
