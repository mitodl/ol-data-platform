"""Tests for ml.lib.summarize."""

import polars as pl
import pytest
from anthropic import Anthropic
from ml.lib import summarize
from openai import OpenAI


class _FakeSummaryClient:
    model_version = "test-model"

    def summarize(self, conversation_text: str) -> str:
        return f"summary of: {conversation_text}"


def _conversation_row(**overrides: object) -> dict[str, object]:
    row = {
        "source_slug": "zendesk",
        "conversation_ref": "1",
        "turn_count": 2,
        "conversation_text": "turn one\n---\nturn two",
        "conversation_text_chars": 600,
    }
    row.update(overrides)
    return row


def test_needs_summary_skips_single_turn_conversations() -> None:
    row = _conversation_row(turn_count=1, conversation_text_chars=10_000)

    assert summarize.needs_summary(row) is False


def test_needs_summary_skips_short_multi_turn_conversations() -> None:
    row = _conversation_row(conversation_text_chars=499)

    assert summarize.needs_summary(row) is False


def test_needs_summary_summarizes_long_multi_turn_conversations() -> None:
    row = _conversation_row(conversation_text_chars=500)

    assert summarize.needs_summary(row) is True


def test_needs_summary_rejects_null_conversation_text() -> None:
    """conversation_text_chars is pre-redaction length; conversation_text can be
    null (the redaction join isn't wired in upstream yet) even when chars clears
    the threshold. Sending None to the LLM must never happen.
    """
    row = _conversation_row(conversation_text=None, conversation_text_chars=10_000)

    assert summarize.needs_summary(row) is False


def test_summarize_conversations_applies_skip_rule() -> None:
    df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="2", turn_count=1),
        ]
    )

    result = summarize.summarize_conversations(df, _FakeSummaryClient())

    summarized = result.filter(pl.col("conversation_ref") == "1").row(0, named=True)
    assert summarized["conversation_summary"] == "summary of: turn one\n---\nturn two"
    assert summarized["summary_model_version"] == "test-model"
    assert summarized["embedding_input"] == "summary"
    assert summarized["turn_count"] == 2

    skipped = result.filter(pl.col("conversation_ref") == "2").row(0, named=True)
    assert skipped["conversation_summary"] is None
    assert skipped["summary_model_version"] is None
    assert skipped["embedding_input"] == "concatenated_turns"
    assert skipped["turn_count"] == 1


def test_summarize_conversations_types_null_columns_when_batch_is_all_skipped() -> None:
    """An all-skipped batch must not produce a Null-typed column.

    Regression: Polars infers dtype=Null for an all-None Series, which Iceberg
    (format v2) rejects outright when writing the table.
    """
    df = pl.DataFrame([_conversation_row(conversation_ref="1", turn_count=1)])

    result = summarize.summarize_conversations(df, _FakeSummaryClient())

    assert result.schema["conversation_summary"] == pl.String
    assert result.schema["summary_model_version"] == pl.String


def test_filter_unsummarized_drops_already_summarized_rows_with_same_turn_count() -> (
    None
):
    source_df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1", turn_count=2),
            _conversation_row(conversation_ref="2", turn_count=2),
        ]
    )
    already_summarized_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [2],
        }
    )

    result = summarize.filter_unsummarized(source_df, already_summarized_df)

    assert result["conversation_ref"].to_list() == ["2"]


class _FakeLLM:
    def __init__(self, client: object) -> None:
        self._client = client

    def get_client(self) -> object:
        return self._client


def test_build_summary_client_uses_configured_model_for_openai(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SUMMARY_MODEL_VERSION is vendor-agnostic: whatever it's set to is what gets
    sent to whichever backend is configured -- the caller is responsible for
    setting it to an id that backend actually recognizes.
    """
    monkeypatch.setattr(summarize, "SUMMARY_MODEL_VERSION", "gpt-4o-mini")

    client = summarize.build_summary_client(_FakeLLM(OpenAI(api_key="sk-test")))

    assert isinstance(client, summarize.OpenAISummaryClient)
    assert client.model_version == "gpt-4o-mini"


def test_build_summary_client_uses_configured_model_for_anthropic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(summarize, "SUMMARY_MODEL_VERSION", "claude-haiku-4-5")

    client = summarize.build_summary_client(_FakeLLM(Anthropic(api_key="sk-ant-test")))

    assert isinstance(client, summarize.AnthropicSummaryClient)
    assert client.model_version == "claude-haiku-4-5"


def test_filter_unsummarized_resubmits_conversations_with_new_turns() -> None:
    """A ticket that gained a comment since it was summarized is re-submitted."""
    source_df = pl.DataFrame([_conversation_row(conversation_ref="1", turn_count=3)])
    already_summarized_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [2],
        }
    )

    result = summarize.filter_unsummarized(source_df, already_summarized_df)

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unsummarized_resubmits_on_stale_model_version() -> None:
    """A conversation LLM-summarized under an old model/prompt is re-submitted."""
    source_df = pl.DataFrame([_conversation_row(conversation_ref="1", turn_count=2)])
    already_summarized_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [2],
            "summary_model_version": ["old-model"],
        }
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, current_model_version="new-model"
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unsummarized_does_not_resubmit_skipped_rows_on_model_change() -> None:
    """A row skipped last time (null summary_model_version) isn't touched by a
    model change -- the skip decision was never model-dependent.
    """
    source_df = pl.DataFrame([_conversation_row(conversation_ref="1", turn_count=1)])
    already_summarized_df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [1],
            "summary_model_version": [None],
        }
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, current_model_version="new-model"
    )

    assert result.height == 0


class _FailingSummaryClient:
    model_version = "test-model"

    def summarize(self, conversation_text: str) -> str:  # noqa: ARG002
        msg = "simulated API failure"
        raise RuntimeError(msg)


def test_summarize_conversations_drops_failed_conversations_without_losing_batch() -> (
    None
):
    """One LLM failure must not cost the rest of the batch (#2542 checkpointing)."""
    df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="2"),
        ]
    )

    result = summarize.summarize_conversations(df, _FailingSummaryClient())

    assert result.height == 0


def test_summarize_conversations_keeps_successful_rows_when_one_fails() -> None:
    class _PartiallyFailingClient:
        model_version = "test-model"

        def summarize(self, conversation_text: str) -> str:
            if conversation_text == "fail me":
                msg = "simulated API failure"
                raise RuntimeError(msg)
            return f"summary of: {conversation_text}"

    df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1", conversation_text="fail me"),
            _conversation_row(conversation_ref="2"),
        ]
    )

    result = summarize.summarize_conversations(df, _PartiallyFailingClient())

    assert result["conversation_ref"].to_list() == ["2"]
    assert result.row(0, named=True)["conversation_summary"] == (
        "summary of: turn one\n---\nturn two"
    )
