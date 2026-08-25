"""Tests for ml.lib.summarize."""

import polars as pl
from ml.lib import summarize


class _FakeSummaryClient:
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
    assert summarized["summary_model_version"] == summarize.SUMMARY_MODEL_VERSION
    assert summarized["embedding_input"] == "summary"

    skipped = result.filter(pl.col("conversation_ref") == "2").row(0, named=True)
    assert skipped["conversation_summary"] is None
    assert skipped["summary_model_version"] is None
    assert skipped["embedding_input"] == "concatenated_turns"


def test_filter_unsummarized_drops_already_summarized_rows() -> None:
    source_df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="2"),
        ]
    )
    already_summarized_df = pl.DataFrame(
        {"source_slug": ["zendesk"], "conversation_ref": ["1"]}
    )

    result = summarize.filter_unsummarized(source_df, already_summarized_df)

    assert result["conversation_ref"].to_list() == ["2"]
