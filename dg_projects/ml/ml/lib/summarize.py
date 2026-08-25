"""LLM summarization of assembled feedback conversations."""

from typing import Any, Protocol

import polars as pl

JOIN_COLS = ["source_slug", "conversation_ref"]

# §A.1 of feedback_ml_approach.md: sits below the measured p25 (601 chars), so it skips
# only the shortest multi-turn conversations rather than trading away summary quality
# for a bigger cost cut.
SKIP_CHAR_THRESHOLD = 500

SUMMARY_MODEL_VERSION = "claude-haiku-4-5"

SUMMARY_PROMPT = (
    "Summarize the following support conversation in 1-2 sentences, from the "
    "requester's point of view. Focus on the problem reported and its resolution "
    "if one is present. Do not include names or contact details.\n\n{conversation_text}"
)


class SummaryClient(Protocol):
    def summarize(self, conversation_text: str) -> str: ...


def filter_unsummarized(
    source_df: pl.DataFrame, already_summarized_df: pl.DataFrame
) -> pl.DataFrame:
    """Drop conversations already present in the feedback_summaries output."""
    return source_df.join(
        already_summarized_df.select(JOIN_COLS), on=JOIN_COLS, how="anti"
    )


def needs_summary(row: dict[str, Any]) -> bool:
    """Apply the skip rule: single-turn or short conversations are not summarized.

    The raw text already is the summary in those cases, so embedding_input falls back
    to concatenated_turns rather than an LLM call.
    """
    if row["turn_count"] == 1:
        return False
    text_chars = row["conversation_text_chars"]
    return text_chars is not None and text_chars >= SKIP_CHAR_THRESHOLD


def summarize_conversations(df: pl.DataFrame, client: SummaryClient) -> pl.DataFrame:
    """Summarize each conversation that clears the skip rule.

    Args:
        df: a frame with (at least) source_slug, conversation_ref, turn_count,
            conversation_text, conversation_text_chars columns, e.g.
            int__feedback__conversation.
        client: an object with a `summarize(conversation_text: str) -> str` method,
            e.g. an AnthropicSummaryClient wrapping LLMClientFactory.

    Returns:
        pl.DataFrame: source_slug, conversation_ref, conversation_summary,
            summary_model_version, embedding_input - keyed the same way
            feedback_conversation_pk is minted, for afact_feedback_conversation to
            left-join.
    """
    rows = df.to_dicts()
    summaries: list[str | None] = []
    model_versions: list[str | None] = []
    embedding_inputs = []
    for row in rows:
        if needs_summary(row):
            summaries.append(client.summarize(row["conversation_text"]))
            model_versions.append(SUMMARY_MODEL_VERSION)
            embedding_inputs.append("summary")
        else:
            summaries.append(None)
            model_versions.append(None)
            embedding_inputs.append("concatenated_turns")

    return df.select(pl.col("source_slug"), pl.col("conversation_ref")).with_columns(
        pl.Series("conversation_summary", summaries),
        pl.Series("summary_model_version", model_versions),
        pl.Series("embedding_input", embedding_inputs),
    )
