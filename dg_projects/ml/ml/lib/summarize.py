"""LLM summarization of assembled feedback conversations."""

import os
from typing import Any, Protocol

import polars as pl
from anthropic import Anthropic, AnthropicBedrockMantle
from ml.resources.llm import LLMClientFactory
from openai import OpenAI

JOIN_COLS = ["source_slug", "conversation_ref"]

# §A.1 of feedback_ml_approach.md: sits below the measured p25 (601 chars), so it skips
# only the shortest multi-turn conversations rather than trading away summary quality
# for a bigger cost cut.
SKIP_CHAR_THRESHOLD = 500

SUMMARY_MODEL_VERSION = os.environ.get("SUMMARY_MODEL_VERSION", "claude-haiku-4-5")

SUMMARY_PROMPT = (
    "Summarize the following support conversation from the requester's point of "
    "view. Focus on the problem reported and its resolution if one is present. "
    "Do not include names or contact details.\n\n{conversation_text}"
)


class SummaryClient(Protocol):
    def summarize(self, conversation_text: str) -> str: ...


class AnthropicSummaryClient:
    """Adapts an Anthropic-compatible client to the SummaryClient protocol.

    Also covers AnthropicBedrockMantle, which exposes the same messages.create
    interface but is not an Anthropic subclass.
    """

    def __init__(self, client: Anthropic | AnthropicBedrockMantle) -> None:
        self._client = client

    def summarize(self, conversation_text: str) -> str:
        message = self._client.messages.create(
            model=SUMMARY_MODEL_VERSION,
            max_tokens=200,
            messages=[
                {
                    "role": "user",
                    "content": SUMMARY_PROMPT.format(
                        conversation_text=conversation_text
                    ),
                }
            ],
        )
        return message.content[0].text


class OpenAISummaryClient:
    """Adapts an OpenAI-compatible client to the SummaryClient protocol."""

    def __init__(self, client: OpenAI, model: str) -> None:
        self._client = client
        self._model = model

    def summarize(self, conversation_text: str) -> str:
        response = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {
                    "role": "user",
                    "content": SUMMARY_PROMPT.format(
                        conversation_text=conversation_text
                    ),
                }
            ],
        )
        return response.choices[0].message.content


def build_summary_client(
    llm: LLMClientFactory,
) -> AnthropicSummaryClient | OpenAISummaryClient:
    client = llm.get_client()
    if isinstance(client, Anthropic | AnthropicBedrockMantle):
        return AnthropicSummaryClient(client)
    return OpenAISummaryClient(client, model=SUMMARY_MODEL_VERSION)


def filter_unsummarized(
    source_df: pl.DataFrame, already_summarized_df: pl.DataFrame
) -> pl.DataFrame:
    """Drop conversations already summarized with their current turn_count.

    A conversation whose turn_count has grown since it was last summarized (e.g. the
    requester added a comment to an already-summarized ticket) is treated as changed
    and re-submitted, rather than being skipped forever like a pure presence check
    would do.
    """
    joined = source_df.join(
        already_summarized_df.select([*JOIN_COLS, "turn_count"]),
        on=JOIN_COLS,
        how="left",
        suffix="_summarized",
    )
    return joined.filter(
        pl.col("turn_count_summarized").is_null()
        | (pl.col("turn_count") != pl.col("turn_count_summarized"))
    ).select(source_df.columns)


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
            summary_model_version, embedding_input, turn_count - keyed the same way
            feedback_conversation_pk is minted, for afact_feedback_conversation to
            left-join. A row is written for every conversation, summarized or
            skipped, so filter_unsummarized and embedding_input stay available for
            all of them; conversation_summary itself stays null for skipped rows,
            and summary_model_version is the "was this LLM-generated" signal.
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

    return df.select(
        pl.col("source_slug"), pl.col("conversation_ref"), pl.col("turn_count")
    ).with_columns(
        # dtype=pl.String pinned explicitly: an all-skipped batch makes summaries/
        # model_versions all-None, which Polars would otherwise infer as its Null
        # dtype -- Iceberg (format v2) rejects a null-typed column outright.
        pl.Series("conversation_summary", summaries, dtype=pl.String),
        pl.Series("summary_model_version", model_versions, dtype=pl.String),
        pl.Series("embedding_input", embedding_inputs, dtype=pl.String),
    )
