"""LLM summarization of assembled feedback conversations."""

import logging
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

# Defaults to an Anthropic model id, matching LLMClientFactory's own
# client_class="anthropic" default. A model id is only valid for one vendor's API,
# so switching client_class to "openai"/"openai_compatible" requires overriding
# this to a matching id (e.g. "gpt-4o-mini") -- there is no one id valid everywhere.
SUMMARY_MODEL_VERSION = os.environ.get("SUMMARY_MODEL_VERSION", "claude-haiku-4-5")

SUMMARY_PROMPT = (
    "Summarize the following support conversation from the requester's point of "
    "view. Focus on the problem reported and its resolution if one is present. "
    "Do not include names or contact details.\n\n{conversation_text}"
)

logger = logging.getLogger(__name__)


class SummaryClient(Protocol):
    model_version: str

    def summarize(self, conversation_text: str) -> str: ...


class AnthropicSummaryClient:
    """Adapts an Anthropic-compatible client to the SummaryClient protocol.

    Also covers AnthropicBedrockMantle, which exposes the same messages.create
    interface but is not an Anthropic subclass.
    """

    def __init__(self, client: Anthropic | AnthropicBedrockMantle) -> None:
        self._client = client
        self.model_version = SUMMARY_MODEL_VERSION

    def summarize(self, conversation_text: str) -> str:
        message = self._client.messages.create(
            model=self.model_version,
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

    def __init__(self, client: OpenAI) -> None:
        if SUMMARY_MODEL_VERSION.startswith("claude"):
            # Can't validate a model id belongs to OpenAI in general, but a Claude
            # id can never work here -- catches the default-left-unset case rather
            # than failing later with an opaque error from OpenAI's API.
            msg = (
                f"SUMMARY_MODEL_VERSION={SUMMARY_MODEL_VERSION!r} looks like an "
                "Anthropic model id, but client_class='openai' is configured. Set "
                "SUMMARY_MODEL_VERSION to an OpenAI model id (e.g. 'gpt-4o-mini')."
            )
            raise ValueError(msg)
        self._client = client
        self.model_version = SUMMARY_MODEL_VERSION

    def summarize(self, conversation_text: str) -> str:
        response = self._client.chat.completions.create(
            model=self.model_version,
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
    return OpenAISummaryClient(client)


def filter_unsummarized(
    source_df: pl.DataFrame,
    already_summarized_df: pl.DataFrame,
    current_model_version: str | None = None,
) -> pl.DataFrame:
    """Drop conversations already summarized with their current turn_count and model.

    Re-submits a conversation whose turn_count grew (a new comment) or whose stored
    summary_model_version is stale (a model/prompt change) -- but a row skipped last
    time (summary_model_version null there) isn't touched by a model change, since
    the skip decision was never model-dependent. current_model_version=None disables
    the model check.
    """
    already_summarized_cols = [*JOIN_COLS, "turn_count"]
    has_model_version_col = "summary_model_version" in already_summarized_df.columns
    check_model_version = current_model_version is not None and has_model_version_col
    if check_model_version:
        already_summarized_cols.append("summary_model_version")

    already_summarized_selected = already_summarized_df.select(already_summarized_cols)
    if check_model_version:
        # join(suffix=...) only applies where source_df has a same-named column to
        # collide with (true for turn_count, not summary_model_version), so this
        # needs an explicit rename to get a predictable joined column name.
        already_summarized_selected = already_summarized_selected.rename(
            {"summary_model_version": "summary_model_version_summarized"}
        )

    joined = source_df.join(
        already_summarized_selected,
        on=JOIN_COLS,
        how="left",
        suffix="_summarized",
    )
    is_new_or_changed = pl.col("turn_count_summarized").is_null() | (
        pl.col("turn_count") != pl.col("turn_count_summarized")
    )
    if check_model_version:
        is_new_or_changed = is_new_or_changed | (
            pl.col("summary_model_version_summarized").is_not_null()
            & (pl.col("summary_model_version_summarized") != current_model_version)
        )
    return joined.filter(is_new_or_changed).select(source_df.columns)


def needs_summary(row: dict[str, Any]) -> bool:
    """Apply the skip rule: single-turn or short conversations are not summarized.

    The raw text already is the summary in those cases, so embedding_input falls back
    to concatenated_turns rather than an LLM call. A null conversation_text (the
    redaction join upstream isn't wired in yet) is also rejected here, rather than
    sending the literal string "None" to the LLM.
    """
    if row["turn_count"] == 1:
        return False
    if row["conversation_text"] is None:
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
            left-join. conversation_summary stays null for skipped rows;
            summary_model_version is the "was this LLM-generated" signal. A
            conversation whose LLM call raises is dropped from the output entirely
            (#2542 checkpointing) rather than failing the batch -- absent from
            feedback_summaries, it's picked up again as new on the next run.
    """
    rows = df.to_dicts()
    source_slugs: list[str] = []
    conversation_refs: list[str] = []
    turn_counts: list[int] = []
    summaries: list[str | None] = []
    model_versions: list[str | None] = []
    embedding_inputs: list[str] = []
    for row in rows:
        if needs_summary(row):
            try:
                summary = client.summarize(row["conversation_text"])
            except Exception:
                logger.warning(
                    "Failed to summarize conversation %s/%s; will retry next run",
                    row["source_slug"],
                    row["conversation_ref"],
                    exc_info=True,
                )
                continue
            summaries.append(summary)
            model_versions.append(client.model_version)
            embedding_inputs.append("summary")
        else:
            summaries.append(None)
            model_versions.append(None)
            embedding_inputs.append("concatenated_turns")
        source_slugs.append(row["source_slug"])
        conversation_refs.append(row["conversation_ref"])
        turn_counts.append(row["turn_count"])

    # Built from lists, not a slice of df: a failed conversation is skipped above,
    # so the surviving rows no longer line up with df's original row order/count.
    return pl.DataFrame(
        {
            "source_slug": pl.Series(source_slugs, dtype=pl.String),
            "conversation_ref": pl.Series(conversation_refs, dtype=pl.String),
            "turn_count": pl.Series(turn_counts, dtype=pl.Int64),
        }
    ).with_columns(
        # dtype=pl.String pinned explicitly: an all-skipped batch makes summaries/
        # model_versions all-None, which Polars would otherwise infer as its Null
        # dtype -- Iceberg (format v2) rejects a null-typed column outright.
        pl.Series("conversation_summary", summaries, dtype=pl.String),
        pl.Series("summary_model_version", model_versions, dtype=pl.String),
        pl.Series("embedding_input", embedding_inputs, dtype=pl.String),
    )
