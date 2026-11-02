"""LLM summarization of assembled feedback conversations, through ol-llm."""

import logging
import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import polars as pl
from ml.lib.iceberg_helpers import upsert_chunk
from ml.resources.opik_auth import OPIK_PROJECT_NAME, seed_prompt_environment
from ol_llm import (
    Attribution,
    BatchKeys,
    BatchReport,
    Chunk,
    LLMResult,
    ManagedPrompt,
    TextTemplate,
    UseCase,
    complete_sync,
    get_prompt,
    process_batch,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV
from pydantic_ai import ModelRetry, RunContext, TextOutput
from pydantic_ai.messages import ModelResponse
from pydantic_ai.settings import ThinkingEffort
from pyiceberg.catalog import Catalog

JOIN_COLS = ["feedback_conversation_pk"]

# Columns added after the table first existed go last, in the order they were
# added: upsert_chunk's union_by_name appends them to an existing table, and
# the io_manager's upsert of the asset's return casts by position.
SUMMARIZE_CHECKPOINT_SCHEMA = {
    **dict.fromkeys([*JOIN_COLS, "source_slug", "conversation_ref"], pl.String),
    "turn_count": pl.Int64,
    "conversation_summary": pl.String,
    "summary_model_version": pl.String,
    "prompt_version": pl.String,
    "embedding_input": pl.String,
    "summarized_at": pl.Datetime(time_zone="UTC"),
    "prompt_template_hash": pl.String,
    "idempotency_key": pl.String,
}

# Bounds how many LLM calls a crash can lose (feedback_dagster_asset_spec.md).
SUMMARIZE_CHECKPOINT_BATCH_SIZE = int(
    os.environ.get("SUMMARIZE_CHECKPOINT_BATCH_SIZE", "200")
)

# Each conversation is its own blocking request, so wall-clock time scales with
# call count unless several run at once.
SUMMARIZE_MAX_CONCURRENCY = int(os.environ.get("SUMMARIZE_MAX_CONCURRENCY", "20"))

# A model id in ol-llm's provider:model form. Dev has no Bedrock IAM role, so it
# calls Anthropic with ANTHROPIC_API_KEY; every deployed environment uses Bedrock,
# whose ids are inference profiles. FeedbackSummariesConfig.model overrides this
# per run. The evaluation gate (ml.lib.summarize_gate) tests the deployed model.
DEPLOYED_SUMMARY_MODEL = "bedrock:us.anthropic.claude-haiku-5-5"
SUMMARY_MODEL = os.environ.get(
    "SUMMARY_MODEL",
    "anthropic:claude-haiku-5-5" if DAGSTER_ENV == "dev" else DEPLOYED_SUMMARY_MODEL,
)

# 200 was enough for claude-haiku-4-5 (no thinking), but claude-haiku-5-5 thinks by
# default and thinking counts toward max_tokens, so it can spend the whole budget
# on hidden thinking before any visible output. This leaves headroom for thinking
# plus the summary. Without a cap, pydantic-ai-slim 2.60 sends a Bedrock Claude
# model its maximum output, and Bedrock reserves that much of the
# tokens-per-minute quota for every concurrent request.
SUMMARY_MAX_TOKENS = int(os.environ.get("SUMMARY_MAX_TOKENS", "1024"))
# A 1-3 sentence summary needs little reasoning; low keeps thinking short.
SUMMARY_THINKING: ThinkingEffort = "low"

SUMMARY_PROMPT_NAME = "feedback-summary"

# The embedded default: what a run uses with tracing off (local development) or
# when Opik cannot be reached at the start of a run. Deployed runs use the Opik
# version labeled with their environment, so editing this does not change their
# output.
SUMMARY_PROMPT = (
    "You are analyzing feedback about MIT Learn, MIT Open Learning's platform for "
    "courses and AI learning assistants. The text below is feedback from a "
    "learner. It may be a support ticket, messages to an AI assistant (a tutor, "
    "syllabus, or course recommendation bot), a forum post, or a comment on a "
    "piece of course content. Only the learner's own words are included, but they "
    "may quote a reply.\n\n"
    "In 1-3 sentences, describe from the learner's point of view:\n"
    "- what they were trying to do,\n"
    "- any problem, confusion, or blocker they had with the platform, a course or "
    "its materials, or the assistant,\n"
    "- any feature or content they asked for or said was missing.\n\n"
    "Describe the need in general terms. Refer to courses by subject (for "
    'example, "a finance course"), not by title. Do not describe how support '
    "resolved it.\n\n"
    "Describe a problem or request only if the learner states one about the "
    "platform, a course, its materials, or the assistant. Problems the learner "
    "analyzes as part of the course subject, such as a weakness in a policy or "
    "case study they are discussing, are not feedback. If the text is an "
    "introduction, an answer to an assignment or discussion prompt, or a "
    "reflection on the course subject, say that in a few words and name the "
    'topic, for example "The learner shared a reflection on supply chain '
    'trade-offs."\n\n'
    "The text may be very short. Never ask for more text and never say there is "
    "nothing to summarize. If the text is too short to tell what the learner "
    "wanted, restate what it says in a few words.\n\n"
    "Write plain text only, with no headings, lists, or Markdown. Do not include "
    "names, email addresses, or other personal details.\n\n"
    "<learner_text>\n{{conversation_text}}\n</learner_text>"
)

logger = logging.getLogger(__name__)


def summary_use_case(model: str) -> UseCase:
    """Return the feedback-summary use case calling ``model``.

    :param model: the model id in ``provider:model`` form.
    :returns: the use case, on the batch profile.
    :rtype: UseCase
    """
    return UseCase(
        app=OPIK_PROJECT_NAME, name=SUMMARY_PROMPT_NAME, profile="batch", model=model
    )


def fetch_summary_prompt() -> ManagedPrompt[TextTemplate]:
    """Fetch the summary prompt labeled with this deployment's environment.

    CI and QA without a labeled version get their own version of SUMMARY_PROMPT
    on first use. Production's label moves only by hand or from the promotion
    job, so there a missing label raises.

    Warns when the labeled version's text differs from SUMMARY_PROMPT. The
    template hash is part of every row's key, so a run that then falls back to
    SUMMARY_PROMPT re-summarizes every conversation, and the next run that
    reaches Opik does it again.

    :returns: the prompt, or SUMMARY_PROMPT marked degraded when Opik could not
        be reached.
    :rtype: ManagedPrompt[TextTemplate]
    :raises PromptLabelMissingError: in Production, if Opik has no version of the
        prompt labeled production.
    """
    embedded = TextTemplate(template=SUMMARY_PROMPT)
    try:
        prompt = get_prompt(
            SUMMARY_PROMPT_NAME, project_name=OPIK_PROJECT_NAME, default=embedded
        )
    except LookupError:
        seed_prompt_environment(SUMMARY_PROMPT_NAME, SUMMARY_PROMPT)
        prompt = get_prompt(
            SUMMARY_PROMPT_NAME, project_name=OPIK_PROJECT_NAME, default=embedded
        )
    if prompt.source == "opik" and prompt.template_hash != embedded.template_hash:
        logger.warning(
            "Opik's %s %s differs from SUMMARY_PROMPT; copy it into the code so "
            "an Opik outage does not re-summarize every conversation",
            prompt.name,
            prompt.version,
        )
    return prompt


def needs_summary(row: dict[str, Any]) -> bool:
    """Whether a conversation gets an LLM call: every conversation with text does.

    A null conversation_text (redaction hasn't reached it yet) is rejected rather
    than sending the literal string "None" to the LLM; its embedding_input falls
    back to concatenated_turns. Which conversations reach this at all is decided
    upstream, by feedback_summaries' config filters.
    """
    return row["conversation_text"] is not None


def filter_unsummarized(
    source_df: pl.DataFrame, already_summarized_df: pl.DataFrame, keys: BatchKeys
) -> pl.DataFrame:
    """Drop conversations whose stored summary came from this text, prompt, and model.

    The idempotency key covers the conversation text, the prompt's template hash,
    and the model, so a new turn, a prompt edit, or a model change re-submits a
    conversation that has a key (see below for one that doesn't). A run that
    fell back to the embedded prompt computes the same key as one that fetched
    the same text, so an Opik outage re-submits nothing.

    A conversation not stored yet is always kept, with or without text, so a
    skipped one gets its row. A stored one without text is dropped, which leaves
    a summary in place if its text later goes missing. A skipped row has no key
    and is re-submitted once it has text.

    A row summarized before the key existed is re-submitted only when its
    turn_count changed. Re-submitting every one of them, for a deploy or a model
    change, would rewrite every summary and, through the embeddings, move every
    conversation's cluster assignment. Its prompt_version is not compared,
    because it names an Opik version from before the key and the deploy creates
    a new version of the same text, and its model is not, so a model change
    reaches it with its next new turn. The row gets a key the next time it is
    summarized.

    :param source_df: the in-scope conversations.
    :param already_summarized_df: feedback_summaries' join columns, turn_count,
        and summary_model_version, with idempotency_key when the table has it.
    :param keys: the run's prompt and model.
    :returns: the rows of source_df to summarize or record as skipped.
    :rtype: pl.DataFrame
    """
    stored_key = (
        pl.col("idempotency_key")
        if "idempotency_key" in already_summarized_df.columns
        else pl.lit(None, dtype=pl.String)
    )
    stored = already_summarized_df.select(
        *JOIN_COLS,
        stored_key.alias("_stored_key"),
        pl.col("turn_count").alias("_stored_turn_count"),
        pl.col("summary_model_version").alias("_stored_model"),
        pl.lit(True).alias("_stored"),  # noqa: FBT003
    )
    current_key = pl.col("conversation_text").map_elements(
        keys.key, return_dtype=pl.String
    )
    # A keyless row with no model was skipped, not summarized.
    keyless_changed = pl.col("_stored_model").is_null() | (
        pl.col("turn_count").ne_missing(pl.col("_stored_turn_count"))
    )
    return (
        source_df.with_columns(current_key.alias("_current_key"))
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


# Finish reasons that can come with partial text: max_tokens (thinking shares it
# with the answer) and Bedrock's malformed_model_output.
_CUT_OFF_FINISH_REASONS = frozenset({"length", "error"})


def _complete_summary(ctx: RunContext[None], summary: str) -> str:
    """Send a cut-off or blank summary back to the model once before the row fails.

    A partial summary stored under the current key is never re-submitted. A reply
    with no text at all doesn't reach this: PydanticAI fails it at once when it
    stopped at max_tokens (thinking used the whole budget) and otherwise asks
    again itself.
    """
    response = ctx.messages[-1]
    if (
        isinstance(response, ModelResponse)
        and response.finish_reason in _CUT_OFF_FINISH_REASONS
    ):
        msg = "The summary was cut off. Write the 1-3 sentence summary."
        raise ModelRetry(msg)
    if not summary.strip():
        msg = "The summary was empty. Write the 1-3 sentence summary."
        raise ModelRetry(msg)
    return summary


@dataclass(frozen=True, kw_only=True)
class Summarizer:
    """Summarizes one conversation per call under one prompt and model.

    :param use_case: the feedback-summary use case, from summary_use_case.
    :param prompt: the prompt the run fetched once at its start.
    :param run_id: the Dagster run, for the trace.
    """

    use_case: UseCase
    prompt: ManagedPrompt[TextTemplate]
    run_id: str | None = None

    @property
    def keys(self) -> BatchKeys:
        """The idempotency keys for this prompt and model."""
        return BatchKeys(model=self.use_case.model, prompt=self.prompt)

    def summarize(self, row: dict[str, Any]) -> LLMResult[str]:
        """Summarize one conversation.

        Its trace is filed under the conversation's pk as the Opik thread, so
        every summary of one conversation is listed together.

        :param row: a conversation with text.
        :returns: the result.
        :rtype: LLMResult[str]
        :raises LLMError: on a model failure, a content filter, a reply whose
            thinking used the whole max_tokens, or a summary that was still cut
            off or blank after one retry.
        """
        return complete_sync(
            self.use_case,
            self.prompt.format(conversation_text=row["conversation_text"]),
            output_type=TextOutput(_complete_summary),
            model_settings={
                "max_tokens": SUMMARY_MAX_TOKENS,
                "thinking": SUMMARY_THINKING,
            },
            attribution=Attribution(
                thread_id=row["feedback_conversation_pk"], run_id=self.run_id
            ),
            prompt=self.prompt,
        )

    def summarized_frame(
        self, succeeded: Sequence[tuple[dict[str, Any], LLMResult[str]]]
    ) -> pl.DataFrame:
        """Return the feedback_summaries rows for summarized conversations.

        :param succeeded: each conversation with its result.
        :rtype: pl.DataFrame
        """
        records = []
        for row, result in succeeded:
            provenance = self.keys.provenance(row["conversation_text"], result)
            records.append(
                {
                    **_identity(row),
                    "conversation_summary": result.output,
                    "summary_model_version": provenance.model,
                    "prompt_version": provenance.prompt_version,
                    "prompt_template_hash": provenance.prompt_template_hash,
                    "idempotency_key": provenance.idempotency_key,
                    "embedding_input": "summary",
                    "summarized_at": provenance.processed_at,
                }
            )
        return pl.DataFrame(records, schema=SUMMARIZE_CHECKPOINT_SCHEMA)


def skipped_frame(rows: Sequence[dict[str, Any]]) -> pl.DataFrame:
    """Return the feedback_summaries rows for conversations with no text.

    Every provenance column stays null: summary_model_version is the "was this
    LLM-generated" signal, and a null key re-submits the row once it has text.

    :param rows: conversations needs_summary rejected.
    :rtype: pl.DataFrame
    """
    return pl.DataFrame(
        [{**_identity(row), "embedding_input": "concatenated_turns"} for row in rows],
        schema=SUMMARIZE_CHECKPOINT_SCHEMA,
    )


def _identity(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "feedback_conversation_pk": row["feedback_conversation_pk"],
        "source_slug": row["source_slug"],
        "conversation_ref": row["conversation_ref"],
        "turn_count": row["turn_count"],
    }


def summarize_and_checkpoint(  # noqa: PLR0913 -- each is an independent tuning knob
    unsummarized_df: pl.DataFrame,
    summarizer: Summarizer,
    checkpoint_target: tuple[Catalog, str],
    *,
    batch_size: int = SUMMARIZE_CHECKPOINT_BATCH_SIZE,
    max_concurrency: int = SUMMARIZE_MAX_CONCURRENCY,
    log: logging.Logger = logger,
) -> tuple[pl.DataFrame, BatchReport[dict[str, Any]]]:
    """Summarize unsummarized_df in chunks, upserting each as it completes.

    Conversations without text are upserted first as skipped rows. A failed
    conversation is left out of the table, so the next run submits it again.
    process_batch stops the run when its circuit breaker opens (an outage, a
    rate limit that outlasts the retries, or a rejected credential); everything
    upserted before that stays.

    :param unsummarized_df: the rows filter_unsummarized kept.
    :param summarizer: the run's summarizer.
    :param checkpoint_target: the catalog and "database.table" to upsert into.
    :param batch_size: conversations per chunk, so per upsert.
    :param max_concurrency: conversations of a chunk summarized at once.
    :param log: where to log progress, e.g. Dagster's context.log.
    :returns: every row upserted, and what the batch did.
    :rtype: tuple[pl.DataFrame, BatchReport]
    :raises BatchFailed: if the circuit opened, or every attempted call failed.
    """
    catalog, table_identifier = checkpoint_target
    upserted: list[pl.DataFrame] = []

    def checkpoint(chunk_df: pl.DataFrame) -> None:
        # feedback_summaries is in non_dbt_singleton_tables()
        # (ol_orchestrate.lib.iceberg_maintenance), so nightly maintenance expires
        # the snapshots each chunk writes.
        written = upsert_chunk(catalog, table_identifier, chunk_df, JOIN_COLS)
        # An empty chunk comes back in SUMMARIZE_CHECKPOINT_SCHEMA order, not the
        # table's, and concat matches columns by position.
        if written.height:
            upserted.append(written)

    rows = unsummarized_df.to_dicts()
    checkpoint(skipped_frame([row for row in rows if not needs_summary(row)]))

    def persist(chunk: Chunk[dict[str, Any], LLMResult[str]]) -> None:
        checkpoint(summarizer.summarized_frame(chunk.succeeded))

    report = process_batch(
        [row for row in rows if needs_summary(row)],
        summarizer.summarize,
        persist,
        chunk_size=batch_size,
        max_workers=max_concurrency,
        logger=log,
    )
    if not upserted:
        return pl.DataFrame(schema=SUMMARIZE_CHECKPOINT_SCHEMA), report
    return pl.concat(upserted), report
