"""LLM summarization of assembled feedback conversations."""

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any, NamedTuple, Protocol

import polars as pl
from anthropic import Anthropic, AnthropicBedrock
from dagster import AssetExecutionContext
from ml.lib.llm_client_adapters import (
    build_llm_client,
    call_anthropic,
    call_openai,
    raise_if_claude_model_on_openai,
)
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import (
    attach_span_metadata,
    get_prompt_version,
    render_prompt,
    traced,
)
from openai import OpenAI
from pyiceberg.catalog import Catalog

JOIN_COLS = ["feedback_conversation_pk"]

SUMMARIZE_CHECKPOINT_SCHEMA = {
    **dict.fromkeys([*JOIN_COLS, "source_slug", "conversation_ref"], pl.String),
    "turn_count": pl.Int64,
    "conversation_summary": pl.String,
    "summary_model_version": pl.String,
    "prompt_version": pl.String,
    "embedding_input": pl.String,
    "summarized_at": pl.Datetime(time_zone="UTC"),
}

# Bounds how many LLM calls a crash can lose (feedback_dagster_asset_spec.md).
SUMMARIZE_CHECKPOINT_BATCH_SIZE = int(
    os.environ.get("SUMMARIZE_CHECKPOINT_BATCH_SIZE", "200")
)

# Each summarize() call is one blocking, independent network request -- unlike
# embed_batch (one request covers many conversations), so wall-clock time here
# scales with call count unless several run at once. Concurrent, not batched:
# still one request per conversation, just not waiting for each to finish
# before starting the next.
SUMMARIZE_MAX_CONCURRENCY = int(os.environ.get("SUMMARIZE_MAX_CONCURRENCY", "20"))

# Abort after this many whole chunks in a row come back with zero successful LLM
# calls, rather than burning through every remaining chunk with the same (e.g.
# credential) error. No existing consecutive-failure precedent elsewhere in this
# repo to anchor to (only per-request retry counts, a different concept); 1
# means a single fully-failed chunk (SUMMARIZE_CHECKPOINT_BATCH_SIZE calls) is
# already enough to call it systemic rather than bad luck.
MAX_CONSECUTIVE_FAILED_CHUNKS = int(
    os.environ.get("SUMMARIZE_MAX_CONSECUTIVE_FAILED_CHUNKS", "1")
)

# §A.1 of feedback_ml_approach.md: sits below the measured p25 (601 chars), so it skips
# only the shortest multi-turn conversations rather than trading away summary quality
# for a bigger cost cut.
SKIP_CHAR_THRESHOLD = 500

# Defaults to an Anthropic model id, matching LLMClientFactory's own
# client_class="anthropic" default. A model id is only valid for one vendor's API,
# so switching client_class to "openai"/"openai_compatible" requires overriding
# this to a matching id (e.g. "gpt-4o-mini") -- there is no one id valid everywhere.
# FeedbackSummariesConfig.model_version (a per-run Dagster config field) overrides
# this; the env var/default here is only the fallback when a run doesn't set it.
SUMMARY_MODEL_VERSION = os.environ.get("SUMMARY_MODEL_VERSION", "claude-haiku-4-5")

# Bedrock uses its own model id namespace (a Bedrock model or inference-profile
# id, e.g. "global.anthropic.claude-haiku-4-5-20251001-v1:0"), never the plain
# Anthropic API id above -- client_class="bedrock" needs this override instead.
# FeedbackSummariesConfig.bedrock_model_version overrides this the same way.
BEDROCK_SUMMARY_MODEL_VERSION = os.environ.get(
    "BEDROCK_SUMMARY_MODEL_VERSION",
    "global.anthropic.claude-haiku-4-5-20251001-v1:0",
)

# 200 was enough for claude-haiku-4-5 (no thinking), but a model with adaptive
# thinking on by default (e.g. claude-sonnet-5/claude-opus-5) can spend the whole
# budget on hidden thinking before any visible output, leaving Anthropic's
# response content empty rather than erroring -- this needs enough headroom for
# thinking plus the actual summary across whichever model is configured.
SUMMARY_MAX_TOKENS = int(os.environ.get("SUMMARY_MAX_TOKENS", "1024"))

SUMMARY_PROMPT = (
    "Summarize the following support conversation from the requester's point of "
    "view. Focus on the problem reported and its resolution if one is present. "
    "Do not include names or contact details.\n\n{{conversation_text}}"
)

logger = logging.getLogger(__name__)


SUMMARY_PROMPT_NAME = "feedback-summary"


def _summary_prompt(conversation_text: str) -> str:
    """SUMMARY_PROMPT rendered, preferring Opik's Prompt Library entry if set up."""
    return render_prompt(
        SUMMARY_PROMPT_NAME, SUMMARY_PROMPT, conversation_text=conversation_text
    )


class SummaryClient(Protocol):
    model_version: str

    def summarize(
        self, conversation_text: str, *, trace_metadata: dict[str, Any]
    ) -> str | None: ...


class AnthropicSummaryClient:
    """Adapts an Anthropic-compatible client to the SummaryClient protocol.

    Also covers AnthropicBedrock, which exposes the same messages.create
    interface but is not an Anthropic subclass.
    """

    def __init__(
        self, client: Anthropic | AnthropicBedrock, model_version: str
    ) -> None:
        self._client = client
        self.model_version = model_version

    @traced(
        "feedback_summarize_anthropic",
        tags=["feedback_summary"],
        ignore_arguments=["trace_metadata"],
    )
    def summarize(
        self, conversation_text: str, *, trace_metadata: dict[str, Any]
    ) -> str | None:
        attach_span_metadata(trace_metadata)
        message = call_anthropic(
            self._client,
            self.model_version,
            max_tokens=SUMMARY_MAX_TOKENS,
            prompt=_summary_prompt(conversation_text),
        )
        if not message.content:
            # A model with thinking on by default can spend the whole max_tokens
            # budget on hidden thinking and return no visible output at all
            # (stop_reason="max_tokens", content=[]) rather than raising --
            # surfaced as an empty summary (like a refusal) instead of an
            # IndexError, so the caller's existing empty-summary handling
            # (retry next run) applies here too.
            return None
        return message.content[0].text


class OpenAISummaryClient:
    """Adapts an OpenAI-compatible client to the SummaryClient protocol."""

    def __init__(
        self, client: OpenAI, model_version: str, *, client_class: str = "openai"
    ) -> None:
        raise_if_claude_model_on_openai(
            client_class=client_class,
            model_version=model_version,
            config_hint=(
                "FeedbackSummariesConfig.model_version (or SUMMARY_MODEL_VERSION)"
            ),
        )
        self._client = client
        self.model_version = model_version

    @traced(
        "feedback_summarize_openai",
        tags=["feedback_summary"],
        ignore_arguments=["trace_metadata"],
    )
    def summarize(
        self, conversation_text: str, *, trace_metadata: dict[str, Any]
    ) -> str | None:
        attach_span_metadata(trace_metadata)
        response = call_openai(
            self._client, self.model_version, prompt=_summary_prompt(conversation_text)
        )
        return response.choices[0].message.content


def build_summary_client(
    llm: LLMClientFactory,
    model_version: str | None = None,
    bedrock_model_version: str | None = None,
) -> AnthropicSummaryClient | OpenAISummaryClient:
    """Build the client whose model_version comes from run config, else a default.

    model_version/bedrock_model_version are the feedback_summaries asset's own
    per-run Config fields (FeedbackSummariesConfig) -- None means the run didn't
    override them, so SUMMARY_MODEL_VERSION/BEDROCK_SUMMARY_MODEL_VERSION apply.
    """
    return build_llm_client(
        llm,
        anthropic_client_cls=AnthropicSummaryClient,
        openai_client_cls=OpenAISummaryClient,
        model_version=model_version,
        bedrock_model_version=bedrock_model_version,
        default_model_version=SUMMARY_MODEL_VERSION,
        default_bedrock_model_version=BEDROCK_SUMMARY_MODEL_VERSION,
    )


def filter_unsummarized(
    source_df: pl.DataFrame,
    already_summarized_df: pl.DataFrame,
    current_model_version: str | None = None,
    current_prompt_version: str | None = None,
) -> pl.DataFrame:
    """Drop conversations already summarized with their current turn_count/model/prompt.

    Re-submits a conversation whose turn_count grew (a new comment), whose stored
    summary_model_version is stale (a model change), or whose stored prompt_version
    is stale (a Prompt Library edit) -- but a row skipped last time (both columns
    null there) isn't touched by either change, since the skip decision was never
    model/prompt-dependent. current_model_version/current_prompt_version=None
    disables the respective check.
    """
    already_summarized_cols = [*JOIN_COLS, "turn_count"]
    checks = [
        ("summary_model_version", current_model_version),
        ("prompt_version", current_prompt_version),
    ]
    active_checks = [
        (col, current)
        for col, current in checks
        if current is not None and col in already_summarized_df.columns
    ]
    already_summarized_cols += [col for col, _ in active_checks]

    already_summarized_selected = already_summarized_df.select(already_summarized_cols)
    # join(suffix=...) only applies where source_df has a same-named column to
    # collide with (true for turn_count, not these version columns), so this
    # needs an explicit rename to get a predictable joined column name.
    already_summarized_selected = already_summarized_selected.rename(
        {col: f"{col}_summarized" for col, _ in active_checks}
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
    for col, current in active_checks:
        is_new_or_changed = is_new_or_changed | (
            pl.col(f"{col}_summarized").is_not_null()
            & (pl.col(f"{col}_summarized") != current)
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


class _SummarizeOutcome(NamedTuple):
    """Result of one summarize() call, with any failure captured as data
    instead of a raised exception -- lets the caller run these concurrently
    via a thread pool without needing to unwrap each future's exception
    itself. summary is None on failure; error_message is None on success.
    exception is only set when the call itself raised (not for an empty/
    refused summary), so the caller can still log exc_info.
    """

    summary: str | None
    error_message: str | None
    exception: Exception | None


def _call_summarize(client: "SummaryClient", row: dict[str, Any]) -> _SummarizeOutcome:
    """Run one summarize() call, translating a raised exception into a
    _SummarizeOutcome instead of letting it propagate.
    """
    try:
        summary = client.summarize(
            row["conversation_text"],
            trace_metadata={
                "feedback_conversation_pk": row["feedback_conversation_pk"],
                "source_slug": row["source_slug"],
                "conversation_ref": row["conversation_ref"],
                "turn_count": row["turn_count"],
                "conversation_text_chars": row["conversation_text_chars"],
            },
        )
    except Exception as e:  # noqa: BLE001 -- translated to a return value, not swallowed
        return _SummarizeOutcome(None, f"{type(e).__name__}: {e}", e)
    if not summary:
        return _SummarizeOutcome(
            None, "empty/null summary (refusal or content filter)", None
        )
    return _SummarizeOutcome(summary, None, None)


def summarize_conversations(
    df: pl.DataFrame,
    client: SummaryClient,
    errors: list[str] | None = None,
    max_concurrency: int = SUMMARIZE_MAX_CONCURRENCY,
) -> pl.DataFrame:
    """Summarize each conversation that clears the skip rule.

    Args:
        df: a frame with (at least) feedback_conversation_pk, source_slug,
            conversation_ref, turn_count, conversation_text, conversation_text_chars
            columns, e.g. int__feedback__conversation.
        client: an object with a `summarize(conversation_text: str) -> str` method,
            e.g. an AnthropicSummaryClient wrapping LLMClientFactory.
        errors: if given, each failure's message is appended here -- lets a caller
            surface *why* calls failed (e.g. in a Failure message) without changing
            this function's return type.
        max_concurrency: how many summarize() calls run at once. Each is an
            independent, blocking network request, so this is the lever for
            wall-clock time at scale -- unlike embed_batch, there's no way to
            cover several conversations in one request here.

    Returns:
        pl.DataFrame: feedback_conversation_pk, source_slug, conversation_ref,
            conversation_summary, summary_model_version, prompt_version,
            embedding_input, summarized_at, turn_count - keyed by
            feedback_conversation_pk, for afact_feedback_conversation to
            left-join. conversation_summary/summarized_at/prompt_version all
            stay null for skipped rows; summary_model_version is the "was this
            LLM-generated" signal. A conversation whose LLM call raises is
            dropped from the output entirely (#2542 checkpointing) rather than
            failing the batch -- absent from feedback_summaries, it's picked up
            again as new on the next run.
    """
    # Once per batch, not per-row. SUMMARY_PROMPT makes this create-if-missing
    # like render_prompt below, so both agree on the version -- otherwise a
    # prompt's first run reads "local" here and the next run wrongly resubmits
    # everything as "prompt changed".
    prompt_version = get_prompt_version(SUMMARY_PROMPT_NAME, SUMMARY_PROMPT)
    rows = df.to_dicts()
    needs_summary_indices = [i for i, row in enumerate(rows) if needs_summary(row)]
    results: dict[int, _SummarizeOutcome] = {}
    if needs_summary_indices:
        with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            future_to_index = {
                executor.submit(_call_summarize, client, rows[i]): i
                for i in needs_summary_indices
            }
            for future, i in future_to_index.items():
                results[i] = future.result()

    feedback_conversation_pks: list[str] = []
    source_slugs: list[str] = []
    conversation_refs: list[str] = []
    turn_counts: list[int] = []
    summaries: list[str | None] = []
    model_versions: list[str | None] = []
    prompt_versions: list[str | None] = []
    embedding_inputs: list[str] = []
    summarized_ats: list[datetime | None] = []
    for i, row in enumerate(rows):
        if i in results:
            outcome = results[i]
            if outcome.error_message is not None:
                logger.warning(
                    "Failed to summarize conversation %s/%s; will retry next run",
                    row["source_slug"],
                    row["conversation_ref"],
                    exc_info=outcome.exception,
                )
                if errors is not None:
                    errors.append(
                        f"{row['source_slug']}/{row['conversation_ref']}: "
                        f"{outcome.error_message}"
                    )
                continue
            summaries.append(outcome.summary)
            model_versions.append(client.model_version)
            prompt_versions.append(prompt_version)
            embedding_inputs.append("summary")
            summarized_ats.append(datetime.now(tz=UTC))
        else:
            summaries.append(None)
            model_versions.append(None)
            prompt_versions.append(None)
            embedding_inputs.append("concatenated_turns")
            summarized_ats.append(None)
        feedback_conversation_pks.append(row["feedback_conversation_pk"])
        source_slugs.append(row["source_slug"])
        conversation_refs.append(row["conversation_ref"])
        turn_counts.append(row["turn_count"])

    # Built from lists, not a slice of df: a failed conversation is skipped above,
    # so the surviving rows no longer line up with df's original row order/count.
    return pl.DataFrame(
        {
            "feedback_conversation_pk": pl.Series(
                feedback_conversation_pks, dtype=pl.String
            ),
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
        pl.Series("prompt_version", prompt_versions, dtype=pl.String),
        pl.Series("embedding_input", embedding_inputs, dtype=pl.String),
        pl.Series("summarized_at", summarized_ats, dtype=pl.Datetime(time_zone="UTC")),
    )


def checkpoint_chunk(
    catalog: Catalog, table_identifier: str, chunk_df: pl.DataFrame
) -> None:
    """Upsert one chunk directly into the real feedback_summaries table.

    A crash after this call keeps everything upserted so far -- the next run's
    ordinary filter_unsummarized pass sees it as already summarized, no separate
    recovery step needed. table_identifier is "database.table", e.g.
    "ol_warehouse_production_intermediate.feedback_summaries". The table is
    registered in non_dbt_singleton_tables() (ol_orchestrate.lib.iceberg_maintenance)
    so nightly maintenance expires the resulting one-snapshot-per-chunk history.

    create_table_if_not_exists rather than load_table: a brand-new deployment (or a
    dropped dev table) has no feedback_summaries table yet, and this call -- not the
    io_manager's write of the asset's final return -- is the first write of any run,
    so it must be able to bootstrap the table itself.
    """
    if chunk_df.height == 0:
        return
    table = catalog.create_table_if_not_exists(
        table_identifier, schema=chunk_df.to_arrow().schema
    )
    # A table from before summarized_at/prompt_version existed has an older schema
    # than chunk_df -- union_by_name adds the new column(s) (nulled on existing
    # rows) instead of failing the upsert; a no-op once the table already has them.
    with table.update_schema() as update:
        update.union_by_name(chunk_df.to_arrow().schema)
    # union_by_name appends new columns at the table's end regardless of chunk_df's
    # order, and upsert's pyarrow cast is positional -- so it must be reordered
    # to match the table, not chunk_df.
    ordered_chunk_df = chunk_df.select(table.schema().column_names)
    table.upsert(
        df=ordered_chunk_df.to_arrow(),
        join_cols=JOIN_COLS,
        when_matched_update_all=True,
        when_not_matched_insert_all=True,
    )


def summarize_and_checkpoint(  # noqa: PLR0913 -- each is an independent tuning knob
    unsummarized_df: pl.DataFrame,
    client: SummaryClient,
    checkpoint_target: tuple[Catalog, str],
    batch_size: int = SUMMARIZE_CHECKPOINT_BATCH_SIZE,
    errors: list[str] | None = None,
    max_concurrency: int = SUMMARIZE_MAX_CONCURRENCY,
    context: AssetExecutionContext | None = None,
) -> pl.DataFrame:
    """Summarize unsummarized_df in chunks, upserting each as it completes.

    errors, if given, collects every failure's message (see summarize_conversations)
    so a caller can surface *why* calls failed, e.g. in a Failure message.

    context, if given, logs per-chunk progress via context.log.info -- lands in
    Dagster's own structured per-run event log, unlike the plain module logger,
    whose stdout capture can miss lines across a step retry/resume (e.g. after
    a pod eviction). Optional so this stays callable outside a Dagster run
    (tests, scripts) -- same pattern as canvas.py's context-taking lib
    functions, just optional here since summarize_and_checkpoint predates it.

    Stops the whole loop (not just the current chunk) after
    MAX_CONSECUTIVE_FAILED_CHUNKS chunks in a row come back with zero successful
    LLM calls -- a systemic error (bad credential) isn't going to start succeeding
    on the next chunk either, so there's no point burning through the rest of
    unsummarized_df with the same failure. Everything summarized before the abort
    is already upserted into the real table.

    Returns the full concatenated output for the caller's normal return/metadata
    handling -- the caller's own write of this (e.g. via the io_manager) upserts
    the same rows again, which is a harmless no-op since they're already there.
    """
    log = context.log if context is not None else logger
    catalog, table_identifier = checkpoint_target
    consecutive_failed_chunks = 0
    summary_chunks: list[pl.DataFrame] = []
    chunk_starts = range(0, unsummarized_df.height, batch_size)
    total_chunks = len(chunk_starts)
    for chunk_index, chunk_start in enumerate(chunk_starts, start=1):
        chunk = unsummarized_df.slice(chunk_start, batch_size)
        chunk_summaries = summarize_conversations(
            chunk, client, errors=errors, max_concurrency=max_concurrency
        )
        summary_chunks.append(chunk_summaries)
        checkpoint_chunk(catalog, table_identifier, chunk_summaries)
        log.info(
            "Upserted chunk %d/%d (%d rows) into %s",
            chunk_index,
            total_chunks,
            chunk_summaries.height,
            table_identifier,
        )

        chunk_llm_successes = chunk_summaries.filter(
            pl.col("summary_model_version").is_not_null()
        ).height
        chunk_attempted = chunk_llm_successes + (chunk.height - chunk_summaries.height)
        if chunk_attempted > 0 and chunk_llm_successes == 0:
            consecutive_failed_chunks += 1
            if consecutive_failed_chunks >= MAX_CONSECUTIVE_FAILED_CHUNKS:
                break
        else:
            consecutive_failed_chunks = 0

    if summary_chunks:
        return pl.concat(summary_chunks)
    return pl.DataFrame(schema=SUMMARIZE_CHECKPOINT_SCHEMA)
