"""Ask a small model whether a courseware source reads as an answer key.

courseware_answer_key (dbt) decides from a document's name, title and opening, and
misses keys whose name and opening say nothing, e.g. a combined problem set file
with "Solution:" inline. This is the second check: each source's excerpt is read by
a small model, and the verdict is recorded with the text's checksum and the
checker version. A source is checked again only when its text or the checker
changes, and a copy of a text already checked (a B2B course copy) reuses the
verdict without a call.

Flagged sources are listed in int__courseware__answer_document_review for a person
to confirm. Nothing here removes a source from extraction.
"""

import hashlib
import json
import logging
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from itertools import batched
from typing import Any

import polars as pl
from anthropic import Anthropic, AnthropicBedrock, APIError
from ml.lib.courseware.schemas import Strict
from pydantic import Field, ValidationError

log = logging.getLogger(__name__)

CHECKER_ID = "courseware_answer_check_v1"
CHECK_PROMPT = """\
You screen course files before another model reads them to describe what a course \
teaches. Answer keys and worked solutions to a course's graded work must not be \
read: they would leak answers into what learners see.

Each entry below is one source: an id, its kind, its title or file name, and an \
excerpt (its opening, and its end when it is long).

For each source decide is_answer_document:
- true when the source is mainly answers: an answer key, a solution set or \
solution manual, worked solutions to the course's problem sets, homework, quizzes \
or exams, or graded work with its answers filled in.
- false for teaching material, even when it works examples ("Example 3. \
Solution: ..."), for exercises, problem sets and exams that state questions without \
answers, and for a reading with a few answers at its end.
When unsure, decide true only if a learner could use the source to look up answers \
to graded work. Give a reason of at most 20 words.

Call record_verdicts once with one entry per id.
"""
# Answer keys announce themselves early and, when they don't, read as answers at
# the end; the middle of a long document adds cost and little signal.
EXCERPT_HEAD_CHARS = 4_000
EXCERPT_TAIL_CHARS = 2_000
CHECK_BATCH_SIZE = 20
CHECK_MAX_TOKENS = 8_000
DEFAULT_SOURCE_KINDS = ("document",)


class SourceVerdict(Strict):
    source_id: int
    is_answer_document: bool
    reason: str = Field(description="At most 20 words")


class VerdictBatch(Strict):
    verdicts: list[SourceVerdict] = Field(
        description="One entry per source id in the list"
    )


CHECK_TOOL = {
    "name": "record_verdicts",
    "description": "Record a verdict for every source. Call it once.",
    "input_schema": VerdictBatch.model_json_schema(),
}

ANSWER_CHECK_SCHEMA = {
    "platform": pl.String,
    "courserun_readable_id": pl.String,
    "source_ref": pl.String,
    "source_kind": pl.String,
    "source_title": pl.String,
    "source_checksum": pl.String,
    "is_answer_document": pl.Boolean,
    "reason": pl.String,
    "checker_id": pl.String,
    "check_version": pl.String,
    "model_id": pl.String,
    "checked_at": pl.Datetime(time_zone="UTC"),
}
_SOURCE_KEY = ("platform", "courserun_readable_id", "source_ref", "source_checksum")


def check_version(model_id: str, effort: str) -> str:
    """Hash everything a verdict depends on besides the source's text."""
    return hashlib.sha256(
        json.dumps(
            [
                CHECKER_ID,
                CHECK_PROMPT,
                VerdictBatch.model_json_schema(),
                EXCERPT_HEAD_CHARS,
                EXCERPT_TAIL_CHARS,
                model_id,
                effort,
            ],
            sort_keys=True,
        ).encode()
    ).hexdigest()[:12]


def excerpt(text: str) -> str:
    """Cut a text to its opening, and its end when the text is long."""
    if len(text) <= EXCERPT_HEAD_CHARS + EXCERPT_TAIL_CHARS:
        return text
    return (
        f"{text[:EXCERPT_HEAD_CHARS]}\n[... {len(text):,} characters in all ...]\n"
        f"{text[-EXCERPT_TAIL_CHARS:]}"
    )


def candidate_sources(
    rows: pl.DataFrame, source_kinds: Iterable[str] = DEFAULT_SOURCE_KINDS
) -> pl.DataFrame:
    """Pick the distinct sources to check from int__courseware__extraction_sources.

    A source read in several scopes of a run (a document linked from two units)
    is checked once.
    """
    sources = (
        rows.filter(
            pl.col("source_kind").is_in(list(source_kinds))
            & (pl.col("source_text").str.strip_chars().str.len_chars() > 0)
        )
        .select(
            "platform",
            "courserun_readable_id",
            "source_ref",
            "source_kind",
            "source_title",
            "source_text",
        )
        .unique(["platform", "courserun_readable_id", "source_ref"], keep="first")
    )
    return sources.with_columns(
        pl.col("source_text")
        .map_elements(
            lambda text: hashlib.sha256(text.encode()).hexdigest(),
            return_dtype=pl.String,
        )
        .alias("source_checksum")
    )


def _check_batch(
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    effort: str,
    batch: list[dict[str, Any]],
) -> list[SourceVerdict]:
    listing = "\n\n".join(
        f"=== {index} | kind: {entry['source_kind']} | title: "
        f"{entry['source_title']!r} ===\n{excerpt(entry['source_text'])}"
        for index, entry in enumerate(batch)
    )
    with client.messages.stream(
        model=model_id,
        max_tokens=CHECK_MAX_TOKENS,
        system=CHECK_PROMPT,
        tools=[CHECK_TOOL],
        tool_choice={"type": "auto"},
        messages=[{"role": "user", "content": listing}],
        output_config={"effort": effort},
    ) as stream:
        response = stream.get_final_message()
    tool_input = next(
        (
            block.input
            for block in response.content
            if block.type == "tool_use" and block.name == CHECK_TOOL["name"]
        ),
        None,
    )
    if tool_input is None:
        msg = f"no {CHECK_TOOL['name']} call (stop_reason={response.stop_reason})"
        raise ValueError(msg)
    try:
        verdicts = VerdictBatch.model_validate(tool_input).verdicts
    except ValidationError as error:
        msg = f"invalid verdicts: {error}"
        raise ValueError(msg) from error
    # an id outside the listing (e.g. counted from 1) or given twice means the
    # verdicts cannot be matched to their sources
    ids = [verdict.source_id for verdict in verdicts]
    if len(set(ids)) != len(ids) or not set(ids) <= set(range(len(batch))):
        msg = f"verdict ids {sorted(ids)} do not match sources 0-{len(batch) - 1}"
        raise ValueError(msg)
    return verdicts


def pending_sources(
    rows: pl.DataFrame,
    version: str,
    existing: pl.DataFrame | None = None,
    source_kinds: Iterable[str] = DEFAULT_SOURCE_KINDS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Split the candidate sources into what needs a verdict and what is sent.

    :param rows: int__courseware__extraction_sources rows
    :param version: check_version of this run
    :param existing: verdicts already stored
    :param source_kinds: the source kinds to check
    :returns: the sources whose newest verdict at this version is not for their
        text, one source per distinct text among those with no stored verdict to
        reuse, and the stored verdicts at this version by text checksum
    :rtype: tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]
    """
    known: dict[str, dict[str, Any]] = {}
    done: set[tuple[str, ...]] = set()
    if existing is not None:
        current = existing.filter(pl.col("check_version") == version)
        # done only when a source's newest verdict is for its text: one whose text
        # changed and changed back gets a new row (reusing the stored verdict), as
        # the review queue shows a source's newest verdict
        newest = current.sort("checked_at").unique(
            ["platform", "courserun_readable_id", "source_ref"], keep="last"
        )
        done = set(newest.select(_SOURCE_KEY).iter_rows())
        for row in current.sort("checked_at").iter_rows(named=True):
            known[row["source_checksum"]] = row
    pending = [
        source
        for source in candidate_sources(rows, source_kinds).iter_rows(named=True)
        if tuple(source[column] for column in _SOURCE_KEY) not in done
    ]
    # one call per distinct text: copies of a text share its verdict
    to_check = list(
        {
            source["source_checksum"]: source
            for source in pending
            if source["source_checksum"] not in known
        }.values()
    )
    return pending, to_check, known


def check_sources(  # noqa: PLR0913
    rows: pl.DataFrame,
    client: Anthropic | AnthropicBedrock,
    *,
    model_id: str,
    effort: str = "low",
    existing: pl.DataFrame | None = None,
    source_kinds: Iterable[str] = DEFAULT_SOURCE_KINDS,
    max_workers: int = 4,
) -> pl.DataFrame:
    """Check every source with no verdict at this checker version for its text.

    :param rows: int__courseware__extraction_sources rows
    :param client: an Anthropic or AnthropicBedrock client
    :param model_id: the model id the client expects
    :param effort: output_config.effort
    :param existing: verdicts already stored; their texts are not sent again
    :param source_kinds: the source kinds to check
    :param max_workers: concurrent calls
    :returns: the new verdict rows, one per source
    :rtype: pl.DataFrame
    """
    version = check_version(model_id, effort)
    pending, to_check, known = pending_sources(rows, version, existing, source_kinds)
    batches = [list(batch) for batch in batched(to_check, CHECK_BATCH_SIZE)]
    checked_at = datetime.now(tz=UTC)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(_check_batch, client, model_id, effort, batch)
            for batch in batches
        ]
        for batch, future in zip(batches, futures, strict=True):
            # a failed batch is left out; the next run checks only what is missing
            try:
                verdicts = future.result()
            except (ValueError, APIError):
                log.exception("Answer check batch of %d sources failed", len(batch))
                continue
            by_id = {verdict.source_id: verdict for verdict in verdicts}
            missing = len(batch) - sum(index in by_id for index in range(len(batch)))
            if missing:
                log.warning("No verdict returned for %d sources", missing)
            for index, entry in enumerate(batch):
                if index in by_id:
                    known[entry["source_checksum"]] = {
                        "is_answer_document": by_id[index].is_answer_document,
                        "reason": by_id[index].reason,
                    }
    return pl.DataFrame(
        [
            {
                "platform": source["platform"],
                "courserun_readable_id": source["courserun_readable_id"],
                "source_ref": source["source_ref"],
                "source_kind": source["source_kind"],
                "source_title": source["source_title"],
                "source_checksum": source["source_checksum"],
                "is_answer_document": verdict["is_answer_document"],
                "reason": verdict["reason"],
                "checker_id": CHECKER_ID,
                "check_version": version,
                "model_id": model_id,
                # when the verdict was recorded for this source, also for a reused
                # one, so the newest row of a source is its newest text's verdict
                "checked_at": checked_at,
            }
            for source in pending
            if (verdict := known.get(source["source_checksum"])) is not None
        ],
        schema=ANSWER_CHECK_SCHEMA,
    )
