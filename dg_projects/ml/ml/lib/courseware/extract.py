"""Extract claims from courseware scopes in two stages and flatten them into rows.

The text stage reads only a scope part's own sources (no course title, outline or
description) and records claims with quoted evidence, verified against the sources.
Its output is keyed by the exact input it read (text_extraction_key), so text that
is read again under the same extractor version and model reuses the stored reading
instead of calling the model.

The context stage takes those claims with the course header and settles what
depends on the course: a concept's teaching status in this curriculum and the sense
of an ambiguous label. It cannot add claims or evidence. Its output is keyed by its
own exact input (extraction_key), which covers the header and the text stage's
output, so editing a course's description or outline re-runs only this stage.

Tables: courseware_text_extraction (one row per text-stage call),
courseware_claim_extraction (one row per scope part and context), and
courseware_claim and courseware_claim_evidence (keyed by extraction_key). dbt builds
the dimensional facts, concepts and bridges from them; nothing here resolves labels
across scopes or courses.
"""

import hashlib
import json
import logging
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from typing import Any, Literal

import httpx2
import polars as pl
from anthropic import Anthropic, AnthropicBedrock, APIError
from ml.lib.courseware.prompts import (
    CONTEXT_SYSTEM_PROMPT,
    TEXT_SYSTEM_PROMPT,
    claim_id,
    context_message,
    text_message,
)
from ml.lib.courseware.schemas import ContextJudgment, ScopeExtraction
from ml.lib.courseware.scopes import Course, Scope
from ml.lib.courseware.tracing import TRACE_TAGS, attach_call
from ml.lib.courseware.verify import verify_passage
from ml.resources.opik_auth import traced
from pydantic import BaseModel

log = logging.getLogger(__name__)

EXTRACTOR_ID = "courseware_claims_v2"
DEFAULT_MODEL = "claude-haiku-5-5"
BEDROCK_DEFAULT_MODEL = "us.anthropic.claude-haiku-5-5"
Effort = Literal["low", "medium", "high", "xhigh", "max"]
DEFAULT_EFFORT: Effort = "medium"
# Streaming, so the SDK's HTTP timeout does not bound long outputs.
MAX_OUTPUT_TOKENS = 64_000
# A response that fails validation (e.g. cut off at max_tokens) is requested again.
ATTEMPTS = 2
# stop_reason of a context-stage row written without a call: the text stage found
# no claims, so there is nothing to place in the course
NO_CLAIMS_STOP_REASON = "no_claims"
# claim kinds that carry a teaching_status
_STATUS_KINDS = frozenset({"concept", "skill"})


def _digest(*parts: Any) -> str:
    return hashlib.sha256(
        json.dumps(parts, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@dataclass(frozen=True)
class _Stage[T: BaseModel]:
    name: str
    system_prompt: str
    tool: dict[str, Any]
    output: type[T]

    @property
    def prompt_version(self) -> str:
        return _digest(self.system_prompt)[:12]

    @property
    def schema_version(self) -> str:
        return _digest(self.tool["input_schema"])[:12]


TEXT_STAGE = _Stage(
    name="text",
    system_prompt=TEXT_SYSTEM_PROMPT,
    tool={
        "name": "record_extraction",
        "description": "Record the complete extraction for this scope. Call it once.",
        "input_schema": ScopeExtraction.model_json_schema(),
    },
    output=ScopeExtraction,
)
CONTEXT_STAGE = _Stage(
    name="context",
    system_prompt=CONTEXT_SYSTEM_PROMPT,
    tool={
        "name": "record_context",
        "description": "Record the revisions the course context settles. Call once.",
        "input_schema": ContextJudgment.model_json_schema(),
    },
    output=ContextJudgment,
)
TEXT_PROMPT_VERSION = TEXT_STAGE.prompt_version
TEXT_SCHEMA_VERSION = TEXT_STAGE.schema_version
CONTEXT_PROMPT_VERSION = CONTEXT_STAGE.prompt_version
CONTEXT_SCHEMA_VERSION = CONTEXT_STAGE.schema_version
# What courseware_extraction_allowlist approves: both stages' prompts and schemas.
PROMPT_VERSION = _digest(TEXT_PROMPT_VERSION, CONTEXT_PROMPT_VERSION)[:12]
SCHEMA_VERSION = _digest(TEXT_SCHEMA_VERSION, CONTEXT_SCHEMA_VERSION)[:12]

_TOKEN_SCHEMA = {
    "input_tokens": pl.Int64,
    "output_tokens": pl.Int64,
    "cache_read_input_tokens": pl.Int64,
    "cache_creation_input_tokens": pl.Int64,
}
# One row per text-stage call. The scope it names is the first one read with this
# text in the run; another scope or run with the same text reuses the row.
TEXT_EXTRACTION_SCHEMA = {
    "text_extraction_key": pl.String,
    "extractor_id": pl.String,
    "prompt_version": pl.String,
    "schema_version": pl.String,
    "model_id": pl.String,
    "effort": pl.String,
    "text_input_digest": pl.String,
    "platform": pl.String,
    "courserun_readable_id": pl.String,
    "scope_id": pl.String,
    "scope_kind": pl.String,
    "scope_part": pl.Int64,
    "scope_part_count": pl.Int64,
    "input_text_checksum": pl.String,
    "summary": pl.String,
    "notes_json": pl.String,
    # the claim and evidence rows read from the text, as JSON lists of objects
    # without extraction_key (and evidence without source_ref, which names a block
    # of the scope that copies them)
    "claims_json": pl.String,
    "evidence_json": pl.String,
    "stop_reason": pl.String,
    # this text-stage call's usage, as a per-call diagnostic; cost is in Opik, under
    # the courseware:text tag
    **_TOKEN_SCHEMA,
    "extracted_at": pl.Datetime(time_zone="UTC"),
}
EXTRACTION_SCHEMA = {
    "extraction_key": pl.String,
    "extractor_id": pl.String,
    "prompt_version": pl.String,
    "schema_version": pl.String,
    "model_id": pl.String,
    "effort": pl.String,
    "platform": pl.String,
    "courserun_readable_id": pl.String,
    "scope_id": pl.String,
    "scope_kind": pl.String,
    "scope_title": pl.String,
    "scope_position": pl.Int64,
    "scope_part": pl.Int64,
    "scope_part_count": pl.Int64,
    "input_text_checksum": pl.String,
    "scope_part_checksums": pl.String,
    "header_version": pl.String,
    # the text-stage reading the claims came from, and that stage's exact input;
    # extractions with one text_input_digest read the same text, so their claims
    # are one source's, not independent corroboration
    "text_extraction_key": pl.String,
    "text_input_digest": pl.String,
    # SHA-256 of the context stage's exact message (header and text claims)
    "context_input_digest": pl.String,
    "source_count": pl.Int64,
    "truncated_source_count": pl.Int64,
    "sources_json": pl.String,
    "summary": pl.String,
    "notes_json": pl.String,
    "stop_reason": pl.String,
    # the context-stage call's usage, as a per-call diagnostic (zero when no call
    # was made); the text stage's is on its own row, and cost is in Opik
    **_TOKEN_SCHEMA,
    "extracted_at": pl.Datetime(time_zone="UTC"),
}
CLAIM_SCHEMA = {
    "extraction_key": pl.String,
    "claim_ordinal": pl.Int64,
    "claim_kind": pl.String,
    "label": pl.String,
    "description": pl.String,
    "concept_kind": pl.String,
    "teaching_status": pl.String,
    # the status the text stage read, before the course context revised it
    "text_teaching_status": pl.String,
    "sense_hint": pl.String,
    "how_used": pl.String,
    "flagged_as_prior": pl.Boolean,
    "bloom_process": pl.String,
    "knowledge_type": pl.String,
    "prerequisite_kind": pl.String,
    "course_ref": pl.String,
    "related_concept_labels_json": pl.String,
}
EVIDENCE_SCHEMA = {
    "extraction_key": pl.String,
    "claim_ordinal": pl.Int64,
    "evidence_ordinal": pl.Int64,
    "source_id": pl.String,
    "source_ref": pl.String,
    "passage": pl.String,
    "verification_status": pl.String,
    "char_start": pl.Int64,
    "char_end": pl.Int64,
}
# the claim columns the text stage fills; the context stage adds the rest
_TEXT_CLAIM_COLUMNS = tuple(
    column
    for column in CLAIM_SCHEMA
    if column not in {"extraction_key", "text_teaching_status", "sense_hint"}
)
_TEXT_EVIDENCE_COLUMNS = tuple(
    column
    for column in EVIDENCE_SCHEMA
    if column not in {"extraction_key", "source_ref"}
)

# One row per scope part each time the extractor reads a run, extracted or skipped.
# dbt matches a scope's stored readings to its latest one to pick the current
# extraction; input_text_checksum, the split and the header version are computed
# only here, and a skipped scope otherwise leaves no trace of them.
SCOPE_INPUT_SCHEMA = {
    "platform": pl.String,
    "courserun_readable_id": pl.String,
    "scope_id": pl.String,
    "scope_part": pl.Int64,
    "scope_part_count": pl.Int64,
    "input_text_checksum": pl.String,
    "header_version": pl.String,
    "observed_at": pl.Datetime(time_zone="UTC"),
}

# list field on ScopeExtraction -> claim_kind, and how its items map onto a claim row
_CLAIM_LISTS = {
    "concepts": "concept",
    "skills": "skill",
    "assumed_knowledge": "assumed_knowledge",
    "assessment_expectations": "assessment_expectation",
    "learning_outcomes": "learning_outcome",
    "declared_prerequisites": "declared_prerequisite",
    "audience": "audience",
}


class ExtractionError(Exception):
    """The model gave no valid output for a scope."""


@dataclass(frozen=True)
class ExtractionTables:
    text_extractions: pl.DataFrame
    extractions: pl.DataFrame
    claims: pl.DataFrame
    evidence: pl.DataFrame


@dataclass(frozen=True)
class TextReading:
    """The text stage's output for one scope part's text, stored or new."""

    text_extraction_key: str
    text_input_digest: str
    summary: str
    notes: list[str]
    # claim rows in _TEXT_CLAIM_COLUMNS, evidence rows in _TEXT_EVIDENCE_COLUMNS
    claims: list[dict[str, Any]]
    evidence: list[dict[str, Any]]


# (platform, courserun_readable_id, text_extraction_key): a reading is reused only
# within the run whose scope it was read for
ReadingKey = tuple[str, str, str]


def header_version(course: Course) -> str:
    """Hash the course header the context stage reads.

    :param course: the scope's course
    :returns: a 12-character version of the title, outline and description
    :rtype: str
    """
    return _digest(course.title, course.outline, course.description)[:12]


def text_extraction_key(scope: Scope, model_id: str, effort: str) -> str:
    """Key a text-stage call by the extractor version and its exact input.

    :param scope: the scope part
    :param model_id: the model id the client expects
    :param effort: output_config.effort
    :returns: the text extraction key
    :rtype: str
    """
    return _digest(
        EXTRACTOR_ID,
        TEXT_PROMPT_VERSION,
        TEXT_SCHEMA_VERSION,
        model_id,
        effort,
        _sha256(text_message(scope)),
    )


def _context(
    scope: Scope, model_id: str, effort: str, reading: TextReading
) -> tuple[str, str, str]:
    """Return the context stage's message, its digest and the extraction key."""
    message = context_message(scope, reading.claims, reading.evidence)
    digest = _sha256(message)
    key = _digest(
        EXTRACTOR_ID,
        PROMPT_VERSION,
        SCHEMA_VERSION,
        model_id,
        effort,
        scope.course.platform,
        scope.course.courserun_readable_id,
        scope.scope_id,
        scope.part,
        scope.part_count,
        reading.text_extraction_key,
        digest,
    )
    return message, digest, key


def extraction_key(
    scope: Scope, model_id: str, effort: str, reading: TextReading
) -> str:
    """Key a scope part's extraction by everything the context stage reads.

    That is the course header, the scope's title and the text stage's claims, so a
    change to the header, or a new text reading, gives a new key and a new
    extraction next to the old one.

    :param scope: the scope part
    :param model_id: the model id the client expects
    :param effort: output_config.effort
    :param reading: the text stage's reading of the scope part
    :returns: the extraction key
    :rtype: str
    """
    return _context(scope, model_id, effort, reading)[2]


def _reading_key(scope: Scope, key: str) -> ReadingKey:
    return (scope.course.platform, scope.course.courserun_readable_id, key)


def stored_readings(rows: pl.DataFrame) -> dict[ReadingKey, TextReading]:
    """Index stored courseware_text_extraction rows for extract_scopes.

    :param rows: courseware_text_extraction rows
    :returns: the earliest reading of each key, by run
    :rtype: dict[ReadingKey, TextReading]
    """
    readings: dict[ReadingKey, TextReading] = {}
    for row in rows.sort("extracted_at").iter_rows(named=True):
        readings.setdefault(
            (row["platform"], row["courserun_readable_id"], row["text_extraction_key"]),
            TextReading(
                text_extraction_key=row["text_extraction_key"],
                text_input_digest=row["text_input_digest"],
                summary=row["summary"],
                notes=json.loads(row["notes_json"]),
                claims=json.loads(row["claims_json"]),
                evidence=json.loads(row["evidence_json"]),
            ),
        )
    return readings


@dataclass(frozen=True)
class PlannedScope:
    scope: Scope
    text_extraction_key: str
    # the stored reading of the scope's text, when its run has one
    reading: TextReading | None
    # known only when the reading is stored
    extraction_key: str | None


def plan_scopes(
    scopes: Iterable[Scope],
    *,
    model_id: str,
    effort: str,
    readings: Mapping[ReadingKey, TextReading],
) -> list[PlannedScope]:
    """Work out which stored reading and extraction key each scope part has.

    :param scopes: the scope parts
    :param model_id: the model id the client expects
    :param effort: output_config.effort
    :param readings: stored text readings (stored_readings)
    :returns: one PlannedScope per scope part
    :rtype: list[PlannedScope]
    """
    planned = []
    for scope in scopes:
        key = text_extraction_key(scope, model_id, effort)
        reading = readings.get(_reading_key(scope, key))
        planned.append(
            PlannedScope(
                scope=scope,
                text_extraction_key=key,
                reading=reading,
                extraction_key=(
                    extraction_key(scope, model_id, effort, reading)
                    if reading is not None
                    else None
                ),
            )
        )
    return planned


def scope_inputs(scopes: Iterable[Scope]) -> pl.DataFrame:
    """Record the input of every scope part as it is now, extracted or not.

    Write it for every scope of each course run read, so dbt can tell which stored
    reading matches the current text and header (e.g. after a scope's text goes back
    to an earlier version, whose extraction is stored and so is skipped).

    :param scopes: every scope part of the runs read
    :returns: courseware_scope_input rows, all with one observed_at
    :rtype: pl.DataFrame
    """
    observed_at = datetime.now(tz=UTC)
    return pl.DataFrame(
        [
            {
                "platform": scope.course.platform,
                "courserun_readable_id": scope.course.courserun_readable_id,
                "scope_id": scope.scope_id,
                "scope_part": scope.part,
                "scope_part_count": scope.part_count,
                "input_text_checksum": scope.input_text_checksum,
                "header_version": header_version(scope.course),
                "observed_at": observed_at,
            }
            for scope in scopes
        ],
        schema=SCOPE_INPUT_SCHEMA,
    )


@dataclass(frozen=True)
class _ModelResult[T: BaseModel]:
    output: T
    stop_reason: str
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int
    cache_creation_input_tokens: int

    @property
    def tokens(self) -> dict[str, int]:
        return {column: getattr(self, column) for column in _TOKEN_SCHEMA}


def call_model[T: BaseModel](  # noqa: PLR0913
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    effort: Effort,
    message: str,
    *,
    stage: _Stage[T],
    trace_metadata: dict[str, Any],
    check: Callable[[T], None] | None = None,
) -> _ModelResult[T]:
    """Ask for one stage's output for one scope, as that stage's own Opik span.

    :param stage: TEXT_STAGE or CONTEXT_STAGE
    :param trace_metadata: what identifies the call on its Opik span
    :param check: see _request
    :raises ExtractionError: when no attempt returns a valid output
    """
    traced_request: Callable[..., _ModelResult[T]] = _TRACED_REQUESTS[stage.name]
    return traced_request(
        client,
        model_id,
        effort,
        message,
        stage=stage,
        trace_metadata=trace_metadata,
        check=check,
    )


def _request[T: BaseModel](  # noqa: PLR0913
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    effort: Effort,
    message: str,
    *,
    stage: _Stage[T],
    trace_metadata: dict[str, Any],
    check: Callable[[T], None] | None = None,
) -> _ModelResult[T]:
    """Ask for one stage's output for one scope as a tool call.

    Neither constrained option is available for the text stage's schema:
    structured outputs (and strict tools) reject it as too large a grammar, and
    Opus 5.5 and Sonnet 5.5 reject a forced tool_choice. So the tool is offered
    with tool_choice auto, the prompt asks for it, and the input is validated here.
    The stage's system prompt is the cached prefix shared by its calls.

    :param stage: TEXT_STAGE or CONTEXT_STAGE
    :param trace_metadata: what identifies the call on its Opik span
    :param check: raises ValueError when a valid output still contradicts its input
        (the context stage revising a claim it was not given); retried like an
        invalid output
    :raises ExtractionError: when no attempt returns a valid output
    """
    error: Exception | str | None = None
    usages = []
    for _ in range(ATTEMPTS):
        with client.messages.stream(
            model=model_id,
            max_tokens=MAX_OUTPUT_TOKENS,
            system=[
                {
                    "type": "text",
                    "text": stage.system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=[stage.tool],
            tool_choice={"type": "auto"},
            messages=[{"role": "user", "content": message}],
            output_config={"effort": effort},
        ) as stream:
            response = stream.get_final_message()
        usages.append(response.usage)
        attach_call(
            client,
            model_id,
            usages,
            template=stage.system_prompt,
            metadata=trace_metadata,
        )
        if response.stop_reason == "refusal":
            msg = f"refused: {response.stop_details}"
            raise ExtractionError(msg)
        tool_input = next(
            (
                block.input
                for block in response.content
                if block.type == "tool_use" and block.name == stage.tool["name"]
            ),
            None,
        )
        if tool_input is None:
            error = f"no tool call (stop_reason={response.stop_reason})"
            log.warning("%s, retrying", error)
            continue
        try:
            output = stage.output.model_validate(tool_input)
            if check is not None:
                check(output)
        # pydantic's ValidationError is a ValueError
        except ValueError as invalid:
            error = invalid
            log.warning(
                "Invalid %s output (stop_reason=%s), retrying",
                stage.name,
                response.stop_reason,
            )
            continue
        return _ModelResult(
            output=output,
            stop_reason=str(response.stop_reason),
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
            cache_read_input_tokens=response.usage.cache_read_input_tokens or 0,
            cache_creation_input_tokens=response.usage.cache_creation_input_tokens or 0,
        )
    msg = f"no valid {stage.name} output after {ATTEMPTS} attempts: {error}"
    raise ExtractionError(msg)


def _traced_stage(stage: _Stage[Any]) -> Callable[..., _ModelResult[Any]]:
    """Wrap _request in an Opik span named and tagged for one stage.

    Cost lives in Opik (project OPIK_PROJECT_NAME, dagster-ml by default), not in the
    warehouse. The tag (courseware:text or courseware:context) beside "courseware" is
    what splits it by stage. A text reading that is reused makes no call, so no span.
    """
    decorate = traced(
        f"courseware_extract_{stage.name}",
        tags=[*TRACE_TAGS, f"courseware:{stage.name}"],
        ignore_arguments=["client", "stage", "check", "trace_metadata"],
    )
    return decorate(_request)


_TRACED_REQUESTS = {
    stage.name: _traced_stage(stage) for stage in (TEXT_STAGE, CONTEXT_STAGE)
}


def _claim_row(ordinal: int, kind: str, claim: BaseModel) -> dict[str, Any]:
    values = claim.model_dump()
    statement = values.get("statement")
    return {
        "claim_ordinal": ordinal,
        "claim_kind": kind,
        # outcomes and audience have only a statement, which is their label
        "label": values.get("label") or statement,
        "description": values.get("description")
        or (statement if "label" in values else None),
        "concept_kind": values.get("concept_kind"),
        "teaching_status": values.get("teaching_status"),
        "how_used": values.get("how_used"),
        "flagged_as_prior": values.get("flagged_as_prior"),
        "bloom_process": values.get("bloom_process"),
        "knowledge_type": values.get("knowledge_type"),
        "prerequisite_kind": values.get("prerequisite_kind"),
        "course_ref": values.get("course_ref"),
        "related_concept_labels_json": (
            json.dumps(values["concept_labels"]) if "concept_labels" in values else None
        ),
    }


def _text_reading(
    scope: Scope,
    key: str,
    model_id: str,
    effort: str,
    result: _ModelResult[ScopeExtraction],
) -> tuple[TextReading, dict[str, Any]]:
    """Flatten a text-stage result, verifying each passage against its source."""
    extraction = result.output
    sources = {source.source_id: source for source in scope.sources}
    claims, evidence = [], []
    ordinal = 0
    for field_name, kind in _CLAIM_LISTS.items():
        for claim in getattr(extraction, field_name):
            ordinal += 1
            claims.append(_claim_row(ordinal, kind, claim))
            for evidence_ordinal, item in enumerate(claim.evidence, 1):
                source = sources.get(item.source_id)
                match = verify_passage(item.passage, source.text if source else None)
                evidence.append(
                    {
                        "claim_ordinal": ordinal,
                        "evidence_ordinal": evidence_ordinal,
                        "source_id": item.source_id,
                        "passage": item.passage,
                        "verification_status": match.status,
                        "char_start": match.char_start,
                        "char_end": match.char_end,
                    }
                )
    reading = TextReading(
        text_extraction_key=key,
        text_input_digest=_sha256(text_message(scope)),
        summary=extraction.summary,
        notes=list(extraction.notes),
        claims=claims,
        evidence=evidence,
    )
    row = {
        "text_extraction_key": key,
        "extractor_id": EXTRACTOR_ID,
        "prompt_version": TEXT_PROMPT_VERSION,
        "schema_version": TEXT_SCHEMA_VERSION,
        "model_id": model_id,
        "effort": effort,
        "text_input_digest": reading.text_input_digest,
        "platform": scope.course.platform,
        "courserun_readable_id": scope.course.courserun_readable_id,
        "scope_id": scope.scope_id,
        "scope_kind": scope.scope_kind,
        "scope_part": scope.part,
        "scope_part_count": scope.part_count,
        "input_text_checksum": scope.input_text_checksum,
        "summary": reading.summary,
        "notes_json": json.dumps(reading.notes),
        "claims_json": json.dumps(claims),
        "evidence_json": json.dumps(evidence),
        "stop_reason": result.stop_reason,
        **result.tokens,
        "extracted_at": datetime.now(tz=UTC),
    }
    return reading, row


def check_revisions(reading: TextReading, judgment: ContextJudgment) -> None:
    """Refuse a context-stage output that revises claims it was not given.

    :raises ValueError: on an unknown or repeated claim id, or a teaching status
        for a claim kind that has none
    """
    kinds = {
        claim_id(claim["claim_ordinal"]): claim["claim_kind"]
        for claim in reading.claims
    }
    seen: set[str] = set()
    for revision in judgment.revisions:
        if revision.claim_id not in kinds:
            msg = f"revision of unknown claim {revision.claim_id}"
            raise ValueError(msg)
        if revision.claim_id in seen:
            msg = f"claim {revision.claim_id} revised twice"
            raise ValueError(msg)
        seen.add(revision.claim_id)
        if (
            revision.teaching_status is not None
            and kinds[revision.claim_id] not in _STATUS_KINDS
        ):
            msg = f"{kinds[revision.claim_id]} claim {revision.claim_id} has no status"
            raise ValueError(msg)


# one scope part's extraction row, claim rows and evidence rows
_Rows = tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]


@dataclass(frozen=True)
class _ContextInput:
    scope: Scope
    reading: TextReading
    key: str
    digest: str
    message: str


def _context_rows(  # noqa: PLR0913
    item: _ContextInput,
    model_id: str,
    effort: str,
    judgment: ContextJudgment | None,
    stop_reason: str,
    tokens: Mapping[str, int],
) -> _Rows:
    """Apply the context stage's revisions to the text claims, for one scope part.

    Evidence is the text stage's, verification and offsets included (the text is
    identical); only source_ref is this scope's, since a source id names a block of
    the scope whose sources were numbered.
    """
    scope, reading, key = item.scope, item.reading, item.key
    revisions = {
        revision.claim_id: revision
        for revision in (judgment.revisions if judgment is not None else [])
    }
    claims = []
    for claim in reading.claims:
        revision = revisions.get(claim_id(claim["claim_ordinal"]))
        claims.append(
            {
                **claim,
                "extraction_key": key,
                "teaching_status": (
                    revision.teaching_status
                    if revision is not None and revision.teaching_status is not None
                    else claim["teaching_status"]
                ),
                "text_teaching_status": claim["teaching_status"],
                "sense_hint": revision.sense_hint if revision is not None else None,
            }
        )
    sources = {source.source_id: source for source in scope.sources}
    evidence = [
        {
            **passage,
            "extraction_key": key,
            "source_ref": (
                sources[passage["source_id"]].source_ref
                if passage["source_id"] in sources
                else None
            ),
        }
        for passage in reading.evidence
    ]
    notes = [
        *reading.notes,
        *(judgment.notes if judgment is not None else []),
        *(
            f"{source.source_id} ({source.source_ref}) was cut to "
            f"{len(source.text):,} of {source.total_chars:,} characters"
            for source in scope.sources
            if source.truncated
        ),
    ]
    extraction = {
        "extraction_key": key,
        "extractor_id": EXTRACTOR_ID,
        "prompt_version": PROMPT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "model_id": model_id,
        "effort": effort,
        "platform": scope.course.platform,
        "courserun_readable_id": scope.course.courserun_readable_id,
        "scope_id": scope.scope_id,
        "scope_kind": scope.scope_kind,
        "scope_title": scope.scope_title,
        "scope_position": scope.scope_position,
        "scope_part": scope.part,
        "scope_part_count": scope.part_count,
        "input_text_checksum": scope.input_text_checksum,
        "scope_part_checksums": scope.scope_part_checksums,
        "header_version": header_version(scope.course),
        "text_extraction_key": reading.text_extraction_key,
        "text_input_digest": reading.text_input_digest,
        "context_input_digest": item.digest,
        "source_count": len(scope.sources),
        "truncated_source_count": sum(source.truncated for source in scope.sources),
        "sources_json": json.dumps(
            [
                {
                    "source_id": source.source_id,
                    "source_ref": source.source_ref,
                    "included_chars": len(source.text),
                    "total_chars": source.total_chars,
                }
                for source in scope.sources
            ]
        ),
        "summary": reading.summary,
        "notes_json": json.dumps(notes),
        "stop_reason": stop_reason,
        **tokens,
        "extracted_at": datetime.now(tz=UTC),
    }
    return (
        extraction,
        claims,
        [{column: row[column] for column in EVIDENCE_SCHEMA} for row in evidence],
    )


def _trace_metadata(
    scope: Scope, stage: _Stage[Any], key: str, effort: str
) -> dict[str, Any]:
    return {
        "stage": stage.name,
        "key": key,
        "prompt_version": stage.prompt_version,
        "effort": effort,
        "platform": scope.course.platform,
        "courserun_readable_id": scope.course.courserun_readable_id,
        "scope_key": scope.scope_key,
    }


# the SDK does not wrap a connection that drops mid-stream in an APIError
_CALL_ERRORS = (ExtractionError, APIError, httpx2.TransportError)


def _read_texts(
    pool: ThreadPoolExecutor,
    to_read: Mapping[ReadingKey, Scope],
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    effort: Effort,
) -> tuple[dict[ReadingKey, TextReading], list[dict[str, Any]]]:
    """Call the text stage once per distinct reading key."""
    futures: dict[Future[_ModelResult[ScopeExtraction]], ReadingKey] = {
        pool.submit(
            call_model,
            client,
            model_id,
            effort,
            text_message(scope),
            stage=TEXT_STAGE,
            trace_metadata=_trace_metadata(scope, TEXT_STAGE, key[2], effort),
        ): key
        for key, scope in to_read.items()
    }
    readings: dict[ReadingKey, TextReading] = {}
    rows = []
    for future in as_completed(futures):
        key = futures[future]
        scope = to_read[key]
        try:
            result = future.result()
        except _CALL_ERRORS:
            log.exception(
                "Text stage failed for %s %s",
                scope.course.courserun_readable_id,
                scope.scope_key,
            )
            continue
        readings[key], row = _text_reading(scope, key[2], model_id, effort, result)
        rows.append(row)
    return readings, rows


def _place_in_context(
    pool: ThreadPoolExecutor,
    contexts: list[_ContextInput],
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    effort: Effort,
) -> list[_Rows]:
    """Call the context stage for each scope part whose text reading has claims."""
    placed = [
        _context_rows(
            context,
            model_id,
            effort,
            None,
            NO_CLAIMS_STOP_REASON,
            dict.fromkeys(_TOKEN_SCHEMA, 0),
        )
        for context in contexts
        if not context.reading.claims
    ]
    futures: dict[Future[_ModelResult[ContextJudgment]], _ContextInput] = {
        pool.submit(
            call_model,
            client,
            model_id,
            effort,
            context.message,
            stage=CONTEXT_STAGE,
            trace_metadata=_trace_metadata(
                context.scope, CONTEXT_STAGE, context.key, effort
            ),
            check=partial(check_revisions, context.reading),
        ): context
        for context in contexts
        if context.reading.claims
    }
    for future in as_completed(futures):
        context = futures[future]
        try:
            result = future.result()
        except _CALL_ERRORS:
            log.exception(
                "Context stage failed for %s %s",
                context.scope.course.courserun_readable_id,
                context.scope.scope_key,
            )
            continue
        placed.append(
            _context_rows(
                context,
                model_id,
                effort,
                result.output,
                result.stop_reason,
                result.tokens,
            )
        )
        log.info(
            "Extracted %s %r: %d claims, %d revised",
            context.scope.course.courserun_readable_id,
            context.scope.scope_title,
            len(context.reading.claims),
            len(result.output.revisions),
        )
    return placed


def extract_scopes(  # noqa: PLR0913
    scopes: Iterable[Scope],
    client: Anthropic | AnthropicBedrock,
    *,
    model_id: str,
    effort: Effort = DEFAULT_EFFORT,
    existing_keys: set[str] | frozenset[str] = frozenset(),
    readings: Mapping[ReadingKey, TextReading] | None = None,
    max_workers: int = 8,
) -> ExtractionTables:
    """Extract every scope part whose extraction key is not already stored.

    A scope part with a stored reading of its text in its run skips the text stage.
    The text stage runs once per distinct text in a run. A scope part whose text
    stage fails gets no extraction; one whose context stage fails keeps its new
    text reading. Either is logged and left out, so the next run retries only it.

    :param scopes: the scope parts to extract
    :param client: an Anthropic or AnthropicBedrock client (LLMClientFactory)
    :param model_id: the model id the client expects
    :param effort: output_config.effort for every call
    :param existing_keys: extraction keys already stored, which are skipped
    :param readings: stored text readings (stored_readings)
    :param max_workers: concurrent model calls
    :returns: the new rows
    :rtype: ExtractionTables
    """
    plan = plan_scopes(
        scopes, model_id=model_id, effort=effort, readings=readings or {}
    )
    pending = [item for item in plan if item.extraction_key not in existing_keys]
    to_read: dict[ReadingKey, Scope] = {}
    for item in pending:
        if item.reading is None:
            to_read.setdefault(
                _reading_key(item.scope, item.text_extraction_key), item.scope
            )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        new_readings, text_rows = _read_texts(pool, to_read, client, model_id, effort)
        contexts = []
        for item in pending:
            reading = item.reading or new_readings.get(
                _reading_key(item.scope, item.text_extraction_key)
            )
            if reading is not None:
                message, digest, key = _context(item.scope, model_id, effort, reading)
                contexts.append(
                    _ContextInput(item.scope, reading, key, digest, message)
                )
        placed = _place_in_context(pool, contexts, client, model_id, effort)
    return ExtractionTables(
        text_extractions=pl.DataFrame(text_rows, schema=TEXT_EXTRACTION_SCHEMA),
        extractions=pl.DataFrame(
            [rows[0] for rows in placed], schema=EXTRACTION_SCHEMA
        ),
        claims=pl.DataFrame(
            [claim for rows in placed for claim in rows[1]], schema=CLAIM_SCHEMA
        ),
        evidence=pl.DataFrame(
            [item for rows in placed for item in rows[2]], schema=EVIDENCE_SCHEMA
        ),
    )
