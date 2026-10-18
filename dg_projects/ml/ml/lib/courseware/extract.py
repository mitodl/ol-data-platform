"""Extract claims from courseware scopes and flatten them into warehouse rows.

One model call per distinct scope text. The output is three tables keyed by
extraction_key, a hash of the extractor version and the scope's own input (not the
course header): courseware_claim_extraction (one row per scope part), courseware_claim
and courseware_claim_evidence. A scope part whose text was already extracted in
another run (content_key) gets a copy of that extraction's rows under its own key,
with no model call. dbt builds the dimensional facts, concepts and bridges from them;
nothing here resolves labels across scopes or courses.
"""

import hashlib
import json
import logging
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import polars as pl
from anthropic import Anthropic, AnthropicBedrock, APIError
from ml.lib.courseware.prompts import SYSTEM_PROMPT, scope_message
from ml.lib.courseware.schemas import ScopeExtraction
from ml.lib.courseware.scopes import Course, Scope
from ml.lib.courseware.verify import verify_passage
from pydantic import BaseModel, ValidationError

log = logging.getLogger(__name__)

# Bump when scope_message's layout changes: the extraction key does not hash the
# message, only the scope's own inputs (KEY_COLUMNS).
EXTRACTOR_ID = "courseware_claims_v1"
DEFAULT_MODEL = "claude-haiku-5-5"
BEDROCK_DEFAULT_MODEL = "us.anthropic.claude-haiku-5-5"
Effort = Literal["low", "medium", "high", "xhigh", "max"]
DEFAULT_EFFORT: Effort = "medium"
# Streaming, so the SDK's HTTP timeout does not bound long outputs.
MAX_OUTPUT_TOKENS = 64_000
# A response that fails validation (e.g. cut off at max_tokens) is requested again.
ATTEMPTS = 2
OUTPUT_SCHEMA = ScopeExtraction.model_json_schema()
EXTRACTION_TOOL = {
    "name": "record_extraction",
    "description": "Record the complete extraction for this scope. Call it once.",
    "input_schema": OUTPUT_SCHEMA,
}


def _digest(*parts: Any) -> str:
    return hashlib.sha256(
        json.dumps(parts, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


PROMPT_VERSION = _digest(SYSTEM_PROMPT)[:12]
SCHEMA_VERSION = _digest(OUTPUT_SCHEMA)[:12]
# What an extraction key hashes: the extractor version and the scope's own input,
# all stored on the extraction row, so the key of a row extracted without
# key_on_header can be recomputed from the row (one extracted with it also hashes
# header_version, and the row does not record which).
# The course header (title, outline, description) is left out, so editing a
# course's description or renaming one sequential re-extracts only the scopes whose
# own text changed; header_version records it on the row instead.
KEY_COLUMNS = (
    "extractor_id",
    "prompt_version",
    "schema_version",
    "model_id",
    "effort",
    "platform",
    "courserun_readable_id",
    "scope_id",
    "scope_kind",
    "scope_title",
    "scope_part",
    "scope_part_count",
    "input_text_checksum",
)

# What a model call reads besides the course header: the extractor version and the
# scope part's title, kind, split and source text. Unlike KEY_COLUMNS it leaves out
# the run and the scope id, so a course copied into another run (B2B copies share
# text under new block ids) has the same content key, and its scopes reuse the
# extraction instead of being sent again. Source refs are not in the text the model
# reads (sources are numbered S01, S02, ...), so evidence maps onto the copy's own
# blocks by source_id.
CONTENT_KEY_COLUMNS = (
    "extractor_id",
    "prompt_version",
    "schema_version",
    "model_id",
    "effort",
    "scope_kind",
    "scope_title",
    "scope_part",
    "scope_part_count",
    "input_text_checksum",
)
# stop_reason of an extraction copied from another with the same content key
REUSED_STOP_REASON = "reused"

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
    "header_version": pl.String,
    "source_count": pl.Int64,
    "truncated_source_count": pl.Int64,
    "sources_json": pl.String,
    "summary": pl.String,
    "notes_json": pl.String,
    "stop_reason": pl.String,
    "input_tokens": pl.Int64,
    "output_tokens": pl.Int64,
    "cache_read_input_tokens": pl.Int64,
    "cache_creation_input_tokens": pl.Int64,
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

# One row per scope part as the extractor last read it. dbt picks each scope's
# current extraction by matching input_text_checksum and the split, which only
# Python computes (the sources block and the split by input budget), and which a
# skipped scope otherwise leaves no trace of. header_version is the course header
# as read, so dbt can flag a current extraction made under an older header (the key
# leaves the header out, so a header edit alone re-extracts nothing).
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
    """The model gave no valid extraction for a scope."""


@dataclass(frozen=True)
class ExtractionTables:
    extractions: pl.DataFrame
    claims: pl.DataFrame
    evidence: pl.DataFrame


def header_version(course: Course) -> str:
    """Hash the course header that every scope's message carries.

    :param course: the scope's course
    :returns: a 12-character version of the title, outline and description
    :rtype: str
    """
    return _digest(course.title, course.outline, course.description)[:12]


def _key_fields(scope: Scope, model_id: str, effort: str) -> dict[str, Any]:
    return {
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
        "scope_part": scope.part,
        "scope_part_count": scope.part_count,
        "input_text_checksum": scope.input_text_checksum,
        "header_version": header_version(scope.course),
    }


def stored_extraction_key(
    row: Mapping[str, Any], *, key_on_header: bool = False
) -> str:
    """Compute an extraction's key from its courseware_claim_extraction columns.

    :param row: a stored extraction row, or the fields of a new one
    :param key_on_header: also hash header_version (see extraction_key)
    :returns: the extraction key
    :rtype: str
    """
    columns = (*KEY_COLUMNS, "header_version") if key_on_header else KEY_COLUMNS
    return _digest(*(row[column] for column in columns))


def extraction_key(
    scope: Scope, model_id: str, effort: str, *, key_on_header: bool = False
) -> str:
    """Key one call by the extractor version and the scope's own input.

    A change to the prompt, the schema, the model, the effort, or the scope's title,
    split or source text gives a new key, so a new extraction is added next to the
    old one rather than replacing it. A change to the course header does not.

    :param scope: the scope part
    :param model_id: the model id the client expects
    :param effort: output_config.effort
    :param key_on_header: also hash the header version, to re-extract on purpose the
        scopes whose header changed since they were extracted (see covered_keys)
    :returns: the extraction key
    :rtype: str
    """
    return stored_extraction_key(
        _key_fields(scope, model_id, effort), key_on_header=key_on_header
    )


def covered_keys(stored: pl.DataFrame) -> set[str]:
    """Return every key the stored extractions already answer, under either scheme.

    A stored row's key says nothing of which scheme made it, so each row covers its
    header-free key and, when it records header_version, its header-keyed one too.
    A run under either scheme then skips a scope that the other already extracted
    with the same text (and, for key_on_header, the same header). A row from before
    header_version was recorded covers only its header-free key.

    :param stored: courseware_claim_extraction rows, with header_version
    :returns: the keys to pass to extract_scopes as existing_keys
    :rtype: set[str]
    """
    keys = set(stored["extraction_key"])
    for row in stored.iter_rows(named=True):
        keys.add(stored_extraction_key(row))
        if row["header_version"] is not None:
            keys.add(stored_extraction_key(row, key_on_header=True))
    return keys


def stored_content_key(row: Mapping[str, Any], *, key_on_header: bool = False) -> str:
    """Compute an extraction's content key from its courseware_claim_extraction columns.

    :param row: a stored extraction row, or the fields of a new one
    :param key_on_header: also hash header_version, so a run that re-extracts
        changed headers reuses only an extraction made under the same header
    :returns: the content key
    :rtype: str
    """
    columns = (
        (*CONTENT_KEY_COLUMNS, "header_version")
        if key_on_header
        else CONTENT_KEY_COLUMNS
    )
    return _digest("content", *(row[column] for column in columns))


def content_key(
    scope: Scope, model_id: str, effort: str, *, key_on_header: bool = False
) -> str:
    """Key what one model call reads, whichever run and scope it is for.

    Two scope parts with the same content key get the same extraction, so only the
    first is sent; the others copy its claims (extract_scopes).

    :param scope: the scope part
    :param model_id: the model id the client expects
    :param effort: output_config.effort
    :param key_on_header: also hash the header version (see extraction_key)
    :returns: the content key
    :rtype: str
    """
    return stored_content_key(
        _key_fields(scope, model_id, effort), key_on_header=key_on_header
    )


def reusable_content_keys(stored: pl.DataFrame) -> set[str]:
    """Return every content key the stored extractions can be copied for.

    Like covered_keys, a row with header_version counts under both schemes.

    :param stored: courseware_claim_extraction rows, with header_version
    :returns: the content keys a new scope part would be copied for
    :rtype: set[str]
    """
    return {
        key for row in stored.iter_rows(named=True) for key in _row_content_keys(row)
    }


def _row_content_keys(row: Mapping[str, Any]) -> list[str]:
    keys = [stored_content_key(row)]
    if row["header_version"] is not None:
        keys.append(stored_content_key(row, key_on_header=True))
    return keys


def scope_inputs(scopes: Iterable[Scope]) -> pl.DataFrame:
    """Record the input of every scope part as it is now, extracted or not.

    Write it for every scope of each course run read, so dbt can tell which stored
    extraction matches the current text (e.g. after a scope's text goes back to an
    earlier version, whose extraction is stored and so is skipped).

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
class _ModelResult:
    extraction: ScopeExtraction
    stop_reason: str
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int
    cache_creation_input_tokens: int


def call_model(
    client: Anthropic | AnthropicBedrock, model_id: str, effort: Effort, message: str
) -> _ModelResult:
    """Ask for one scope's extraction as a record_extraction tool call.

    Neither constrained option is available for this schema: structured outputs
    (and strict tools) reject it as too large a grammar, and Opus 5.5 and Sonnet 5.5
    reject a forced tool_choice. So the tool is offered with tool_choice auto, the
    prompt asks for it, and the input is validated here. The system prompt is the
    cached prefix shared by every call.

    :raises ExtractionError: when no attempt returns a valid extraction
    """
    error: Exception | str | None = None
    for _ in range(ATTEMPTS):
        with client.messages.stream(
            model=model_id,
            max_tokens=MAX_OUTPUT_TOKENS,
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=[EXTRACTION_TOOL],
            tool_choice={"type": "auto"},
            messages=[{"role": "user", "content": message}],
            output_config={"effort": effort},
        ) as stream:
            response = stream.get_final_message()
        if response.stop_reason == "refusal":
            msg = f"refused: {response.stop_details}"
            raise ExtractionError(msg)
        tool_input = next(
            (
                block.input
                for block in response.content
                if block.type == "tool_use" and block.name == EXTRACTION_TOOL["name"]
            ),
            None,
        )
        if tool_input is None:
            error = f"no tool call (stop_reason={response.stop_reason})"
            log.warning("%s, retrying", error)
            continue
        try:
            extraction = ScopeExtraction.model_validate(tool_input)
        except ValidationError as validation_error:
            error = validation_error
            log.warning(
                "Invalid extraction (stop_reason=%s), retrying", response.stop_reason
            )
            continue
        return _ModelResult(
            extraction=extraction,
            stop_reason=str(response.stop_reason),
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
            cache_read_input_tokens=response.usage.cache_read_input_tokens or 0,
            cache_creation_input_tokens=response.usage.cache_creation_input_tokens or 0,
        )
    msg = f"no valid extraction after {ATTEMPTS} attempts: {error}"
    raise ExtractionError(msg)


def _claim_row(key: str, ordinal: int, kind: str, claim: BaseModel) -> dict[str, Any]:
    values = claim.model_dump()
    statement = values.get("statement")
    return {
        "extraction_key": key,
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


def _truncation_notes(scope: Scope) -> list[str]:
    return [
        f"{source.source_id} ({source.source_ref}) was cut to "
        f"{len(source.text):,} of {source.total_chars:,} characters"
        for source in scope.sources
        if source.truncated
    ]


def _extraction_row(  # noqa: PLR0913
    scope: Scope,
    key: str,
    model_id: str,
    effort: str,
    *,
    summary: str,
    model_notes: list[str],
    stop_reason: str,
    tokens: Mapping[str, int],
) -> dict[str, Any]:
    return {
        "extraction_key": key,
        **_key_fields(scope, model_id, effort),
        "scope_position": scope.scope_position,
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
        "summary": summary,
        "notes_json": json.dumps(model_notes + _truncation_notes(scope)),
        "stop_reason": stop_reason,
        **tokens,
        "extracted_at": datetime.now(tz=UTC),
    }


_TOKEN_COLUMNS = (
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
)


def _rows(
    scope: Scope, key: str, model_id: str, effort: str, result: _ModelResult
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    sources = {source.source_id: source for source in scope.sources}
    claims, evidence = [], []
    ordinal = 0
    for field_name, kind in _CLAIM_LISTS.items():
        for claim in getattr(result.extraction, field_name):
            ordinal += 1
            claims.append(_claim_row(key, ordinal, kind, claim))
            for evidence_ordinal, item in enumerate(claim.evidence, 1):
                source = sources.get(item.source_id)
                match = verify_passage(item.passage, source.text if source else None)
                evidence.append(
                    {
                        "extraction_key": key,
                        "claim_ordinal": ordinal,
                        "evidence_ordinal": evidence_ordinal,
                        "source_id": item.source_id,
                        "source_ref": source.source_ref if source else None,
                        "passage": item.passage,
                        "verification_status": match.status,
                        "char_start": match.char_start,
                        "char_end": match.char_end,
                    }
                )
    extraction = _extraction_row(
        scope,
        key,
        model_id,
        effort,
        summary=result.extraction.summary,
        model_notes=list(result.extraction.notes),
        stop_reason=result.stop_reason,
        tokens={column: getattr(result, column) for column in _TOKEN_COLUMNS},
    )
    return extraction, claims, evidence


@dataclass(frozen=True)
class _Extracted:
    """One extraction's rows, to copy onto other scope parts with its content key."""

    extraction: Mapping[str, Any]
    claims: list[dict[str, Any]]
    evidence: list[dict[str, Any]]


def _reused_rows(
    scope: Scope, key: str, model_id: str, effort: str, original: _Extracted
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Copy an extraction of the same text onto another scope part, with no call.

    The text, and so each passage's verification and offsets, is identical; only the
    block each source id names differs, so evidence takes this scope's source_ref.
    """
    sources = {source.source_id: source for source in scope.sources}
    notes = json.loads(original.extraction["notes_json"])
    # the model's notes come first, then one note per truncated source
    model_notes = notes[: len(notes) - original.extraction["truncated_source_count"]]
    extraction = {
        **_extraction_row(
            scope,
            key,
            model_id,
            effort,
            summary=original.extraction["summary"],
            model_notes=model_notes,
            stop_reason=REUSED_STOP_REASON,
            tokens=dict.fromkeys(_TOKEN_COLUMNS, 0),
        ),
        # the header the claims were made under, not this scope's: covered_keys
        # hashes it, so a key_on_header run re-extracts a copy whose header differs
        "header_version": original.extraction["header_version"],
    }
    claims = [{**claim, "extraction_key": key} for claim in original.claims]
    evidence = [
        {
            **item,
            "extraction_key": key,
            "source_ref": (
                sources[item["source_id"]].source_ref
                if item["source_id"] in sources
                else None
            ),
        }
        for item in original.evidence
    ]
    return extraction, claims, evidence


def _reusable_extractions(stored: ExtractionTables | None) -> dict[str, _Extracted]:
    """Index stored extractions by content key, under both key schemes."""
    if stored is None or stored.extractions.is_empty():
        return {}
    # one extraction per content key is enough: copies of it hold the same claims
    picked: dict[str, Mapping[str, Any]] = {}
    for row in stored.extractions.sort("extracted_at").iter_rows(named=True):
        for key in _row_content_keys(row):
            picked.setdefault(key, row)
    keys = {row["extraction_key"] for row in picked.values()}
    claims: dict[str, list[dict[str, Any]]] = {key: [] for key in keys}
    for claim in stored.claims.filter(pl.col("extraction_key").is_in(keys)).iter_rows(
        named=True
    ):
        claims[claim["extraction_key"]].append(claim)
    evidence: dict[str, list[dict[str, Any]]] = {key: [] for key in keys}
    for item in stored.evidence.filter(pl.col("extraction_key").is_in(keys)).iter_rows(
        named=True
    ):
        evidence[item["extraction_key"]].append(item)
    return {
        content: _Extracted(
            extraction=row,
            claims=claims[row["extraction_key"]],
            evidence=evidence[row["extraction_key"]],
        )
        for content, row in picked.items()
    }


def extract_scopes(  # noqa: PLR0913
    scopes: Iterable[Scope],
    client: Anthropic | AnthropicBedrock,
    *,
    model_id: str,
    effort: Effort = DEFAULT_EFFORT,
    existing_keys: set[str] | frozenset[str] = frozenset(),
    reusable: ExtractionTables | None = None,
    max_workers: int = 8,
    key_on_header: bool = False,
) -> ExtractionTables:
    """Extract every scope whose key is not already stored, once per distinct text.

    Scope parts with the same content key (e.g. a course and its B2B copies) share
    one model call: the first is extracted, or a stored extraction from reusable is
    taken, and the others get copies of its rows under their own extraction keys. A
    scope that fails is logged and left out, along with the parts that would have
    copied it, so the next run retries only them.

    :param scopes: the scopes to extract
    :param client: an Anthropic or AnthropicBedrock client (LLMClientFactory)
    :param model_id: the model id the client expects
    :param effort: output_config.effort for every call
    :param existing_keys: keys already stored, which are skipped (covered_keys)
    :param reusable: stored extractions with their claims and evidence, to copy for
        a scope part with the same content key; it needs only the rows whose content
        keys the pending scopes have
    :param max_workers: concurrent model calls
    :param key_on_header: hash the header version into the keys (extraction_key)
    :returns: the new rows, copies included
    :rtype: ExtractionTables
    """
    groups: dict[str, list[tuple[Scope, str]]] = {}
    for scope in scopes:
        key = extraction_key(scope, model_id, effort, key_on_header=key_on_header)
        if key in existing_keys:
            continue
        groups.setdefault(
            content_key(scope, model_id, effort, key_on_header=key_on_header), []
        ).append((scope, key))
    extracted = _reusable_extractions(reusable)
    extractions, claims, evidence = [], [], []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                call_model, client, model_id, effort, scope_message(members[0][0])
            ): content
            for content, members in groups.items()
            if content not in extracted
        }
        for future in as_completed(futures):
            content = futures[future]
            scope, key = groups[content][0]
            try:
                result = future.result()
            except (ExtractionError, APIError):
                log.exception(
                    "Extraction failed for %s %s",
                    scope.course.courserun_readable_id,
                    scope.scope_key,
                )
                continue
            extraction, scope_claims, scope_evidence = _rows(
                scope, key, model_id, effort, result
            )
            extracted[content] = _Extracted(extraction, scope_claims, scope_evidence)
            extractions.append(extraction)
            claims.extend(scope_claims)
            evidence.extend(scope_evidence)
            log.info(
                "Extracted %s %r: %d claims, %d in / %d out tokens",
                scope.course.courserun_readable_id,
                scope.scope_title,
                len(scope_claims),
                result.input_tokens,
                result.output_tokens,
            )
    called = {row["extraction_key"] for row in extractions}
    reused = 0
    for content, members in groups.items():
        if content not in extracted:
            continue
        for scope, key in members:
            if key in called:
                continue
            extraction, scope_claims, scope_evidence = _reused_rows(
                scope, key, model_id, effort, extracted[content]
            )
            extractions.append(extraction)
            claims.extend(scope_claims)
            evidence.extend(scope_evidence)
            reused += 1
    log.info(
        "%d scope parts extracted, %d copied from an extraction of the same text",
        len(called),
        reused,
    )
    return ExtractionTables(
        extractions=pl.DataFrame(extractions, schema=EXTRACTION_SCHEMA),
        claims=pl.DataFrame(claims, schema=CLAIM_SCHEMA),
        evidence=pl.DataFrame(evidence, schema=EVIDENCE_SCHEMA),
    )
