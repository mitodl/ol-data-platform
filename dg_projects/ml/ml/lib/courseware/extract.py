"""Extract claims from courseware scopes and flatten them into warehouse rows.

One model call per scope part. The output is three tables keyed by extraction_key:
courseware_claim_extraction (one row per call), courseware_claim and
courseware_claim_evidence. dbt builds the dimensional facts, concepts and bridges
from them; nothing here resolves labels across scopes or courses.
"""

import hashlib
import json
import logging
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import httpx2
import polars as pl
from anthropic import Anthropic, AnthropicBedrock, APIError
from ml.lib.courseware.prompts import SYSTEM_PROMPT, scope_message
from ml.lib.courseware.schemas import ScopeExtraction
from ml.lib.courseware.scopes import Scope
from ml.lib.courseware.tracing import TRACE_TAGS, attach_call
from ml.lib.courseware.verify import verify_passage
from ml.resources.opik_auth import traced
from pydantic import BaseModel, ValidationError

log = logging.getLogger(__name__)

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


def extraction_key(scope: Scope, model_id: str, effort: str) -> str:
    """Key one call by everything that determines its output.

    A change to the prompt, the schema, the model, the effort, or any text the call
    reads (outline and description included) gives a new key, so a new extraction
    is added next to the old one rather than replacing it.
    """
    return _digest(
        EXTRACTOR_ID,
        PROMPT_VERSION,
        SCHEMA_VERSION,
        model_id,
        effort,
        scope.course.platform,
        scope.course.courserun_readable_id,
        scope.scope_key,
        hashlib.sha256(scope_message(scope).encode()).hexdigest(),
    )


@dataclass(frozen=True)
class _ModelResult:
    extraction: ScopeExtraction
    stop_reason: str
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int
    cache_creation_input_tokens: int


@traced(
    "courseware_extract_scope",
    tags=TRACE_TAGS,
    ignore_arguments=["client", "trace_metadata"],
)
def call_model(
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    effort: Effort,
    message: str,
    *,
    trace_metadata: dict[str, Any],
) -> _ModelResult:
    """Ask for one scope's extraction as a record_extraction tool call.

    Neither constrained option is available for this schema: structured outputs
    (and strict tools) reject it as too large a grammar, and Opus 5.5 and Sonnet 5.5
    reject a forced tool_choice. So the tool is offered with tool_choice auto, the
    prompt asks for it, and the input is validated here. The system prompt is the
    cached prefix shared by every call.

    :param trace_metadata: what identifies the call on its Opik span
        (extraction_key and scope)
    :raises ExtractionError: when no attempt returns a valid extraction
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
        usages.append(response.usage)
        attach_call(
            client,
            model_id,
            usages,
            template=SYSTEM_PROMPT,
            metadata=trace_metadata,
        )
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
    notes = list(result.extraction.notes) + [
        f"{source.source_id} ({source.source_ref}) was cut to "
        f"{len(source.text):,} of {source.total_chars:,} characters"
        for source in scope.sources
        if source.truncated
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
        "summary": result.extraction.summary,
        "notes_json": json.dumps(notes),
        "stop_reason": result.stop_reason,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "cache_read_input_tokens": result.cache_read_input_tokens,
        "cache_creation_input_tokens": result.cache_creation_input_tokens,
        "extracted_at": datetime.now(tz=UTC),
    }
    return extraction, claims, evidence


def extract_scopes(  # noqa: PLR0913
    scopes: Iterable[Scope],
    client: Anthropic | AnthropicBedrock,
    *,
    model_id: str,
    effort: Effort = DEFAULT_EFFORT,
    existing_keys: set[str] | frozenset[str] = frozenset(),
    max_workers: int = 8,
) -> ExtractionTables:
    """Extract every scope whose key is not already stored.

    A scope that fails is logged and left out, so the next run retries only it.

    :param scopes: the scopes to extract
    :param client: an Anthropic or AnthropicBedrock client (LLMClientFactory)
    :param model_id: the model id the client expects
    :param effort: output_config.effort for every call
    :param existing_keys: extraction keys already stored, which are skipped
    :param max_workers: concurrent model calls
    :returns: the new rows
    :rtype: ExtractionTables
    """
    pending = [
        (scope, key)
        for scope in scopes
        if (key := extraction_key(scope, model_id, effort)) not in existing_keys
    ]
    extractions, claims, evidence = [], [], []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                call_model,
                client,
                model_id,
                effort,
                scope_message(scope),
                trace_metadata={
                    "extraction_key": key,
                    "prompt_version": PROMPT_VERSION,
                    "effort": effort,
                    "platform": scope.course.platform,
                    "courserun_readable_id": scope.course.courserun_readable_id,
                    "scope_key": scope.scope_key,
                },
            ): (scope, key)
            for scope, key in pending
        }
        for future in as_completed(futures):
            scope, key = futures[future]
            try:
                result = future.result()
            # the SDK does not wrap a connection that drops mid-stream in an APIError
            except (ExtractionError, APIError, httpx2.TransportError):
                log.exception(
                    "Extraction failed for %s %s",
                    scope.course.courserun_readable_id,
                    scope.scope_key,
                )
                continue
            extraction, scope_claims, scope_evidence = _rows(
                scope, key, model_id, effort, result
            )
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
    return ExtractionTables(
        extractions=pl.DataFrame(extractions, schema=EXTRACTION_SCHEMA),
        claims=pl.DataFrame(claims, schema=CLAIM_SCHEMA),
        evidence=pl.DataFrame(evidence, schema=EVIDENCE_SCHEMA),
    )
