"""The courseware model calls' Opik spans, with Opik's span update stubbed.

@traced decorates at import, when OPIK_URL_OVERRIDE is unset in tests, so no real
span is opened; these check what each call attaches to its span, and that nothing
is attached (or can fail the call) when Opik is not configured.
"""

from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any

import polars as pl
import pytest
from anthropic import Anthropic, AnthropicBedrock
from ml.lib.courseware import scopes
from ml.lib.courseware.describe import (
    DESCRIPTION_PROMPT,
    describe_concepts,
    description_version,
)
from ml.lib.courseware.extract import PROMPT_VERSION, extract_scopes
from ml.lib.courseware.prompts import SYSTEM_PROMPT
from ml.lib.courseware.resolve import ADJUDICATION_PROMPT, adjudicate
from ml.lib.courseware.tracing import attach_call
from ml.resources import opik_auth

MODEL = "claude-haiku-5-5"
PROMPT = object()


def _usage(input_tokens: int, output_tokens: int, cache_read: int) -> SimpleNamespace:
    return SimpleNamespace(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_read_input_tokens=cache_read,
        cache_creation_input_tokens=0,
    )


def _response(
    tool: str, tool_input: dict[str, Any] | None, usage: SimpleNamespace
) -> SimpleNamespace:
    return SimpleNamespace(
        stop_reason="tool_use" if tool_input else "end_turn",
        stop_details=None,
        content=[SimpleNamespace(type="tool_use", name=tool, input=tool_input)]
        if tool_input
        else [],
        usage=usage,
    )


def _client(*responses: SimpleNamespace) -> Anthropic:
    """Build an Anthropic client whose messages.stream answers with responses."""
    queue = list(responses)
    client = Anthropic(api_key="test")  # pragma: allowlist secret
    client.messages = SimpleNamespace(
        stream=lambda **_: nullcontext(
            SimpleNamespace(get_final_message=lambda: queue.pop(0))
        )
    )
    return client


EXTRACTION = {
    "summary": "About lecture 1.",
    "concepts": [],
    "skills": [],
    "assumed_knowledge": [],
    "assessment_expectations": [],
    "learning_outcomes": [],
    "declared_prerequisites": [],
    "audience": [],
    "notes": [],
}


def _scope() -> scopes.Scope:
    return scopes.build_scopes(
        pl.DataFrame(
            {
                "platform": "mitxonline",
                "courserun_readable_id": "course-v1:X+Y+Z",
                "course_title": "Course",
                "scope_id": "seq-1",
                "scope_kind": "unit",
                "scope_title": "Lecture 1",
                "scope_position": 1,
                "source_position": [1],
                "source_ref": ["block-0"],
                "source_kind": "html",
                "source_title": None,
                "source_text": ["Some lecture text."],
            }
        )
    )[0]


@pytest.fixture
def span(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Configure Opik and record what reaches opik_context.update_current_span.

    Usage is replaced on each update and metadata merged, as Opik's span does.
    """
    monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://opik.example.com")
    monkeypatch.setenv("OPIK_KEYCLOAK_VAULT_MOUNT", "secret-opik")
    monkeypatch.setenv("OPIK_KEYCLOAK_VAULT_PATH", "sdk-client")
    monkeypatch.setattr(
        opik_auth,
        "_published_prompts",
        {
            SYSTEM_PROMPT: PROMPT,
            ADJUDICATION_PROMPT: PROMPT,
            DESCRIPTION_PROMPT: PROMPT,
        },
    )
    recorded: dict[str, Any] = {"metadata": {}}

    def update_current_span(**kwargs: Any) -> None:
        recorded["metadata"].update(kwargs.pop("metadata", {}))
        recorded.update(kwargs)

    monkeypatch.setattr(
        opik_auth.opik_context, "update_current_span", update_current_span
    )
    return recorded


def test_extraction_span_carries_its_key_and_the_usage_of_every_attempt(span):
    # the first response has no tool call, so the scope is requested again
    client = _client(
        _response("record_extraction", None, _usage(100, 20, 900)),
        _response("record_extraction", EXTRACTION, _usage(50, 30, 900)),
    )

    tables = extract_scopes([_scope()], client, model_id=MODEL, effort="low")

    assert span["metadata"] == {
        "extraction_key": tables.extractions["extraction_key"][0],
        "prompt_version": PROMPT_VERSION,
        "effort": "low",
        "platform": "mitxonline",
        "courserun_readable_id": "course-v1:X+Y+Z",
        "scope_key": _scope().scope_key,
        "model_id": MODEL,
        "attempts": 2,
    }
    assert span["usage"] == {
        "input_tokens": 150,
        "output_tokens": 50,
        "cache_read_input_tokens": 1800,
        "cache_creation_input_tokens": 0,
    }
    assert (span["model"], span["provider"]) == (MODEL, "anthropic")
    assert span["prompts"] == [PROMPT]


def test_adjudication_span_carries_the_resolution_run(span):
    rows = {
        key: {"preferred_label": key, "concept_kind": None, "description": None}
        for key in ("auc", "area under the curve")
    }
    verdicts = {
        "queries": [
            {"query_id": 0, "verdicts": [{"candidate_id": 0, "verdict": "exact"}]}
        ]
    }
    client = _client(_response("record_verdicts", verdicts, _usage(40, 10, 0)))

    decisions, _, _ = adjudicate(
        client, MODEL, {"auc": ["area under the curve"]}, rows, run_id="run-1"
    )

    assert decisions == [("auc", "area under the curve", "exact")]
    assert span["metadata"]["resolution_run_id"] == "run-1"
    assert span["metadata"]["batch_ordinal"] == 0
    assert span["metadata"]["query_count"] == 1
    assert span["usage"]["input_tokens"] == 40
    assert span["prompts"] == [PROMPT]


def test_description_span_carries_the_batch_and_description_version(span):
    inputs = pl.DataFrame(
        {
            "concept_id": ["auc"],
            "label": ["AUC"],
            "concept_kind": ["concept"],
            "description": ["Ranks models."],
            "course_title": ["Course A"],
            "context_rank": [1],
        }
    )
    descriptions = {
        "descriptions": [
            {
                "concept_id": 0,
                "short_description": "a ranking measure",
                "description": "A measure of ranking quality.",
                "is_learner_facing": True,
            }
        ]
    }
    client = _client(_response("record_descriptions", descriptions, _usage(30, 15, 0)))

    described = describe_concepts(inputs, client, model_id=MODEL, effort="low")

    assert described.height == 1
    assert span["metadata"]["description_version"] == description_version(MODEL, "low")
    assert span["metadata"]["batch_ordinal"] == 0
    assert span["metadata"]["concept_count"] == 1
    assert span["usage"]["output_tokens"] == 15
    assert span["prompts"] == [PROMPT]


def test_bedrock_usage_is_attached_in_bedrock_keys(span):
    client = AnthropicBedrock(
        aws_region="us-east-1",
        aws_access_key="test",  # pragma: allowlist secret
        aws_secret_key="test",  # pragma: allowlist secret
    )

    attach_call(
        client,
        "us.anthropic.claude-haiku-5-5",
        [_usage(10, 5, 7)],
        template=SYSTEM_PROMPT,
        metadata={},
    )

    assert span["usage"] == {
        "inputTokens": 10,
        "outputTokens": 5,
        "cacheReadInputTokens": 7,
        "cacheWriteInputTokens": 0,
    }
    assert span["provider"] == "bedrock"


def test_nothing_is_attached_when_opik_is_not_configured(monkeypatch):
    monkeypatch.delenv("OPIK_URL_OVERRIDE", raising=False)
    monkeypatch.setattr(opik_auth, "_published_prompts", {SYSTEM_PROMPT: PROMPT})

    def update_current_span(**_kwargs: Any) -> None:
        msg = "no span is open without Opik"
        raise AssertionError(msg)

    monkeypatch.setattr(
        opik_auth.opik_context, "update_current_span", update_current_span
    )
    client = _client(_response("record_extraction", EXTRACTION, _usage(50, 30, 0)))

    tables = extract_scopes([_scope()], client, model_id=MODEL, effort="low")

    assert tables.extractions.height == 1
