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
from ml.lib.courseware.extract import TEXT_PROMPT_VERSION, extract_scopes
from ml.lib.courseware.prompts import TEXT_SYSTEM_PROMPT
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
    monkeypatch.setattr(
        opik_auth,
        "_published_prompts",
        {TEXT_SYSTEM_PROMPT: PROMPT},
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

    # the reading has no claims, so the text stage's is the only call
    assert span["metadata"] == {
        "stage": "text",
        "key": tables.text_extractions["text_extraction_key"][0],
        "prompt_version": TEXT_PROMPT_VERSION,
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
        template=TEXT_SYSTEM_PROMPT,
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
    monkeypatch.setattr(opik_auth, "_published_prompts", {TEXT_SYSTEM_PROMPT: PROMPT})

    def update_current_span(**_kwargs: Any) -> None:
        msg = "no span is open without Opik"
        raise AssertionError(msg)

    monkeypatch.setattr(
        opik_auth.opik_context, "update_current_span", update_current_span
    )
    client = _client(_response("record_extraction", EXTRACTION, _usage(50, 30, 0)))

    tables = extract_scopes([_scope()], client, model_id=MODEL, effort="low")

    assert tables.extractions.height == 1
