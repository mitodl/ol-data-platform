import re
from contextlib import contextmanager
from types import SimpleNamespace

import polars as pl
from ml.lib.courseware.describe import describe_concepts

MODEL = "haiku"


class _Client:
    """Stands in for an Anthropic client: describes every concept in the listing."""

    def __init__(self):
        self.calls = 0
        self.messages = self

    @contextmanager
    def stream(self, **kwargs):
        self.calls += 1
        listing = kwargs["messages"][0]["content"]
        indexes = re.findall(r"(?m)^(\d+) \|", listing)
        tool_input = {
            "descriptions": [
                {
                    "concept_id": int(index),
                    "short_description": f"call {self.calls}",
                    "description": f"call {self.calls}",
                    "is_learner_facing": True,
                }
                for index in indexes
            ]
        }
        response = SimpleNamespace(
            stop_reason="tool_use",
            content=[
                SimpleNamespace(
                    type="tool_use", name="record_descriptions", input=tool_input
                )
            ],
            usage=SimpleNamespace(
                input_tokens=100,
                output_tokens=20,
                cache_read_input_tokens=0,
                cache_creation_input_tokens=0,
            ),
        )
        yield SimpleNamespace(get_final_message=lambda: response)


def _inputs(note: str) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "concept_id": "kernel",
                "context_rank": 1,
                "course_title": "Operating Systems",
                "description": note,
                "label": "kernel",
                "concept_kind": "concept",
            }
        ]
    )


def test_inputs_changed_back_are_described_again():
    client = _Client()
    first = describe_concepts(_inputs("the OS core"), client, model_id=MODEL)
    second = describe_concepts(
        _inputs("schedules processes"), client, model_id=MODEL, existing=first
    )
    stored = pl.concat([first, second])
    # A's row is older than B's, so dim_concept shows B until A is described again
    third = describe_concepts(
        _inputs("the OS core"), client, model_id=MODEL, existing=stored
    )
    assert third.height == 1
    assert client.calls == 3


def test_unchanged_inputs_are_not_described_again():
    client = _Client()
    first = describe_concepts(_inputs("the OS core"), client, model_id=MODEL)
    again = describe_concepts(
        _inputs("the OS core"), client, model_id=MODEL, existing=first
    )
    assert again.is_empty()
    assert client.calls == 1
