"""Tests for ml.lib.summarize_gate and the feedback-summary gate declaration."""

from pathlib import Path

import pytest
from ml.lib import summarize, summarize_gate
from ol_llm import ManagedPrompt, TextTemplate
from ol_llm.evaluation import judge_model, load_items
from ol_llm.gate import GateContext, GateDeclaration, load_suite
from ol_llm.testing import override_models
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

GATE = Path(__file__).parents[1] / "evaluation" / "feedback-summary.gate.toml"


def test_the_declaration_loads_and_names_this_suite() -> None:
    declaration = GateDeclaration.load(GATE)

    assert declaration.use_case == summarize.SUMMARY_PROMPT_NAME
    assert list(declaration.prompts) == [summarize.SUMMARY_PROMPT_NAME]
    assert load_suite(declaration.suite, GATE.parent) is summarize_gate.suite
    metrics = {t.metric for t in declaration.thresholds}
    assert metrics == {
        "plain_text",
        "withholds_details",
        "at_most_three_sentences",
        "hallucination_metric",
    }


def test_every_golden_item_lists_what_must_not_appear() -> None:
    declaration = GateDeclaration.load(GATE)
    assert declaration.dataset.file is not None

    items = load_items(declaration.dataset.file)

    assert items
    for item in items:
        assert set(item) == {"conversation_text", "must_not_appear"}
        for detail in item["must_not_appear"]:
            assert detail in item["conversation_text"]


@pytest.mark.parametrize(
    ("output", "problems"),
    [
        ("The learner could not download the slides.", []),
        ("The learner's __init__ method failed the autograder.", []),
        ("The snake_case_name and _private names failed.", []),
        ("They hoped to finish by 2024.\nThey asked for a deadline.", []),
        ("## Problem\nThe slides 404.", ["Markdown '##'"]),
        ("- slides are missing", ["Markdown '-'"]),
        ("The **slides** are missing.", ["Markdown '**'"]),
        ("The *slides* are missing.", ["Markdown '*slides*'"]),
        ("The _slides_ are missing.", ["Markdown '_slides_'"]),
        ("Run `pip install` first.", ["Markdown '`'"]),
        ("See [the page](https://x.example).", ["Markdown '[the page]('"]),
        ("> The learner quoted support.", ["Markdown '>'"]),
    ],
)
def test_plain_text(output: str, problems: list[str]) -> None:
    assert summarize_gate.plain_text(output) == problems


@pytest.mark.parametrize(
    ("output", "must_not_appear", "problems"),
    [
        ("A learner hit a video bug.", ["Priya"], []),
        ("Priya reported a video bug.", ["priya", "Raman"], ["kept 'priya'"]),
        (
            "Reach them at a@example.com, (617) 555-0143 or 6175550143.",
            [],
            [
                "contact detail 'a@example.com'",
                "contact detail '(617) 555-0143'",
                "contact detail '6175550143'",
            ],
        ),
        ("The link https://learn.mit.edu/x returns a 404.", [], []),
    ],
)
def test_withholds_details(
    output: str, must_not_appear: list[str], problems: list[str]
) -> None:
    assert summarize_gate.withholds_details(output, must_not_appear) == problems


@pytest.mark.parametrize(
    ("output", "problems"),
    [
        ("One. Two? Three!", []),
        ("A learner asked for captions on version 2.1 of the lab.", []),
        ("The learner asked about the M.I.T. finance course. It stalls. Okay.", []),
        ("They want a course, e.g. a finance one. It is missing. Add it.", []),
        ("A learner asked for captions", []),
        ("One. Two. Three. Four.", ["4 sentences"]),
        ("One. Two. Three. Four", ["4 sentences"]),
        (
            (
                "El vídeo falla. Él pidió ayuda. Él no recibió respuesta. "
                "Él dejó el curso."
            ),
            ["4 sentences"],
        ),
        ("¿Por qué falla? Lo intentó. Pidió ayuda. ¡Nadie respondió!", ["4 sentences"]),
        ('They said "Fix this." It stalls. It fails. They left.', ["4 sentences"]),
    ],
)
def test_at_most_three_sentences(output: str, problems: list[str]) -> None:
    assert summarize_gate.at_most_three_sentences(output) == problems


def _summary(messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
    del messages
    return ModelResponse(parts=[TextPart("The learner could not download slides.")])


def test_the_suite_summarizes_through_the_deployed_call() -> None:
    template = TextTemplate(template=summarize.SUMMARY_PROMPT, version="v4")
    prompt = ManagedPrompt(name="feedback-summary", template=template, source="opik")
    context = GateContext(
        app="dagster-ml",
        use_case="feedback-summary",
        prompts={"feedback-summary": prompt},
        judge=judge_model("dagster-ml", "bedrock:us.anthropic.claude-sonnet-5"),
    )

    with override_models(FunctionModel(_summary)):
        built = summarize_gate.suite(context)
        output = built.task({"conversation_text": "the slides link is a 404"})

    assert output == {
        "input": "the slides link is a 404",
        "output": "The learner could not download slides.",
        "context": ["the slides link is a 404"],
    }
    assert built.experiment_config == {"model": summarize.DEPLOYED_SUMMARY_MODEL}
    assert [m.name for m in built.metrics] == [
        "plain_text",
        "withholds_details",
        "at_most_three_sentences",
        "hallucination_metric",
    ]
