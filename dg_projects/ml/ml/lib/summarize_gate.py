"""The feedback-summary evaluation gate's suite.

``ol-llm-eval gate evaluation/feedback-summary.gate.toml`` builds it to score the
pinned prompt version against the golden items in
evaluation/feedback_summary_golden.json. The items are synthetic: this
repository is public, so no learner's text goes in them. See
https://github.com/mitodl/ol-llm/blob/main/docs/evaluation-gate.md.
"""

import re
from typing import Any

from ml.lib.summarize import (
    DEPLOYED_SUMMARY_MODEL,
    SUMMARY_PROMPT_NAME,
    Summarizer,
    summary_use_case,
)
from ol_llm.evaluation import ValidatorMetric
from ol_llm.gate import GateContext, Suite
from opik.evaluation.metrics import Hallucination

# Markdown the prompt rules out: a heading, list item, quote, or table row at a
# line start, emphasis, code, or a link. A bare __ and an underscore inside a word
# are left alone, since a summary of a Python course's feedback can name __init__
# or snake_case names.
_MARKDOWN = re.compile(
    r"^\s*(?:#{1,6}\s|[-*+]\s|\d{1,2}[.)]\s|>|\|)"
    r"|\*\*|(?<![\w*])\*[^*\s][^*\n]*\*(?!\w)"
    r"|(?<!\w)_[^_\s][^_\n]*_(?!\w)|`|\[[^\]\n]+\]\(",
    re.MULTILINE,
)
# An email address or a North American phone number. URLs are allowed: the
# redaction step keeps them (ml.lib.redact), and a broken link is feedback.
_CONTACT = re.compile(
    r"[\w.+-]+@[\w-]+\.[\w.]+"
    r"|(?:\(\d{3}\)\s?|\b\d{3}[-.\s])\d{3}[-.\s]\d{4}\b|\b\d{10}\b"
)
# The prompt asks for 1-3 sentences. A sentence ends at a mark, optionally inside
# a closing quote or bracket, before a capital (in any script, so Spanish "Él"
# counts), a digit, or the end of the text, so "e.g. a finance course" and "M.I.T.
# finance" do not end one. Text after the last end is a sentence without its mark.
MAX_SENTENCES = 3
_SENTENCE_BREAK = re.compile(
    r"(?P<end>[.!?]+[\"'\u201d\u2019)\]]*)"
    r"(?:\s+[\"'\u201c(\u00bf\u00a1]?(?P<next>.)|\s*$)"
)


def plain_text(output: str) -> list[str]:
    """Return the Markdown found in a summary."""
    found = (f"Markdown {m.group(0).strip()!r}" for m in _MARKDOWN.finditer(output))
    return list(dict.fromkeys(found))


def withholds_details(output: str, must_not_appear: list[str]) -> list[str]:
    """Return what the summary kept that the prompt rules out.

    :param output: the summary.
    :param must_not_appear: the item's names, contact details, and course titles,
        which the prompt says to leave out or refer to by subject.
    :returns: each one kept, and any email address or phone number.
    :rtype: list[str]
    """
    kept = [d for d in must_not_appear if d.casefold() in output.casefold()]
    return [f"kept {d!r}" for d in kept] + [
        f"contact detail {m.group(0)!r}" for m in _CONTACT.finditer(output)
    ]


def at_most_three_sentences(output: str) -> list[str]:
    """Return a problem when the summary runs past three sentences."""
    text = output.strip()
    count, last_end = 0, 0
    for match in _SENTENCE_BREAK.finditer(text):
        following = match.group("next")
        if following is None or following.isupper() or following.isdigit():
            count += 1
            last_end = match.end("end")
    if text[last_end:].strip():
        count += 1
    return [f"{count} sentences"] if count > MAX_SENTENCES else []


def suite(context: GateContext) -> Suite:
    """Summarize each golden item as the deployed asset does, then score it.

    :param context: the prompt under test and the judge.
    :returns: the suite.
    :rtype: Suite
    """
    summarizer = Summarizer(
        use_case=summary_use_case(DEPLOYED_SUMMARY_MODEL),
        prompt=context.prompts[SUMMARY_PROMPT_NAME],
    )

    def task(item: dict[str, Any]) -> dict[str, Any]:
        text = item["conversation_text"]
        result = summarizer.summarize(
            {"feedback_conversation_pk": None, "conversation_text": text}
        )
        # Without a context, Opik's Hallucination judges the output against
        # general knowledge and ignores the input.
        return {"input": text, "output": result.output, "context": [text]}

    return Suite(
        task=task,
        metrics=[
            ValidatorMetric("plain_text", plain_text),
            ValidatorMetric("withholds_details", withholds_details),
            ValidatorMetric("at_most_three_sentences", at_most_three_sentences),
            Hallucination(model=context.require_judge()),
        ],
        experiment_config={"model": DEPLOYED_SUMMARY_MODEL},
    )
