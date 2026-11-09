"""The system prompts and per-scope messages for the two courseware extraction stages.

The text stage reads only a scope part's own sources. The context stage reads the
course header and the text stage's claims, never the sources.
"""

from collections.abc import Mapping, Sequence
from typing import Any

from ml.lib.courseware.scopes import Scope

TEXT_SYSTEM_PROMPT = """\
You analyze one scope of an MIT course and record, with evidence, what it teaches \
and what it expects of learners. A scope is a subsection of the course (a lecture, \
recitation or assignment), the course context (about page and course-wide pages), \
or the course's documents (slides and handouts). You get the scope's kind and its \
sources, and nothing about the course they belong to: not its title, outline or \
description. The same text can appear in more than one course, so record only what \
the text itself supports. Each source sits under a \
"=== SOURCE Sxx | kind=... | title=... ===" header. Lecture videos appear only as \
transcripts. Problems appear as their question text; answers and solutions were \
removed before you see them.

What to record:

concepts: what the scope deals with, as canonical labels another course would \
also use (e.g. "logistic regression", "confusion matrix", "silhouette score"). Aim \
for the granularity of an encyclopedia article, usually 5 to 25 per scope. Write \
labels as singular, lowercase nouns except for proper names ("decision tree", \
"logarithm", "pandas", "Gini impurity"), and give the full name when the material \
also uses an abbreviation ("area under the ROC curve (AUC)"). Set \
teaching_status from what the material does with the concept: taught when it \
defines, explains, derives or demonstrates it; reviewed when it explains it again \
and the text itself says learners have seen it ("recall that", "as we saw"); used \
when it applies it without explaining it; mentioned when it only names it. Record \
used and mentioned concepts only when they tell a reader what the scope depends on.

skills: what a learner can do after the scope, phrased as an action, from what the \
material demonstrates or has learners practice.

assumed_knowledge: what the material relies on without explaining it here (e.g. \
reading a regression table, matrix multiplication, R data frames). Report it; do \
not decide whether another part of the course teaches it. Name the knowledge \
itself with a canonical label ("decision tree"), never a pointer into the course \
("concepts from Lecture 3"), and do not list datasets.

assessment_expectations: what the problems ask learners to do, with the concept \
labels each requires. Only from problem sources.

learning_outcomes: only outcomes the material states as goals or objectives, with \
the Bloom process and knowledge type the statement implies.

declared_prerequisites and audience: only what the material states. A declared \
prerequisite is something from outside this course (another course, background \
knowledge, a skill); a reference to an earlier lecture, week or module is not one. \
If it says there are no prerequisites, record one with prerequisite_kind \
"explicitly_none". Never infer a prerequisite from difficulty or content.

Evidence: every claim carries one or two evidence items. A passage is 8 to 60 \
consecutive words copied exactly from the source named by source_id, including its \
spelling and punctuation. Do not quote headers. Leave out any claim you cannot \
support with such a passage. Leave a list empty when the scope has nothing for it.

Record the result by calling record_extraction once, with every field filled.
"""

CONTEXT_SYSTEM_PROMPT = """\
You place claims read from one scope of an MIT course into the course they come \
from. Another reader recorded the claims from the scope's own text without seeing \
the course, each with the passages it quoted as evidence. You get the course title, \
its outline (> marks this scope), its description, and those claims, each under an \
id such as C03.

Revise only what the course context settles:

teaching_status, for concept and skill claims only. The reader set it from the \
text: taught (defined, explained, derived or demonstrated), reviewed (explained \
again as something learners have seen), used (applied without being explained) or \
mentioned (named only). Change it when the course shows the text's reading is \
wrong for this course, e.g. taught becomes reviewed when the outline shows an \
earlier scope of this course on the same concept.

sense_hint: when a claim's label names different things in different fields \
(e.g. "regression" in statistics and in software testing, "kernel" in operating \
systems and in machine learning), the field or sense this course means, in a few \
words. Leave it null when the label is unambiguous.

You cannot add, remove or relabel claims, and you cannot add evidence. Record a \
revision only for a listed claim id, and only when it changes something; leave \
every other claim out. Record the result by calling record_context once, with \
every field filled.
"""


def claim_id(ordinal: int) -> str:
    """Return the id a claim is listed under in the context stage's message."""
    return f"C{ordinal:02d}"


def text_message(scope: Scope) -> str:
    """Return the text stage's message: the scope part's kind and sources only.

    Nothing about the course goes in, so the message, and the claims read from it,
    are the same wherever the same text appears.

    :param scope: the scope part to read
    :returns: the message text
    :rtype: str
    """
    part = f"\n{scope.part_note}" if scope.part_note else ""
    return f"SCOPE KIND: {scope.scope_kind}{part}\n\n{scope.sources_block}"


def _claim_lines(claim: Mapping[str, Any], passages: Sequence[str]) -> list[str]:
    qualifiers = ", ".join(
        value
        for value in (claim["concept_kind"], claim["teaching_status"])
        if value is not None
    )
    heading = f"{claim_id(claim['claim_ordinal'])} {claim['claim_kind']}: "
    lines = [heading + claim["label"] + (f" ({qualifiers})" if qualifiers else "")]
    if claim["description"]:
        lines.append(f"  {claim['description']}")
    lines.extend(f'  evidence: "{passage}"' for passage in passages)
    return lines


def context_message(
    scope: Scope,
    claims: Sequence[Mapping[str, Any]],
    evidence: Sequence[Mapping[str, Any]],
) -> str:
    """Return the context stage's message: the course header and the text claims.

    The course header comes from the warehouse (outline from dim_course_content,
    description from the about pages), not from an earlier model call, so every
    scope of a run can be placed independently and in parallel.

    :param scope: the scope part the claims were read from
    :param claims: the text stage's claim rows, in ordinal order
    :param evidence: the text stage's evidence rows for those claims
    :returns: the message text
    :rtype: str
    """
    course = scope.course
    outline = "\n".join(
        f"{'>' if title == scope.scope_title else '-'} {title}"
        for title in course.outline
    )
    part = f"\n{scope.part_note}" if scope.part_note else ""
    passages: dict[int, list[str]] = {}
    for item in evidence:
        passages.setdefault(item["claim_ordinal"], []).append(item["passage"])
    listed = "\n".join(
        line
        for claim in claims
        for line in _claim_lines(claim, passages.get(claim["claim_ordinal"], []))
    )
    return f"""COURSE: {course.title}

OUTLINE (> marks this scope)
{outline}

DESCRIPTION
{course.description or "None available."}

SCOPE: {scope.scope_title} (kind={scope.scope_kind}){part}

CLAIMS READ FROM THIS SCOPE'S TEXT
{listed}"""
