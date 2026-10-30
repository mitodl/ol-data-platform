"""The output contract for courseware claim extraction.

The JSON schema the model is constrained to is generated from these models, so the
field descriptions are part of the prompt. Structured outputs accept no length or
numeric constraints, and every field is required (nullable where it can be absent).
"""

import json
from types import UnionType
from typing import Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, model_validator

TeachingStatus = Literal["taught", "reviewed", "used", "mentioned"]
BloomProcess = Literal[
    "remember", "understand", "apply", "analyze", "evaluate", "create"
]
KnowledgeType = Literal["factual", "conceptual", "procedural", "metacognitive"]

_JSON_DECODER = json.JSONDecoder()


def _container_types(annotation: Any) -> tuple[type, ...]:
    """Return the JSON container types (list, dict) a field accepts."""
    origin = get_origin(annotation)
    if origin in (Union, UnionType):
        return tuple(
            kind for arg in get_args(annotation) for kind in _container_types(arg)
        )
    if origin is list:
        return (list,)
    if origin is dict or (
        isinstance(annotation, type) and issubclass(annotation, BaseModel)
    ):
        return (dict,)
    return ()


def _decode_json_container(value: str, expected: tuple[type, ...]) -> Any:
    """Return the list or object a string JSON-encodes, else the string unchanged.

    Closing brackets after the value are dropped: a model that encodes an array as
    a string sometimes closes the enclosing object inside it too ('[{...}]}').

    :param value: the string the model sent
    :param expected: the container types the field accepts
    :returns: the decoded container when it is one of expected, else value
    """
    text = value.strip()
    try:
        decoded, end = _JSON_DECODER.raw_decode(text)
    except json.JSONDecodeError:
        return value
    if text[end:].strip("]} \t\r\n") or not isinstance(decoded, expected):
        return value
    return decoded


class Strict(BaseModel):
    # The schema the model sees forbids extra keys; validation drops any it adds
    # anyway (e.g. a stray "position" on one evidence item) rather than failing the
    # whole scope over it.
    model_config = ConfigDict(
        extra="ignore", json_schema_extra={"additionalProperties": False}
    )

    @model_validator(mode="before")
    @classmethod
    def _decode_json_strings(cls, data: Any) -> Any:
        """Decode a list or object the model sent JSON-encoded as a string.

        Without strict tools the arguments are not constrained, and a model now and
        then returns a nested array as the string '[{...}]' instead of the array,
        or an item of an array as the string '{...}'. Every nested model is a
        Strict, so this covers items as well as the top-level fields.
        """
        if isinstance(data, str):
            data = _decode_json_container(data, (dict,))
        if not isinstance(data, dict):
            return data
        decoded = dict(data)
        for key, value in data.items():
            # a string field keeps its text even when that text parses as JSON
            field = cls.model_fields.get(key)
            if field is None or not isinstance(value, str):
                continue
            expected = _container_types(field.annotation)
            if expected:
                decoded[key] = _decode_json_container(value, expected)
        return decoded


class Evidence(Strict):
    source_id: str = Field(
        description="The source id exactly as its SOURCE header gives it, e.g. S03"
    )
    passage: str = Field(
        description=(
            "8 to 60 consecutive words copied character for character from that "
            "source. Never paraphrase, correct, or join text from different places."
        )
    )


class ConceptClaim(Strict):
    label: str = Field(
        description=(
            "Short canonical name another course would use for the same thing, at "
            "the granularity of an encyclopedia article, e.g. 'logistic regression'"
        )
    )
    description: str = Field(
        description="One or two self-contained sentences on how this scope treats it"
    )
    concept_kind: Literal["concept", "method", "tool", "dataset", "application"]
    teaching_status: TeachingStatus = Field(
        description=(
            "taught: defined, explained, derived or demonstrated here. reviewed: "
            "explained again as something learners have seen. used: applied without "
            "being explained. mentioned: named only."
        )
    )
    evidence: list[Evidence]


class SkillClaim(Strict):
    label: str = Field(
        description="An ability phrased as an action, e.g. 'Fit a k-means model in R'"
    )
    description: str
    teaching_status: TeachingStatus
    evidence: list[Evidence]


class AssumedKnowledgeClaim(Strict):
    label: str = Field(description="Canonical name of what the material relies on")
    description: str
    how_used: str = Field(description="Where the material relies on it unexplained")
    flagged_as_prior: bool = Field(
        description=(
            "True only when the material itself frames it as prior knowledge "
            "('recall that', 'as you know', 'from the previous course')"
        )
    )
    evidence: list[Evidence]


class AssessmentClaim(Strict):
    label: str
    description: str = Field(description="What the item asks the learner to do")
    concept_labels: list[str] = Field(
        description="Labels of the concepts the item requires, as used in concepts"
    )
    bloom_process: BloomProcess
    evidence: list[Evidence]


class LearningOutcomeClaim(Strict):
    statement: str = Field(
        description="The outcome as the material states it, lightly normalized"
    )
    bloom_process: BloomProcess
    knowledge_type: KnowledgeType
    concept_labels: list[str]
    evidence: list[Evidence]


class DeclaredPrerequisiteClaim(Strict):
    label: str
    statement: str = Field(description="What the source says, closely paraphrased")
    prerequisite_kind: Literal[
        "prerequisite", "corequisite", "recommended", "explicitly_none"
    ]
    course_ref: str | None = Field(
        description="A course the statement names, e.g. 'UAI.2' or '18.06', else null"
    )
    evidence: list[Evidence]


class AudienceClaim(Strict):
    statement: str
    evidence: list[Evidence]


class ScopeExtraction(Strict):
    summary: str = Field(
        description="Three to five sentences on what the scope teaches"
    )
    concepts: list[ConceptClaim]
    skills: list[SkillClaim]
    assumed_knowledge: list[AssumedKnowledgeClaim]
    assessment_expectations: list[AssessmentClaim] = Field(
        description="Only from problem sources: what the graded items ask for"
    )
    learning_outcomes: list[LearningOutcomeClaim] = Field(
        description="Only outcomes the material states (learning goals, objectives)"
    )
    declared_prerequisites: list[DeclaredPrerequisiteClaim] = Field(
        description="Only prerequisites the material states. Never infer one."
    )
    audience: list[AudienceClaim] = Field(
        description="Who the material says the course is for, when it says so"
    )
    notes: list[str] = Field(
        description="Material you could not use, gaps, and ambiguities"
    )


class ClaimRevision(Strict):
    claim_id: str = Field(description="The claim's id exactly as listed, e.g. C03")
    teaching_status: TeachingStatus | None = Field(
        description=(
            "The status in this course when the course context changes it; null "
            "keeps the status read from the text. Only for concept and skill claims."
        )
    )
    sense_hint: str | None = Field(
        description=(
            "The field or sense this course means when the label names different "
            "things in different fields, e.g. 'statistics'; else null"
        )
    )


class ContextJudgment(Strict):
    revisions: list[ClaimRevision] = Field(
        description="Only the claims the course context changes; leave out the rest"
    )
    notes: list[str] = Field(
        description="Context you could not use, and claims the course leaves unclear"
    )
