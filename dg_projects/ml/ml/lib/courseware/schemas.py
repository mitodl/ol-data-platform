"""The output contract for courseware claim extraction.

The JSON schema the model is constrained to is generated from these models, so the
field descriptions are part of the prompt. Structured outputs accept no length or
numeric constraints, and every field is required (nullable where it can be absent).
"""

import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

TeachingStatus = Literal["taught", "reviewed", "used", "mentioned"]
BloomProcess = Literal[
    "remember", "understand", "apply", "analyze", "evaluate", "create"
]
KnowledgeType = Literal["factual", "conceptual", "procedural", "metacognitive"]


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
        then returns a nested array as the string '[{...}]' instead of the array.
        """
        if not isinstance(data, dict):
            return data
        decoded = dict(data)
        for key, value in data.items():
            if isinstance(value, str) and value.lstrip()[:1] in ("[", "{"):
                try:
                    decoded[key] = json.loads(value)
                except json.JSONDecodeError:
                    continue
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
