"""Label Studio labeling configs, task import files and export parsing.

One Label Studio project per annotation set. Import the set's tasks JSON (it
carries pre-annotations as `predictions`, which annotators start from when the
project's "use predictions to prelabel" setting is on), paste the set's labeling
config, and export with the JSON format. Pre-annotations carry spans, concept
labels and statuses only; salience and central-explanation roles are always the
annotator's. The project API's `maximum_annotations`
(and `overlap_cohort_percentage`) sets how many annotators see each task: 2 on a
quarter of the claims, subsections and pairs, 3 on every prerequisite pair.

Every task's data carries the sample's item_id, which is how an export is joined
back to the population.
"""

import json
from collections import Counter
from collections.abc import Hashable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.sax.saxutils import quoteattr

import polars as pl

CLAIM_VERDICTS = ("supported", "partially_supported", "unsupported")
STATUS_JUDGMENTS = ("correct", "wrong", "not_applicable")
LABEL_JUDGMENTS = ("good", "too_broad", "too_narrow", "not_a_concept", "not_applicable")
GRANULARITY_ERRORS = ("too_broad", "too_narrow")
GOLD_STATUSES = ("taught", "reviewed", "used", "mentioned", "assumed")
# Whether the subsection is about the concept, independent of its teaching status.
SALIENCE = ("central", "supporting", "incidental")
# Whether the subsection has one line of explanation the rest of it serves.
CENTRAL_EXPLANATION = ("present", "absent")
# A concept's place in that explanation; a concept with neither is left unmarked.
CENTRAL_ROLES = ("objective", "needed")
PAIR_RELATIONS = ("exact", "close", "a_narrower", "a_broader", "related", "distinct")
# Hierarchy and dependency are judged separately: a pair can be both.
UNDECIDED = ("context_dependent", "uncertain")
HIERARCHY_DIRECTIONS = ("a_kind_of_b", "b_kind_of_a", "a_part_of_b", "b_part_of_a")
HIERARCHY_LABELS = (*HIERARCHY_DIRECTIONS, "none", *UNDECIDED)
DEPENDENCY_DIRECTIONS = ("b_before_a", "a_before_b")
DEPENDENCY_LABELS = (*DEPENDENCY_DIRECTIONS, "none", *UNDECIDED)
DEPTHS = ("introductory", "intermediate", "advanced")
MAX_CLUSTER_GROUPS = 8


def _choices(  # noqa: PLR0913
    name: str,
    to_name: str,
    values: Sequence[str],
    hint: str,
    *,
    choice: str = "single",
    required: bool = True,
    per_region: bool = False,
) -> str:
    options = "\n".join(f"    <Choice value={quoteattr(value)}/>" for value in values)
    attributes = f'choice="{choice}" showInline="true"'
    if required:
        attributes += ' required="true"'
    if per_region:
        attributes += ' perRegion="true"'
    return (
        f'  <Header value={quoteattr(hint)} size="5"/>\n'
        f"  <Choices name={quoteattr(name)} toName={quoteattr(to_name)} "
        f"{attributes}>\n{options}\n  </Choices>"
    )


def _shown_for(tag: str, values: Sequence[str], body: str) -> str:
    """Wrap controls in a View shown only while one of these choices is selected."""
    return (
        f'  <View visibleWhen="choice-selected" whenTagName={quoteattr(tag)} '
        f"whenChoiceValue={quoteattr(','.join(values))}>\n{body}\n  </View>"
    )


CLAIMS_CONFIG = f"""<View>
  <Header value="$heading"/>
  <Text name="claim" value="$claim_text"/>
  <Text name="source" value="$source_text" granularity="word"/>
  <Labels name="evidence" toName="source">
    <Label value="evidence" background="#f5c518"/>
  </Labels>
{
    _choices(
        "verdict",
        "claim",
        CLAIM_VERDICTS,
        "Does the source support the claim? Highlight the passage that does.",
    )
}
{
    _choices(
        "status",
        "claim",
        STATUS_JUDGMENTS,
        "Is the teaching status right (not_applicable for other claim kinds)?",
    )
}
{
    _choices(
        "label_quality",
        "claim",
        LABEL_JUDGMENTS,
        "Is the label a concept another course would name the same way "
        "(not_applicable for claim kinds that are not concepts)?",
    )
}
</View>
"""

SUBSECTIONS_CONFIG = f"""<View>
  <Header value="$heading"/>
  <Header value="Mark every concept this material teaches, reviews, uses, mentions or assumes. One span per concept, at the place that best shows its status; name it in the box and give its salience." size="5"/>
  <Labels name="concept" toName="text">
{chr(10).join(f"    <Label value={quoteattr(status)}/>" for status in GOLD_STATUSES)}
  </Labels>
  <View visibleWhen="region-selected" whenTagName="text">
  <TextArea name="concept_label" toName="text" perRegion="true" editable="true"
            maxSubmissions="1" required="true" placeholder="Concept name"/>
{
    _choices(
        "salience",
        "text",
        SALIENCE,
        "Salience: is the subsection about this concept? central: it is what the "
        "subsection is about. supporting: it serves a central concept. incidental: "
        "named or used in passing.",
        per_region=True,
    )
}
{
    _choices(
        "central_role",
        "text",
        CENTRAL_ROLES,
        "Only when the subsection has a central explanation. objective: the "
        "explanation teaches it. needed: following the explanation directly needs "
        "it. Leave unmarked otherwise.",
        required=False,
        per_region=True,
    )
}
  </View>
  <Text name="text" value="$source_text" granularity="word"/>
{
    _choices(
        "central_explanation",
        "text",
        CENTRAL_EXPLANATION,
        "Does the subsection have one central explanation the rest of it serves "
        "(absent: e.g. a problem set over several topics, an overview)?",
    )
}
</View>
"""  # noqa: E501

PAIRS_CONFIG = f"""<View>
  <Header value="A: $label_a"/>
  <Text name="a" value="$text_a"/>
  <Header value="B: $label_b"/>
  <Text name="b" value="$text_b"/>
{
    _choices(
        "relation",
        "a",
        PAIR_RELATIONS,
        "exact: same concept. close: nearly interchangeable. a_narrower: A is a "
        "kind or part of B. a_broader: B is a kind or part of A. related: "
        "connected otherwise. distinct: unrelated.",
    )
}
</View>
"""


def clusters_config(groups: int) -> str:
    """Return the cluster review config with this many group buckets."""
    buckets = "\n".join(
        f'    <Bucket name="group_{index}" title="Concept {index}"/>'
        for index in range(1, groups + 1)
    )
    return f"""<View>
  <Header value="$heading"/>
  <Header value="Put the members that are one concept in the same group. Put another concept in a group when it is that concept too. Leave the rest unsorted." size="5"/>
  <List name="items" value="$items" title="Members and nearby concepts"/>
  <Ranker name="groups" toName="items">
{buckets}
  </Ranker>
</View>
"""  # noqa: E501


_DEPTH_HINT = (
    "At which depths does it hold? introductory: first exposure, intuition and use. "
    "intermediate: a typical undergraduate course. advanced: a formal or graduate "
    "treatment."
)

PREREQUISITES_CONFIG = f"""<View>
  <Header value="A: $label_a"/>
  <Text name="a" value="$text_a"/>
  <Header value="B: $label_b"/>
  <Text name="b" value="$text_b"/>
{
    _choices(
        "hierarchy",
        "a",
        HIERARCHY_LABELS,
        "Hierarchy: is one a kind or a part of the other? context_dependent: it "
        "depends on the sense, discipline or audience (say which below). uncertain: "
        "you cannot tell.",
    )
}
{
    _shown_for(
        "hierarchy",
        HIERARCHY_DIRECTIONS,
        _choices(
            "hierarchy_depth",
            "a",
            DEPTHS,
            _DEPTH_HINT,
            choice="multiple",
            required=False,
        ),
    )
}
{
    _choices(
        "dependency",
        "a",
        DEPENDENCY_LABELS,
        "Dependency, judged apart from hierarchy: b_before_a: a learner needs B to "
        "learn A. a_before_b: needs A to learn B. context_dependent: it depends on "
        "the sense, discipline or audience (say which below). uncertain: you cannot "
        "tell.",
    )
}
{
    _shown_for(
        "dependency",
        DEPENDENCY_DIRECTIONS,
        _choices(
            "dependency_depth",
            "a",
            DEPTHS,
            _DEPTH_HINT,
            choice="multiple",
            required=False,
        ),
    )
}
  <TextArea name="context" toName="a" editable="true" maxSubmissions="1"
            placeholder="For context_dependent or uncertain: what it depends on"/>
</View>
"""


def _region_results(
    regions: list[dict[str, Any]], labels_name: str, to_name: str
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for index, region in enumerate(regions):
        region_id = f"r{index}"
        span = {"start": region["start"], "end": region["end"], "text": region["text"]}
        results.append(
            {
                "id": region_id,
                "from_name": labels_name,
                "to_name": to_name,
                "type": "labels",
                "value": {**span, "labels": [region["label"]]},
            }
        )
        if "concept" in region:
            results.append(
                {
                    "id": region_id,
                    "from_name": "concept_label",
                    "to_name": to_name,
                    "type": "textarea",
                    "value": {**span, "text": [region["concept"]]},
                }
            )
    return results


def claims_tasks(sample: pl.DataFrame) -> list[dict[str, Any]]:
    """Return Label Studio tasks for claim verification, evidence pre-highlighted."""
    return [
        {
            "data": {
                key: row[key]
                for key in (
                    "item_id",
                    "stratum",
                    "heading",
                    "claim_text",
                    "source_text",
                )
            },
            "predictions": [
                {
                    "model_version": "extractor_evidence",
                    "result": _region_results(
                        json.loads(row["regions_json"]), "evidence", "source"
                    ),
                }
            ],
        }
        for row in sample.iter_rows(named=True)
    ]


def subsections_tasks(sample: pl.DataFrame) -> list[dict[str, Any]]:
    """Return tasks for exhaustive annotation; blind tasks carry no predictions."""
    tasks = []
    for row in sample.iter_rows(named=True):
        task: dict[str, Any] = {
            "data": {
                key: row[key]
                for key in ("item_id", "stratum", "blind", "heading", "source_text")
            }
        }
        if not row["blind"]:
            task["predictions"] = [
                {
                    "model_version": "pooled_extractors",
                    "result": _region_results(
                        json.loads(row["regions_json"]), "concept", "text"
                    ),
                }
            ]
        tasks.append(task)
    return tasks


def pairs_tasks(sample: pl.DataFrame) -> list[dict[str, Any]]:
    """Return concept pair tasks; the resolver's verdict is not shown."""
    keys = ("item_id", "stratum", "key_a", "key_b", "label_a", "label_b")
    return [
        {"data": {key: row[key] for key in (*keys, "text_a", "text_b")}}
        for row in sample.iter_rows(named=True)
    ]


def clusters_tasks(sample: pl.DataFrame) -> list[dict[str, Any]]:
    """Return cluster review tasks, members and neighbours as one list."""
    return [
        {
            "data": {
                "item_id": row["item_id"],
                "stratum": row["stratum"],
                "heading": row["heading"],
                "items": json.loads(row["items_json"]),
            }
        }
        for row in sample.iter_rows(named=True)
    ]


def prerequisites_tasks(sample: pl.DataFrame) -> list[dict[str, Any]]:
    """Return prerequisite pair tasks; the inferred edge is not shown."""
    keys = ("item_id", "stratum", "concept_a", "concept_b", "label_a", "label_b")
    return [
        {"data": {key: row[key] for key in (*keys, "text_a", "text_b")}}
        for row in sample.iter_rows(named=True)
    ]


@dataclass(frozen=True)
class Annotation:
    """One annotator's submitted annotation of one task."""

    item_id: str
    annotator: str
    created_at: str
    results: list[dict[str, Any]]

    def choices(self, name: str) -> tuple[str, ...]:
        """Return the choices made in a task-level Choices control, sorted.

        A per-region control's results share their region's id, so they are not
        task-level choices.
        """
        regions = {
            result["id"] for result in self.results if result["type"] == "labels"
        }
        for result in self.results:
            if (
                result["from_name"] == name
                and result["type"] == "choices"
                and result.get("id") not in regions
            ):
                return tuple(sorted(result["value"]["choices"]))
        return ()

    def choice(self, name: str) -> str | None:
        """Return the single choice made in a task-level Choices control, if any."""
        made = self.choices(name)
        return str(made[0]) if made else None

    def regions(self, labels_name: str) -> dict[str, dict[str, Any]]:
        """Return labelled spans by region id, with per-region controls joined in.

        A region's TextArea text is under "concept" and each per-region Choices
        control's choice under the control's name (e.g. "salience").
        """
        spans = {
            result["id"]: {
                "start": result["value"]["start"],
                "end": result["value"]["end"],
                "text": result["value"].get("text"),
                "label": result["value"]["labels"][0],
            }
            for result in self.results
            if result["from_name"] == labels_name and result["type"] == "labels"
        }
        for result in self.results:
            if result.get("id") not in spans:
                continue
            if result["type"] == "textarea":
                spans[result["id"]]["concept"] = " ".join(result["value"]["text"])
            elif result["type"] == "choices":
                spans[result["id"]][result["from_name"]] = str(
                    result["value"]["choices"][0]
                )
        return spans

    def buckets(self, name: str) -> dict[str, list[str]]:
        """Return a Ranker's buckets, item ids per bucket ("_" is unsorted)."""
        for result in self.results:
            if result["from_name"] == name and result["type"] == "ranker":
                return {
                    bucket: list(items)
                    for bucket, items in result["value"]["ranker"].items()
                }
        return {}


def _annotator(completed_by: Any) -> str:
    if isinstance(completed_by, dict):
        return str(completed_by.get("email") or completed_by["id"])
    return str(completed_by)


def read_export(path: Path) -> list[Annotation]:
    """Read a Label Studio JSON export, skipping cancelled (skipped) annotations.

    :param path: the project exported in the "JSON" format
    :returns: one Annotation per submitted annotation
    :rtype: list[Annotation]
    """
    annotations = []
    for task in json.loads(path.read_text()):
        for annotation in task["annotations"]:
            if annotation.get("was_cancelled"):
                continue
            annotations.append(
                Annotation(
                    item_id=task["data"]["item_id"],
                    annotator=_annotator(annotation["completed_by"]),
                    created_at=annotation["created_at"],
                    results=annotation["result"],
                )
            )
    return annotations


def by_item(annotations: list[Annotation]) -> dict[str, list[Annotation]]:
    """Group annotations by item, each item's in submission order."""
    grouped: dict[str, list[Annotation]] = {}
    for annotation in sorted(annotations, key=lambda a: (a.item_id, a.created_at)):
        grouped.setdefault(annotation.item_id, []).append(annotation)
    return grouped


def majority(values: Sequence[Hashable]) -> Hashable | None:
    """Return the most common value, or None on a tie (left for adjudication)."""
    ranked = Counter(values).most_common()
    if len(ranked) > 1 and ranked[0][1] == ranked[1][1]:
        return None
    return ranked[0][0]
