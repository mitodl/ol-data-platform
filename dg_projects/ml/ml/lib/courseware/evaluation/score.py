"""Score the gold set: join Label Studio exports to the populations and estimate.

Where an item has several annotations, its label is the annotators' majority, and
an item whose annotators tie is left out and listed for adjudication (a third
annotation breaks the tie). Cluster reviews are partitions, which do not vote, so
the latest annotation of a cluster wins.
"""

import json
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from itertools import combinations
from typing import Any

import numpy as np
import polars as pl
from ml.lib.courseware.evaluation.labelstudio import (
    CENTRAL_ROLES,
    DEPENDENCY_DIRECTIONS,
    GRANULARITY_ERRORS,
    HIERARCHY_DIRECTIONS,
    Annotation,
    by_item,
    majority,
)
from ml.lib.courseware.evaluation.matching import MATCHING_RULE, LabelMatcher
from ml.lib.courseware.evaluation.metrics import (
    MIN_LABELS_PER_STRATUM,
    Stratum,
    cluster_bcubed_terms,
    fleiss_kappa,
    mean_pairwise_cohen_kappa,
    stratified_ppi,
)
from ml.lib.courseware.evaluation.samples import CONCEPT_CLAIM_KINDS, STATUS_ORDER

# Labels per item (y, g); g is 1 for a mean.
Labels = Mapping[str, tuple[float, float]]
# A scope id is unique only within its course run (OCW courses reuse session-1).
SCOPE = ("platform", "courserun_readable_id", "scope_id")


def _strata(
    population: pl.DataFrame,
    labels: Labels,
    y_hat: str | None = None,
    g_hat: str | None = None,
) -> list[Stratum]:
    """Build one Stratum per population stratum from item labels and predictions."""
    strata = []
    for (name,), rows in population.sort("item_id").group_by(
        "stratum", maintain_order=True
    ):
        labeled = rows["item_id"].is_in(list(labels)).to_numpy()
        values = np.array(
            [labels[item] for item in rows["item_id"].filter(labeled)], dtype=float
        ).reshape(-1, 2)
        predictions: dict[str, np.ndarray | None] = {}
        if y_hat is not None and rows[y_hat].null_count() == 0:
            y_pred = rows[y_hat].to_numpy().astype(float)
            g_pred = (
                rows[g_hat].to_numpy().astype(float)
                if g_hat is not None
                else np.ones(len(y_pred))
            )
            predictions = {
                "y_hat": y_pred[labeled],
                "g_hat": g_pred[labeled],
                "y_hat_unlabeled": y_pred[~labeled],
                "g_hat_unlabeled": g_pred[~labeled],
            }
        strata.append(
            Stratum(
                name=str(name),
                population=rows.height,
                y=values[:, 0],
                g=values[:, 1],
                **predictions,
            )
        )
    return strata


def _estimate(strata: list[Stratum], *, alpha: float) -> dict[str, Any]:
    """Estimate with stratified PPI, or say why the labels cannot support one.

    Ties and unannotated items can leave a stratum with fewer than
    MIN_LABELS_PER_STRATUM labels, which has no variance estimate. Merging it into
    another stratum here would pool items sampled at different rates as if they
    were one simple random sample, so the estimate is skipped instead, naming the
    strata that need more annotations or a tie-breaking one. An estimate whose
    labelled items all have a zero denominator (e.g. no pair an annotator called
    exact, for merge recall) is undefined and skipped too.
    """
    report: dict[str, Any] = {}
    short = {s.name: s.n for s in strata if s.n < MIN_LABELS_PER_STRATUM}
    if short:
        report["skipped"] = (
            f"strata with fewer than {MIN_LABELS_PER_STRATUM} decided labels; "
            "annotate more of their items or break their ties"
        )
        report["under_filled_strata"] = short
    elif not any(stratum.denominators.sum() for stratum in strata):
        report["skipped"] = "no labelled item counts toward the denominator"
    else:
        report.update(asdict(stratified_ppi(strata, alpha=alpha)))
    return {
        **report,
        "strata": {
            stratum.name: {
                "population": stratum.population,
                "labeled": stratum.n,
                "labeled_ratio": float(stratum.y.sum() / stratum.denominators.sum())
                if stratum.denominators.sum()
                else None,
            }
            for stratum in strata
        },
    }


def _consensus(
    items: Mapping[str, list[Annotation]],
    label: Callable[[Annotation], Any],
) -> tuple[dict[str, Any], list[str]]:
    """Majority label per item, and the items whose annotators tied."""
    decided, ties = {}, []
    for item, annotations in items.items():
        value = majority([label(annotation) for annotation in annotations])
        if value is None:
            ties.append(item)
        else:
            decided[item] = value
    return decided, ties


def _ratings_kappa(ratings: Mapping[Any, Mapping[str, Any]]) -> dict[str, Any]:
    """Mean pairwise Cohen's kappa, and Fleiss' kappa at the modal rater count.

    :param ratings: per rated thing, {annotator: label}; things with one rater are
        left out
    """
    ratings = {key: raters for key, raters in ratings.items() if len(raters) > 1}
    if not ratings:
        return {"items": 0}
    report: dict[str, Any] = {"items": len(ratings)}
    try:
        report["mean_pairwise_cohen"] = mean_pairwise_cohen_kappa(ratings)
    except ValueError as error:
        report["mean_pairwise_cohen"] = str(error)
    modal = Counter(len(raters) for raters in ratings.values()).most_common(1)[0][0]
    same = [
        list(raters.values()) for raters in ratings.values() if len(raters) == modal
    ]
    report["fleiss"] = fleiss_kappa(same)
    report["fleiss_raters"] = modal
    report["fleiss_items"] = len(same)
    return report


def _kappa(
    items: Mapping[str, list[Annotation]], label: Callable[[Annotation], Any]
) -> dict[str, Any]:
    """Kappa of one label per item (_ratings_kappa)."""
    return _ratings_kappa(
        {
            item: {
                annotation.annotator: label(annotation) for annotation in annotations
            }
            for item, annotations in items.items()
        }
    )


def _is_correct(annotation: Annotation, *, is_concept: bool) -> bool:
    # label quality only judges concept labels; a skill or outcome is not one
    return (
        annotation.choice("verdict") == "supported"
        and annotation.choice("status") != "wrong"
        and not (is_concept and annotation.choice("label_quality") == "not_a_concept")
    )


def score_claims(
    population: pl.DataFrame,
    annotations: list[Annotation],
    *,
    alpha: float,
    autorater: pl.DataFrame | None = None,
) -> dict[str, Any]:
    """Estimate per-model claim precision and label granularity with stratified PPI.

    A claim is correct when the source supports it, its teaching status is not
    wrong, and, for a concept claim kind, its label is a concept. "supported"
    counts support alone. A too_broad or too_narrow label does not make a claim
    incorrect, but it changes which concepts resolution merges and which
    preparation pairs match, so granularity_error reports the share of concept
    claim kinds with either label beside precision (no autorater predicts it, so
    it is the classical stratified estimate). With more than one discipline in the
    population every estimate is also reported per discipline.

    :param population: claims_population output
    :param annotations: the claims project's export
    :param alpha: interval error level
    :param autorater: item_id and autorater columns replacing the cross-model
        prediction, e.g. an LLM judge's verdicts on every claim
    :returns: estimates per model and criterion, ties and kappa
    :rtype: dict[str, Any]
    """
    if autorater is not None:
        population = population.drop("autorater").join(
            autorater.select("item_id", "autorater"), on="item_id", how="left"
        )
    items = by_item(annotations)
    is_concept = pl.col("claim_kind").is_in(CONCEPT_CLAIM_KINDS)
    concept_items = set(population.filter(is_concept)["item_id"])
    criteria: tuple[tuple[str, Callable[[Annotation], Any], pl.Expr, str | None], ...]
    criteria = (
        (
            "correct",
            lambda a: _is_correct(a, is_concept=a.item_id in concept_items),
            pl.lit(value=True),
            "autorater",
        ),
        (
            "supported",
            lambda a: a.choice("verdict") == "supported",
            pl.lit(value=True),
            "autorater",
        ),
        (
            "granularity_error",
            lambda a: a.choice("label_quality") in GRANULARITY_ERRORS,
            is_concept,
            None,
        ),
    )
    groups = ["model"]
    if "discipline" in population.columns and population["discipline"].n_unique() > 1:
        groups.append("discipline")
    report: dict[str, Any] = {"matching_rule": MATCHING_RULE, "models": {}}
    for criterion, label, scored, prediction in criteria:
        scored_items = set(population.filter(scored)["item_id"])
        decided, ties = _consensus(
            {item: rows for item, rows in items.items() if item in scored_items}, label
        )
        labels = {item: (float(value), 1.0) for item, value in decided.items()}
        for (model,), rows in population.filter(scored).group_by("model"):
            report["models"].setdefault(model, {})[criterion] = _estimate(
                _strata(rows, labels, prediction), alpha=alpha
            )
        if "discipline" in groups:
            for (model, discipline), rows in population.filter(scored).group_by(groups):
                report["models"][model].setdefault("by_discipline", {}).setdefault(
                    discipline, {}
                )[criterion] = _estimate(_strata(rows, labels, prediction), alpha=alpha)
        report[f"{criterion}_ties"] = ties
    report["label_quality"] = dict(
        Counter(
            annotation.choice("label_quality")
            for item, annotations in items.items()
            if item in concept_items
            for annotation in annotations
        )
    )
    report["kappa_verdict"] = _kappa(items, lambda a: a.choice("verdict"))
    report["kappa_label_quality"] = _kappa(
        {item: rows for item, rows in items.items() if item in concept_items},
        lambda a: a.choice("label_quality"),
    )
    return report


@dataclass(frozen=True)
class GoldConcept:
    """One annotator's mark of one concept in one subsection.

    :param label: the concept name the annotator typed
    :param status: teaching status
    :param salience: central, supporting or incidental; None when left unset
    :param role: objective or needed in the central explanation, or None
    """

    label: str
    status: str
    salience: str | None
    role: str | None


def _gold_concepts(
    annotation: Annotation, matcher: LabelMatcher
) -> dict[str, GoldConcept]:
    """Canonical concept -> the annotator's mark, for one annotation.

    Roles only count where the annotator said the subsection has a central
    explanation; _objective_conflicts lists the annotations that marked roles
    without one.
    """
    present = annotation.choice("central_explanation") == "present"
    gold: dict[str, GoldConcept] = {}
    for region in annotation.regions("concept").values():
        label = region.get("concept") or region["text"]
        gold.setdefault(
            matcher.canonical(label),
            GoldConcept(
                label=label,
                status=region["label"],
                salience=region.get("salience"),
                role=region.get("central_role") if present else None,
            ),
        )
    return gold


def _status(statuses: list[str]) -> str:
    """Return the most common status, ties going to the stronger one."""
    counts = Counter(statuses)
    return min(counts, key=lambda status: (-counts[status], STATUS_ORDER.index(status)))


def _found(
    gold: Mapping[str, str], emitted: Mapping[str, str], *, by_status: bool
) -> tuple[float, float]:
    """(gold concepts the model named, gold concepts) for one subsection."""
    found = sum(
        canonical in emitted and (not by_status or emitted[canonical] == status)
        for canonical, status in gold.items()
    )
    return float(found), float(len(gold))


@dataclass(frozen=True)
class SubsectionGold:
    """The annotators' consensus on one subsection.

    :param marks: each annotation with its concepts, in submission order
    :param status: gold status per canonical concept (majority, ties to the
        stronger status)
    :param salience: majority salience per concept; None on a tie or when no
        annotator set one
    :param central: majority answer to whether a central explanation exists; None
        on a tie
    :param role: majority role per concept among the annotators who marked it
        ("objective", "needed" or "none"); None on a tie, and every concept "none"
        when the central explanation is absent
    """

    marks: list[tuple[Annotation, dict[str, GoldConcept]]]
    status: dict[str, str]
    salience: dict[str, str | None]
    central: str | None
    role: dict[str, str | None]


def _decided(values: list[str]) -> str | None:
    """Return the majority value, or None on a tie or when there is none."""
    value = majority(values) if values else None
    return None if value is None else str(value)


def subsection_gold(
    annotations: list[Annotation], matcher: LabelMatcher
) -> dict[str, SubsectionGold]:
    """Return the consensus of every annotated subsection.

    :param annotations: the subsections project's export
    :param matcher: the matching rule, with any reviewer label map added
    :returns: SubsectionGold per item id
    :rtype: dict[str, SubsectionGold]
    """
    gold = {}
    for item, item_annotations in by_item(annotations).items():
        marks = [(a, _gold_concepts(a, matcher)) for a in item_annotations]
        by_concept: dict[str, list[GoldConcept]] = {}
        for _, concepts in marks:
            for canonical, concept in concepts.items():
                by_concept.setdefault(canonical, []).append(concept)
        central = _decided(
            [
                answer
                for annotation, _ in marks
                if (answer := annotation.choice("central_explanation")) is not None
            ]
        )
        gold[item] = SubsectionGold(
            marks=marks,
            status={c: _status([m.status for m in ms]) for c, ms in by_concept.items()},
            salience={
                canonical: _decided(
                    [m.salience for m in concept_marks if m.salience is not None]
                )
                for canonical, concept_marks in by_concept.items()
            },
            central=central,
            role={
                canonical: (
                    "none"
                    if central == "absent"
                    else _decided([m.role or "none" for m in concept_marks])
                )
                for canonical, concept_marks in by_concept.items()
            },
        )
    return gold


def gold_subsection_concepts(gold: Mapping[str, SubsectionGold]) -> pl.DataFrame:
    """Return one row per gold concept per subsection, for later comparisons.

    The salience and central-objective evaluations join model output to this.

    :param gold: subsection_gold output
    :returns: item_id, canonical, label, status, salience, central_role,
        central_explanation and the number of annotators who marked the concept
    :rtype: pl.DataFrame
    """
    rows = []
    for item, subsection in sorted(gold.items()):
        for canonical, status in sorted(subsection.status.items()):
            marked = [
                concepts[canonical]
                for _, concepts in subsection.marks
                if canonical in concepts
            ]
            rows.append(
                {
                    "item_id": item,
                    "canonical": canonical,
                    "label": marked[0].label,
                    "status": status,
                    "salience": subsection.salience[canonical],
                    "central_role": subsection.role[canonical],
                    "central_explanation": subsection.central,
                    "annotators": len(marked),
                }
            )
    return pl.DataFrame(
        rows,
        schema={
            "item_id": pl.String,
            "canonical": pl.String,
            "label": pl.String,
            "status": pl.String,
            "salience": pl.String,
            "central_role": pl.String,
            "central_explanation": pl.String,
            "annotators": pl.Int64,
        },
    )


def _salience_report(gold: Mapping[str, SubsectionGold]) -> dict[str, Any]:
    """Annotator agreement on salience and how it spreads over teaching status."""
    ratings = {
        (item, canonical): {
            annotation.annotator: concepts[canonical].salience
            for annotation, concepts in subsection.marks
            if canonical in concepts and concepts[canonical].salience is not None
        }
        for item, subsection in gold.items()
        for canonical in subsection.status
    }
    by_status = Counter(
        f"{subsection.status[canonical]}|{salience or 'undecided'}"
        for subsection in gold.values()
        for canonical, salience in subsection.salience.items()
    )
    unset = sum(
        concept.salience is None
        for subsection in gold.values()
        for _, concepts in subsection.marks
        for concept in concepts.values()
    )
    return {
        "kappa": _ratings_kappa(ratings),
        "status_by_salience": dict(sorted(by_status.items())),
        "marks_without_salience": unset,
    }


def score_subsections(  # noqa: PLR0913
    population: pl.DataFrame,
    sample: pl.DataFrame,
    annotations: list[Annotation],
    model_concepts: pl.DataFrame,
    matcher: LabelMatcher,
    *,
    alpha: float,
) -> tuple[dict[str, Any], pl.DataFrame]:
    """Estimate per-model concept recall, status-aware and central-concept recall.

    The gold concepts of a subsection are the union over its annotators; a
    concept's gold status is their majority. Recall for a model is the share of gold
    concepts it named in that scope; status-aware recall also needs its strongest
    status there to equal the gold status. Each is sum(found) / sum(gold) over
    subsections, with the pooled model concepts as the autorater. central_recall is
    recall over the concepts whose gold salience is central, with no autorater (the
    pooled proxy counts every concept). concept_recall, status_recall and
    central_recall use the blind subsections only; the *_prefilled estimates use the
    subsections annotated from the models' pre-annotations, for comparison. The
    report also carries salience agreement (kappa over concepts two annotators
    both marked) and the gold status-by-salience counts.

    :param population: subsections_population output
    :param sample: sample_subsections output (for the blind flag)
    :param annotations: the subsections project's export
    :param model_concepts: (platform, courserun_readable_id, scope_id, model,
        canonical, status) per model concept
    :param matcher: the matching rule, with any reviewer label map added
    :param alpha: interval error level
    :returns: the report, and the gold concepts no model named, for review
    :rtype: tuple[dict[str, Any], pl.DataFrame]
    """
    gold = subsection_gold(annotations, matcher)
    blind = dict(sample.select("item_id", "blind").iter_rows())
    scope_of = {
        row[0]: row[1:] for row in population.select("item_id", *SCOPE).iter_rows()
    }
    named: dict[tuple[tuple[str, ...], str], dict[str, str]] = {}
    for *scope, model, canonical, status in model_concepts.select(
        *SCOPE, "model", "canonical", "status"
    ).iter_rows():
        named.setdefault((tuple(scope), model), {})[canonical] = status
    unmatched: list[dict[str, Any]] = []
    agreement = []
    for item, subsection in gold.items():
        per_annotator = [concepts for _, concepts in subsection.marks]
        for left, right in combinations(per_annotator, 2):
            union = left.keys() | right.keys()
            agreement.append(
                2 * len(left.keys() & right.keys()) / (len(left) + len(right))
                if union
                else 1.0
            )
        found_any = {
            canonical
            for (scope, _), concepts in named.items()
            if scope == scope_of[item]
            for canonical in concepts
        }
        unmatched.extend(
            {
                "item_id": item,
                "blind": blind[item],
                "annotator": annotation.annotator,
                "label": concept.label,
                "status": concept.status,
            }
            for annotation, concepts in subsection.marks
            for canonical, concept in concepts.items()
            if canonical not in found_any
        )
    # every model that extracted a scope, including one that named no concept
    models = sorted(
        {model for value in population["models"] for model in value.split("+")}
    )
    report: dict[str, Any] = {
        "matching_rule": MATCHING_RULE,
        "annotator_f1": float(np.mean(agreement)) if agreement else None,
        "salience": _salience_report(gold),
        "models": {},
    }
    targets: dict[str, Callable[[SubsectionGold], dict[str, str]]] = {
        "concept": lambda subsection: subsection.status,
        "status": lambda subsection: subsection.status,
        "central": lambda subsection: {
            canonical: status
            for canonical, status in subsection.status.items()
            if subsection.salience[canonical] == "central"
        },
    }
    # Pre-annotations anchor annotators on what the models found, which inflates
    # recall, so the blind subsections give the headline number.
    subsets = {"": True, "_prefilled": False}
    for model in models:
        rows = population.filter(pl.col("models").str.split("+").list.contains(model))
        in_population = set(rows["item_id"])
        for criterion, target in targets.items():
            for suffix, is_blind in subsets.items():
                labels = {
                    item: _found(
                        target(subsection),
                        named.get((scope_of[item], model), {}),
                        by_status=criterion == "status",
                    )
                    for item, subsection in gold.items()
                    if item in in_population and blind[item] == is_blind
                }
                strata = (
                    _strata(rows, labels)
                    if criterion == "central"
                    else _strata(rows, labels, f"found_hat_{model}", "gold_hat")
                )
                report["models"].setdefault(model, {})[
                    f"{criterion}_recall{suffix}"
                ] = _estimate(strata, alpha=alpha)
    return report, pl.DataFrame(
        unmatched,
        schema={
            "item_id": pl.String,
            "blind": pl.Boolean,
            "annotator": pl.String,
            "label": pl.String,
            "status": pl.String,
        },
    )


def _objective_conflicts(gold: Mapping[str, SubsectionGold]) -> list[dict[str, str]]:
    """Annotations whose central-explanation answer and role marks disagree."""
    conflicts = []
    for item, subsection in sorted(gold.items()):
        for annotation, _ in subsection.marks:
            central = annotation.choice("central_explanation")
            roles = {
                region.get("central_role")
                for region in annotation.regions("concept").values()
            } - {None}
            problem = None
            if central is None:
                problem = "central_explanation not answered"
            elif central == "absent" and roles:
                problem = "roles marked with no central explanation (ignored)"
            elif central == "present" and "objective" not in roles:
                problem = "central explanation present but no objective marked"
            if problem:
                conflicts.append(
                    {
                        "item_id": item,
                        "annotator": annotation.annotator,
                        "problem": problem,
                    }
                )
    return conflicts


def _dice(left: set[str], right: set[str]) -> float:
    return 2 * len(left & right) / (len(left) + len(right)) if left or right else 1.0


def score_objectives(
    population: pl.DataFrame,
    annotations: list[Annotation],
    matcher: LabelMatcher,
    *,
    alpha: float,
    model_roles: pl.DataFrame | None = None,
) -> dict[str, Any]:
    """Score annotator agreement on central objectives, and a model's roles.

    Agreement: kappa on whether a subsection has a central explanation, the mean
    pairwise Dice overlap of the objectives two annotators chose where both said it
    has one, and kappa on needed vs. not over the non-objective concepts both
    marked.

    With model_roles (the central-objective extractor variant's output), per
    model and over the subsections it extracted:

    - needed_precision: its needed concepts that the gold marks needed.
    - objective_precision and objective_recall against the gold objectives.
    - false_objective_rate: of the subsections with no gold central explanation,
      the share where it chose an objective.

    Concepts whose gold role tied are left out of both sides. Each is a ratio
    estimate over the population's strata with no autorater. Role rows for a scope
    outside the population (a context scope, a course or stratum the sample left
    out) are not scored, and scopes_outside_population counts them per model.

    :param population: subsections_population output
    :param annotations: the subsections project's export
    :param matcher: the matching rule, with any reviewer label map added
    :param alpha: interval error level
    :param model_roles: platform, courserun_readable_id, scope_id, model,
        canonical and role (objective, needed, or no_central with a null canonical
        for a scope the model found no central explanation in); every scope a model
        extracted needs at least one row
    :returns: agreement, gold counts, conflicts and per-model estimates
    :rtype: dict[str, Any]
    """
    gold = subsection_gold(annotations, matcher)
    report: dict[str, Any] = {
        **_objective_agreement(gold, by_item(annotations)),
        "central_explanation": dict(
            Counter(subsection.central or "tied" for subsection in gold.values())
        ),
        "roles": dict(
            Counter(
                role or "tied"
                for subsection in gold.values()
                for role in subsection.role.values()
            )
        ),
        "conflicts": _objective_conflicts(gold),
        "models": {},
    }
    if model_roles is None:
        return report
    item_of = {
        row[1:]: row[0] for row in population.select("item_id", *SCOPE).iter_rows()
    }
    for (model,), rows in model_roles.group_by("model", maintain_order=True):
        flagged: dict[str, dict[str, set[str]]] = {}
        outside = set()
        for *scope, canonical, role in rows.select(
            *SCOPE, "canonical", "role"
        ).iter_rows():
            item = item_of.get(tuple(scope))
            if item is None:
                outside.add(tuple(scope))
                continue
            marks = flagged.setdefault(item, {r: set() for r in CENTRAL_ROLES})
            if role in CENTRAL_ROLES:
                marks[role].add(canonical)
        extracted = population.filter(pl.col("item_id").is_in(list(flagged)))
        report["models"][model] = {
            name: _estimate(_strata(extracted, values), alpha=alpha)
            for name, values in _model_role_labels(gold, flagged).items()
        } | {"scopes_outside_population": len(outside)}
    return report


def _objective_agreement(
    gold: Mapping[str, SubsectionGold], items: Mapping[str, list[Annotation]]
) -> dict[str, Any]:
    """Annotator agreement on central explanations, objectives and needed concepts."""
    objective_overlap: list[float] = []
    needed_ratings: dict[tuple[str, str], dict[str, bool]] = {}
    for item, subsection in gold.items():
        present = [
            concepts
            for annotation, concepts in subsection.marks
            if annotation.choice("central_explanation") == "present"
        ]
        chosen = [
            {
                canonical
                for canonical, mark in concepts.items()
                if mark.role == "objective"
            }
            for concepts in present
        ]
        objective_overlap.extend(
            _dice(left, right) for left, right in combinations(chosen, 2)
        )
        objectives = set().union(*chosen)
        for annotation, concepts in subsection.marks:
            if annotation.choice("central_explanation") != "present":
                continue
            for canonical, mark in concepts.items():
                if canonical not in objectives:
                    needed_ratings.setdefault((item, canonical), {})[
                        annotation.annotator
                    ] = mark.role == "needed"
    return {
        "kappa_central_explanation": _kappa(
            items, lambda a: a.choice("central_explanation")
        ),
        "objective_dice": float(np.mean(objective_overlap))
        if objective_overlap
        else None,
        "objective_dice_pairs": len(objective_overlap),
        "kappa_needed": _ratings_kappa(needed_ratings),
    }


def _model_role_labels(
    gold: Mapping[str, SubsectionGold], flagged: Mapping[str, Mapping[str, set[str]]]
) -> dict[str, dict[str, tuple[float, float]]]:
    """(numerator, denominator) per subsection for each model role estimate."""
    labels: dict[str, dict[str, tuple[float, float]]] = {
        "needed_precision": {},
        "objective_precision": {},
        "objective_recall": {},
        "false_objective_rate": {},
    }
    for item, marks in flagged.items():
        subsection = gold.get(item)
        if subsection is None or subsection.central is None:
            continue
        tied = {c for c, role in subsection.role.items() if role is None}
        gold_roles = {
            role: {c for c, value in subsection.role.items() if value == role}
            for role in CENTRAL_ROLES
        }
        needed = marks["needed"] - tied
        objectives = marks["objective"] - tied
        labels["needed_precision"][item] = (
            float(len(needed & gold_roles["needed"])),
            float(len(needed)),
        )
        labels["objective_precision"][item] = (
            float(len(objectives & gold_roles["objective"])),
            float(len(objectives)),
        )
        labels["objective_recall"][item] = (
            float(len(gold_roles["objective"] & objectives)),
            float(len(gold_roles["objective"])),
        )
        absent = subsection.central == "absent"
        labels["false_objective_rate"][item] = (
            float(absent and bool(marks["objective"])),
            float(absent),
        )
    return labels


_FLIP = {"a_narrower": "a_broader", "a_broader": "a_narrower"}


def score_pairs(
    population: pl.DataFrame,
    sample: pl.DataFrame,
    annotations: list[Annotation],
    *,
    alpha: float,
) -> dict[str, Any]:
    """Estimate merge precision and recall and relation agreement on label pairs.

    A pair counts as merged by the resolver when both labels belong to the same
    surviving concept (pairs_population), not when the adjudicator said exact.
    Precision is over merged pairs; recall is the share of pairs annotators call
    exact that the resolver merged; relation agreement compares annotators with the
    resolver's final relation of each pair.

    :param population: pairs_population output
    :param sample: sample_pairs output (for the order each pair was shown in)
    :param annotations: the pairs project's export
    :param alpha: interval error level
    :returns: estimates, ties and kappa
    :rtype: dict[str, Any]
    """
    items = by_item(annotations)
    shown_a = dict(sample.select("item_id", "key_a").iter_rows())

    def relation(annotation: Annotation) -> str | None:
        value = annotation.choice("relation")
        if (
            value is None
            or shown_a[annotation.item_id] == annotation.item_id.split("||")[0]
        ):
            return value
        return _FLIP.get(value, value)

    decided, ties = _consensus(items, relation)
    resolver = dict(population.select("item_id", "resolver_relation").iter_rows())
    merged = population.filter(pl.col("merged"))
    precision_strata = _strata(
        merged, {item: (float(decided[item] == "exact"), 1.0) for item in decided}
    )
    recall_strata = _strata(
        population,
        {
            item: (
                float(value == "exact" and resolver[item] == "exact"),
                float(value == "exact"),
            )
            for item, value in decided.items()
        },
    )
    agreement_strata = _strata(
        population,
        {
            item: (float(value == resolver[item]), 1.0)
            for item, value in decided.items()
        },
    )
    return {
        "merge_precision": _estimate(precision_strata, alpha=alpha),
        "merge_recall": _estimate(recall_strata, alpha=alpha),
        "relation_agreement": _estimate(agreement_strata, alpha=alpha),
        "ties": ties,
        "kappa": _kappa(items, relation),
    }


def score_clusters(
    population: pl.DataFrame,
    sample: pl.DataFrame,
    annotations: list[Annotation],
    *,
    alpha: float,
) -> dict[str, Any]:
    """Estimate B-cubed precision and recall of the resolved concepts.

    Members left unsorted are each their own concept. Recall only sees true
    members among the nearby concepts the task showed, so it is an upper bound.

    :param population: clusters_population output
    :param sample: sample_clusters output (for the neighbours' sizes)
    :param annotations: the clusters project's export
    :param alpha: interval error level
    :returns: B-cubed precision and recall estimates
    :rtype: dict[str, Any]
    """
    latest = {
        item: annotations[-1] for item, annotations in by_item(annotations).items()
    }
    neighbour_sizes = {
        row["item_id"]: json.loads(row["neighbour_sizes_json"])
        for row in sample.iter_rows(named=True)
    }
    members = {
        row["item_id"]: row["members"] for row in population.iter_rows(named=True)
    }
    precision_labels, recall_labels = {}, {}
    for item, annotation in latest.items():
        buckets = annotation.buckets("groups")
        groups, outside = [], []
        sorted_members = set()
        for bucket, entries in buckets.items():
            in_cluster = [entry for entry in entries if entry.startswith("m:")]
            if bucket == "_" or not in_cluster:
                continue
            sorted_members.update(in_cluster)
            groups.append(len(in_cluster))
            outside.append(
                sum(
                    neighbour_sizes[item][entry.removeprefix("n:")]
                    for entry in entries
                    if entry.startswith("n:")
                )
            )
        unsorted = len(members[item]) - len(sorted_members)
        groups.extend([1] * unsorted)
        outside.extend([0] * unsorted)
        precision, recall = cluster_bcubed_terms(groups, outside)
        size = float(len(members[item]))
        precision_labels[item] = (precision, size)
        recall_labels[item] = (recall, size)
    precision_strata = _strata(population, precision_labels)
    recall_strata = _strata(population, recall_labels)
    return {
        "bcubed_precision": _estimate(precision_strata, alpha=alpha),
        "bcubed_recall_upper_bound": _estimate(recall_strata, alpha=alpha),
    }


_SWAP = {
    "a_before_b": "b_before_a",
    "b_before_a": "a_before_b",
    "a_kind_of_b": "b_kind_of_a",
    "b_kind_of_a": "a_kind_of_b",
    "a_part_of_b": "b_part_of_a",
    "b_part_of_a": "a_part_of_b",
}


def _confirmed_edges(
    direction: str, value: str, *, lenient: bool
) -> tuple[float, float]:
    """Return (emitted edges the consensus confirms, emitted edges) for one pair.

    :param direction: the pair's edge direction, "both" for edges both ways
    :param value: the dependency consensus
    :param lenient: count a context_dependent consensus as confirming every edge
    """
    emitted = DEPENDENCY_DIRECTIONS if direction == "both" else (direction,)
    if lenient and value == "context_dependent":
        return float(len(emitted)), float(len(emitted))
    return float(value in emitted), float(len(emitted))


def score_prerequisites(
    population: pl.DataFrame,
    sample: pl.DataFrame,
    annotations: list[Annotation],
    *,
    alpha: float,
) -> dict[str, Any]:
    """Score hierarchy and dependency judgments on concept pairs, separately.

    Annotators answer two predicates per pair, each with context_dependent and
    uncertain outcomes and, for a directed answer, the depths it holds at. Each
    predicate gets its own consensus, ties and kappa. From them:

    - edge_precision: inferred builds_on edges whose dependency consensus is the
      edge's direction, counted per directed edge: a pair with edges both ways
      emits two, and a directed consensus confirms at most one of them.
      edge_precision_lenient also counts a context_dependent consensus as
      confirming every edge of the pair. A practiced_with edge asserts no
      dependency (the guideline's answer for a tool and what is practiced with it
      is none), so it is left out. Pairs with a builds_on edge cross the strata
      (a pair's stratum follows its best-supported edge), so this is a
      subpopulation estimate.
    - sequence_edge_precision: the same over builds_on and practiced_with edges,
      each stratum reported on its own, which is how practiced_with is read.
    - edge_recall: pairs with a directed dependency consensus that have a
      builds_on edge that way. A practiced_with edge asserts no dependency, so it
      does not find one. sequence_edge_recall also counts practiced_with edges.
    - dependency_given_hierarchy: of the pairs annotators call a kind or part of
      one another, the share that also have a directed dependency. A hierarchy
      relation does not rule a dependency out, which this measures.
    - pipeline_hierarchy_precision: pairs the pipeline links by hierarchy that
      annotators call a kind or part either way (direction is not compared). The
      pairs cross the edge and non-edge strata, so it is a subpopulation estimate
      and can be skipped for want of labels.

    :param population: prerequisites_population output
    :param sample: sample_prerequisites output (for the order each pair was shown)
    :param annotations: the prerequisites project's export
    :param alpha: interval error level
    :returns: estimates, label and depth counts, direction confusions, ties and
        kappa per predicate
    :rtype: dict[str, Any]
    """
    items = by_item(annotations)
    shown_a = dict(sample.select("item_id", "concept_a").iter_rows())

    def oriented(value: str | None, item: str) -> str | None:
        if value is None or shown_a[item] == item.split("||", maxsplit=1)[0]:
            return value
        return _SWAP.get(value, value)

    def answer(name: str) -> Callable[[Annotation], str | None]:
        return lambda annotation: oriented(annotation.choice(name), annotation.item_id)

    hierarchy, hierarchy_ties = _consensus(items, answer("hierarchy"))
    dependency, dependency_ties = _consensus(items, answer("dependency"))
    edges = dict(population.select("item_id", "edge_direction").iter_rows())
    builds_on = dict(population.select("item_id", "builds_on_direction").iter_rows())

    def found(item: str, value: str, directions: Mapping[str, str]) -> bool:
        """Whether an edge runs the way the consensus does, for recall."""
        return value in DEPENDENCY_DIRECTIONS and directions[item] in (value, "both")

    def edge_precision(column: str, directions: Mapping[str, str], *, lenient: bool):
        return _strata(
            population.filter(pl.col(column) != "none"),
            {
                item: _confirmed_edges(directions[item], value, lenient=lenient)
                for item, value in dependency.items()
            },
        )

    precision = edge_precision("builds_on_direction", builds_on, lenient=False)
    lenient = edge_precision("builds_on_direction", builds_on, lenient=True)
    sequence_precision = edge_precision("edge_direction", edges, lenient=False)

    def edge_recall(directions: Mapping[str, str]) -> list[Stratum]:
        return _strata(
            population,
            {
                item: (
                    float(found(item, value, directions)),
                    float(value in DEPENDENCY_DIRECTIONS),
                )
                for item, value in dependency.items()
            },
        )

    recall = edge_recall(builds_on)
    sequence_recall = edge_recall(edges)
    both = dependency.keys() & hierarchy.keys()
    given_hierarchy = _strata(
        population,
        {
            item: (
                float(
                    hierarchy[item] in HIERARCHY_DIRECTIONS
                    and dependency[item] in DEPENDENCY_DIRECTIONS
                ),
                float(hierarchy[item] in HIERARCHY_DIRECTIONS),
            )
            for item in both
        },
    )
    pipeline_hierarchy = _strata(
        population.filter(pl.col("pipeline_hierarchy")),
        {
            item: (float(value in HIERARCHY_DIRECTIONS), 1.0)
            for item, value in hierarchy.items()
        },
    )
    reversed_edges = sum(
        edges[item] in _SWAP and value == _SWAP[edges[item]]
        for item, value in dependency.items()
    )

    def depths(name: str, decided: Mapping[str, str]) -> dict[str, int]:
        """Depths marked by annotators who agreed with a directed consensus."""
        return dict(
            Counter(
                depth
                for item, value in decided.items()
                if value in (*DEPENDENCY_DIRECTIONS, *HIERARCHY_DIRECTIONS)
                for annotation in items[item]
                if answer(name)(annotation) == value
                for depth in annotation.choices(f"{name}_depth") or ("unset",)
            )
        )

    return {
        "edge_precision": _estimate(precision, alpha=alpha),
        "edge_precision_lenient": _estimate(lenient, alpha=alpha),
        "sequence_edge_precision": _estimate(sequence_precision, alpha=alpha),
        "edge_recall": _estimate(recall, alpha=alpha),
        "sequence_edge_recall": _estimate(sequence_recall, alpha=alpha),
        "dependency_given_hierarchy": _estimate(given_hierarchy, alpha=alpha),
        "pipeline_hierarchy_precision": _estimate(pipeline_hierarchy, alpha=alpha),
        "labelled_edges_reversed": reversed_edges,
        "hierarchy_by_dependency": dict(
            sorted(
                Counter(
                    f"{hierarchy[item]}|{dependency[item]}" for item in both
                ).items()
            )
        ),
        "depths": {
            "hierarchy": depths("hierarchy", hierarchy),
            "dependency": depths("dependency", dependency),
        },
        "ties": {"hierarchy": hierarchy_ties, "dependency": dependency_ties},
        "kappa": {
            "hierarchy": _kappa(items, answer("hierarchy")),
            "dependency": _kappa(items, answer("dependency")),
        },
    }
