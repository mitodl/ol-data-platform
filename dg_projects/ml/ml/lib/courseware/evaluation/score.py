"""Score the gold set: join Label Studio exports to the populations and estimate.

Where an item has several annotations, its label is the annotators' majority, and
an item whose annotators tie is left out and listed for adjudication (a third
annotation breaks the tie). Cluster reviews are partitions, which do not vote, so
the latest annotation of a cluster wins.
"""

import json
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import asdict
from itertools import combinations
from typing import Any

import numpy as np
import polars as pl
from ml.lib.courseware.evaluation.labelstudio import Annotation, by_item, majority
from ml.lib.courseware.evaluation.matching import MATCHING_RULE, LabelMatcher
from ml.lib.courseware.evaluation.metrics import (
    Estimate,
    Stratum,
    cluster_bcubed_terms,
    fleiss_kappa,
    mean_pairwise_cohen_kappa,
    stratified_ppi,
)
from ml.lib.courseware.evaluation.samples import STATUS_ORDER

# Labels per item (y, g); g is 1 for a mean.
Labels = Mapping[str, tuple[float, float]]


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


def _report(estimate: Estimate, strata: list[Stratum]) -> dict[str, Any]:
    return {
        **asdict(estimate),
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


def _kappa(
    items: Mapping[str, list[Annotation]], label: Callable[[Annotation], Any]
) -> dict[str, Any]:
    """Mean pairwise Cohen's kappa, and Fleiss' kappa at the modal rater count."""
    ratings = {
        item: {annotation.annotator: label(annotation) for annotation in annotations}
        for item, annotations in items.items()
        if len({annotation.annotator for annotation in annotations}) > 1
    }
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


def _is_correct(annotation: Annotation) -> bool:
    return (
        annotation.choice("verdict") == "supported"
        and annotation.choice("status") != "wrong"
        and annotation.choice("label_quality") != "not_a_concept"
    )


def score_claims(
    population: pl.DataFrame,
    annotations: list[Annotation],
    *,
    alpha: float,
    autorater: pl.DataFrame | None = None,
) -> dict[str, Any]:
    """Estimate per-model claim precision with stratified PPI.

    A claim is correct when the source supports it, its teaching status is not
    wrong, and its label is a concept. "supported" counts support alone.

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
    report: dict[str, Any] = {"matching_rule": MATCHING_RULE, "models": {}}
    for criterion, label in (
        ("correct", _is_correct),
        ("supported", lambda a: a.choice("verdict") == "supported"),
    ):
        decided, ties = _consensus(items, label)
        labels = {item: (float(value), 1.0) for item, value in decided.items()}
        for (model,), rows in population.group_by("model"):
            strata = _strata(rows, labels, "autorater")
            report["models"].setdefault(model, {})[criterion] = _report(
                stratified_ppi(strata, alpha=alpha), strata
            )
        report[f"{criterion}_ties"] = ties
    report["kappa_verdict"] = _kappa(items, lambda a: a.choice("verdict"))
    return report


def _gold_concepts(
    annotation: Annotation, matcher: LabelMatcher
) -> dict[str, tuple[str, str]]:
    """Canonical concept -> (status, annotator's label) for one annotation."""
    gold: dict[str, tuple[str, str]] = {}
    for region in annotation.regions("concept").values():
        label = region.get("concept") or region["text"]
        gold.setdefault(matcher.canonical(label), (region["label"], label))
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


def score_subsections(  # noqa: PLR0913
    population: pl.DataFrame,
    sample: pl.DataFrame,
    annotations: list[Annotation],
    model_concepts: pl.DataFrame,
    matcher: LabelMatcher,
    *,
    alpha: float,
) -> tuple[dict[str, Any], pl.DataFrame]:
    """Estimate per-model concept recall and status-aware recall.

    The gold concepts of a subsection are the union over its annotators; a
    concept's gold status is their majority. Recall for a model is the share of gold
    concepts it named in that scope; status-aware recall also needs its strongest
    status there to equal the gold status. Each is sum(found) / sum(gold) over
    subsections, with the pooled model concepts as the autorater. concept_recall
    and status_recall use the blind subsections only; the *_prefilled estimates use
    the subsections annotated from the models' pre-annotations, for comparison.

    :param population: subsections_population output
    :param sample: sample_subsections output (for the blind flag)
    :param annotations: the subsections project's export
    :param model_concepts: (scope_id, model, canonical, status) per model concept
    :param matcher: the matching rule, with any reviewer label map added
    :param alpha: interval error level
    :returns: the report, and the gold concepts no model named, for review
    :rtype: tuple[dict[str, Any], pl.DataFrame]
    """
    items = by_item(annotations)
    blind = dict(sample.select("item_id", "blind").iter_rows())
    scope_of = dict(population.select("item_id", "scope_id").iter_rows())
    named: dict[tuple[str, str], dict[str, str]] = {}
    for scope_id, model, canonical, status in model_concepts.select(
        "scope_id", "model", "canonical", "status"
    ).iter_rows():
        named.setdefault((scope_id, model), {})[canonical] = status
    gold: dict[str, dict[str, str]] = {}
    unmatched: list[dict[str, Any]] = []
    agreement = []
    for item, item_annotations in items.items():
        per_annotator = [_gold_concepts(a, matcher) for a in item_annotations]
        statuses: dict[str, list[str]] = {}
        for concepts in per_annotator:
            for canonical, (status, _) in concepts.items():
                statuses.setdefault(canonical, []).append(status)
        gold[item] = {
            canonical: _status(values) for canonical, values in statuses.items()
        }
        for left, right in combinations(per_annotator, 2):
            union = left.keys() | right.keys()
            agreement.append(
                2 * len(left.keys() & right.keys()) / (len(left) + len(right))
                if union
                else 1.0
            )
        found_any = {
            canonical
            for (scope_id, _), concepts in named.items()
            if scope_id == scope_of[item]
            for canonical in concepts
        }
        unmatched.extend(
            {
                "item_id": item,
                "blind": blind[item],
                "annotator": annotation.annotator,
                "label": label,
                "status": status,
            }
            for annotation, concepts in zip(
                item_annotations, per_annotator, strict=True
            )
            for canonical, (status, label) in concepts.items()
            if canonical not in found_any
        )
    models = sorted({model for _, model in named})
    report: dict[str, Any] = {
        "matching_rule": MATCHING_RULE,
        "annotator_f1": float(np.mean(agreement)) if agreement else None,
        "models": {},
    }
    # Pre-annotations anchor annotators on what the models found, which inflates
    # recall, so the blind subsections give the headline number.
    subsets = {"": True, "_prefilled": False}
    for model in models:
        rows = population.filter(pl.col("models").str.split("+").list.contains(model))
        in_population = set(rows["item_id"])
        for criterion in ("concept", "status"):
            for suffix, is_blind in subsets.items():
                labels = {
                    item: _found(
                        gold_concepts,
                        named.get((scope_of[item], model), {}),
                        by_status=criterion == "status",
                    )
                    for item, gold_concepts in gold.items()
                    if item in in_population and blind[item] == is_blind
                }
                strata = _strata(rows, labels, f"found_hat_{model}", "gold_hat")
                report["models"].setdefault(model, {})[
                    f"{criterion}_recall{suffix}"
                ] = _report(stratified_ppi(strata, alpha=alpha), strata)
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
        "merge_precision": _report(
            stratified_ppi(precision_strata, alpha=alpha), precision_strata
        ),
        "merge_recall": _report(
            stratified_ppi(recall_strata, alpha=alpha), recall_strata
        ),
        "relation_agreement": _report(
            stratified_ppi(agreement_strata, alpha=alpha), agreement_strata
        ),
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
        "bcubed_precision": _report(
            stratified_ppi(precision_strata, alpha=alpha), precision_strata
        ),
        "bcubed_recall_upper_bound": _report(
            stratified_ppi(recall_strata, alpha=alpha), recall_strata
        ),
    }


_SWAP = {"a_before_b": "b_before_a", "b_before_a": "a_before_b"}


def score_prerequisites(
    population: pl.DataFrame,
    sample: pl.DataFrame,
    annotations: list[Annotation],
    *,
    alpha: float,
) -> dict[str, Any]:
    """Estimate prerequisite edge precision and recall, with annotator kappa.

    :param population: prerequisites_population output
    :param sample: sample_prerequisites output (for the order each pair was shown)
    :param annotations: the prerequisites project's export
    :param alpha: interval error level
    :returns: estimates, direction confusions, ties and kappa
    :rtype: dict[str, Any]
    """
    items = by_item(annotations)
    shown_a = dict(sample.select("item_id", "concept_a").iter_rows())

    def direction(annotation: Annotation) -> str | None:
        value = annotation.choice("prerequisite")
        if (
            value is None
            or shown_a[annotation.item_id] == annotation.item_id.split("||")[0]
        ):
            return value
        return _SWAP.get(value, value)

    decided, ties = _consensus(items, direction)
    edges = dict(population.select("item_id", "edge_direction").iter_rows())

    def found(item: str, value: str) -> bool:
        return value != "neither" and edges[item] in (value, "both")

    edge_population = population.filter(pl.col("edge_direction") != "none")
    precision_strata = _strata(
        edge_population,
        {item: (float(found(item, value)), 1.0) for item, value in decided.items()},
    )
    recall_strata = _strata(
        population,
        {
            item: (float(found(item, value)), float(value != "neither"))
            for item, value in decided.items()
        },
    )
    reversed_edges = sum(
        edges[item] in _SWAP and value == _SWAP[edges[item]]
        for item, value in decided.items()
    )
    return {
        "edge_precision": _report(
            stratified_ppi(precision_strata, alpha=alpha), precision_strata
        ),
        "edge_recall": _report(
            stratified_ppi(recall_strata, alpha=alpha), recall_strata
        ),
        "labelled_edges_reversed": reversed_edges,
        "ties": ties,
        "kappa": _kappa(items, direction),
    }
