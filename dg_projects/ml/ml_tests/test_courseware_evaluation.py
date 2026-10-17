import json
from datetime import UTC, datetime

import numpy as np
import polars as pl
import pytest
from ml.lib.courseware.evaluation import labelstudio, samples, score
from ml.lib.courseware.evaluation.matching import LabelMatcher, concept_label_key
from ml.lib.courseware.evaluation.metrics import (
    Stratum,
    allocate,
    bcubed,
    cluster_bcubed_terms,
    cohen_kappa,
    fleiss_kappa,
    mean_pairwise_cohen_kappa,
    stratified_ppi,
)

# Three strata where the autorater's quality differs, the case StratPPI targets.
TRUE_PRECISION = (0.9, 0.7, 0.5)
AUTORATER_ACCURACY = (0.95, 0.8, 0.6)
STRATUM_SHARE = (0.5, 0.3, 0.2)
UNLABELED = 2_000
LABELED = 40
TRIALS = 400


def _draw(rng, precision, accuracy, size):
    truth = (rng.random(size) < precision).astype(float)
    agrees = rng.random(size) < accuracy
    return truth, np.where(agrees, truth, 1 - truth)


def _simulated_strata(rng, *, with_autorater=True):
    strata = []
    for index, (precision, accuracy, share) in enumerate(
        zip(TRUE_PRECISION, AUTORATER_ACCURACY, STRATUM_SHARE, strict=True)
    ):
        y, y_hat = _draw(rng, precision, accuracy, LABELED)
        _, y_hat_unlabeled = _draw(rng, precision, accuracy, UNLABELED)
        strata.append(
            Stratum(
                name=f"s{index}",
                population=int(share * 10_000),
                y=y,
                y_hat=y_hat if with_autorater else None,
                y_hat_unlabeled=y_hat_unlabeled if with_autorater else None,
            )
        )
    return strata


def test_ppi_interval_covers_true_precision_at_nominal_rate():
    rng = np.random.default_rng(7)
    target = sum(p * w for p, w in zip(TRUE_PRECISION, STRATUM_SHARE, strict=True))
    covered, widths, classical_widths = 0, [], []
    for _ in range(TRIALS):
        estimate = stratified_ppi(_simulated_strata(rng), alpha=0.1)
        covered += estimate.lower <= target <= estimate.upper
        widths.append(estimate.upper - estimate.lower)
        classical = stratified_ppi(
            _simulated_strata(rng, with_autorater=False), alpha=0.1
        )
        classical_widths.append(classical.upper - classical.lower)
    # 400 trials at 90%: a binomial 99% band is roughly 0.86 to 0.94
    assert 0.86 <= covered / TRIALS <= 0.94
    assert np.mean(widths) < np.mean(classical_widths)


def test_ppi_ratio_interval_covers_true_recall():
    """Recall over subsections: found / gold, with a noisy pooled proxy."""
    rng = np.random.default_rng(11)
    true_recall = (0.8, 0.6)

    def subsections(recall, size):
        gold = rng.poisson(12, size).astype(float) + 1
        found = rng.binomial(gold.astype(int), recall).astype(float)
        # the proxy misses what both models miss, so it overstates recall
        gold_hat = found + rng.binomial((gold - found).astype(int), 0.4)
        return found, gold, found, gold_hat

    covered = 0
    for _ in range(TRIALS):
        strata = []
        for index, recall in enumerate(true_recall):
            y, g, y_hat, g_hat = subsections(recall, 15)
            _, _, y_hat_u, g_hat_u = subsections(recall, 500)
            strata.append(
                Stratum(f"s{index}", 1_000, y, g, y_hat, g_hat, y_hat_u, g_hat_u)
            )
        estimate = stratified_ppi(strata, alpha=0.1)
        # equal populations and equal expected gold counts: the pooled recall
        covered += estimate.lower <= np.mean(true_recall) <= estimate.upper
    assert 0.85 <= covered / TRIALS <= 0.95


def test_ppi_without_autorater_is_the_classical_mean():
    y = np.array([1, 1, 0, 1, 0, 1, 1, 1], dtype=float)
    estimate = stratified_ppi([Stratum("only", 100, y)])
    assert estimate.point == pytest.approx(y.mean())
    assert estimate.std_error == pytest.approx(y.std(ddof=1) / np.sqrt(len(y)))
    assert estimate.lambdas == {"only": 0.0}


def test_untuned_ppi_point_is_imputed_mean_plus_rectifier():
    y = np.array([1, 0, 1, 1], dtype=float)
    y_hat = np.array([1, 1, 1, 0], dtype=float)
    unlabeled = np.array([1, 1, 0, 1, 1, 0], dtype=float)
    estimate = stratified_ppi(
        [Stratum("only", 10, y, y_hat=y_hat, y_hat_unlabeled=unlabeled)],
        power_tuning=False,
    )
    assert estimate.point == pytest.approx(unlabeled.mean() + (y - y_hat).mean())


def test_ppi_rejects_a_stratum_with_one_label():
    with pytest.raises(ValueError, match="fewer than 2"):
        stratified_ppi([Stratum("thin", 50, np.array([1.0]))])


def test_allocate_floors_caps_and_oversamples():
    sizes = {"big": 1_000, "small": 5, "disagree": 100}
    allocation = allocate(sizes, 100, weights={"disagree": 4.0}, minimum=10)
    assert sum(allocation.values()) == 100
    assert allocation["small"] == 5
    proportional = allocate(sizes, 100, minimum=10)
    assert allocation["disagree"] > proportional["disagree"]
    assert allocate(sizes, 100, minimum=10) == proportional


def test_bcubed_hand_computed():
    precision, recall = bcubed({"a": 1, "b": 1, "c": 2}, {"a": 1, "b": 2, "c": 2})
    assert precision == pytest.approx(2 / 3)
    assert recall == pytest.approx(2 / 3)
    lumped = bcubed(dict.fromkeys("abcd", 0), {key: key for key in "abcd"})
    assert lumped == pytest.approx((0.25, 1.0))


def test_cluster_terms_sum_to_full_bcubed():
    predicted = {"a": 1, "b": 1, "c": 1, "d": 2, "e": 2, "f": 3}
    truth = {"a": "x", "b": "x", "c": "y", "d": "y", "e": "z", "f": "x"}
    precision_sum = recall_sum = 0.0
    for cluster in set(predicted.values()):
        members = [item for item in predicted if predicted[item] == cluster]
        true_ids = sorted({truth[item] for item in members})
        groups = [sum(truth[m] == t for m in members) for t in true_ids]
        outside = [
            sum(truth[i] == t and predicted[i] != cluster for i in truth)
            for t in true_ids
        ]
        p, r = cluster_bcubed_terms(groups, outside)
        precision_sum += p
        recall_sum += r
    expected = bcubed(predicted, truth)
    assert (precision_sum / 6, recall_sum / 6) == pytest.approx(expected)


def test_cohen_kappa_textbook():
    # Wikipedia, Cohen's kappa: 20 yes/yes, 5 yes/no, 10 no/yes, 15 no/no -> 0.4
    first = ["y"] * 25 + ["n"] * 25
    second = ["y"] * 20 + ["n"] * 5 + ["y"] * 10 + ["n"] * 15
    assert cohen_kappa(first, second) == pytest.approx(0.4)


def test_fleiss_kappa_textbook():
    # Fleiss (1971) as worked on Wikipedia: 10 items, 14 raters, kappa 0.210
    table = [
        [0, 0, 0, 0, 14],
        [0, 2, 6, 4, 2],
        [0, 0, 3, 5, 6],
        [0, 3, 9, 2, 0],
        [2, 2, 8, 1, 1],
        [7, 7, 0, 0, 0],
        [3, 2, 6, 3, 0],
        [2, 5, 3, 2, 2],
        [6, 5, 2, 1, 0],
        [0, 2, 2, 3, 7],
    ]
    ratings = [
        [category for category, count in enumerate(row) for _ in range(count)]
        for row in table
    ]
    assert fleiss_kappa(ratings) == pytest.approx(0.20993, abs=1e-4)


def test_mean_pairwise_cohen_matches_two_rater_kappa():
    first = ["y", "y", "n", "n", "y"]
    second = ["y", "n", "n", "n", "y"]
    ratings = {
        index: {"ann": a, "bob": b}
        for index, (a, b) in enumerate(zip(first, second, strict=True))
    }
    assert mean_pairwise_cohen_kappa(ratings) == pytest.approx(
        cohen_kappa(first, second)
    )


def test_label_matcher_rule():
    matcher = LabelMatcher().add_assignments(
        pl.DataFrame(
            {
                "concept_label_key": ["recall", "true positive rate"],
                "concept_id": ["c1", "c1"],
            }
        )
    )
    matcher.add_mapping([("shap values", "shap")])
    assert concept_label_key("K-Means  Clustering!") == "k means clustering"
    assert matcher.canonical("Recall") == matcher.canonical("true-positive rate")
    assert matcher.canonical("SHAP") == matcher.canonical("SHAP values")
    assert matcher.canonical("recall") != matcher.canonical("precision")


def _inputs(claims: pl.DataFrame) -> samples.GoldInputs:
    empty = pl.DataFrame()
    return samples.GoldInputs(
        claims=claims,
        evidence=empty,
        sources=empty,
        assignments=empty,
        registry=empty,
        decisions=empty,
        labels=empty,
        embeddings=empty,
        concepts=empty,
        relations=empty,
        matcher=LabelMatcher(),
    )


def _claim(model, scope, ordinal, label, status="taught", kind="concept"):
    return {
        "extraction_key": f"{model}-{scope}",
        "claim_ordinal": ordinal,
        "claim_kind": kind,
        "label": label,
        "description": "",
        "teaching_status": status if kind == "concept" else None,
        "model_id": f"us.anthropic.claude-{model}-5-5",
        "platform": "mitxonline",
        "courserun_readable_id": "course-v1:UAI_SOURCE+UAI.4+1T2026",
        "scope_id": scope,
        "scope_title": scope,
        "scope_part": 1,
    }


def test_claims_population_agreement_strata():
    claims = pl.DataFrame(
        [
            _claim("opus", "u1", 1, "Linear regression"),
            _claim("haiku", "u1", 1, "linear-regression", status="used"),
            _claim("haiku", "u1", 2, "SHAP"),
            _claim("opus", "u1", 2, "Fit a model", kind="skill"),
            _claim("opus", "u2", 1, "k-means"),
        ]
    )
    population = samples.claims_population(_inputs(claims))
    by_claim = {
        (row["model"], row["label"]): (row["agreement"], row["autorater"])
        for row in population.iter_rows(named=True)
    }
    assert by_claim["opus", "Linear regression"] == ("both", 0.0)  # status differs
    assert by_claim["haiku", "SHAP"] == ("haiku_only", 0.0)
    assert by_claim["opus", "Fit a model"] == ("not_compared", None)
    assert by_claim["opus", "k-means"] == ("unpaired", None)
    assert set(population["stratum"]) >= {"UAI|opus|both", "UAI|haiku|haiku_only"}


def test_stratified_sample_is_deterministic_per_stratum():
    population = pl.DataFrame(
        {
            "item_id": [f"i{n:03d}" for n in range(200)],
            "stratum": ["a"] * 100 + ["b"] * 100,
        }
    )
    first = samples.stratified_sample(
        population, {"a": 10, "b": 5}, seed=1, set_name="x"
    )
    again = samples.stratified_sample(
        population, {"a": 10, "b": 5}, seed=1, set_name="x"
    )
    more_b = samples.stratified_sample(
        population, {"a": 10, "b": 9}, seed=1, set_name="x"
    )
    assert first.equals(again)
    assert first.filter(pl.col("stratum") == "a").equals(
        more_b.filter(pl.col("stratum") == "a")
    )


def test_course_family():
    assert samples.course_family("course-v1:UAI_SOURCE+UAI.4+1T2026") == "UAI"
    assert samples.course_family("course-v1:MITx+6.00.1x+2T2026") == "6"


def _export(tmp_path, tasks):
    path = tmp_path / "export.json"
    path.write_text(json.dumps(tasks))
    return labelstudio.read_export(path)


def _choice_task(item_id, annotations):
    return {
        "data": {"item_id": item_id},
        "annotations": [
            {
                "completed_by": annotator,
                "created_at": datetime(2026, 10, 17, minute, tzinfo=UTC).isoformat(),
                "was_cancelled": False,
                "result": [
                    {
                        "from_name": name,
                        "type": "choices",
                        "value": {"choices": [value]},
                    }
                    for name, value in choices.items()
                ],
            }
            for minute, (annotator, choices) in enumerate(annotations)
        ],
    }


def test_score_claims_from_an_export(tmp_path):
    population = pl.DataFrame(
        {
            "item_id": [f"c{n}" for n in range(8)],
            "model": ["haiku"] * 8,
            "stratum": ["a"] * 4 + ["b"] * 4,
            "autorater": [1.0, 0.0, 1.0, 1.0, None, None, None, None],
        }
    )
    good = {"verdict": "supported", "status": "correct", "label_quality": "good"}
    bad = {**good, "verdict": "unsupported"}
    tasks = [
        _choice_task("c0", [(1, good), (2, good)]),
        _choice_task("c1", [(1, bad), (2, good)]),  # tie, left out
        _choice_task("c2", [(1, bad)]),
        _choice_task("c4", [(1, good)]),
        _choice_task("c5", [(1, {**good, "status": "wrong"})]),
    ]
    report = score.score_claims(population, _export(tmp_path, tasks), alpha=0.05)
    correct = report["models"]["haiku"]["correct"]
    assert report["correct_ties"] == ["c1"]
    assert correct["strata"]["a"]["labeled"] == 2
    assert correct["strata"]["b"]["labeled_ratio"] == pytest.approx(0.5)
    assert report["kappa_verdict"]["items"] == 2


def test_score_clusters_reads_ranker_buckets(tmp_path):
    population = pl.DataFrame(
        {
            "item_id": ["k1", "k2", "k3", "k4"],
            "members": [["a", "b", "c"], ["d"], ["e", "f"], ["g"]],
            "stratum": ["multi", "single", "multi", "single"],
        }
    )
    sample = pl.DataFrame(
        {
            "item_id": ["k1", "k2", "k3", "k4"],
            "neighbour_sizes_json": [json.dumps({"k2": 1})] + [json.dumps({})] * 3,
        }
    )

    def task(item_id, buckets):
        return {
            "data": {"item_id": item_id},
            "annotations": [
                {
                    "completed_by": 1,
                    "created_at": "2026-10-17T00:00:00+00:00",
                    "result": [
                        {
                            "from_name": "groups",
                            "type": "ranker",
                            "value": {"ranker": buckets},
                        }
                    ],
                }
            ],
        }

    tasks = [
        # a and b are one concept, which neighbour k2 is too; c stands alone
        task("k1", {"group_1": ["m:a", "m:b", "n:k2"], "_": ["m:c"]}),
        task("k2", {"_": ["m:d"]}),
        task("k3", {"group_1": ["m:e", "m:f"]}),
        task("k4", {"_": ["m:g"]}),
    ]
    report = score.score_clusters(
        population, sample, _export(tmp_path, tasks), alpha=0.05
    )
    # k1: precision (4 + 1) / 3, recall 4 / 3 + 1; k3: precision 2, recall 2
    multi = report["bcubed_precision"]["strata"]["multi"]
    assert multi["labeled_ratio"] == pytest.approx((5 / 3 + 2) / 5)
    recall = report["bcubed_recall_upper_bound"]["strata"]["multi"]
    assert recall["labeled_ratio"] == pytest.approx((4 / 3 + 1 + 2) / 5)


def test_majority_ties_return_none():
    assert labelstudio.majority(["a", "a", "b"]) == "a"
    assert labelstudio.majority(["a", "b"]) is None


def test_allocate_rejects_floors_over_budget_and_tiny_strata():
    with pytest.raises(ValueError, match="more than the budget"):
        allocate({f"s{n}": 100 for n in range(24)}, 400, minimum=20)
    with pytest.raises(ValueError, match="smaller than 2"):
        allocate({"big": 100, "one": 1}, 50)
    # every stratum gets at least the two labels stratified_ppi needs
    assert min(allocate({"big": 1_000, "small": 3}, 20).values()) >= 2


def test_coarsen_strata_merges_then_drops():
    population = pl.DataFrame(
        {"item_id": list("abcdefg"), "stratum": ["x"] * 4 + ["y", "z", "w"]}
    )
    merged, dropped = samples.coarsen_strata(population)
    assert dropped.is_empty()
    assert merged.filter(pl.col("stratum") == samples.SMALL_STRATA).height == 3
    kept, dropped = samples.coarsen_strata(population.head(5))
    assert kept.height == 4
    assert dropped.to_dicts() == [{"stratum": "y", "population": 1}]


def test_coarsen_strata_merges_only_within_a_scoring_group():
    population = pl.DataFrame(
        {
            "item_id": [f"i{n}" for n in range(9)],
            "models": ["haiku"] * 4
            + ["haiku", "haiku+opus", "haiku+opus", "opus"]
            + ["opus"],
            "stratum": ["UAI|long|haiku"] * 4
            + ["6|long|haiku", "UAI|short|haiku+opus", "6|short|haiku+opus"]
            + ["UAI|long|opus", "6|long|opus"],
        }
    )
    merged, dropped = samples.coarsen_strata(
        population, 2, samples.SCORING_GROUPS["subsections"]
    )
    assert dict(merged.group_by("stratum").len().sort("stratum").iter_rows()) == {
        "UAI|long|haiku": 4,
        "small_strata|haiku+opus": 2,
        "small_strata|opus": 2,
    }
    assert dropped.to_dicts() == [{"stratum": "6|long|haiku", "population": 1}]


def test_score_claims_on_a_sample_of_cross_model_small_strata(tmp_path):
    strata = (
        ["UAI|opus|both"] * 30
        + ["UAI|haiku|both"] * 30
        + ["UAI|opus|opus_only", "UAI|opus|not_compared", "UAI|haiku|haiku_only"]
    )
    population = pl.DataFrame(
        {
            "item_id": [f"c{n:02d}" for n in range(len(strata))],
            "model": [stratum.split("|")[1] for stratum in strata],
            "stratum": strata,
            "autorater": [None] * len(strata),
        },
        schema_overrides={"autorater": pl.Float64},
    )
    population, dropped = samples.coarsen_strata(
        population, within=samples.SCORING_GROUPS["claims"]
    )
    sizes = dict(population.group_by("stratum").len().iter_rows())
    allocation = allocate(sizes, 60, minimum=samples.CLAIMS_MIN_PER_STRATUM)
    sample = samples.stratified_sample(
        population, allocation, seed=1, set_name="claims"
    )
    good = {"verdict": "supported", "status": "correct", "label_quality": "good"}
    tasks = [_choice_task(item, [(1, good)]) for item in sample["item_id"]]
    # merged across models, the opus and haiku halves of one stratum would get
    # two labels and one, and haiku's could not be estimated
    report = score.score_claims(population, _export(tmp_path, tasks), alpha=0.05)
    assert set(report["models"]["opus"]["correct"]["strata"]) == {
        "UAI|opus|both",
        "small_strata|opus",
    }
    assert set(report["models"]["haiku"]["correct"]["strata"]) == {"UAI|haiku|both"}
    assert dropped["stratum"].to_list() == ["UAI|haiku|haiku_only"]


def test_one_unlabeled_item_gets_lambda_zero():
    y = np.array([1, 0, 1, 1], dtype=float)
    one_left = Stratum(
        "s", 5, y, y_hat=np.array([1, 0, 1, 0.0]), y_hat_unlabeled=np.array([1.0])
    )
    estimate = stratified_ppi([one_left])
    assert np.isfinite(estimate.std_error)
    assert estimate.lambdas == {"s": 0.0}
    assert estimate.point == pytest.approx(y.mean())


def test_blind_items_are_half_of_each_stratum_and_stable():
    sample = pl.DataFrame(
        {"item_id": [f"u{n}" for n in range(9)], "stratum": ["a"] * 4 + ["b"] * 5}
    )
    blind = samples.blind_items(sample, 3)
    assert len(blind & {"u0", "u1", "u2", "u3"}) == 2
    assert len(blind - {"u0", "u1", "u2", "u3"}) == 3
    grown = pl.concat(
        [sample, pl.DataFrame({"item_id": ["v0", "v1"], "stratum": ["c", "c"]})]
    )
    assert samples.blind_items(grown, 3) - {"v0", "v1"} == blind


def _labels(*keys):
    return pl.DataFrame(
        {
            "concept_label_key": list(keys),
            "preferred_label": list(keys),
            "concept_kind": ["concept"] * len(keys),
            "description": [None] * len(keys),
            "courserun_count": [1] * len(keys),
            "mention_count": [1] * len(keys),
        },
        schema_overrides={"description": pl.String},
    )


def test_pairs_population_uses_the_final_clustering():
    inputs = samples.GoldInputs(
        claims=pl.DataFrame(),
        evidence=pl.DataFrame(),
        sources=pl.DataFrame(),
        assignments=pl.DataFrame(
            {
                "concept_label_key": ["a", "b", "c", "d", "e"],
                "concept_id": ["k1", "k1", "k2", "k3", "k1"],
            }
        ),
        registry=pl.DataFrame(
            {"concept_id": ["k1", "k2", "k3"], "merged_into_concept_id": [None] * 3},
            schema={"concept_id": pl.String, "merged_into_concept_id": pl.String},
        ),
        decisions=pl.DataFrame(
            {
                "key_a": ["a", "c", "a"],
                "key_b": ["b", "d", "c"],
                "query_label_key": ["a", "c", "a"],
                "verdict": ["close", "exact", "narrower"],
            }
        ),
        labels=_labels("a", "b", "c", "d", "e"),
        embeddings=pl.DataFrame(
            {
                "concept_label_key": ["a", "b", "c", "d"],
                "embedding": [[1.0, 0.0], [0.9, 0.1], [0.0, 1.0], [0.1, 0.9]],
            }
        ),
        concepts=pl.DataFrame(),
        relations=pl.DataFrame(),
        matcher=LabelMatcher(),
    )
    population = samples.pairs_population(inputs)
    by_pair = {
        row["item_id"]: (row["stratum"], row["resolver_relation"], row["autorater"])
        for row in population.iter_rows(named=True)
    }
    # judged close but merged through another label: counted as merged
    assert by_pair["a||b"] == ("merged", "exact", 1.0)
    # judged exact but not merged (e.g. a rejected component): nothing links them
    assert by_pair["c||d"] == ("exact", "distinct", 0.0)
    assert by_pair["a||c"] == ("narrower", "a_narrower", 0.0)
    # co-assigned but neither adjudicated nor embedded
    assert by_pair["a||e"] == ("merged", "exact", 1.0)


def _span_task(item_id, concepts):
    return {
        "data": {"item_id": item_id},
        "annotations": [
            {
                "completed_by": 1,
                "created_at": "2026-10-17T00:00:00+00:00",
                "result": [
                    result
                    for index, (label, status) in enumerate(concepts)
                    for result in (
                        {
                            "id": f"r{index}",
                            "from_name": "concept",
                            "type": "labels",
                            "value": {
                                "start": 0,
                                "end": 1,
                                "text": "x",
                                "labels": [status],
                            },
                        },
                        {
                            "id": f"r{index}",
                            "from_name": "concept_label",
                            "type": "textarea",
                            "value": {"start": 0, "end": 1, "text": [label]},
                        },
                    )
                ],
            }
        ],
    }


def test_score_subsections_reports_blind_recall_separately(tmp_path):
    items = [f"u{n}" for n in range(4)]
    population = pl.DataFrame(
        {
            "item_id": items,
            "scope_id": items,
            "stratum": ["s"] * 4,
            "models": ["haiku"] * 4,
            "found_hat_haiku": [1, 1, 2, 2],
            "gold_hat": [2, 2, 2, 2],
        }
    )
    sample = pl.DataFrame({"item_id": items, "blind": [True, True, False, False]})
    model_concepts = pl.DataFrame(
        {
            "scope_id": items,
            "model": ["haiku"] * 4,
            "canonical": [LabelMatcher().canonical("pandas")] * 4,
            "status": ["taught"] * 4,
        }
    )
    tasks = [
        # blind: annotators find a concept the model missed
        _span_task("u0", [("pandas", "taught"), ("numpy", "used")]),
        _span_task("u1", [("pandas", "used"), ("numpy", "used")]),
        _span_task("u2", [("pandas", "taught")]),
        _span_task("u3", [("pandas", "taught")]),
    ]
    report, unmatched = score.score_subsections(
        population,
        sample,
        _export(tmp_path, tasks),
        model_concepts,
        LabelMatcher(),
        alpha=0.05,
    )
    haiku = report["models"]["haiku"]
    assert haiku["concept_recall"]["strata"]["s"]["labeled_ratio"] == pytest.approx(0.5)
    assert haiku["concept_recall_prefilled"]["strata"]["s"]["labeled_ratio"] == 1.0
    assert haiku["status_recall"]["strata"]["s"]["labeled_ratio"] == pytest.approx(0.25)
    assert sorted(unmatched["item_id"]) == ["u0", "u1"]


def test_pairs_leave_out_labels_no_current_claim_uses():
    # x was a label in an earlier run: its decisions and embedding remain
    inputs = samples.GoldInputs(
        claims=pl.DataFrame(),
        evidence=pl.DataFrame(),
        sources=pl.DataFrame(),
        assignments=pl.DataFrame(
            {
                "concept_label_key": ["a", "b", "c", "d", "x"],
                "concept_id": ["k1", "k1", "k2", "k3", "k1"],
            }
        ),
        registry=pl.DataFrame(
            {"concept_id": ["k1", "k2", "k3"], "merged_into_concept_id": [None] * 3},
            schema={"concept_id": pl.String, "merged_into_concept_id": pl.String},
        ),
        decisions=pl.DataFrame(
            {
                "key_a": ["a", "c", "a", "a", "c"],
                "key_b": ["x", "x", "c", "d", "d"],
                "query_label_key": ["a", "c", "a", "a", "c"],
                "verdict": ["exact", "distinct", "distinct", "distinct", "distinct"],
            }
        ),
        labels=_labels("a", "b", "c", "d"),
        embeddings=pl.DataFrame(
            {
                "concept_label_key": ["a", "b", "c", "d", "x"],
                "embedding": [
                    [1.0, 0.0],
                    [0.9, 0.1],
                    [0.0, 1.0],
                    [0.1, 0.9],
                    [0.5, 0.5],
                ],
            }
        ),
        concepts=pl.DataFrame(),
        relations=pl.DataFrame(),
        matcher=LabelMatcher(),
    )
    population = samples.pairs_population(inputs)
    assert not population.filter(
        (pl.col("key_a") == "x") | (pl.col("key_b") == "x")
    ).height
    population, _ = samples.coarsen_strata(population)
    sample = samples.sample_pairs(inputs, population, size=100, seed=1)
    assert sample.height == population.height
