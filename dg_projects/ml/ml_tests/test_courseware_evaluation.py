import dataclasses
import json
from datetime import UTC, datetime
from itertools import combinations

import numpy as np
import polars as pl
import pytest
from ml.lib.courseware.evaluation import cli, labelstudio, samples, score
from ml.lib.courseware.evaluation.matching import LabelMatcher
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
from ml.lib.courseware.tagging import label_key

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


def test_ppi_with_no_positive_corrected_denominator_is_classical():
    # lam = 1 gives 1 + (1 - 2) = 0 for the denominator; the labels give 1
    stratum = Stratum(
        "s",
        4,
        np.ones(2),
        g=np.ones(2),
        y_hat=np.full(2, 2.0),
        g_hat=np.full(2, 2.0),
        y_hat_unlabeled=np.ones(2),
        g_hat_unlabeled=np.ones(2),
    )
    for tuning in (True, False):
        estimate = stratified_ppi([stratum], power_tuning=tuning)
        assert estimate.point == 1.0
        assert estimate.lambdas == {"s": 0.0}


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


# expected keys of the courseware_label_key macro (src/ol_dbt/macros)
@pytest.mark.parametrize(
    ("label", "key"),
    [
        ("C", "c"),
        ("C++", "c plus plus"),
        ("C#", "c sharp"),
        ("F#", "f sharp"),
        (".NET", "dot net"),
        ("ASP.NET", "asp net"),
        ("the .csv file", "the dot csv file"),
        ("a + b", "a b"),
        ("+ 5", "5"),
        ("# of rows", "of rows"),
        ("R", "r"),
        ("R²", "r²"),
        ("adjusted R²", "adjusted r²"),
        ("Schrödinger equation", "schrödinger equation"),
        ("Naïve Bayes", "naïve bayes"),
        ("K-Means  Clustering!", "k means clustering"),
        ("snake_case", "snake case"),
        ("  area under the ROC curve (AUC) ", "area under the roc curve auc"),
        ("+", None),
        ("--", None),
        ("#", None),
        (".", None),
        ("..", None),
        (".+", None),
        ("+.", None),
        ("#.", None),
    ],
)
def test_concept_label_key_matches_the_sql_macro(label, key):
    assert label_key(label) == key


def test_the_matcher_keys_a_label_as_the_sql_macro_does():
    # a no-break space is not \s to the SQL engines (ASCII only), so the + after it
    # is spelled out rather than taken as a separator
    matcher = LabelMatcher()
    assert matcher.canonical("a\u00a0+ b") == matcher.canonical("a plus b")
    assert matcher.canonical("a\u00a0+ b") != matcher.canonical("a b")


def test_labels_without_a_key_match_only_themselves():
    matcher = LabelMatcher()
    assert matcher.canonical("+") == matcher.canonical(" + ")
    assert matcher.canonical("+") != matcher.canonical("#")
    assert matcher.canonical("C++") != matcher.canonical("C#")
    assert matcher.canonical("C++") != matcher.canonical("C")


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
    assert label_key("K-Means  Clustering!") == "k means clustering"
    assert matcher.canonical("Recall") == matcher.canonical("true-positive rate")
    assert matcher.canonical("SHAP") == matcher.canonical("SHAP values")
    assert matcher.canonical("recall") != matcher.canonical("precision")


def test_label_matcher_round_trips_and_takes_more_mappings(tmp_path):
    matcher = LabelMatcher().add_assignments(
        pl.DataFrame(
            {
                "concept_label_key": ["recall", "true positive rate"],
                "concept_id": ["c1", "c1"],
            }
        )
    )
    matcher.to_frame().write_parquet(tmp_path / "matcher.parquet")
    restored = LabelMatcher.from_frame(pl.read_parquet(tmp_path / "matcher.parquet"))
    assert restored.canonical("Recall") == matcher.canonical("true-positive rate")
    # a reviewer's label map joins an annotator's name to the stored class
    restored.add_mapping([("sensitivity", "recall")])
    assert restored.canonical("sensitivity") == restored.canonical("true positive rate")


_NO_RELATIONS = pl.DataFrame(
    schema={
        "from_concept_id": pl.String,
        "to_concept_id": pl.String,
        "relation": pl.String,
    }
)


_UAI4 = "course-v1:UAI_SOURCE+UAI.4+1T2026"


def _senses(assignments, run=_UAI4, status="assigned"):
    """Sense assignments of every label in one run, each to its given concept."""
    return assignments.select(
        "concept_label_key",
        pl.lit("mitxonline").alias("platform"),
        pl.lit(run).alias("courserun_readable_id"),
        "concept_id",
        pl.lit(status).alias("sense_status"),
    )


_NO_SENSES = _senses(
    pl.DataFrame(schema={"concept_label_key": pl.String, "concept_id": pl.String})
)


def _inputs(claims: pl.DataFrame) -> samples.GoldInputs:
    empty = pl.DataFrame()
    return samples.GoldInputs(
        extractions=claims.select(
            "extraction_key",
            "model_id",
            "platform",
            "courserun_readable_id",
            "scope_id",
            "scope_part",
        ).unique(),
        claims=claims,
        evidence=empty,
        sources=empty,
        assignments=empty,
        registry=empty,
        concept_relations=_NO_RELATIONS,
        sense_assignments=_NO_SENSES,
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
            "claim_kind": ["concept"] * 8,
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
        allocate({f"s{n}": 100 for n in range(24)}, 40, minimum=20)
    with pytest.raises(ValueError, match="floor of 4 need 72"):
        allocate({f"s{n}": 6 for n in range(18)}, 30, minimum=4, required=4)
    with pytest.raises(ValueError, match="smaller than 2"):
        allocate({"big": 100, "one": 1}, 50)
    # every stratum gets at least the two labels stratified_ppi needs
    assert min(allocate({"big": 1_000, "small": 3}, 20).values()) >= 2


def test_allocate_lowers_the_wanted_floor_to_fit_the_budget():
    allocation = allocate({f"s{n}": 100 for n in range(24)}, 400, minimum=20)
    assert sum(allocation.values()) == 400
    assert min(allocation.values()) == 16


def _subsection_strata(families):
    rows = [
        (f"F{family}", band, models)
        for family in range(families)
        for band in ("short", "long")
        for models in ("haiku", "opus", "haiku+opus")
        for _ in range(6)
    ]
    return pl.DataFrame(
        {
            "item_id": [f"u{n:03d}" for n in range(len(rows))],
            "course_family": [family for family, _, _ in rows],
            "size_band": [band for _, band, _ in rows],
            "models": [models for _, _, models in rows],
        }
    ).with_columns(
        pl.format("{}|{}|{}", "course_family", "size_band", "models").alias("stratum")
    )


def test_fit_strata_pools_course_families_when_the_floors_exceed_the_budget():
    population = _subsection_strata(3)
    floor = samples.SUBSECTIONS_MIN_PER_STRATUM
    # one family fits as is: 6 strata at 4 labels each
    one, _ = samples.fit_strata(_subsection_strata(1), "subsections", 30, floor)
    assert one["stratum"].n_unique() == 6
    # three families need 72 labels, so they are pooled into 6 strata again
    pooled, dropped = samples.fit_strata(population, "subsections", 30, floor)
    assert dropped.is_empty()
    assert set(pooled["stratum"]) == {
        f"{band}|{models}"
        for band in ("short", "long")
        for models in ("haiku", "opus", "haiku+opus")
    }
    sizes = dict(pooled.group_by("stratum").len().iter_rows())
    allocation = allocate(sizes, 30, minimum=floor, required=floor)
    assert sum(allocation.values()) == 30
    # a budget no stratification fits still fails loudly
    coarsest, _ = samples.fit_strata(population, "subsections", 10, floor)
    assert coarsest["stratum"].n_unique() == 3
    with pytest.raises(ValueError, match="more than the budget of 10"):
        allocate(
            dict(coarsest.group_by("stratum").len().iter_rows()),
            10,
            minimum=floor,
            required=floor,
        )


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
            "claim_kind": ["concept"] * len(strata),
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
        extractions=pl.DataFrame(),
        claims=pl.DataFrame(),
        evidence=pl.DataFrame(),
        sources=pl.DataFrame(),
        assignments=pl.DataFrame(
            {
                "concept_label_key": ["a", "b", "c", "d", "e", "f"],
                "concept_id": ["k1", "k1", "k2", "k3", "k1", "k5"],
            }
        ),
        registry=pl.DataFrame(
            {
                "concept_id": ["k1", "k2", "k3", "k4", "k5"],
                "merged_into_concept_id": [None, None, None, "k5", None],
            },
            schema={"concept_id": pl.String, "merged_into_concept_id": pl.String},
        ),
        # the resolver's output: k1 narrower than k2; k4 (since merged into k5)
        # related to k1, as two opposing narrower verdicts leave a pair
        concept_relations=pl.DataFrame(
            {
                "from_concept_id": ["k1", "k1", "k4"],
                "to_concept_id": ["k2", "k2", "k1"],
                "relation": ["broader", "related", "related"],
            }
        ),
        sense_assignments=_NO_SENSES,
        decisions=pl.DataFrame(
            {
                "key_a": ["a", "c", "a", "a"],
                "key_b": ["b", "d", "c", "f"],
                "query_label_key": ["a", "c", "a", "a"],
                "verdict": ["close", "exact", "narrower", "narrower"],
            }
        ),
        labels=_labels("a", "b", "c", "d", "e", "f"),
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
    # the retained verdict says narrower, the resolver kept related
    assert by_pair["a||f"] == ("narrower", "related", 0.0)
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


# the course run of the subsection-scoring fixtures, a column per frame
_RUN = {"platform": "mitxonline", "courserun_readable_id": "run1"}


def test_score_subsections_reports_blind_recall_separately(tmp_path):
    items = [f"u{n}" for n in range(4)]
    population = pl.DataFrame(
        {
            "item_id": items,
            **_RUN,
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
            **_RUN,
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


def test_subsections_in_two_runs_with_one_scope_id_stay_apart(tmp_path):
    # OCW runs reuse scope ids; each run's model concepts score only its own scope
    runs = ["run1", "run1", "run2", "run2"]
    scopes = ["u0", "u1", "u0", "u1"]
    population = pl.DataFrame(
        {
            "platform": ["ocw"] * 4,
            "courserun_readable_id": runs,
            "scope_id": scopes,
            "stratum": ["s"] * 4,
            "models": ["haiku"] * 4,
            "found_hat_haiku": [1] * 4,
            "gold_hat": [1] * 4,
        }
    ).with_columns(
        pl.format("{}|{}|{}", "platform", "courserun_readable_id", "scope_id").alias(
            "item_id"
        )
    )
    items = population["item_id"].to_list()
    sample = pl.DataFrame({"item_id": items, "blind": [True] * 4})
    model_concepts = pl.DataFrame(
        {
            "platform": ["ocw"] * 2,
            "courserun_readable_id": ["run1"] * 2,
            "scope_id": ["u0", "u1"],
            "model": ["haiku"] * 2,
            "canonical": [LabelMatcher().canonical("pandas")] * 2,
            "status": ["taught"] * 2,
        }
    )
    tasks = [_span_task(item, [("pandas", "taught")]) for item in items]
    exported = _export(tmp_path, tasks)
    report, unmatched = score.score_subsections(
        population, sample, exported, model_concepts, LabelMatcher(), alpha=0.05
    )
    recall = report["models"]["haiku"]["concept_recall"]["strata"]["s"]
    assert recall["labeled_ratio"] == 0.5
    assert sorted(unmatched["item_id"]) == items[2:]
    roles = model_concepts.select(
        *score.SCOPE, "model", "canonical", pl.lit("objective").alias("role")
    )
    # a scope the population does not hold (a context scope, say) is counted only
    outside = roles.head(1).with_columns(pl.lit("overview").alias("scope_id"))
    objectives = score.score_objectives(
        population,
        exported,
        LabelMatcher(),
        alpha=0.05,
        model_roles=pl.concat([roles, outside]),
    )
    assert objectives["models"]["haiku"]["scopes_outside_population"] == 1


def test_pairs_leave_out_labels_no_current_claim_uses():
    # x was a label in an earlier run: its decisions and embedding remain
    inputs = samples.GoldInputs(
        extractions=pl.DataFrame(),
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
        concept_relations=_NO_RELATIONS,
        sense_assignments=_NO_SENSES,
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


def _course_titles(*runs):
    return pl.DataFrame(
        {"courserun_readable_id": list(runs), "course_title": [f"T {r}" for r in runs]}
    )


def test_clusters_are_built_from_course_run_senses():
    # x was a label of k1 in an earlier extraction; k3 has no current label at all
    keys = ["a", "b", "c", "d", "e", "f", "g"]
    assignments = pl.DataFrame(
        {
            "concept_label_key": [*keys, "x", "y"],
            "concept_id": ["k1", "k1", "k2", "k4", "k5", "k5", "k6", "k1", "k3"],
        }
    )
    senses = pl.concat(
        [
            _senses(assignments),
            # run2 uses a in k2's sense, and d in a sense not yet decided
            _senses(
                pl.DataFrame({"concept_label_key": ["a"], "concept_id": ["k2"]}),
                run="run2",
            ),
            _senses(
                pl.DataFrame(
                    {"concept_label_key": ["d"], "concept_id": [None]},
                    schema={"concept_label_key": pl.String, "concept_id": pl.String},
                ),
                run="run2",
                status="needs_review",
            ),
        ]
    )
    inputs = samples.GoldInputs(
        extractions=pl.DataFrame(),
        claims=pl.DataFrame(),
        evidence=pl.DataFrame(),
        sources=_course_titles(_UAI4, "run2"),
        assignments=assignments,
        registry=pl.DataFrame(
            {
                "concept_id": ["k1", "k2", "k3", "k4", "k5", "k6"],
                "merged_into_concept_id": [None] * 6,
            },
            schema={"concept_id": pl.String, "merged_into_concept_id": pl.String},
        ),
        concept_relations=_NO_RELATIONS,
        sense_assignments=senses,
        decisions=pl.DataFrame(),
        labels=_labels(*keys),
        embeddings=pl.DataFrame(
            {
                "concept_label_key": [*keys, "x", "y"],
                "embedding": [[1.0, float(n)] for n in range(len(keys) + 2)],
            }
        ),
        concepts=pl.DataFrame(
            {
                "concept_pk": ["k1", "k2", "k4", "k5", "k6"],
                "preferred_label": list("ACDEG"),
            }
        ),
        relations=pl.DataFrame(),
        matcher=LabelMatcher(),
    )
    population = samples.clusters_population(inputs)
    assert population.select("item_id", "members", "stratum").to_dicts() == [
        {"item_id": "k1", "members": ["a", "b"], "stratum": "multi"},
        {"item_id": "k2", "members": ["a", "c"], "stratum": "multi"},
        {"item_id": "k4", "members": ["d"], "stratum": "singleton"},
        {"item_id": "k5", "members": ["e", "f"], "stratum": "multi"},
        {"item_id": "k6", "members": ["g"], "stratum": "singleton"},
    ]
    sample = samples.sample_clusters(inputs, population, size=10, seed=1)
    assert sorted(sample["item_id"]) == ["k1", "k2", "k4", "k5", "k6"]
    [k2] = sample.filter(pl.col("item_id") == "k2")["items_json"]
    bodies = {item["id"]: item["body"] for item in json.loads(k2)}
    assert bodies["m:a"].endswith("used in: T run2")
    assert bodies["m:c"].endswith(f"used in: T {_UAI4}")


def test_subsections_population_with_a_model_that_named_no_concept():
    claims = pl.DataFrame(
        [
            _claim("opus", "u1", 1, "Linear regression"),
            _claim("haiku", "u1", 1, "Fit a model", kind="skill"),
        ]
    )
    sources = pl.DataFrame(
        {
            "platform": ["mitxonline"],
            "courserun_readable_id": ["course-v1:UAI_SOURCE+UAI.4+1T2026"],
            "scope_id": ["u1"],
            "scope_kind": ["unit"],
            "scope_title": ["u1"],
            "scope_position": [1],
            "source_text": ["text"],
        }
    )
    inputs = dataclasses.replace(_inputs(claims), sources=sources)
    [row] = samples.subsections_population(inputs).to_dicts()
    assert (row["found_hat_haiku"], row["found_hat_opus"], row["gold_hat"]) == (0, 1, 1)
    assert row["models"] == "haiku+opus"
    # what the sample step writes for scoring: the skill is not a concept
    concepts = samples.scope_concepts(samples.concept_claims(inputs), inputs.matcher)
    assert concepts.select("scope_id", "model", "status").to_dicts() == [
        {"scope_id": "u1", "model": "opus", "status": "taught"}
    ]


def test_score_subsections_scores_a_model_that_named_nothing(tmp_path):
    items = [f"u{n}" for n in range(4)]
    population = pl.DataFrame(
        {
            "item_id": items,
            **_RUN,
            "scope_id": items,
            "stratum": ["s"] * 4,
            "models": ["haiku+opus"] * 4,
            "found_hat_haiku": [1] * 4,
            "found_hat_opus": [0] * 4,
            "gold_hat": [1] * 4,
        }
    )
    sample = pl.DataFrame({"item_id": items, "blind": [True, True, False, False]})
    model_concepts = pl.DataFrame(
        {
            **_RUN,
            "scope_id": items,
            "model": ["haiku"] * 4,
            "canonical": [LabelMatcher().canonical("pandas")] * 4,
            "status": ["taught"] * 4,
        }
    )
    tasks = [_span_task(item, [("pandas", "taught")]) for item in items]
    report, _ = score.score_subsections(
        population,
        sample,
        _export(tmp_path, tasks),
        model_concepts,
        LabelMatcher(),
        alpha=0.05,
    )
    assert report["models"]["haiku"]["concept_recall"]["point"] == 1.0
    assert report["models"]["opus"]["concept_recall"]["point"] == 0.0


def test_few_labels_are_not_their_own_neighbours():
    # with no more labels than the neighbour count, the top ranks hold the query too
    keys = ["a", "b", "c", "d"]
    assignments = pl.DataFrame(
        {"concept_label_key": keys, "concept_id": ["k1", "k2", "k3", "k3"]}
    )
    inputs = samples.GoldInputs(
        extractions=pl.DataFrame(),
        claims=pl.DataFrame(),
        evidence=pl.DataFrame(),
        sources=_course_titles(_UAI4),
        assignments=assignments,
        registry=pl.DataFrame(
            {"concept_id": ["k1", "k2", "k3"], "merged_into_concept_id": [None] * 3},
            schema={"concept_id": pl.String, "merged_into_concept_id": pl.String},
        ),
        concept_relations=_NO_RELATIONS,
        sense_assignments=_senses(assignments),
        decisions=pl.DataFrame(
            schema={
                "key_a": pl.String,
                "key_b": pl.String,
                "query_label_key": pl.String,
                "verdict": pl.String,
            }
        ),
        labels=_labels(*keys),
        embeddings=pl.DataFrame(
            {
                "concept_label_key": keys,
                "embedding": [[1.0, 0.0], [0.9, 0.1], [0.0, 1.0], [0.1, 0.9]],
            }
        ),
        concepts=pl.DataFrame(
            {"concept_pk": ["k1", "k2", "k3"], "preferred_label": list("ABC")}
        ),
        relations=pl.DataFrame(),
        matcher=LabelMatcher(),
    )
    pairs = samples.pairs_population(inputs)
    assert pairs.filter(pl.col("key_a") == pl.col("key_b")).is_empty()
    assert pairs.height == 6
    clusters = samples.clusters_population(inputs)
    sample = samples.sample_clusters(
        inputs, clusters.with_columns(pl.lit("s").alias("stratum")), size=3, seed=1
    )
    for row in sample.iter_rows(named=True):
        shown = {item["id"] for item in json.loads(row["items_json"])}
        assert f"n:{row['item_id']}" not in shown
        assert row["item_id"] not in json.loads(row["neighbour_sizes_json"])


def _extraction(key, model, scope, *, part=1, checksums=None, day=17):
    checksums = checksums or f"{scope}-text"
    return {
        "extraction_key": key,
        "extractor_id": "x1",
        "model_id": f"us.anthropic.claude-{model}-5-5",
        "prompt_version": "v1",
        "schema_version": "s1",
        "effort": "high",
        "platform": "mitxonline",
        "courserun_readable_id": "course-v1:UAI_SOURCE+UAI.4+1T2026",
        "scope_id": scope,
        "scope_kind": "unit",
        "scope_title": scope,
        "scope_position": 1,
        "scope_part": part,
        "scope_part_count": len(checksums.split(",")),
        "input_text_checksum": checksums.split(",")[part - 1],
        "scope_part_checksums": checksums,
        "extracted_at": datetime(2026, 10, day, tzinfo=UTC),
    }


def _sources(*scopes):
    """extraction_sources rows for unit scopes of _extraction's course run."""
    return pl.DataFrame(
        {
            "platform": ["mitxonline"] * len(scopes),
            "courserun_readable_id": ["course-v1:UAI_SOURCE+UAI.4+1T2026"]
            * len(scopes),
            "scope_id": list(scopes),
            "scope_kind": ["unit"] * len(scopes),
            "scope_title": list(scopes),
            "scope_position": list(range(1, len(scopes) + 1)),
            "source_text": ["text"] * len(scopes),
        }
    )


def test_current_extractions_take_each_models_newest_complete_reading():
    extractions = pl.DataFrame(
        [
            # u1: an older reading in two parts, then a complete one-part reading
            _extraction("o1-old1", "opus", "u1", part=1, checksums="a,b", day=1),
            _extraction("o1-old2", "opus", "u1", part=2, checksums="a,b", day=1),
            _extraction("o1-new", "opus", "u1", checksums="c", day=2),
            # u2: a new two-part reading with only its first part extracted
            _extraction("o2-old", "opus", "u2", checksums="d", day=1),
            _extraction("o2-new1", "opus", "u2", part=1, checksums="e,f", day=2),
            # another model's reading is current on its own
            _extraction("h1", "haiku", "u1", checksums="c", day=1),
            # a part whose text did not change keeps its older extraction
            _extraction("h2-1", "haiku", "u2", part=1, checksums="g,h", day=1),
            _extraction("h2-2", "haiku", "u2", part=2, checksums="g,i", day=2),
            _extraction("h2-2old", "haiku", "u2", part=2, checksums="g,h", day=1),
        ]
    )
    current = samples.current_extractions(extractions, _sources("u1", "u2"))
    assert sorted(current["extraction_key"]) == [
        "h1",
        "h2-1",
        "h2-2",
        "o1-new",
        "o2-old",
    ]


def _write_gold_inputs(
    directory,
    *,
    assignments,
    registry,
    decisions,
    extractions=None,
    claims=None,
    sources=None,
    concept_relations=_NO_RELATIONS,
    sense_assignments=None,
):
    """Write the parquet files load_inputs reads; one run, by default one claim."""
    extraction = directory / "extraction"
    identity = directory / "identity"
    extraction.mkdir(parents=True)
    identity.mkdir()
    if extractions is None:
        extractions = pl.DataFrame([_extraction("e1", "opus", "u1")])
    if claims is None:
        claims = pl.DataFrame({"extraction_key": ["e1"], "claim_ordinal": [1]})
    extractions.write_parquet(extraction / "courseware_claim_extraction.parquet")
    claims.write_parquet(extraction / "courseware_claim.parquet")
    claims.select("extraction_key", "claim_ordinal").with_columns(
        pl.lit(1).alias("evidence_ordinal")
    ).write_parquet(extraction / "courseware_claim_evidence.parquet")
    run = "r1"
    pl.DataFrame(
        {"resolution_run_id": [run], "started_at": [datetime(2026, 10, 17, tzinfo=UTC)]}
    ).write_parquet(identity / "courseware_resolution_run.parquet")
    for table, frame in (
        ("courseware_concept_assignment", assignments),
        ("courseware_concept_registry", registry),
        ("courseware_concept_relation", concept_relations),
        (
            "courseware_concept_sense_assignment",
            _senses(assignments) if sense_assignments is None else sense_assignments,
        ),
        ("courseware_resolution_decision", decisions),
    ):
        frame.with_columns(pl.lit(run).alias("resolution_run_id")).write_parquet(
            identity / f"{table}.parquet"
        )
    keys = assignments["concept_label_key"].to_list()
    pl.DataFrame(
        {
            "concept_label_key": keys,
            "embedding": [[1.0, float(n)] for n in range(len(keys))],
        }
    ).write_parquet(identity / "courseware_concept_label_embedding.parquet")
    _labels(*keys).write_parquet(directory / "concept_labels.parquet")
    empty = pl.DataFrame({"unused": [None]})
    for name in ("concepts", "relations"):
        empty.write_parquet(directory / f"{name}.parquet")
    (_sources("u1") if sources is None else sources).write_parquet(
        directory / "sources.parquet"
    )
    return samples.load_inputs(
        [extraction],
        directory / "sources.parquet",
        identity,
        directory / "concept_labels.parquet",
        directory / "concepts.parquet",
        directory / "relations.parquet",
    )


def test_a_claim_with_no_located_passage_shows_the_whole_source():
    text = "x" * 5_000 + "the supporting sentence"
    texts = {("run1", "s1"): ("Lecture", text)}
    claim = {"courserun_readable_id": "run1"}
    unlocated = {"source_ref": "s1", "char_start": None, "char_end": None}
    window, regions = samples._claim_window(claim, [unlocated], texts)
    assert window == f"=== Lecture ===\n{text}\n\n"
    assert regions == []
    located = {"source_ref": "s1", "char_start": 10, "char_end": 20}
    window, _ = samples._claim_window(claim, [located], texts)
    assert "the supporting sentence" not in window


def test_matcher_follows_merged_concepts_to_their_survivor(tmp_path):
    inputs = _write_gold_inputs(
        tmp_path,
        assignments=pl.DataFrame(
            {"concept_label_key": ["recall", "sensitivity"], "concept_id": ["k1", "k2"]}
        ),
        registry=pl.DataFrame(
            {"concept_id": ["k1", "k2"], "merged_into_concept_id": [None, "k1"]}
        ),
        decisions=pl.DataFrame(
            {
                "query_label_key": ["recall"],
                "candidate_label_key": ["sensitivity"],
                "verdict": ["exact"],
            }
        ),
    )
    assert inputs.matcher.canonical("Recall") == inputs.matcher.canonical("sensitivity")


def test_pairs_do_not_depend_on_the_order_decisions_are_read_in(tmp_path):
    keys = [f"l{n}" for n in range(6)]
    # each pair judged both ways in one run, with verdicts that disagree
    decisions = pl.DataFrame(
        [
            (query, candidate, verdict)
            for index, (a, b) in enumerate(combinations(keys, 2))
            for query, candidate, verdict in (
                (a, b, ("broader", "close", "distinct")[index % 3]),
                (b, a, ("related", "distinct", "close")[index % 3]),
            )
        ],
        schema=["query_label_key", "candidate_label_key", "verdict"],
        orient="row",
    )
    frames = {
        "assignments": pl.DataFrame(
            {"concept_label_key": keys, "concept_id": [f"k{n}" for n in range(6)]}
        ),
        "registry": pl.DataFrame(
            {
                "concept_id": [f"k{n}" for n in range(6)],
                "merged_into_concept_id": [None] * 6,
            },
            schema={"concept_id": pl.String, "merged_into_concept_id": pl.String},
        ),
    }
    drawn = []
    for name, rows in (
        ("read_forward", decisions),
        ("read_reversed", decisions.reverse()),
        ("read_shuffled", decisions.sample(fraction=1.0, shuffle=True, seed=3)),
    ):
        inputs = _write_gold_inputs(tmp_path / name, decisions=rows, **frames)
        population, _ = samples.coarsen_strata(samples.pairs_population(inputs))
        drawn.append(samples.sample_pairs(inputs, population, size=10, seed=1))
    assert drawn[0].equals(drawn[1])
    assert drawn[0].equals(drawn[2])


def test_label_quality_only_judges_concept_claims(tmp_path):
    population = pl.DataFrame(
        {
            "item_id": [f"c{n}" for n in range(4)],
            "model": ["haiku"] * 4,
            "claim_kind": ["skill", "skill", "concept", "concept"],
            "stratum": ["s"] * 4,
            "autorater": [None] * 4,
        },
        schema_overrides={"autorater": pl.Float64},
    )
    supported = {"verdict": "supported", "status": "not_applicable"}
    tasks = [
        # a skill is not a concept, whichever label judgment the annotator gave
        _choice_task("c0", [(1, {**supported, "label_quality": "not_a_concept"})]),
        _choice_task("c1", [(1, {**supported, "label_quality": "not_applicable"})]),
        _choice_task(
            "c2", [(1, {**supported, "status": "correct", "label_quality": "good"})]
        ),
        _choice_task(
            "c3",
            [(1, {**supported, "status": "correct", "label_quality": "not_a_concept"})],
        ),
    ]
    report = score.score_claims(population, _export(tmp_path, tasks), alpha=0.05)
    assert report["models"]["haiku"]["correct"]["point"] == pytest.approx(0.75)
    assert "not_applicable" in labelstudio.LABEL_JUDGMENTS


def test_an_estimate_its_labels_cannot_support_is_skipped_with_the_reason(tmp_path):
    population = pl.DataFrame(
        {
            "item_id": [f"c{n}" for n in range(8)],
            "model": ["haiku"] * 8,
            "claim_kind": ["concept"] * 8,
            "stratum": ["a"] * 4 + ["b"] * 4,
            "autorater": [None] * 8,
        },
        schema_overrides={"autorater": pl.Float64},
    )
    good = {"verdict": "supported", "status": "correct", "label_quality": "good"}
    bad = {**good, "verdict": "unsupported"}
    tasks = [
        _choice_task("c0", [(1, good)]),
        _choice_task("c1", [(1, bad)]),
        # b: one decided label, and a tie
        _choice_task("c4", [(1, good)]),
        _choice_task("c5", [(1, good), (2, bad)]),
    ]
    report = score.score_claims(population, _export(tmp_path, tasks), alpha=0.05)
    correct = report["models"]["haiku"]["correct"]
    assert "point" not in correct
    assert correct["under_filled_strata"] == {"b": 1}
    assert correct["strata"]["a"]["labeled"] == 2
    reason = (
        "strata with fewer than 2 decided labels; "
        "annotate more of their items or break their ties {'b': 1}"
    )
    assert cli._skipped({"claims": report}) == [
        f"claims.models.haiku.correct: {reason}",
        f"claims.models.haiku.supported: {reason}",
    ]


def test_recall_with_no_labelled_denominator_is_skipped():
    strata = [Stratum("s", 10, np.zeros(3), g=np.zeros(3))]
    assert score._estimate(strata, alpha=0.05)["skipped"] == (
        "no labelled item counts toward the denominator"
    )


def _one_concept():
    """Identity frames for _write_gold_inputs: one label of one concept, no verdicts."""
    return {
        "assignments": pl.DataFrame({"concept_label_key": ["x"], "concept_id": ["k1"]}),
        "registry": pl.DataFrame(
            {"concept_id": ["k1"], "merged_into_concept_id": [None]},
            schema={"concept_id": pl.String, "merged_into_concept_id": pl.String},
        ),
        "decisions": pl.DataFrame(
            schema={
                "query_label_key": pl.String,
                "candidate_label_key": pl.String,
                "verdict": pl.String,
            }
        ),
    }


def test_a_model_that_extracted_no_claims_still_counts_for_the_scope(tmp_path):
    # haiku extracted u1 and u2 and returned no claims; opus named nothing in u2
    claim = _claim("opus", "u1", 1, "Linear regression")
    inputs = _write_gold_inputs(
        tmp_path,
        **_one_concept(),
        extractions=pl.DataFrame(
            [
                _extraction("opus-u1", "opus", "u1"),
                _extraction("haiku-u1", "haiku", "u1"),
                _extraction("opus-u2", "opus", "u2"),
                _extraction("haiku-u2", "haiku", "u2"),
            ]
        ),
        claims=pl.DataFrame([claim]).select(
            "extraction_key",
            "claim_ordinal",
            "claim_kind",
            "label",
            "description",
            "teaching_status",
        ),
        sources=_sources("u1", "u2"),
    )
    [row] = samples.claims_population(inputs).to_dicts()
    # haiku saw the scope and did not name it: a disagreement, not unpaired
    assert (row["agreement"], row["autorater"]) == ("opus_only", 0.0)
    subsections = {
        row["scope_id"]: row
        for row in samples.subsections_population(inputs).iter_rows(named=True)
    }
    assert sorted(subsections) == ["u1", "u2"]
    u1, u2 = subsections["u1"], subsections["u2"]
    assert (u1["models"], u1["found_hat_haiku"], u1["found_hat_opus"]) == (
        "haiku+opus",
        0,
        1,
    )
    assert (u2["models"], u2["gold_hat"]) == ("haiku+opus", 0)


def test_a_retired_scope_has_no_current_claims_or_evidence(tmp_path):
    # opus extracted u1 and u2; u2 has since left the course, so it is not in sources
    inputs = _write_gold_inputs(
        tmp_path,
        **_one_concept(),
        extractions=pl.DataFrame(
            [
                _extraction("opus-u1", "opus", "u1"),
                _extraction("opus-u2", "opus", "u2"),
            ]
        ),
        claims=pl.DataFrame(
            [
                _claim("opus", "u1", 1, "Linear regression"),
                _claim("opus", "u2", 1, "k-means"),
            ]
        ).select(
            "extraction_key",
            "claim_ordinal",
            "claim_kind",
            "label",
            "description",
            "teaching_status",
        ),
        sources=_sources("u1"),
    )
    assert inputs.extractions["scope_id"].to_list() == ["u1"]
    assert inputs.evidence["extraction_key"].to_list() == ["opus-u1"]
    # claims and subsections sample the same scopes
    assert samples.claims_population(inputs)["scope_id"].to_list() == ["u1"]
    assert samples.subsections_population(inputs)["scope_id"].to_list() == ["u1"]


def _annotation(annotator, minute, results):
    return {
        "completed_by": annotator,
        "created_at": datetime(2026, 10, 28, minute, tzinfo=UTC).isoformat(),
        "result": results,
    }


def _prerequisite_task(item_id, answers):
    """answers: (annotator, hierarchy, dependency, dependency depths) per annotator."""
    return {
        "data": {"item_id": item_id},
        "annotations": [
            _annotation(
                annotator,
                minute,
                [
                    {
                        "id": f"h{minute}",
                        "from_name": "hierarchy",
                        "type": "choices",
                        "value": {"choices": [hierarchy]},
                    },
                    {
                        "id": f"d{minute}",
                        "from_name": "dependency",
                        "type": "choices",
                        "value": {"choices": [dependency]},
                    },
                    {
                        "id": f"dd{minute}",
                        "from_name": "dependency_depth",
                        "type": "choices",
                        "value": {"choices": list(depths)},
                    },
                ],
            )
            for minute, (annotator, hierarchy, dependency, depths) in enumerate(answers)
        ],
    }


def test_prerequisites_score_hierarchy_and_dependency_separately(tmp_path):
    items = [f"a{n}||b{n}" for n in range(8)]
    population = pl.DataFrame(
        {
            "item_id": items,
            "concept_a": [f"a{n}" for n in range(8)],
            "stratum": ["builds_on|support1"] * 3
            + ["non_edge|hierarchy"] * 3
            + ["practiced_with|support1"] * 2,
            "edge_direction": ["b_before_a"] * 3 + ["none"] * 3 + ["a_before_b"] * 2,
            "builds_on_direction": ["b_before_a"] * 3 + ["none"] * 5,
            "pipeline_hierarchy": [True, False, False, True, True, False] + [False] * 2,
        }
    )
    # item 1 is shown flipped: the annotator's B is the population's a1
    sample = population.select(
        "item_id",
        pl.when(pl.col("item_id") == "a1||b1")
        .then(pl.lit("b1"))
        .otherwise(pl.col("concept_a"))
        .alias("concept_a"),
    )
    intro = ("introductory",)
    tasks = [
        # a kind of b, and still needs b: hierarchy and dependency coexist
        _prerequisite_task(
            "a0||b0",
            [
                (1, "a_kind_of_b", "b_before_a", intro),
                (2, "a_kind_of_b", "b_before_a", ("introductory", "advanced")),
            ],
        ),
        # shown flipped, so a_before_b as shown is b_before_a in population order
        _prerequisite_task(
            "a1||b1", [(1, "none", "a_before_b", intro), (2, "none", "a_before_b", ())]
        ),
        _prerequisite_task(
            "a2||b2",
            [
                (1, "none", "context_dependent", ()),
                (2, "none", "context_dependent", ()),
            ],
        ),
        _prerequisite_task(
            "a3||b3", [(1, "b_part_of_a", "none", ()), (2, "b_part_of_a", "none", ())]
        ),
        _prerequisite_task(
            "a4||b4",
            [(1, "a_kind_of_b", "uncertain", ()), (2, "uncertain", "none", ())],
        ),
        _prerequisite_task(
            "a5||b5",
            [(1, "none", "a_before_b", intro), (2, "none", "a_before_b", intro)],
        ),
    ]
    # a tool and what is practiced with it: no dependency, as the guideline says
    tasks += [
        _prerequisite_task(
            "a6||b6", [(1, "none", "none", ()), (2, "none", "none", ())]
        ),
        # a dependency the practiced_with edge happens to point the same way as
        _prerequisite_task(
            "a7||b7", [(1, "none", "a_before_b", ()), (2, "none", "a_before_b", ())]
        ),
    ]
    report = score.score_prerequisites(
        population, sample, _export(tmp_path, tasks), alpha=0.05
    )
    # the headline is builds_on edges only; practiced_with is its own stratum of
    # the mixed sequence-edge figure
    assert set(report["edge_precision"]["strata"]) == {"builds_on|support1"}
    edges = report["edge_precision"]["strata"]["builds_on|support1"]
    assert edges["labeled_ratio"] == pytest.approx(2 / 3)
    sequence = report["sequence_edge_precision"]["strata"]
    assert sequence["practiced_with|support1"]["labeled_ratio"] == pytest.approx(0.5)
    assert sequence["builds_on|support1"]["labeled_ratio"] == pytest.approx(2 / 3)
    # a7's dependency has only a practiced_with edge, which does not find it
    recall = report["edge_recall"]["strata"]
    assert recall["practiced_with|support1"]["labeled_ratio"] == 0.0
    assert recall["builds_on|support1"]["labeled_ratio"] == 1.0
    sequence_recall = report["sequence_edge_recall"]["strata"]
    assert sequence_recall["practiced_with|support1"]["labeled_ratio"] == 1.0
    lenient = report["edge_precision_lenient"]["strata"]["builds_on|support1"]
    assert lenient["labeled_ratio"] == 1.0
    # a0 is both a kind of b0 and dependent on it; a3 is a part, not dependent
    given = report["dependency_given_hierarchy"]["strata"]
    assert given["builds_on|support1"]["labeled_ratio"] == 1.0
    assert given["non_edge|hierarchy"]["labeled_ratio"] == 0.0
    assert report["ties"] == {"hierarchy": ["a4||b4"], "dependency": ["a4||b4"]}
    assert report["hierarchy_by_dependency"]["a_kind_of_b|b_before_a"] == 1
    assert report["depths"]["dependency"] == {
        "introductory": 5,
        "advanced": 1,
        "unset": 3,
    }
    assert report["kappa"]["hierarchy"]["items"] == 8
    assert report["kappa"]["dependency"]["items"] == 8
    assert report["labelled_edges_reversed"] == 0


def test_a_pair_with_edges_both_ways_is_two_edges_for_precision(tmp_path):
    items = [f"a{n}||b{n}" for n in range(3)]
    population = pl.DataFrame(
        {
            "item_id": items,
            "concept_a": [f"a{n}" for n in range(3)],
            "stratum": ["bidirectional"] * 3,
            "edge_direction": ["both"] * 3,
            "builds_on_direction": ["both"] * 3,
            "pipeline_hierarchy": [False] * 3,
        }
    )
    tasks = [
        _prerequisite_task(item, [(1, "none", answer, ()), (2, "none", answer, ())])
        for item, answer in zip(
            items, ("b_before_a", "context_dependent", "none"), strict=True
        )
    ]
    report = score.score_prerequisites(
        population,
        population.select("item_id", "concept_a"),
        _export(tmp_path, tasks),
        alpha=0.05,
    )
    # one of a0's two edges is right; a2's none confirms neither
    strict = report["edge_precision"]["strata"]["bidirectional"]
    assert strict["labeled_ratio"] == pytest.approx(1 / 6)
    lenient = report["edge_precision_lenient"]["strata"]["bidirectional"]
    assert lenient["labeled_ratio"] == pytest.approx(3 / 6)
    # recall still needs only one edge the way the consensus runs
    assert report["edge_recall"]["strata"]["bidirectional"]["labeled_ratio"] == 1.0


def test_prerequisites_config_judges_hierarchy_and_dependency_apart():
    config = labelstudio.PREREQUISITES_CONFIG
    for name in ("hierarchy", "hierarchy_depth", "dependency", "dependency_depth"):
        assert f'name="{name}"' in config
    assert "neither" not in labelstudio.DEPENDENCY_LABELS
    assert {"context_dependent", "uncertain"} <= set(labelstudio.HIERARCHY_LABELS)
    assert 'whenChoiceValue="b_before_a,a_before_b"' in config


def _region_task(item_id, annotations):
    """annotations: (annotator, central_explanation, concepts), each concept
    (label, status, salience, role).
    """
    results_per = []
    for annotator, central, concepts in annotations:
        results = [
            {
                "id": "central",
                "from_name": "central_explanation",
                "type": "choices",
                "value": {"choices": [central]},
            }
        ]
        for index, (label, status, salience, role) in enumerate(concepts):
            span = {"start": index, "end": index + 1, "text": "x"}
            results.extend(
                [
                    {
                        "id": f"r{index}",
                        "from_name": "concept",
                        "type": "labels",
                        "value": {**span, "labels": [status]},
                    },
                    {
                        "id": f"r{index}",
                        "from_name": "concept_label",
                        "type": "textarea",
                        "value": {**span, "text": [label]},
                    },
                    {
                        "id": f"r{index}",
                        "from_name": "salience",
                        "type": "choices",
                        "value": {**span, "choices": [salience]},
                    },
                ]
            )
            if role:
                results.append(
                    {
                        "id": f"r{index}",
                        "from_name": "central_role",
                        "type": "choices",
                        "value": {**span, "choices": [role]},
                    }
                )
        results_per.append((annotator, results))
    return {
        "data": {"item_id": item_id},
        "annotations": [
            _annotation(annotator, minute, results)
            for minute, (annotator, results) in enumerate(results_per)
        ],
    }


def test_per_region_choices_join_their_region_and_are_not_task_choices(tmp_path):
    (annotation,) = _export(
        tmp_path,
        [
            _region_task(
                "u0",
                [(1, "present", [("k-means", "reviewed", "supporting", "needed")])],
            )
        ],
    )
    (region,) = annotation.regions("concept").values()
    assert region["salience"] == "supporting"
    assert region["central_role"] == "needed"
    assert annotation.choice("salience") is None
    assert annotation.choice("central_explanation") == "present"


_LECTURE = [
    ("interpretable clustering", "taught", "central", "objective"),
    ("k-means clustering", "reviewed", "supporting", "needed"),
    ("Bluebikes", "used", "supporting", None),
]


def test_subsections_score_salience_and_central_recall(tmp_path):
    items = ["u0", "u1"]
    population = pl.DataFrame(
        {
            "item_id": items,
            **_RUN,
            "scope_id": items,
            "stratum": ["s"] * 2,
            "models": ["haiku"] * 2,
            "found_hat_haiku": [1, 1],
            "gold_hat": [2, 2],
        }
    )
    sample = pl.DataFrame({"item_id": items, "blind": [True, True]})
    matcher = LabelMatcher()
    model_concepts = pl.DataFrame(
        {
            **_RUN,
            "scope_id": items,
            "model": ["haiku"] * 2,
            "canonical": [matcher.canonical("interpretable clustering")] * 2,
            "status": ["taught"] * 2,
        }
    )
    tasks = [
        _region_task("u0", [(1, "present", _LECTURE), (2, "present", _LECTURE)]),
        _region_task(
            "u1",
            [
                (1, "present", _LECTURE),
                (2, "present", [(*_LECTURE[0][:2], "supporting", "objective")]),
            ],
        ),
    ]
    exported = _export(tmp_path, tasks)
    report, _ = score.score_subsections(
        population, sample, exported, model_concepts, matcher, alpha=0.05
    )
    central = report["models"]["haiku"]["central_recall"]["strata"]["s"]
    # u1's salience of interpretable clustering tied, so only u0's counts
    assert central["labeled_ratio"] == 1.0
    concept = report["models"]["haiku"]["concept_recall"]["strata"]["s"]
    assert concept["labeled_ratio"] == pytest.approx(2 / 6)
    salience = report["salience"]
    assert salience["kappa"]["items"] == 4
    assert salience["status_by_salience"]["taught|undecided"] == 1
    gold = score.gold_subsection_concepts(score.subsection_gold(exported, matcher))
    assert gold.filter(pl.col("item_id") == "u0").sort("canonical")[
        "central_role"
    ].to_list() == ["none", "objective", "needed"]


def test_objectives_agreement_conflicts_and_model_hooks(tmp_path):
    items = ["u0", "u1", "u2"]
    population = pl.DataFrame(
        {"item_id": items, **_RUN, "scope_id": items, "stratum": ["s"] * 3}
    )
    assignment = [("pivot table", "used", "central", None)]
    tasks = [
        _region_task("u0", [(1, "present", _LECTURE), (2, "present", _LECTURE)]),
        _region_task(
            "u1",
            [
                (1, "absent", assignment),
                # roles with no central explanation are ignored and listed
                (2, "absent", [(*assignment[0][:3], "objective")]),
            ],
        ),
        _region_task("u2", [(1, "absent", assignment)]),
    ]
    matcher = LabelMatcher()
    canonical = matcher.canonical
    model_roles = pl.DataFrame(
        {
            "platform": ["mitxonline"] * 5,
            "courserun_readable_id": ["run1"] * 5,
            "scope_id": ["u0", "u0", "u0", "u1", "u2"],
            "model": ["variant"] * 5,
            "canonical": [
                canonical("interpretable clustering"),
                canonical("k-means clustering"),
                canonical("Bluebikes"),
                canonical("pivot table"),
                None,
            ],
            "role": ["objective", "needed", "needed", "objective", "no_central"],
        }
    )
    report = score.score_objectives(
        population,
        _export(tmp_path, tasks),
        matcher,
        alpha=0.05,
        model_roles=model_roles,
    )
    assert report["objective_dice"] == 1.0
    assert report["kappa_needed"]["items"] == 2
    assert report["central_explanation"] == {"present": 1, "absent": 2}
    assert report["conflicts"] == [
        {
            "item_id": "u1",
            "annotator": "2",
            "problem": "roles marked with no central explanation (ignored)",
        }
    ]
    variant = report["models"]["variant"]
    assert variant["needed_precision"]["strata"]["s"]["labeled_ratio"] == 0.5
    assert variant["objective_recall"]["strata"]["s"]["labeled_ratio"] == 1.0
    assert variant["objective_precision"]["strata"]["s"]["labeled_ratio"] == 0.5
    # u1 and u2 have no central explanation; the variant chose one in u1
    assert variant["false_objective_rate"]["strata"]["s"]["labeled_ratio"] == 0.5


def test_subsections_config_has_salience_and_objectives():
    config = labelstudio.SUBSECTIONS_CONFIG
    assert 'name="salience"' in config
    assert 'name="central_role"' in config
    assert 'name="central_explanation"' in config
    salience = config[config.index('name="salience"') :].split(">", 1)[0]
    assert 'perRegion="true"' in salience
    assert 'required="true"' in salience


def test_claims_report_granularity_errors_beside_precision(tmp_path):
    population = pl.DataFrame(
        {
            "item_id": [f"c{n}" for n in range(8)],
            "model": ["haiku"] * 8,
            "claim_kind": ["concept"] * 6 + ["skill"] * 2,
            "discipline": ["UAI"] * 4 + ["18"] * 4,
            "stratum": ["UAI|s"] * 4 + ["18|s"] * 4,
            "autorater": [None] * 8,
        },
        schema_overrides={"autorater": pl.Float64},
    )
    good = {"verdict": "supported", "status": "correct", "label_quality": "good"}
    broad = {**good, "label_quality": "too_broad"}
    narrow = {**good, "label_quality": "too_narrow"}
    skill = {"verdict": "supported", "status": "not_applicable"}
    tasks = [
        _choice_task("c0", [(1, good)]),
        _choice_task("c1", [(1, broad)]),
        _choice_task("c2", [(1, narrow)]),
        _choice_task("c3", [(1, good)]),
        _choice_task("c4", [(1, good)]),
        _choice_task("c5", [(1, broad)]),
        _choice_task("c6", [(1, {**skill, "label_quality": "too_broad"})]),
        _choice_task("c7", [(1, {**skill, "label_quality": "not_applicable"})]),
    ]
    report = score.score_claims(population, _export(tmp_path, tasks), alpha=0.05)
    haiku = report["models"]["haiku"]
    # granularity does not lower precision, and is reported over concept kinds only
    assert haiku["correct"]["point"] == 1.0
    assert haiku["granularity_error"]["strata"]["UAI|s"]["labeled_ratio"] == 0.5
    assert haiku["granularity_error"]["strata"]["18|s"]["labeled"] == 2
    by_discipline = haiku["by_discipline"]
    assert by_discipline["18"]["granularity_error"]["point"] == pytest.approx(0.5)
    assert set(by_discipline) == {"UAI", "18"}


def test_declared_disciplines_select_and_name_the_strata(tmp_path):
    claims = pl.DataFrame(
        [
            _claim("opus", "u1", 1, "Linear regression"),
            {
                **_claim("opus", "u2", 1, "eigenvalue"),
                "extraction_key": "opus-ocw",
                "courserun_readable_id": "97db384ef34009a64df7cb86cf701979",
            },
            {
                **_claim("opus", "u3", 1, "recursion"),
                "extraction_key": "opus-6",
                "courserun_readable_id": "course-v1:MITx+6.100A+2T2026",
            },
        ]
    )
    path = tmp_path / "disciplines.csv"
    path.write_text(
        "course,discipline\nUAI,ai\n97db384ef34009a64df7cb86cf701979,mathematics\n"
    )
    inputs = dataclasses.replace(
        _inputs(claims), disciplines=samples.read_disciplines(path)
    )
    population = samples.claims_population(inputs)
    # the MITx 6 run is not declared, so it is not sampled
    assert dict(population.select("label", "discipline").iter_rows()) == {
        "Linear regression": "ai",
        "eigenvalue": "mathematics",
    }
    assert set(population["stratum"]) == {
        "ai|opus|unpaired",
        "mathematics|opus|unpaired",
    }
    path.write_text("course,discipline\nUAI,ai\n18,mathematics\n")
    with pytest.raises(ValueError, match=r"declared disciplines \['mathematics'\]"):
        samples.claims_population(
            dataclasses.replace(inputs, disciplines=samples.read_disciplines(path))
        )
    path.write_text("course,discipline\nUAI,ai\nUAI,cs\n")
    with pytest.raises(ValueError, match="listed twice"):
        samples.read_disciplines(path)


def test_fit_strata_keeps_declared_disciplines_apart():
    population = _subsection_strata(3).with_columns(
        pl.col("course_family").alias("discipline")
    )
    floor = samples.SUBSECTIONS_MIN_PER_STRATUM
    kept, dropped = samples.fit_strata(
        population, "subsections", 40, floor, keep_discipline=True
    )
    assert dropped.is_empty()
    assert set(kept["stratum"]) == {
        f"F{family}|{models}"
        for family in range(3)
        for models in ("haiku", "opus", "haiku+opus")
    }


def test_prerequisite_population_flags_pipeline_hierarchy():
    concepts = pl.DataFrame({"concept_pk": ["a", "b", "c"]})
    relations = pl.DataFrame(
        {
            "from_concept_fk": ["a", "a"],
            "to_concept_fk": ["b", "c"],
            "relation": ["broader", "builds_on"],
            "relation_family": ["hierarchy", "sequence"],
            "courserun_support": [1, 1],
        }
    )
    claims = pl.DataFrame(
        [_claim("opus", "u1", n, label) for n, label in enumerate("xyz")]
    )
    assignments = pl.DataFrame(
        {"concept_label_key": list("xyz"), "concept_id": list("abc")}
    )
    inputs = dataclasses.replace(
        _inputs(claims),
        relations=relations,
        concepts=concepts,
        assignments=assignments,
        sense_assignments=_senses(assignments),
        registry=pl.DataFrame(
            {"concept_id": list("abc"), "merged_into_concept_id": [None] * 3},
            schema_overrides={"merged_into_concept_id": pl.String},
        ),
        labels=pl.DataFrame({"concept_label_key": list("xyz")}),
    )
    population = samples.prerequisites_population(inputs)
    flags = dict(population.select("item_id", "pipeline_hierarchy").iter_rows())
    # a kind of b with no sequence edge, and a builds on c with no hierarchy
    assert flags == {"a||b": True, "a||c": False, "b||c": False}
    builds_on = dict(population.select("item_id", "builds_on_direction").iter_rows())
    assert builds_on == {"a||b": "none", "a||c": "b_before_a", "b||c": "none"}


def test_prerequisite_pairs_co_occur_as_each_runs_sense():
    # run2 uses x in a sense split off its primary concept a
    claims = pl.DataFrame(
        [_claim("opus", "u1", n, label) for n, label in enumerate("xy")]
        + [
            {**_claim("opus", "u1", n, label), "courserun_readable_id": "run2"}
            for n, label in enumerate("xy")
        ]
    )
    assignments = pl.DataFrame(
        {"concept_label_key": ["x", "y"], "concept_id": ["a", "b"]}
    )
    senses = pl.concat(
        [
            _senses(assignments),
            _senses(
                pl.DataFrame(
                    {"concept_label_key": ["x", "y"], "concept_id": ["c", "b"]}
                ),
                run="run2",
            ),
        ]
    )
    inputs = dataclasses.replace(
        _inputs(claims),
        relations=pl.DataFrame(
            schema={
                "from_concept_fk": pl.String,
                "to_concept_fk": pl.String,
                "relation": pl.String,
                "relation_family": pl.String,
                "courserun_support": pl.Int64,
            }
        ),
        assignments=assignments,
        sense_assignments=senses,
        registry=pl.DataFrame(
            {"concept_id": list("abc"), "merged_into_concept_id": [None] * 3},
            schema_overrides={"merged_into_concept_id": pl.String},
        ),
        labels=pl.DataFrame({"concept_label_key": ["x", "y"]}),
    )
    population = samples.prerequisites_population(inputs)
    assert sorted(population["item_id"]) == ["a||b", "b||c"]


def test_claim_fallback_with_disciplines_keeps_the_autorater_strata_apart():
    agreements = [
        agreement
        for agreement in ("both", "haiku_only", "not_compared", "unpaired")
        for _ in range(5)
    ]
    population = pl.DataFrame(
        {
            "item_id": [f"c{n:02d}" for n in range(len(agreements))],
            "model": ["haiku"] * len(agreements),
            "discipline": ["math"] * len(agreements),
            "agreement": agreements,
        }
    ).with_columns(
        pl.format("{}|{}|{}", "discipline", "model", "agreement").alias("stratum")
    )
    # four strata need 16 labels; pooling both and haiku_only fits 12
    fitted, _ = samples.fit_strata(population, "claims", 12, 4, keep_discipline=True)
    assert set(fitted["stratum"]) == {
        "math|haiku|compared",
        "math|haiku|not_compared",
        "math|haiku|unpaired",
    }
