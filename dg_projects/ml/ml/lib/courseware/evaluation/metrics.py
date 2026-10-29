"""Estimators for the gold set: stratified PPI, B-cubed and inter-annotator kappa.

Stratified prediction-powered inference follows Fisch et al., "Stratified
Prediction-Powered Inference for Hybrid Language Model Evaluation" (NeurIPS 2024,
arXiv 2406.04291), Algorithm 1 and Propositions 2 and 3, on top of PPI++
(Angelopoulos, Duchi and Zrnic, arXiv 2311.01453). The power-tuning formula matches
`ppi_py.ppi._calc_lam_glm` (MIT, aangelopoulos/ppi_py). We implement it here rather
than depend on ppi_py because ppi_py has no stratified estimator and pulls in numba,
statsmodels and gdown for the few lines we need.

Every estimate here is a ratio of two population means, theta = E[y] / E[g]: the
M-estimator with gradient theta * g - y. A mean (precision) is the case g = 1. Recall
over subsections is sum(found) / sum(gold), with g the gold count of a subsection.
B-cubed over sampled clusters is sum(per-cluster score) / sum(cluster size).

Within stratum k, with n_k labelled items (y, g) and autorater predictions (y_hat,
g_hat) on those and on the stratum's N_k unlabelled items, and the stratum's
population share w_k:

    theta = sum_k w_k [mean(lam) mean(y_hat_u) + mean(y - lam y_hat)]
          / sum_k w_k [mean(lam) mean(g_hat_u) + mean(g - lam g_hat)]

    Var(theta) = sum_k w_k^2 [mean(lam)^2 Var(d_hat_u) / N_k
                              + Var(d - lam d_hat) / n_k] / A^2

where d = theta g - y is the gradient on true labels, d_hat the same on predictions,
and A = sum_k w_k mean(g) is the Hessian. lam is tuned per stratum (Example 1 of
the paper): Cov(d, d_hat) / ((1 + n_k / N_k) Var(d_hat)), clipped to [0, 1]. A
stratum without autorater predictions gets lam = 0, which is the classical
stratified estimator for that stratum.

One departure from the paper: lam is cross-fitted. The labelled items of a stratum
are split into two folds and each fold's rectifier uses the lam tuned on the other
fold. Tuning lam on the same items it weights biases the estimate at the stratum
sizes we label (40 or so). Over 8,000 simulated trials (four seeds, three strata,
n_k = 40, alpha = 0.1) the same-sample PPI++ interval covered 86.8% of the time and
the cross-fitted one 88.8%, against 89.1% for the classical interval, with a
standard error about 15% below the classical estimator's. Coverage is still
slightly below nominal at these sizes, as it is for the classical normal interval.
lam tuned on all of a stratum's rows matches ppi_py's on the same data, and with
lam = 1 so does the point estimate. The cross-fitted estimate differs from ppi_py's
by design, and the interval is slightly wider because variances here use ddof=1.

A stratum with fewer than two unlabelled items has no estimable autorater
variance and gets lam = 0.

The corrected denominator, sum_k w_k [mean(lam) mean(g_hat_u) + mean(g - lam g_hat)],
can be zero or negative even when the labelled denominators are positive (e.g. one
labelled g = 1 predicted g_hat = 2 beside an unlabelled g_hat = 1). Where it is, the
estimate falls back to lam = 0 in every stratum, the classical stratified ratio,
whose denominator is the labelled mean of g; the reported lambdas show the
fallback. Power tuning starts from lam = 1, as ppi_py does, or from the classical
ratio when lam = 1 has no positive denominator.
"""

from collections import Counter
from collections.abc import Hashable, Mapping, Sequence
from dataclasses import dataclass
from itertools import combinations
from statistics import NormalDist

import numpy as np

# Every stratum needs this many labels for its variance to be estimable.
MIN_LABELS_PER_STRATUM = 2


@dataclass(frozen=True)
class Stratum:
    """One stratum's labelled sample, its autorater predictions and its size.

    :param name: stratum key, for error messages and reports
    :param population: number of items in the stratum, labelled or not
    :param y: numerator on each labelled item (1 if correct, items found, ...)
    :param g: denominator on each labelled item; None means all ones (a mean)
    :param y_hat: autorater numerator on each labelled item; None when the stratum
        has no autorater
    :param g_hat: autorater denominator on each labelled item; None means all ones
    :param y_hat_unlabeled: autorater numerator on each unlabelled item
    :param g_hat_unlabeled: autorater denominator on each unlabelled item
    """

    name: str
    population: int
    y: np.ndarray
    g: np.ndarray | None = None
    y_hat: np.ndarray | None = None
    g_hat: np.ndarray | None = None
    y_hat_unlabeled: np.ndarray | None = None
    g_hat_unlabeled: np.ndarray | None = None

    @property
    def n(self) -> int:
        """Return the number of labelled items."""
        return len(self.y)

    @property
    def denominators(self) -> np.ndarray:
        """Return g, or ones when the estimand is a mean."""
        return np.ones(self.n) if self.g is None else self.g

    @property
    def has_autorater(self) -> bool:
        """Return whether there are predictions on labels and on two or more others."""
        return (
            self.y_hat is not None
            and self.y_hat_unlabeled is not None
            and len(self.y_hat_unlabeled) >= MIN_LABELS_PER_STRATUM
        )

    def predicted(self) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """(y_hat, g_hat, y_hat_unlabeled, g_hat_unlabeled), with ones filled in."""
        if self.y_hat is None or self.y_hat_unlabeled is None:
            msg = f"stratum {self.name} has no autorater predictions"
            raise ValueError(msg)
        g_hat = np.ones(self.n) if self.g_hat is None else self.g_hat
        g_hat_unlabeled = (
            np.ones(len(self.y_hat_unlabeled))
            if self.g_hat_unlabeled is None
            else self.g_hat_unlabeled
        )
        return self.y_hat, g_hat, self.y_hat_unlabeled, g_hat_unlabeled


@dataclass(frozen=True)
class Estimate:
    """A point estimate with its standard error and 1 - alpha interval."""

    point: float
    std_error: float
    lower: float
    upper: float
    alpha: float
    labeled: int
    population: int
    lambdas: dict[str, float]


def _folds(count: int) -> np.ndarray:
    """Assign labelled items to two cross-fitting folds, alternating."""
    return np.arange(count) % 2 == 0


def _ratio(
    strata: Sequence[Stratum], weights: np.ndarray, lambdas: Sequence[np.ndarray]
) -> float | None:
    """Return the rectified ratio, or None where its denominator is not positive."""
    numerator = 0.0
    denominator = 0.0
    for stratum, weight, lam in zip(strata, weights, lambdas, strict=True):
        y_part = stratum.y.mean()
        g_part = stratum.denominators.mean()
        if lam.any():
            y_hat, g_hat, y_hat_u, g_hat_u = stratum.predicted()
            y_part += lam.mean() * y_hat_u.mean() - (lam * y_hat).mean()
            g_part += lam.mean() * g_hat_u.mean() - (lam * g_hat).mean()
        numerator += weight * y_part
        denominator += weight * g_part
    return numerator / denominator if denominator > 0 else None


def _tuned_lambda(stratum: Stratum, theta: float, rows: np.ndarray) -> float:
    """PPI++ power tuning on some labelled rows, as ppi_py._calc_lam_glm computes it."""
    y_hat, g_hat, y_hat_u, g_hat_u = stratum.predicted()
    grads = theta * stratum.denominators[rows] - stratum.y[rows]
    grads_hat = theta * g_hat[rows] - y_hat[rows]
    grads_hat_u = theta * g_hat_u - y_hat_u
    variance = np.var(np.concatenate([grads_hat, grads_hat_u]), ddof=1)
    if variance == 0:
        return 0.0
    covariance = np.mean((grads - grads.mean()) * (grads_hat - grads_hat.mean()))
    lam = covariance / ((1 + rows.sum() / len(y_hat_u)) * variance)
    return float(np.clip(lam, 0, 1))


def _cross_fitted_lambda(stratum: Stratum, theta: float) -> np.ndarray:
    """Per labelled item, the lam tuned on the other fold."""
    fold = _folds(stratum.n)
    return np.where(
        fold,
        _tuned_lambda(stratum, theta, ~fold),
        _tuned_lambda(stratum, theta, fold),
    )


def stratified_ppi(
    strata: Sequence[Stratum],
    *,
    alpha: float = 0.05,
    power_tuning: bool = True,
    bounds: tuple[float, float] = (0.0, 1.0),
) -> Estimate:
    """Estimate E[y] / E[g] over the union of the strata, with a confidence interval.

    :param strata: every stratum of the population, each with at least
        MIN_LABELS_PER_STRATUM labels
    :param alpha: the interval targets 1 - alpha coverage
    :param power_tuning: tune lam per stratum (PPI++, cross-fitted); False fixes
        lam = 1 wherever there is an autorater (plain PPI)
    :param bounds: the estimand's range, to which the interval is clipped; every
        rate here (precision, recall, B-cubed) lies in [0, 1]
    :returns: point estimate, standard error and normal interval
    :rtype: Estimate
    :raises ValueError: when a stratum has too few labels or no labelled
        denominator is positive
    """
    short = [s.name for s in strata if s.n < MIN_LABELS_PER_STRATUM]
    if short:
        msg = f"strata with fewer than {MIN_LABELS_PER_STRATUM} labels: {short}"
        raise ValueError(msg)
    population = np.array([s.population for s in strata], dtype=float)
    weights = population / population.sum()
    classical = [np.zeros(s.n) for s in strata]
    classical_theta = _ratio(strata, weights, classical)
    if classical_theta is None:
        msg = "no labelled denominator is positive"
        raise ValueError(msg)
    lambdas = [np.full(s.n, 1.0 if s.has_autorater else 0.0) for s in strata]
    theta = _ratio(strata, weights, lambdas)
    if power_tuning:
        start = classical_theta if theta is None else theta
        lambdas = [
            _cross_fitted_lambda(s, start) if s.has_autorater else np.zeros(s.n)
            for s in strata
        ]
        theta = _ratio(strata, weights, lambdas)
    if theta is None:
        lambdas, theta = classical, classical_theta

    hessian = sum(
        weight * s.denominators.mean()
        for s, weight in zip(strata, weights, strict=True)
    )
    variance = 0.0
    for stratum, weight, lam in zip(strata, weights, lambdas, strict=True):
        rectifier = theta * stratum.denominators - stratum.y
        stratum_variance = 0.0
        if lam.any():
            y_hat, g_hat, y_hat_u, g_hat_u = stratum.predicted()
            rectifier = rectifier - lam * (theta * g_hat - y_hat)
            imputed = theta * g_hat_u - y_hat_u
            stratum_variance += lam.mean() ** 2 * np.var(imputed, ddof=1) / len(imputed)
        stratum_variance += np.var(rectifier, ddof=1) / stratum.n
        variance += weight**2 * stratum_variance
    std_error = float(np.sqrt(variance) / hessian)
    z = NormalDist().inv_cdf(1 - alpha / 2)
    return Estimate(
        point=float(theta),
        std_error=std_error,
        lower=float(max(bounds[0], theta - z * std_error)),
        upper=float(min(bounds[1], theta + z * std_error)),
        alpha=alpha,
        labeled=sum(s.n for s in strata),
        population=int(population.sum()),
        lambdas={
            s.name: float(lam.mean()) for s, lam in zip(strata, lambdas, strict=True)
        },
    )


def allocate(
    sizes: Mapping[str, int],
    total: int,
    *,
    weights: Mapping[str, float] | None = None,
    minimum: int = 0,
    required: int = MIN_LABELS_PER_STRATUM,
) -> dict[str, int]:
    """Split a labelling budget across strata.

    Each stratum first gets a floor of labels, or all of its items if it has
    fewer: `minimum` where the budget has room for it, otherwise the largest floor
    the budget fits, but never below `required`. The rest is split in proportion to
    size times weight (proportional allocation when every weight is 1; a weight
    above 1 oversamples a stratum the autorater is expected to be worse on, the
    direction Proposition 3 of the StratPPI paper points). No stratum gets more
    than its size. Each round grants
    the whole-label part of every share; the last few labels, once no share reaches
    a whole label, go one each to the strata with the largest weighted size. The
    split is deterministic.

    :param sizes: population size per stratum, each at least MIN_LABELS_PER_STRATUM
        (merge smaller strata first, see samples.coarsen_strata)
    :param total: labels to allocate; capped at the population size
    :param weights: oversampling weight per stratum, default 1
    :param minimum: the floor per stratum wanted, lowered to fit the budget
    :param required: the floor per stratum the estimates need
        (MIN_LABELS_PER_STRATUM; more where a stratum is split, as subsections are)
    :returns: labels per stratum
    :rtype: dict[str, int]
    :raises ValueError: when a stratum is too small to estimate or the required
        floors exceed the budget
    """
    small = sorted(
        name for name, size in sizes.items() if size < MIN_LABELS_PER_STRATUM
    )
    if small:
        msg = f"strata smaller than {MIN_LABELS_PER_STRATUM} items: {small}"
        raise ValueError(msg)
    total = min(total, sum(sizes.values()))
    floor = max(minimum, required)
    while floor > required and sum(min(floor, size) for size in sizes.values()) > total:
        floor -= 1
    allocation = {name: min(floor, size) for name, size in sizes.items()}
    if sum(allocation.values()) > total:
        msg = (
            f"{len(sizes)} strata at a floor of {floor} need "
            f"{sum(allocation.values())} labels, more than the budget of {total}"
        )
        raise ValueError(msg)
    while (remaining := total - sum(allocation.values())) > 0:
        weighted = {
            name: size * (weights or {}).get(name, 1.0)
            for name, size in sizes.items()
            if allocation[name] < size
        }
        mass = sum(weighted.values())
        granted = {
            name: min(int(remaining * value / mass), sizes[name] - allocation[name])
            for name, value in weighted.items()
        }
        if not any(granted.values()):
            for name in sorted(weighted, key=lambda key: (-weighted[key], key))[
                :remaining
            ]:
                granted[name] = 1
        for name, count in granted.items():
            allocation[name] += count
    return allocation


def bcubed(
    predicted: Mapping[Hashable, Hashable], truth: Mapping[Hashable, Hashable]
) -> tuple[float, float]:
    """B-cubed precision and recall of a clustering against the true clustering.

    For each item, precision is the share of its predicted cluster that is in its
    true cluster and recall is the share of its true cluster that is in its
    predicted cluster; both are averaged over items (Bagga and Baldwin 1998).

    :param predicted: item -> predicted cluster id
    :param truth: item -> true cluster id, over the same items
    :returns: (precision, recall)
    :rtype: tuple[float, float]
    """
    if predicted.keys() != truth.keys():
        msg = "predicted and true clusterings cover different items"
        raise ValueError(msg)
    overlap = Counter((predicted[item], truth[item]) for item in predicted)
    predicted_sizes = Counter(predicted.values())
    true_sizes = Counter(truth.values())
    precision = sum(
        overlap[predicted[item], truth[item]] / predicted_sizes[predicted[item]]
        for item in predicted
    )
    recall = sum(
        overlap[predicted[item], truth[item]] / true_sizes[truth[item]]
        for item in predicted
    )
    return precision / len(predicted), recall / len(predicted)


def cluster_bcubed_terms(
    groups: Sequence[int], outside: Sequence[int]
) -> tuple[float, float]:
    """One predicted cluster's B-cubed precision and recall sums over its members.

    A reviewer splits a sampled predicted cluster into groups of members that are
    the same concept, and counts, per group, the labels outside the cluster that
    are the same concept too. A member r of group h then has |C n T(r)| = |h| and
    |T(r)| = |h| + outside_h. Summed over members, divided by summed cluster sizes,
    these give B-cubed precision and recall, so a stratified sample of predicted
    clusters estimates both with stratified_ppi (numerator these sums, denominator
    |C|): a stratified ratio estimator over predicted clusters.

    Precision is exact per cluster, since it needs only the cluster's own members.
    Recall is not: outside_h only counts labels in the nearby concepts the reviewer
    was shown, so true members elsewhere are missed and recall reads high; but a
    neighbour placed in a group counts with all its labels, even ones that are not
    that concept, which reads low. So recall is an upper bound only roughly, not
    strictly. Binette et al. (arXiv 2404.05622) estimate both metrics without this
    gap by sampling true clusters with probability proportional to size, which
    needs reviewers to assemble true clusters from the whole corpus; this does not.

    :param groups: size of each group of the cluster's members
    :param outside: per group, labels outside the cluster in the same true concept
    :returns: (sum of member precisions, sum of member recalls)
    :rtype: tuple[float, float]
    """
    size = sum(groups)
    precision = sum(group * group / size for group in groups)
    recall = sum(
        group * group / (group + extra)
        for group, extra in zip(groups, outside, strict=True)
    )
    return precision, recall


def cohen_kappa(first: Sequence[Hashable], second: Sequence[Hashable]) -> float:
    """Cohen's kappa between two raters over the same items.

    :param first: one rater's label per item
    :param second: the other rater's label per item, in the same order
    :returns: (p_o - p_e) / (1 - p_e)
    :rtype: float
    """
    if len(first) != len(second) or not first:
        msg = "two non-empty rating lists of equal length are required"
        raise ValueError(msg)
    count = len(first)
    observed = sum(a == b for a, b in zip(first, second, strict=True)) / count
    first_counts, second_counts = Counter(first), Counter(second)
    expected = sum(
        first_counts[label] * second_counts[label] for label in first_counts
    ) / (count * count)
    if expected == 1:
        return 1.0
    return (observed - expected) / (1 - expected)


def fleiss_kappa(ratings: Sequence[Sequence[Hashable]]) -> float:
    """Fleiss' kappa for items each rated by the same number of raters.

    :param ratings: per item, the labels its raters gave (any rater order)
    :returns: (P_bar - P_e) / (1 - P_e)
    :rtype: float
    """
    raters = {len(item) for item in ratings}
    if len(raters) != 1 or min(raters) < 2:  # noqa: PLR2004
        msg = f"every item needs the same number (>= 2) of ratings, got {raters}"
        raise ValueError(msg)
    (rater_count,) = raters
    item_count = len(ratings)
    totals: Counter[Hashable] = Counter()
    agreement = 0.0
    for item in ratings:
        counts = Counter(item)
        totals.update(counts)
        agreement += (sum(c * c for c in counts.values()) - rater_count) / (
            rater_count * (rater_count - 1)
        )
    observed = agreement / item_count
    expected = sum(
        (count / (item_count * rater_count)) ** 2 for count in totals.values()
    )
    if expected == 1:
        return 1.0
    return (observed - expected) / (1 - expected)


def mean_pairwise_cohen_kappa(
    ratings: Mapping[Hashable, Mapping[Hashable, Hashable]],
) -> float:
    """Average Cohen's kappa over rater pairs, each on the items both rated.

    The statistic Pan et al. (ACL 2017) report for prerequisite labels.

    :param ratings: item -> {rater: label}
    :returns: mean kappa over rater pairs that share at least two items
    :rtype: float
    """
    raters = sorted({rater for item in ratings.values() for rater in item}, key=str)
    kappas = []
    for left, right in combinations(raters, 2):
        shared = [item for item in ratings.values() if left in item and right in item]
        if len(shared) >= MIN_LABELS_PER_STRATUM:
            kappas.append(
                cohen_kappa(
                    [item[left] for item in shared], [item[right] for item in shared]
                )
            )
    if not kappas:
        msg = "no pair of raters shares two items"
        raise ValueError(msg)
    return float(np.mean(kappas))
