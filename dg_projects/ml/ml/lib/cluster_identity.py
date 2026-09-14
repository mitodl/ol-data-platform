"""Matches a completed clustering run's clusters onto stable cluster_key identities.

A full re-cluster (ml.lib.cluster) produces run-local, arbitrary cluster_id labels
with no relationship to any previous run's. This module maps them onto persistent
cluster_keys by *membership* overlap (Jaccard), not by comparing vectors -- the
same conversations landing together again is what "the same cluster" means here,
and it keeps working across an embedding-model change that reshuffles the vector
space itself.
"""

import os
import uuid
from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import polars as pl
from scipy.optimize import linear_sum_assignment

# Starting points, pending calibration on the labeled sample.
JACCARD_MATCH_THRESHOLD = float(
    os.environ.get("CLUSTER_JACCARD_MATCH_THRESHOLD", "0.5")
)
CLUSTER_RADIUS_PERCENTILE = float(os.environ.get("CLUSTER_RADIUS_PERCENTILE", "5"))
CONTINUITY_FLOOR = float(os.environ.get("CLUSTER_CONTINUITY_FLOOR", "0.5"))

ClusterRelation = Literal["continued", "merged", "split", "new", "retired"]

CLUSTER_SCHEMA = {
    "cluster_key": pl.String,
    "centroid": pl.List(pl.Float32),
    "radius": pl.Float64,
    "embedding_model_version": pl.String,
    "embedding_dim": pl.Int64,
    "member_count": pl.Int64,
    "cluster_status": pl.String,
    "first_seen_run_id": pl.String,
    "last_seen_run_id": pl.String,
}

CLUSTER_LINEAGE_SCHEMA = {
    "cluster_lineage_pk": pl.String,
    "cluster_run_id": pl.String,
    "prior_cluster_key": pl.String,
    "cluster_key": pl.String,
    # This run's run-local cluster_id that produced the row -- null for 'retired'
    # (an old key with no corresponding new cluster). feedback_cluster_assignment
    # joins feedback_cluster_candidate.cluster_id to this to place the run's
    # conversations onto their resolved cluster_key.
    "cluster_id": pl.Int64,
    "relation": pl.String,
    "jaccard": pl.Float64,
}


def new_cluster_key() -> str:
    return str(uuid.uuid4())


def cluster_lineage_pk(
    cluster_run_id: str, prior_cluster_key: str | None, cluster_key: str | None
) -> str:
    """Deterministic id for one lineage edge -- stable across retries of the run."""
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_OID,
            f"{cluster_run_id}|{prior_cluster_key or ''}|{cluster_key or ''}",
        )
    )


def jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a and not b:
        return 0.0
    return len(a & b) / len(a | b)


@dataclass(frozen=True)
class ClusterMatch:
    """One new-run cluster_id's resolved identity."""

    new_cluster_id: int
    cluster_key: str
    relation: ClusterRelation
    prior_cluster_key: str | None
    match_jaccard: float | None


def _continued_matches(
    new_cluster_members: dict[int, frozenset[str]],
    active_cluster_members: dict[str, frozenset[str]],
    match_threshold: float,
) -> dict[int, tuple[str, float]]:
    """One-to-one (new_cluster_id -> (cluster_key, jaccard)) pairs at or above the
    match threshold, chosen by global maximum total Jaccard.

    Modeled as a linear assignment problem padded with a same-size "opt out" block
    per side (cost 0), so a pair below threshold is never forced together just
    because the matrix shape would otherwise require every row/column matched --
    each side can instead be assigned to its own dummy at no cost. Real pairs below
    threshold get a cost worse than any dummy option, so they're never chosen.
    """
    new_ids = sorted(new_cluster_members)
    old_keys = sorted(active_cluster_members)
    if not new_ids or not old_keys:
        return {}

    n, m = len(new_ids), len(old_keys)
    jaccard_matrix = np.zeros((n, m))
    for i, new_id in enumerate(new_ids):
        for j, old_key in enumerate(old_keys):
            jaccard_matrix[i, j] = jaccard(
                new_cluster_members[new_id], active_cluster_members[old_key]
            )

    ineligible_cost = 10.0  # worse than any real pair (-jaccard is in [-1, 0])
    real_block = np.where(
        jaccard_matrix >= match_threshold, -jaccard_matrix, ineligible_cost
    )
    size = n + m
    cost = np.full((size, size), ineligible_cost)
    cost[:n, :m] = real_block
    cost[:n, m:] = np.where(np.eye(n) == 1, 0.0, ineligible_cost)
    cost[n:, :m] = np.where(np.eye(m) == 1, 0.0, ineligible_cost)
    cost[n:, m:] = 0.0

    row_idx, col_idx = linear_sum_assignment(cost)
    matches: dict[int, tuple[str, float]] = {}
    for row, col in zip(row_idx, col_idx, strict=True):
        if row < n and col < m:
            new_id, old_key = new_ids[row], old_keys[col]
            score = jaccard_matrix[row, col]
            if score >= match_threshold:
                matches[new_id] = (old_key, float(score))
    return matches


def _majority_target(
    members: frozenset[str],
    candidates: dict[str, frozenset[str]] | dict[int, frozenset[str]],
) -> tuple[str | int | None, float]:
    """Return the candidate key/id holding the largest share of members, and that
    share. (None, 0.0) if members is empty or nothing overlaps.
    """
    if not members:
        return None, 0.0
    best_id, best_fraction = None, 0.0
    for candidate_id, candidate_members in candidates.items():
        fraction = len(members & candidate_members) / len(members)
        if fraction > best_fraction:
            best_id, best_fraction = candidate_id, fraction
    return best_id, best_fraction


def _merge_targets(
    remaining_old_keys: list[str],
    active_cluster_members: dict[str, frozenset[str]],
    new_cluster_members: dict[int, frozenset[str]],
) -> dict[str, int]:
    """old_key -> new_cluster_id, for each remaining old key whose majority of
    members (>50%) landed in one new cluster.
    """
    merge_target_of: dict[str, int] = {}
    for old_key in remaining_old_keys:
        best_new_id, best_fraction = _majority_target(
            active_cluster_members[old_key], new_cluster_members
        )
        if best_new_id is not None and best_fraction > 0.5:  # noqa: PLR2004
            merge_target_of[old_key] = best_new_id  # type: ignore[assignment]
    return merge_target_of


def match_clusters(
    new_cluster_members: dict[int, frozenset[str]],
    active_cluster_members: dict[str, frozenset[str]],
    match_threshold: float = JACCARD_MATCH_THRESHOLD,
) -> tuple[list[ClusterMatch], list[dict[str, Any]]]:
    """Resolve one completed run's clusters onto stable cluster_keys.

    Args:
        new_cluster_members: this run's cluster_id -> its member pks (noise/-1
            excluded by the caller).
        active_cluster_members: every currently-active cluster_key -> its live
            member pks, from feedback_cluster_membership as of before this run.
        match_threshold: minimum Jaccard for a pair to count as "continued".

    Returns:
        (matches, lineage_rows). matches has one ClusterMatch per new_cluster_id.
        lineage_rows matches CLUSTER_LINEAGE_SCHEMA minus cluster_run_id/
        cluster_lineage_pk, which the caller stamps (same convention as
        ml.lib.cluster: the caller owns run-level identifiers, not this pure
        function) -- one row per edge, plus one 'retired' row per active key that
        neither continued nor merged.
    """
    continued = _continued_matches(
        new_cluster_members, active_cluster_members, match_threshold
    )
    matches: list[ClusterMatch] = []
    lineage_rows: list[dict[str, Any]] = []
    resolved_new_ids = set(continued)

    for new_id, (old_key, score) in continued.items():
        matches.append(ClusterMatch(new_id, old_key, "continued", old_key, score))
        lineage_rows.append(
            {
                "prior_cluster_key": old_key,
                "cluster_key": old_key,
                "cluster_id": new_id,
                "relation": "continued",
                "jaccard": score,
            }
        )

    continued_old_keys = {old_key for old_key, _ in continued.values()}
    remaining_old_keys = [
        old_key
        for old_key in active_cluster_members
        if old_key not in continued_old_keys
    ]

    # Merged: an old key not continued, whose members mostly (>50%) landed in one
    # new cluster. That new cluster's resolved key is its continued key if it has
    # one (the absorbed conversations inherit the surviving cluster's category);
    # otherwise a single fresh key shared by every old key merging into it.
    merge_target_of = _merge_targets(
        remaining_old_keys, active_cluster_members, new_cluster_members
    )
    fresh_key_by_new_id: dict[int, str] = {}
    for old_key, new_id in merge_target_of.items():
        target_key = (
            continued[new_id][0]
            if new_id in continued
            else fresh_key_by_new_id.setdefault(new_id, new_cluster_key())
        )
        lineage_rows.append(
            {
                "prior_cluster_key": old_key,
                "cluster_key": target_key,
                "cluster_id": new_id,
                "relation": "merged",
                "jaccard": None,
            }
        )
    for new_id, key in fresh_key_by_new_id.items():
        matches.append(ClusterMatch(new_id, key, "merged", None, None))
        resolved_new_ids.add(new_id)

    # Split: a new cluster not yet resolved (not continued, not a fresh merge
    # target) that draws most (>50%) of its members from one old key.
    unresolved_new_ids = [
        new_id for new_id in new_cluster_members if new_id not in resolved_new_ids
    ]
    for new_id in unresolved_new_ids:
        best_old_key, best_fraction = _majority_target(
            new_cluster_members[new_id], active_cluster_members
        )
        if best_old_key is not None and best_fraction > 0.5:  # noqa: PLR2004
            key = new_cluster_key()
            matches.append(ClusterMatch(new_id, key, "split", best_old_key, None))  # type: ignore[arg-type]
            lineage_rows.append(
                {
                    "prior_cluster_key": best_old_key,
                    "cluster_key": key,
                    "cluster_id": new_id,
                    "relation": "split",
                    "jaccard": None,
                }
            )
            resolved_new_ids.add(new_id)

    # New: everything else.
    for new_id in new_cluster_members:
        if new_id in resolved_new_ids:
            continue
        key = new_cluster_key()
        matches.append(ClusterMatch(new_id, key, "new", None, None))
        lineage_rows.append(
            {
                "prior_cluster_key": None,
                "cluster_key": key,
                "cluster_id": new_id,
                "relation": "new",
                "jaccard": None,
            }
        )

    # Retired: an active key that neither continued nor merged (it may still have
    # produced one or more 'split' edges above -- that doesn't save it).
    merged_old_keys = set(merge_target_of)
    lineage_rows.extend(
        {
            "prior_cluster_key": old_key,
            "cluster_key": None,
            "cluster_id": None,
            "relation": "retired",
            "jaccard": None,
        }
        for old_key in active_cluster_members
        if old_key not in continued_old_keys and old_key not in merged_old_keys
    )

    return matches, lineage_rows


def compute_continuity(
    matches: list[ClusterMatch], new_cluster_members: dict[int, frozenset[str]]
) -> float:
    """Share of this run's conversations whose cluster kept its prior cluster_key.

    1.0 when new_cluster_members is empty (nothing to disagree about).
    """
    total = sum(len(members) for members in new_cluster_members.values())
    if total == 0:
        return 1.0
    continued_ids = {m.new_cluster_id for m in matches if m.relation == "continued"}
    continued = sum(
        len(members)
        for cluster_id, members in new_cluster_members.items()
        if cluster_id in continued_ids
    )
    return continued / total


def compute_cluster_stats(
    vectors: np.ndarray, radius_percentile: float = CLUSTER_RADIUS_PERCENTILE
) -> tuple[np.ndarray, float]:
    """Centroid (mean vector, L2-normalized) and radius for one cluster's members.

    radius is the similarity at a low percentile of members' cosine similarity to
    the centroid: the placement threshold a future conversation must clear to
    join this cluster.
    """
    centroid = vectors.mean(axis=0)
    norm = np.linalg.norm(centroid)
    if norm > 0:
        centroid = centroid / norm
    vector_norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    normalized = np.divide(
        vectors, vector_norms, out=np.zeros_like(vectors), where=vector_norms > 0
    )
    similarities = normalized @ centroid
    radius = float(np.percentile(similarities, radius_percentile))
    return centroid, radius


MEMBERSHIP_SCHEMA = {
    "feedback_conversation_pk": pl.String,
    "cluster_key": pl.String,
    "cluster_similarity": pl.Float64,
    "cluster_assignment_method": pl.String,
    "cluster_run_id": pl.String,
    "assigned_at": pl.Datetime(time_zone="UTC"),
}


def nearest_active_cluster(
    vector: np.ndarray, active_clusters: list[dict[str, Any]]
) -> tuple[str | None, float | None]:
    """Return the nearest active cluster's key and cosine similarity, if its own
    radius is cleared; (None, None) if not near any live cluster yet.

    active_clusters: [{"cluster_key": ..., "centroid": unit-normalized np.ndarray,
    "radius": float}, ...], already scoped to one embedding_model_version/dim.
    """
    if not active_clusters:
        return None, None
    norm = np.linalg.norm(vector)
    if norm == 0:
        return None, None
    unit_vector = vector / norm
    best_cluster, best_similarity = None, -1.0
    for cluster in active_clusters:
        similarity = float(unit_vector @ cluster["centroid"])
        if similarity >= cluster["radius"] and similarity > best_similarity:
            best_cluster, best_similarity = cluster, similarity
    if best_cluster is not None:
        return best_cluster["cluster_key"], best_similarity
    return None, None
