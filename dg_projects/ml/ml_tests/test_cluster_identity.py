"""Tests for ml.lib.cluster_identity."""

import numpy as np
from ml.lib import cluster_identity


def _members(*pks: str) -> frozenset[str]:
    return frozenset(pks)


def test_jaccard_basic() -> None:
    a = _members("1", "2", "3")
    b = _members("2", "3", "4")
    assert cluster_identity.jaccard(a, b) == 2 / 4


def test_jaccard_both_empty_is_zero() -> None:
    assert cluster_identity.jaccard(frozenset(), frozenset()) == 0.0


def test_match_clusters_continued_when_membership_mostly_overlaps() -> None:
    new_cluster_members = {0: _members("1", "2", "3", "4")}
    active_cluster_members = {"key-a": _members("1", "2", "3")}

    matches, lineage = cluster_identity.match_clusters(
        new_cluster_members, active_cluster_members, match_threshold=0.5
    )

    assert len(matches) == 1
    assert matches[0].relation == "continued"
    assert matches[0].cluster_key == "key-a"
    assert lineage == [
        {
            "prior_cluster_key": "key-a",
            "cluster_key": "key-a",
            "cluster_id": 0,
            "relation": "continued",
            "jaccard": 3 / 4,
        }
    ]


def test_match_clusters_below_threshold_is_new_not_continued() -> None:
    new_cluster_members = {0: _members("1", "2", "9", "10")}
    active_cluster_members = {"key-a": _members("1", "2", "3", "4", "5", "6")}

    matches, lineage = cluster_identity.match_clusters(
        new_cluster_members, active_cluster_members, match_threshold=0.5
    )

    assert matches[0].relation == "new"
    assert matches[0].prior_cluster_key is None
    # The unmatched old key gets retired.
    assert {
        "prior_cluster_key": "key-a",
        "cluster_key": None,
        "cluster_id": None,
        "relation": "retired",
        "jaccard": None,
    } in lineage


def test_match_clusters_picks_globally_best_pairing_not_greedy() -> None:
    # new 0 overlaps best with key-a (0.9) and okay with key-b (0.6);
    # new 1 overlaps only with key-b (0.7). A greedy per-row pick would give
    # new0->key-a, new1->key-b -- which is also the global optimum here, so
    # construct a case where the naive "take the best row match first" would
    # collide: both new clusters prefer key-a, only one can have it.
    new_cluster_members = {
        0: _members(*[str(i) for i in range(9)]),  # 9 members
        1: _members(*[str(i) for i in range(9, 18)]),  # 9 members, disjoint
    }
    active_cluster_members = {
        "key-a": _members(*[str(i) for i in range(9)]),  # perfect match with new 0
        "key-b": _members(*[str(i) for i in range(9, 18)]),  # perfect match with new 1
    }
    matches, _ = cluster_identity.match_clusters(
        new_cluster_members, active_cluster_members, match_threshold=0.5
    )
    by_new_id = {m.new_cluster_id: m for m in matches}
    assert by_new_id[0].cluster_key == "key-a"
    assert by_new_id[1].cluster_key == "key-b"


def test_match_clusters_merge_absorbed_into_continued_cluster_keeps_its_key() -> None:
    # new cluster 0 continues key-a (majority overlap), and also absorbs key-b
    # (key-b's members mostly land in new cluster 0 too).
    new_cluster_members = {
        0: _members("1", "2", "3", "4", "5", "6"),
    }
    active_cluster_members = {
        "key-a": _members("1", "2", "3", "4"),
        "key-b": _members("5", "6"),
    }
    matches, lineage = cluster_identity.match_clusters(
        new_cluster_members, active_cluster_members, match_threshold=0.5
    )
    assert len(matches) == 1
    assert matches[0].cluster_key == "key-a"
    assert matches[0].relation == "continued"
    assert {
        "prior_cluster_key": "key-b",
        "cluster_key": "key-a",
        "cluster_id": 0,
        "relation": "merged",
        "jaccard": None,
    } in lineage


def test_match_clusters_merge_with_no_continuation_mints_one_shared_key() -> None:
    # New cluster 0 doesn't continue anything (no single old key clears the
    # continued threshold against it alone), but two old keys each land their
    # majority there -- both should merge into the SAME fresh key.
    new_cluster_members = {0: _members("1", "2", "3", "4")}
    active_cluster_members = {
        "key-a": _members("1", "2", "9", "10"),  # 50% overlap -- not majority
        "key-b": _members("3", "4", "11"),  # 2/3 = majority
    }
    matches, lineage = cluster_identity.match_clusters(
        new_cluster_members, active_cluster_members, match_threshold=0.5
    )
    assert len(matches) == 1
    assert matches[0].relation == "merged"
    merged_rows = [row for row in lineage if row["relation"] == "merged"]
    assert len(merged_rows) == 1
    assert merged_rows[0]["prior_cluster_key"] == "key-b"
    assert merged_rows[0]["cluster_key"] == matches[0].cluster_key


def test_match_clusters_split_when_new_cluster_draws_most_from_one_old_key() -> None:
    # Neither new cluster clears the continued threshold against key-a (jaccard
    # 2/7 each), but each draws its majority (2 of 3 members) from it -- key-a
    # fragmented into two new clusters, both 'split'.
    new_cluster_members = {
        0: _members("1", "2", "7"),
        1: _members("3", "4", "8"),
    }
    active_cluster_members = {
        "key-a": _members("1", "2", "3", "4", "5", "6"),
    }
    matches, lineage = cluster_identity.match_clusters(
        new_cluster_members, active_cluster_members, match_threshold=0.5
    )
    assert {m.relation for m in matches} == {"split"}
    for match in matches:
        assert match.prior_cluster_key == "key-a"
    split_rows = [row for row in lineage if row["relation"] == "split"]
    assert len(split_rows) == 2
    retired_rows = [row for row in lineage if row["relation"] == "retired"]
    assert retired_rows == [
        {
            "prior_cluster_key": "key-a",
            "cluster_key": None,
            "cluster_id": None,
            "relation": "retired",
            "jaccard": None,
        }
    ]


def test_match_clusters_new_when_no_overlap_with_anything() -> None:
    new_cluster_members = {0: _members("100", "101", "102")}
    active_cluster_members = {"key-a": _members("1", "2", "3")}
    matches, lineage = cluster_identity.match_clusters(
        new_cluster_members, active_cluster_members
    )
    assert matches[0].relation == "new"
    assert {
        "prior_cluster_key": None,
        "cluster_key": matches[0].cluster_key,
        "cluster_id": 0,
        "relation": "new",
        "jaccard": None,
    } in lineage
    assert {
        "prior_cluster_key": "key-a",
        "cluster_key": None,
        "cluster_id": None,
        "relation": "retired",
        "jaccard": None,
    } in lineage


def test_match_clusters_bootstrap_with_no_active_clusters() -> None:
    new_cluster_members = {0: _members("1", "2"), 1: _members("3", "4")}
    matches, lineage = cluster_identity.match_clusters(new_cluster_members, {})
    assert {m.relation for m in matches} == {"new"}
    assert all(row["relation"] == "new" for row in lineage)


def test_compute_cluster_stats_centroid_and_radius() -> None:
    vectors = np.array(
        [
            [1.0, 0.0],
            [0.0, 1.0],
            [1.0, 0.0],
        ]
    )
    centroid, radius = cluster_identity.compute_cluster_stats(
        vectors, radius_percentile=5
    )
    assert centroid.shape == (2,)
    # centroid is unit-normalized
    assert abs(np.linalg.norm(centroid) - 1.0) < 1e-6
    # radius is a valid cosine similarity
    assert -1.0 <= radius <= 1.0


def test_nearest_active_cluster_picks_closest_and_clears_radius() -> None:
    active_clusters = [
        {"cluster_key": "a", "centroid": np.array([1.0, 0.0]), "radius": 0.5},
        {"cluster_key": "b", "centroid": np.array([0.0, 1.0]), "radius": 0.5},
    ]
    key, similarity = cluster_identity.nearest_active_cluster(
        np.array([0.9, 0.1]), active_clusters
    )
    assert key == "a"
    assert similarity is not None
    assert similarity > 0.9


def test_nearest_active_cluster_none_when_radius_not_cleared() -> None:
    active_clusters = [
        {"cluster_key": "a", "centroid": np.array([1.0, 0.0]), "radius": 0.99},
    ]
    key, similarity = cluster_identity.nearest_active_cluster(
        np.array([0.5, 0.5]), active_clusters
    )
    assert key is None
    assert similarity is None


def test_nearest_active_cluster_none_when_no_active_clusters() -> None:
    key, similarity = cluster_identity.nearest_active_cluster(np.array([1.0, 0.0]), [])
    assert key is None
    assert similarity is None


def test_nearest_active_cluster_zero_vector_is_unplaced() -> None:
    active_clusters = [
        {"cluster_key": "a", "centroid": np.array([1.0, 0.0]), "radius": -1.0}
    ]
    key, similarity = cluster_identity.nearest_active_cluster(
        np.array([0.0, 0.0]), active_clusters
    )
    assert key is None
    assert similarity is None
