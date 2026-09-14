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
    # new 0's best option is key-a (jaccard 0.9), with key-b as a weaker second
    # choice (0.6). new 1 overlaps well with key-a (~0.78) but only weakly with
    # key-b (0.3, below the 0.5 threshold -- ineligible). A greedy row-by-row
    # pick (new 0 first) grabs its favorite, key-a, leaving new 1 with only the
    # ineligible key-b and stranding it unmatched (total jaccard 0.9). The
    # global optimum instead gives key-a to new 1 (its only viable option) and
    # key-b to new 0 (still above threshold), matching both (total ~1.38).
    new_cluster_members = {
        0: _members(*[str(i) for i in range(1, 11)]),  # "1".."10"
        1: _members("3", "4", "5", "6", "7", "8", "9"),
    }
    active_cluster_members = {
        "key-a": _members(*[str(i) for i in range(1, 10)]),  # "1".."9"
        "key-b": _members("1", "2", "10", "3", "4", "5"),
    }
    matches, _ = cluster_identity.match_clusters(
        new_cluster_members, active_cluster_members, match_threshold=0.5
    )
    by_new_id = {m.new_cluster_id: m for m in matches}
    assert by_new_id[0].cluster_key == "key-b"
    assert by_new_id[1].cluster_key == "key-a"


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


def test_compute_continuity_all_continued_is_one() -> None:
    new_cluster_members = {0: _members("1", "2", "3")}
    matches, _lineage = cluster_identity.match_clusters(
        new_cluster_members, {"k1": _members("1", "2", "3")}
    )
    assert cluster_identity.compute_continuity(matches, new_cluster_members) == 1.0


def test_compute_continuity_all_new_is_zero() -> None:
    new_cluster_members = {0: _members("1", "2", "3")}
    matches, _lineage = cluster_identity.match_clusters(new_cluster_members, {})
    assert cluster_identity.compute_continuity(matches, new_cluster_members) == 0.0


def test_compute_continuity_weights_by_member_count() -> None:
    new_cluster_members = {
        0: _members("1", "2", "3"),  # continued
        1: _members("4"),  # new
    }
    matches, _lineage = cluster_identity.match_clusters(
        new_cluster_members, {"k1": _members("1", "2", "3")}
    )
    assert cluster_identity.compute_continuity(matches, new_cluster_members) == 0.75


def test_compute_continuity_empty_run_is_one() -> None:
    assert cluster_identity.compute_continuity([], {}) == 1.0


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


def test_nearest_active_cluster_prefers_a_farther_cluster_that_clears_its_radius() -> (
    None
):
    # "a" is the globally closest centroid (similarity 0.8) but its own radius
    # (0.9) isn't cleared; "b" is farther (0.6) but clears its looser radius
    # (0.5). The right answer is "b", not None -- picking the closest centroid
    # first and only checking its radius would wrongly reject the vector.
    active_clusters = [
        {"cluster_key": "a", "centroid": np.array([1.0, 0.0]), "radius": 0.9},
        {"cluster_key": "b", "centroid": np.array([0.0, 1.0]), "radius": 0.5},
    ]
    key, similarity = cluster_identity.nearest_active_cluster(
        np.array([0.8, 0.6]), active_clusters
    )
    assert key == "b"
    assert similarity is not None
    assert similarity == 0.6


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
