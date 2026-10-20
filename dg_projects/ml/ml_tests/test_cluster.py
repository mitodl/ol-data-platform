"""Tests for ml.lib.cluster."""

import numpy as np
import polars as pl
import pytest
from ml.lib import cluster


def test_compute_silhouette_returns_none_when_everything_is_noise() -> None:
    labels = np.array([-1, -1, -1, -1])
    vectors = np.zeros((4, 3))

    assert cluster.compute_silhouette(vectors, labels) is None


def test_compute_silhouette_returns_none_with_only_one_genuine_cluster() -> None:
    labels = np.array([0, 0, 0, -1])
    vectors = np.zeros((4, 3))

    assert cluster.compute_silhouette(vectors, labels) is None


def test_compute_silhouette_returns_a_score_for_two_well_separated_clusters() -> None:
    vectors = np.array(
        [[0.0, 0.0], [0.1, 0.1], [0.0, 0.1], [10.0, 10.0], [10.1, 10.1], [10.0, 10.1]]
    )
    labels = np.array([0, 0, 0, 1, 1, 1])

    score = cluster.compute_silhouette(vectors, labels)

    assert score is not None
    # Tight, well-separated clusters should score close to the maximum of 1.0.
    assert score > 0.9


def test_compute_silhouette_ignores_noise_points() -> None:
    """A noise point sitting between the two clusters must not drag the score down."""
    tight_clusters = np.array(
        [[0.0, 0.0], [0.1, 0.1], [0.0, 0.1], [10.0, 10.0], [10.1, 10.1], [10.0, 10.1]]
    )
    with_noise_between = np.vstack([tight_clusters, [[5.0, 5.0]]])
    labels = np.array([0, 0, 0, 1, 1, 1, -1])

    score = cluster.compute_silhouette(with_noise_between, labels)

    assert score is not None
    assert score > 0.9


def test_compute_cluster_agreement_returns_none_with_fewer_than_two_distinct_tags() -> (
    None
):
    cluster_labels = np.array([0, 0, 1, 1])
    reference_labels = ["billing", "billing", "billing", "billing"]

    assert cluster.compute_cluster_agreement(cluster_labels, reference_labels) is None


def test_compute_cluster_agreement_drops_untagged_conversations() -> None:
    """None entries (no dominant tag) must not count as their own reference group."""
    cluster_labels = np.array([0, 0, 1, 1])
    reference_labels = ["billing", "billing", None, None]

    # Only 2 tagged rows remain, and they share one tag -- same as the
    # fewer-than-two-distinct-tags case once untagged rows are dropped.
    assert cluster.compute_cluster_agreement(cluster_labels, reference_labels) is None


def test_compute_cluster_agreement_scores_perfect_agreement() -> None:
    cluster_labels = np.array([0, 0, 0, 1, 1, 1])
    reference_labels = ["billing", "billing", "billing", "login", "login", "login"]

    result = cluster.compute_cluster_agreement(cluster_labels, reference_labels)

    assert result is not None
    assert result["ari"] == 1.0
    assert result["nmi"] == 1.0


def test_compute_cluster_agreement_scores_low_for_unrelated_labels() -> None:
    """Clusters that don't align with the tags at all should score near 0, not 1."""
    cluster_labels = np.array([0, 0, 1, 1, 0, 1])
    reference_labels = ["billing", "login", "billing", "login", "billing", "login"]

    result = cluster.compute_cluster_agreement(cluster_labels, reference_labels)

    assert result is not None
    assert result["ari"] < 0.2
    assert result["nmi"] < 0.2


def test_cluster_embeddings_produces_one_candidate_row_per_input_conversation() -> None:
    rng = np.random.default_rng(0)
    n_per_cluster = 20
    cluster_a = rng.normal(loc=0.0, scale=0.1, size=(n_per_cluster, 5))
    cluster_b = rng.normal(loc=20.0, scale=0.1, size=(n_per_cluster, 5))
    vectors = np.vstack([cluster_a, cluster_b])
    df = pl.DataFrame(
        {
            "feedback_conversation_pk": [str(i) for i in range(n_per_cluster * 2)],
            "source_slug": ["zendesk"] * (n_per_cluster * 2),
            "conversation_ref": [str(i) for i in range(n_per_cluster * 2)],
            "embedding_input": ["summary"] * (n_per_cluster * 2),
            "embedding_vector": vectors.tolist(),
        }
    )

    candidates_df, run_metadata = cluster.cluster_embeddings(
        df,
        ("text-embedding-3-small", 5, "summary"),
        umap_params=(2, 5),
        min_cluster_size=5,
        random_state=42,
    )

    assert candidates_df.height == df.height
    assert sorted(candidates_df["conversation_ref"].to_list()) == sorted(
        df["conversation_ref"].to_list()
    )
    assert candidates_df["cluster_run_id"].n_unique() == 1
    assert run_metadata["total_conversations"] == df.height
    assert run_metadata["run_status"] == "completed"
    assert run_metadata["algorithm"] == "umap+hdbscan"
    assert run_metadata["embedding_model_version"] == "text-embedding-3-small"
    assert run_metadata["embedding_dim"] == 5
    assert run_metadata["is_promoted"] is False
    assert run_metadata["embedding_input_filter"] == "summary"
    assert run_metadata["random_state"] == 42
    assert run_metadata["hdbscan_min_samples"] == cluster.HDBSCAN_MIN_SAMPLES
    assert run_metadata["hdbscan_cluster_selection_method"] == "leaf"
    # Two well-separated blobs must not collapse into a single cluster.
    assert run_metadata["cluster_count"] >= 2


def test_reduce_and_cluster_raises_min_cluster_size_to_fit_max_clusters() -> None:
    rng = np.random.default_rng(0)
    vectors = np.vstack(
        [rng.normal(loc=offset, scale=0.1, size=(20, 5)) for offset in (0, 20, 40, 60)]
    )

    def count(labels: np.ndarray) -> int:
        return len(set(labels.tolist()) - {cluster.NOISE_CLUSTER_ID})

    uncapped_labels, _, _, uncapped_size = cluster.reduce_and_cluster(
        vectors,
        umap_n_components=2,
        umap_n_neighbors=5,
        min_cluster_size=5,
        max_clusters=None,
    )
    capped_labels, _, _, capped_size = cluster.reduce_and_cluster(
        vectors,
        umap_n_components=2,
        umap_n_neighbors=5,
        min_cluster_size=5,
        max_clusters=2,
    )

    assert uncapped_size == 5
    assert count(uncapped_labels) > 2
    assert capped_size > 5
    assert count(capped_labels) <= 2


def _unit(rows: np.ndarray) -> np.ndarray:
    return rows / np.linalg.norm(rows, axis=1, keepdims=True)


def _fix_noise_groups(monkeypatch, group_labels: np.ndarray) -> None:
    # HDBSCAN splits the same points differently across platforms, so the tests of
    # what happens to each group set the groups themselves
    class FakeUmap:
        def __init__(self, **_kwargs):
            pass

        def fit_transform(self, vectors):
            return vectors

    class FakeHdbscan:
        def __init__(self, **_kwargs):
            self.probabilities_ = np.ones(len(group_labels))

        def fit_predict(self, _reduced):
            return group_labels

    monkeypatch.setattr(cluster, "UMAP", FakeUmap)
    monkeypatch.setattr(cluster, "HDBSCAN", FakeHdbscan)


def test_cluster_new_themes_adds_far_groups_and_places_near_ones(monkeypatch) -> None:
    e = np.eye(3)
    # Group 0 shares cluster 0's direction and is no larger, so it joins cluster 0;
    # group 1 is a new theme
    unit_vectors = np.vstack(
        [np.tile(e[0], (10, 1)), np.tile(e[0], (10, 1)), np.tile(e[1], (10, 1))]
    )
    labels = np.array([0] * 10 + [cluster.NOISE_CLUSTER_ID] * 20)
    _fix_noise_groups(monkeypatch, np.array([0] * 10 + [1] * 10))

    new_labels, new_probabilities, new_count, placed_count, _ = (
        cluster.cluster_new_themes(
            unit_vectors, labels, np.zeros(30), min_cluster_size=2, umap_params=(2, 5)
        )
    )

    assert (new_count, placed_count) == (1, 1)
    assert (new_labels[:20] == 0).all()
    assert (new_labels[20:] == 1).all()
    assert (new_probabilities[20:] > 0).all()


def test_cluster_new_themes_keeps_a_group_near_a_new_cluster_as_noise(
    monkeypatch,
) -> None:
    e = np.eye(3)
    # Both groups are far from cluster 0 and 0.8 similar to each other: too close
    # to be two categories, not close enough to merge
    unit_vectors = _unit(
        np.vstack(
            [
                np.tile(e[0], (10, 1)),
                np.tile(e[1], (10, 1)),
                np.tile(0.8 * e[1] + 0.6 * e[2], (8, 1)),
            ]
        )
    )
    labels = np.array([0] * 10 + [cluster.NOISE_CLUSTER_ID] * 18)
    _fix_noise_groups(monkeypatch, np.array([0] * 10 + [1] * 8))

    new_labels, _, new_count, placed_count, capped_count = cluster.cluster_new_themes(
        unit_vectors, labels, np.zeros(28), min_cluster_size=2, umap_params=(2, 5)
    )

    assert (new_count, placed_count, capped_count) == (1, 0, 0)
    assert (new_labels[10:20] == 1).all()
    assert (new_labels[20:] == cluster.NOISE_CLUSTER_ID).all()


def test_cluster_new_themes_merges_a_split_theme_and_caps_new_clusters(
    monkeypatch,
) -> None:
    e = np.eye(5)
    unit_vectors = _unit(
        np.vstack(
            [
                np.tile(e[0], (20, 1)),  # cluster 0
                np.tile(e[1] + 0.5 * e[2], (5, 1)),  # group 0: small part of a theme
                np.tile(e[1], (10, 1)),  # group 1: its larger part
                np.tile(e[3], (6, 1)),  # group 2: a second new theme
                np.tile(e[0] + 0.1 * e[3], (4, 1)),  # group 3: edge of cluster 0
                np.tile(e[0] + 0.1 * e[4], (4, 1)),  # group 4: another edge
                # group 5: close to group 1's centroid, not to group 0's
                np.tile(e[1] - 0.4 * e[2], (4, 1)),
            ]
        )
    )
    labels = np.array([0] * 20 + [cluster.NOISE_CLUSTER_ID] * 33)
    # Group 2 has the lowest label, so only size order keeps it from the cap's slot
    group_labels = np.array([1] * 5 + [2] * 10 + [0] * 6 + [3] * 4 + [4] * 4 + [5] * 4)

    _fix_noise_groups(monkeypatch, group_labels)

    new_labels, _, new_count, placed_count, capped_count = cluster.cluster_new_themes(
        unit_vectors,
        labels,
        np.zeros(53),
        min_cluster_size=2,
        max_new_clusters=1,
        umap_params=(2, 5),
    )

    # Larger group 1 becomes the cluster, and groups 0 and 5 join it; the cap leaves
    # group 2 as noise; cluster 0 takes group 3 but not a second group
    assert (new_count, placed_count, capped_count) == (1, 3, 1)
    assert (new_labels[20:35] == 1).all()
    assert (new_labels[35:41] == cluster.NOISE_CLUSTER_ID).all()
    assert (new_labels[41:45] == 0).all()
    assert (new_labels[45:49] == cluster.NOISE_CLUSTER_ID).all()
    assert (new_labels[49:] == 1).all()


@pytest.mark.parametrize(
    ("existing", "groups", "expected"),
    [
        # Group 25 deg merges into group 0 deg and moves its centroid to 11 deg, so
        # group 50 deg is then 0.78 similar: too close to be a second category
        ((90, 10), [(0, 600), (25, 500), (50, 400)], (1, 1)),
        # Group 25 deg is closest to new cluster 45 deg, but joining it would bring
        # that cluster within 0.81 of new cluster 0 deg
        ((90, 10), [(0, 600), (45, 550), (25, 500)], (2, 0)),
        # Group 25 deg is close to existing cluster 0 deg, but joining it would bring
        # that cluster within 0.79 of new cluster 45 deg
        ((0, 1000), [(45, 500), (25, 400)], (1, 0)),
    ],
)
def test_cluster_new_themes_keeps_categories_apart_after_a_join(
    monkeypatch, existing, groups, expected
) -> None:
    def at(degrees: float) -> np.ndarray:
        # The existing cluster sits at 90 deg, out of the plane, when it is 90
        radians = np.radians(degrees)
        if degrees == 90:
            return np.array([0.0, 0.0, 1.0])
        return np.array([np.cos(radians), np.sin(radians), 0.0])

    existing_degrees, existing_size = existing
    unit_vectors = np.vstack(
        [np.tile(at(existing_degrees), (existing_size, 1))]
        + [np.tile(at(degrees), (size, 1)) for degrees, size in groups]
    )
    noise_size = sum(size for _, size in groups)
    labels = np.array([0] * existing_size + [cluster.NOISE_CLUSTER_ID] * noise_size)
    _fix_noise_groups(
        monkeypatch,
        np.concatenate([[index] * size for index, (_, size) in enumerate(groups)]),
    )

    _, _, new_count, placed_count, _ = cluster.cluster_new_themes(
        unit_vectors,
        labels,
        np.zeros(len(labels)),
        min_cluster_size=2,
        umap_params=(2, 5),
    )

    assert (new_count, placed_count) == expected


def test_cluster_new_themes_keeps_a_near_group_larger_than_its_cluster_as_noise(
    monkeypatch,
) -> None:
    e = np.eye(3)
    # The near group (15) is larger than cluster 0 (5): a broader theme, left as noise
    unit_vectors = np.vstack(
        [np.tile(e[0], (5, 1)), np.tile(e[0], (15, 1)), np.tile(e[1], (10, 1))]
    )
    labels = np.array([0] * 5 + [cluster.NOISE_CLUSTER_ID] * 25)
    _fix_noise_groups(monkeypatch, np.array([0] * 15 + [1] * 10))

    new_labels, _, new_count, placed_count, capped_count = cluster.cluster_new_themes(
        unit_vectors, labels, np.zeros(30), min_cluster_size=2, umap_params=(2, 5)
    )

    assert (new_count, placed_count, capped_count) == (1, 0, 0)
    assert (new_labels[5:20] == cluster.NOISE_CLUSTER_ID).all()
    assert (new_labels[20:] == 1).all()


def test_extend_clusters_keeps_live_keys_and_adds_new_themes() -> None:
    rng = np.random.default_rng(0)
    vectors = _unit(
        np.vstack(
            [
                np.tile([1.0, 0.0, 0.0], (10, 1)),  # live cluster "a"
                np.tile([1.0, 0.1, 0.0], (5, 1)),  # noise close to "a"
                # Two new themes: HDBSCAN never returns its whole input as one
                # cluster, so a single leftover group would stay noise.
                np.tile([0.0, 0.0, 1.0], (10, 1)),
                np.tile([0.0, -1.0, 0.0], (10, 1)),
            ]
        )
        + rng.normal(scale=0.01, size=(35, 3))
    )
    pks = [str(i) for i in range(35)]
    df = pl.DataFrame(
        {
            "feedback_conversation_pk": pks,
            "source_slug": ["zendesk"] * 35,
            "conversation_ref": pks,
            "embedding_input": ["summary"] * 35,
            "embedding_vector": vectors.tolist(),
        }
    )

    candidates_df, run_metadata = cluster.extend_clusters(
        df,
        ("text-embedding-3-small", 3, "summary"),
        dict.fromkeys(pks[:10], "a"),
        umap_params=(2, 5),
        add_new_clusters=True,
        new_cluster_min_size=5,
    )

    carried = candidates_df["cluster_key"].to_list()
    cluster_ids = candidates_df["cluster_id"].to_list()
    assert carried[:15] == ["a"] * 15
    assert carried[15:] == [None] * 20
    assert len(set(cluster_ids[15:25])) == 1
    assert len(set(cluster_ids[25:])) == 1
    assert len({cluster_ids[0], cluster_ids[15], cluster_ids[25]}) == 3
    assert cluster.NOISE_CLUSTER_ID not in cluster_ids
    assert run_metadata["run_type"] == "incremental"
    assert run_metadata["placed_noise_count"] == 5
    assert run_metadata["new_cluster_count"] == 2
    assert run_metadata["cluster_count"] == 3

    # A live key with no member in this run keeps its id: "z" sorts after "a", so
    # the first new theme would otherwise take it and continue "z".
    gap_df, _ = cluster.extend_clusters(
        df,
        ("text-embedding-3-small", 3, "summary"),
        {**dict.fromkeys(pks[:10], "a"), "missing": "z"},
        umap_params=(2, 5),
        add_new_clusters=True,
        new_cluster_min_size=5,
    )
    assert "z" not in gap_df["cluster_key"].to_list()
    assert gap_df["cluster_key"].to_list()[15:] == [None] * 20

    # The default only places close noise: the new themes stay noise.
    default_df, default_metadata = cluster.extend_clusters(
        df,
        ("text-embedding-3-small", 3, "summary"),
        dict.fromkeys(pks[:10], "a"),
        umap_params=(2, 5),
    )
    assert default_df["cluster_key"].to_list()[:15] == ["a"] * 15
    assert set(default_df["cluster_id"].to_list()[15:]) == {cluster.NOISE_CLUSTER_ID}
    assert default_metadata["new_cluster_count"] is None
    assert default_metadata["cluster_count"] == 1


def test_filter_conversation_scope_drops_conversations_below_source_minimum() -> None:
    embeddings_lf = pl.LazyFrame(
        {"feedback_conversation_pk": ["short", "long", "ticket"]}
    )
    conversations_lf = pl.LazyFrame(
        {
            "feedback_conversation_pk": ["short", "long", "ticket"],
            "source_slug": ["learn_ai_tutor", "learn_ai_tutor", "zendesk"],
            "conversation_text_chars": [26, 80, 10],
            "conversation_text": ["x" * 26, "x" * 80, "x" * 10],
        }
    )

    result = cluster.filter_conversation_scope(embeddings_lf, conversations_lf, None)

    assert result.collect()["feedback_conversation_pk"].to_list() == ["long", "ticket"]


def test_drop_short_conversations_drops_placeholder_only_text() -> None:
    conversations_lf = pl.LazyFrame(
        {
            "feedback_conversation_pk": [
                "empty",
                "redacted",
                "two_empty_turns",
                "short",
                "pending",
            ],
            "source_slug": ["zendesk"] * 5,
            "conversation_text_chars": [12, 18, 24, 2, None],
            "conversation_text": [
                "[No content]",
                "<PERSON>\n <EMAIL_ADDRESS>",
                "[No content]\n---\nSCRUBBED",
                "Hi",
                None,
            ],
        }
    )

    result = cluster.drop_short_conversations(conversations_lf, None)

    assert result.collect()["feedback_conversation_pk"].to_list() == [
        "short",
        "pending",
    ]


def test_should_trigger_early_recluster_with_no_prior_completed_run() -> None:
    assert cluster.should_trigger_early_recluster(1, None) is True
    assert cluster.should_trigger_early_recluster(0, None) is False


def test_should_trigger_early_recluster_below_growth_threshold() -> None:
    assert (
        cluster.should_trigger_early_recluster(1_000, 900, growth_trigger=200) is False
    )


def test_should_trigger_early_recluster_at_or_above_growth_threshold() -> None:
    assert (
        cluster.should_trigger_early_recluster(1_100, 900, growth_trigger=200) is True
    )
    assert (
        cluster.should_trigger_early_recluster(1_200, 900, growth_trigger=200) is True
    )


def test_filter_conversation_scope_keeps_feedback_since_date() -> None:
    embeddings_lf = pl.LazyFrame({"feedback_conversation_pk": ["old", "same", "new"]})
    conversations_lf = pl.LazyFrame(
        {
            "feedback_conversation_pk": ["old", "same", "new"],
            "conversation_opened_at": [
                "2023-12-31T23:59:59.000",
                "2024-01-01T00:00:00.000",
                "2025-06-01T12:00:00.000",
            ],
            "conversation_text": ["text"] * 3,
        }
    )

    result = cluster.filter_conversation_scope(
        embeddings_lf, conversations_lf, "2024-01-01", min_chars_by_source=None
    )

    assert result.collect()["feedback_conversation_pk"].to_list() == ["same", "new"]
    unfiltered = cluster.filter_conversation_scope(
        embeddings_lf, conversations_lf, None, min_chars_by_source=None
    )
    assert unfiltered.collect().height == 3
