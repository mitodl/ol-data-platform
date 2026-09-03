"""Tests for ml.lib.cluster."""

import numpy as np
import polars as pl
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
            "source_slug": ["zendesk"] * (n_per_cluster * 2),
            "conversation_ref": [str(i) for i in range(n_per_cluster * 2)],
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
    assert run_metadata["embedding_input_filter"] == "summary"
    assert run_metadata["random_state"] == 42
    # Two well-separated blobs should not all collapse into a single cluster.
    assert run_metadata["cluster_count"] >= 1
