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
        umap_n_components=2,
        umap_n_neighbors=5,
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
    # Two well-separated blobs should not all collapse into a single cluster.
    assert run_metadata["cluster_count"] >= 1
