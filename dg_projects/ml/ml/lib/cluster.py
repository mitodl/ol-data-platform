"""UMAP + HDBSCAN clustering of feedback conversation embeddings."""

import logging
import os
import uuid
from typing import Any

import numpy as np
import polars as pl
from sklearn.cluster import HDBSCAN
from sklearn.metrics import (
    adjusted_rand_score,
    normalized_mutual_info_score,
    silhouette_score,
)
from umap import UMAP

JOIN_COLS = ["source_slug", "conversation_ref"]

CLUSTER_RUN_SCHEMA = {
    "cluster_run_id": pl.String,
    "algorithm": pl.String,
    "umap_n_components": pl.Int64,
    "umap_n_neighbors": pl.Int64,
    "hdbscan_min_cluster_size": pl.Int64,
    "cluster_count": pl.Int64,
    "noise_count": pl.Int64,
    "total_conversations": pl.Int64,
    "silhouette_score": pl.Float64,
    "run_status": pl.String,
    "run_at": pl.String,
}

CLUSTER_CANDIDATE_SCHEMA = {
    "cluster_run_id": pl.String,
    **dict.fromkeys(JOIN_COLS, pl.String),
    "cluster_id": pl.Int64,
    "cluster_probability": pl.Float64,
}

# §C: UMAP to ~5-15 dims before HDBSCAN. 10 is the midpoint of that range, pending
# tuning against the labeled sample once one exists.
UMAP_N_COMPONENTS = int(os.environ.get("UMAP_N_COMPONENTS", "10"))
UMAP_N_NEIGHBORS = int(os.environ.get("UMAP_N_NEIGHBORS", "15"))

# At conversation grain this reads directly as "how many conversations before
# we call it systemic" (§C) -- a starting point for tuning, not an authoritative
# number from the spec.
HDBSCAN_MIN_CLUSTER_SIZE = int(os.environ.get("HDBSCAN_MIN_CLUSTER_SIZE", "15"))

# Fixed rather than left to UMAP/HDBSCAN's own default (None -- a fresh random
# state per call): a clustering run must be reproducible for the run-vs-run
# comparison the promotion loop depends on.
RANDOM_STATE = int(os.environ.get("CLUSTER_RANDOM_STATE", "42"))

# HDBSCAN's noise label -- kept out of silhouette_score and cluster_count, which
# only describe genuine clusters.
NOISE_CLUSTER_ID = -1

logger = logging.getLogger(__name__)


def new_cluster_run_id() -> str:
    return str(uuid.uuid4())


def reduce_and_cluster(
    vectors: np.ndarray,
    umap_n_components: int = UMAP_N_COMPONENTS,
    umap_n_neighbors: int = UMAP_N_NEIGHBORS,
    min_cluster_size: int = HDBSCAN_MIN_CLUSTER_SIZE,
    random_state: int = RANDOM_STATE,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Reduce vectors' dimensionality via UMAP, then cluster via HDBSCAN.

    Returns (labels, probabilities, reduced): reduced is the UMAP output HDBSCAN
    clustered on, returned so silhouette can be scored in that same space.
    """
    # cosine, not UMAP's euclidean default: embeddings encode meaning in direction.
    reduced = UMAP(
        n_components=umap_n_components,
        n_neighbors=umap_n_neighbors,
        random_state=random_state,
        metric="cosine",
    ).fit_transform(vectors)
    clusterer = HDBSCAN(min_cluster_size=min_cluster_size)
    labels = clusterer.fit_predict(reduced)
    return labels, clusterer.probabilities_, reduced


def compute_silhouette(vectors: np.ndarray, labels: np.ndarray) -> float | None:
    """Silhouette score over the non-noise points only.

    Requires at least 2 genuine clusters with 2+ members each to be defined;
    returns None (not 0.0, which would misleadingly read as "bad but valid")
    when the run doesn't clear that bar -- e.g. everything landed in one
    cluster, or everything is noise.
    """
    non_noise = labels != NOISE_CLUSTER_ID
    if non_noise.sum() < 2:  # noqa: PLR2004
        return None
    distinct_clusters = np.unique(labels[non_noise])
    if len(distinct_clusters) < 2:  # noqa: PLR2004
        return None
    return float(silhouette_score(vectors[non_noise], labels[non_noise]))


def compute_cluster_agreement(
    cluster_labels: np.ndarray, reference_labels: list[str | None]
) -> dict[str, float] | None:
    """ARI/NMI between cluster_id and a noisy reference (e.g. dominant Zendesk tag).

    Untagged conversations are dropped, not treated as their own category. Returns
    None if fewer than 2 tagged points or 2 distinct tags remain.
    """
    reference = np.asarray(reference_labels, dtype=object)
    tagged = reference != None  # noqa: E711 -- np.asarray(dtype=object) needs `!= None`, not `is not None`
    if tagged.sum() < 2 or len(np.unique(reference[tagged])) < 2:  # noqa: PLR2004
        return None
    return {
        "ari": float(adjusted_rand_score(reference[tagged], cluster_labels[tagged])),
        "nmi": float(
            normalized_mutual_info_score(reference[tagged], cluster_labels[tagged])
        ),
    }


def cluster_embeddings(
    embeddings_df: pl.DataFrame,
    umap_n_components: int = UMAP_N_COMPONENTS,
    umap_n_neighbors: int = UMAP_N_NEIGHBORS,
    min_cluster_size: int = HDBSCAN_MIN_CLUSTER_SIZE,
    random_state: int = RANDOM_STATE,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Cluster every row's embedding_vector; produce this run's candidates + summary.

    Args:
        embeddings_df: a frame with (at least) source_slug, conversation_ref,
            embedding_vector columns, e.g. feedback_embeddings filtered to one
            consistent embedding_model_version/embedding_dim.

    Returns:
        (candidates_df, run_metadata): candidates_df has cluster_run_id plus
            JOIN_COLS, cluster_id, cluster_probability -- one row per input
            conversation. run_metadata matches CLUSTER_RUN_SCHEMA minus
            cluster_run_id/run_at, which the caller stamps (the caller owns
            the run id and the wall-clock time, not this pure function).
    """
    cluster_run_id = new_cluster_run_id()
    vectors = np.array(embeddings_df["embedding_vector"].to_list())

    labels, probabilities, reduced = reduce_and_cluster(
        vectors,
        umap_n_components=umap_n_components,
        umap_n_neighbors=umap_n_neighbors,
        min_cluster_size=min_cluster_size,
        random_state=random_state,
    )
    # `reduced`, not `vectors`: score in the same space HDBSCAN clustered on.
    silhouette = compute_silhouette(reduced, labels)

    candidates_df = embeddings_df.select(JOIN_COLS).with_columns(
        pl.lit(cluster_run_id).alias("cluster_run_id"),
        pl.Series("cluster_id", labels, dtype=pl.Int64),
        pl.Series("cluster_probability", probabilities, dtype=pl.Float64),
    )

    noise_count = int((labels == NOISE_CLUSTER_ID).sum())
    cluster_count = len(np.unique(labels[labels != NOISE_CLUSTER_ID]))
    run_metadata = {
        "cluster_run_id": cluster_run_id,
        "algorithm": "umap+hdbscan",
        "umap_n_components": umap_n_components,
        "umap_n_neighbors": umap_n_neighbors,
        "hdbscan_min_cluster_size": min_cluster_size,
        "cluster_count": cluster_count,
        "noise_count": noise_count,
        "total_conversations": embeddings_df.height,
        "silhouette_score": silhouette,
        "run_status": "completed",
    }
    return candidates_df, run_metadata
