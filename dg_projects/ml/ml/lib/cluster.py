"""UMAP + HDBSCAN clustering of feedback conversation embeddings."""

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

JOIN_COLS = ["feedback_conversation_pk"]
# Not part of the join key, but carried through for debugging without a join.
# embedding_input specifically: a mixed-arm run's per-cluster raw-vs-summary
# breakdown is unreconstructable from feedback_embeddings once it's upserted.
DEBUG_COLS = ["source_slug", "conversation_ref", "embedding_input"]

CLUSTER_RUN_SCHEMA = {
    "cluster_run_id": pl.String,
    "algorithm": pl.String,
    "embedding_model_version": pl.String,
    "embedding_dim": pl.Int64,
    "embedding_input_filter": pl.String,
    "umap_n_components": pl.Int64,
    "umap_n_neighbors": pl.Int64,
    "hdbscan_min_cluster_size": pl.Int64,
    "random_state": pl.Int64,
    "cluster_count": pl.Int64,
    "noise_count": pl.Int64,
    "total_conversations": pl.Int64,
    "silhouette_score": pl.Float64,
    "run_status": pl.String,
    "run_at": pl.Datetime(time_zone="UTC"),
}

# Column order matches how cluster_embeddings actually writes it: JOIN_COLS then
# DEBUG_COLS then the added columns, not cluster_run_id first.
CLUSTER_CANDIDATE_SCHEMA = {
    **dict.fromkeys(JOIN_COLS, pl.String),
    **dict.fromkeys(DEBUG_COLS, pl.String),
    "cluster_run_id": pl.String,
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

# silhouette_score is O(n^2) (pairwise distances) -- at the ~198K-conversation
# MVP scale that's tens of billions of distance calculations after clustering
# has already finished. Bounding it to a deterministic random sample keeps the
# metric's cost independent of corpus size.
SILHOUETTE_MAX_SAMPLES = int(os.environ.get("SILHOUETTE_MAX_SAMPLES", "5000"))

# HDBSCAN's noise label -- kept out of silhouette_score and cluster_count, which
# only describe genuine clusters.
NOISE_CLUSTER_ID = -1


def new_cluster_run_id() -> str:
    return str(uuid.uuid4())


def failed_run_metadata(  # noqa: PLR0913 -- same shape as cluster_embeddings's args
    cluster_run_id: str,
    embedding_provenance: tuple[str, int, str | None],
    umap_params: tuple[int, int],
    min_cluster_size: int,
    random_state: int,
    total_conversations: int,
) -> dict[str, Any]:
    """Build a feedback_cluster_run row for an attempt that didn't complete.

    Matches CLUSTER_RUN_SCHEMA minus run_at (the caller stamps that). The
    caller raises after writing this -- it's a record, not error handling.
    """
    embedding_model_version, embedding_dim, embedding_input_filter = (
        embedding_provenance
    )
    umap_n_components, umap_n_neighbors = umap_params
    return {
        "cluster_run_id": cluster_run_id,
        "algorithm": "umap+hdbscan",
        "embedding_model_version": embedding_model_version,
        "embedding_dim": embedding_dim,
        "embedding_input_filter": embedding_input_filter,
        "umap_n_components": umap_n_components,
        "umap_n_neighbors": umap_n_neighbors,
        "hdbscan_min_cluster_size": min_cluster_size,
        "random_state": random_state,
        "cluster_count": 0,
        "noise_count": 0,
        "total_conversations": total_conversations,
        "silhouette_score": None,
        "run_status": "failed",
    }


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


def _stratified_sample_indices(
    labels: np.ndarray, max_samples: int, random_state: int
) -> np.ndarray:
    """Return indices of a <=max_samples subsample with every cluster represented.

    Proportional per cluster, at least 1 index per cluster -- sklearn's own
    silhouette_score(sample_size=...) samples uniformly at random instead, which
    can (and at min_cluster_size scale, often does) exclude a whole small
    cluster rather than just shrinking its contribution.
    """
    rng = np.random.default_rng(random_state)
    total = len(labels)
    selected = []
    for cluster_id in np.unique(labels):
        cluster_idx = np.flatnonzero(labels == cluster_id)
        quota = max(1, round(max_samples * len(cluster_idx) / total))
        take = min(quota, len(cluster_idx))
        selected.append(rng.choice(cluster_idx, size=take, replace=False))
    return np.concatenate(selected)


def compute_silhouette(
    vectors: np.ndarray,
    labels: np.ndarray,
    max_samples: int = SILHOUETTE_MAX_SAMPLES,
    random_state: int = RANDOM_STATE,
) -> float | None:
    """Silhouette score over the non-noise points only.

    Requires at least 2 genuine clusters with 2+ members each to be defined;
    returns None (not 0.0, which would misleadingly read as "bad but valid")
    when the run doesn't clear that bar -- e.g. everything landed in one
    cluster, or everything is noise. Bounded to max_samples once the non-noise
    population exceeds it (the score is O(n^2)), via a stratified subsample so
    a cluster near min_cluster_size still contributes rather than being
    silently dropped from the run-comparison metric.
    """
    non_noise = labels != NOISE_CLUSTER_ID
    if non_noise.sum() < 2:  # noqa: PLR2004
        return None
    non_noise_labels = labels[non_noise]
    distinct_clusters = np.unique(non_noise_labels)
    if len(distinct_clusters) < 2:  # noqa: PLR2004
        return None
    non_noise_vectors = vectors[non_noise]
    if non_noise.sum() > max_samples:
        sample_idx = _stratified_sample_indices(
            non_noise_labels, max_samples, random_state
        )
        non_noise_vectors = non_noise_vectors[sample_idx]
        non_noise_labels = non_noise_labels[sample_idx]
    try:
        return float(
            silhouette_score(
                non_noise_vectors,
                non_noise_labels,
                random_state=random_state,
            )
        )
    except ValueError:
        return None


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


def cluster_embeddings(  # noqa: PLR0913 -- provenance/params/retry-id are each independently meaningful args
    embeddings_df: pl.DataFrame,
    embedding_provenance: tuple[str, int, str | None],
    umap_params: tuple[int, int] = (UMAP_N_COMPONENTS, UMAP_N_NEIGHBORS),
    min_cluster_size: int = HDBSCAN_MIN_CLUSTER_SIZE,
    random_state: int = RANDOM_STATE,
    cluster_run_id: str | None = None,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Cluster every row's embedding_vector; produce this run's candidates + summary.

    Args:
        embeddings_df: a frame with (at least) feedback_conversation_pk,
            source_slug, conversation_ref, embedding_vector columns, e.g.
            feedback_embeddings filtered to one consistent
            embedding_model_version/embedding_dim.
        embedding_provenance: (embedding_model_version, embedding_dim,
            embedding_input_filter), recorded on the run rather than just used to
            build embeddings_df -- feedback_embeddings is upserted in place, so
            this provenance can't be reconstructed from it later. Needed to
            compare a summary-vs-raw or model bake-off run against another after
            the source table has moved on.
        umap_params: (umap_n_components, umap_n_neighbors).
        cluster_run_id: caller-supplied, retry-stable ID (e.g. Dagster's run ID);
            defaults to a fresh UUID for callers that don't need retry-stability.

    Returns:
        (candidates_df, run_metadata): candidates_df has cluster_run_id plus
            JOIN_COLS, cluster_id, cluster_probability -- one row per input
            conversation. run_metadata matches CLUSTER_RUN_SCHEMA minus run_at,
            which the caller stamps (the caller owns the wall-clock time, not
            this pure function).
    """
    embedding_model_version, embedding_dim, embedding_input_filter = (
        embedding_provenance
    )
    umap_n_components, umap_n_neighbors = umap_params
    cluster_run_id = cluster_run_id or new_cluster_run_id()
    # list.to_array + to_numpy gives a contiguous float32 (n, dim) array directly;
    # to_list() would materialize every element as a Python float first, then
    # allocate a second array from that -- several GB of avoidable transient
    # memory at the ~198K-conversation, 1024-dim scale this is meant to run at.
    vectors = embeddings_df["embedding_vector"].list.to_array(embedding_dim).to_numpy()

    labels, probabilities, reduced = reduce_and_cluster(
        vectors,
        umap_n_components=umap_n_components,
        umap_n_neighbors=umap_n_neighbors,
        min_cluster_size=min_cluster_size,
        random_state=random_state,
    )
    # `reduced`, not `vectors`: score in the same space HDBSCAN clustered on.
    silhouette = compute_silhouette(reduced, labels, random_state=random_state)

    candidates_df = embeddings_df.select(JOIN_COLS + DEBUG_COLS).with_columns(
        pl.lit(cluster_run_id).alias("cluster_run_id"),
        pl.Series("cluster_id", labels, dtype=pl.Int64),
        pl.Series("cluster_probability", probabilities, dtype=pl.Float64),
    )

    noise_count = int((labels == NOISE_CLUSTER_ID).sum())
    cluster_count = len(np.unique(labels[labels != NOISE_CLUSTER_ID]))
    run_metadata = {
        "cluster_run_id": cluster_run_id,
        "algorithm": "umap+hdbscan",
        "embedding_model_version": embedding_model_version,
        "embedding_dim": embedding_dim,
        "embedding_input_filter": embedding_input_filter,
        "umap_n_components": umap_n_components,
        "umap_n_neighbors": umap_n_neighbors,
        "hdbscan_min_cluster_size": min_cluster_size,
        "random_state": random_state,
        "cluster_count": cluster_count,
        "noise_count": noise_count,
        "total_conversations": embeddings_df.height,
        "silhouette_score": silhouette,
        "run_status": "completed",
    }
    return candidates_df, run_metadata
