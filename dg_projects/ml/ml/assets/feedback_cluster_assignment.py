import os
from datetime import UTC, datetime
from typing import Any

import numpy as np
import polars as pl
from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset
from ml.lib.cluster import NOISE_CLUSTER_ID
from ml.lib.cluster_identity import MEMBERSHIP_SCHEMA, nearest_active_cluster
from ml.lib.cluster_run_lookup import latest_identity_processed_run
from ml.lib.iceberg_helpers import table_exists
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog

if DAGSTER_ENV == "dev":
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    database_name = f"ol_warehouse_production_{_schema_suffix}_intermediate"
else:
    database_name = "ol_warehouse_production_intermediate"


def _run_already_applied(catalog, cluster_run_id: str) -> bool:
    """Whether cluster_run_id's full rewrite has already been written to
    feedback_cluster_membership by a prior execution of this asset.
    """
    if not table_exists(catalog, f"{database_name}.feedback_cluster_membership"):
        return False
    applied_count = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_membership"
        )
        .filter(
            (pl.col("cluster_run_id") == cluster_run_id)
            & (pl.col("cluster_assignment_method") == "recluster")
        )
        .select(pl.len())
        .collect()
        .item()
    )
    return applied_count > 0


def _vector_lookup(embedding_dim: int, pks: pl.Series) -> dict[str, np.ndarray]:
    embeddings_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_embeddings"
        )
        .filter(pl.col("feedback_conversation_pk").is_in(pks))
        .select(["feedback_conversation_pk", "embedding_vector"])
        .collect()
    )
    return dict(
        zip(
            embeddings_df["feedback_conversation_pk"],
            embeddings_df["embedding_vector"].list.to_array(embedding_dim).to_numpy(),
            strict=True,
        )
    )


def _run_embedding_config(cluster_run_id: str) -> tuple[str, int, str | None]:
    """Return the (embedding_model_version, embedding_dim, embedding_input_filter)
    feedback_cluster_run recorded for this specific completed run -- the
    authoritative source, unlike inferring it from whichever feedback_cluster
    row happens to come first (which can span more than one config after a
    model/dim/arm change, until feedback_cluster_identity retires the old
    config's actives).
    """
    run_row = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_run"
        )
        .filter(pl.col("cluster_run_id") == cluster_run_id)
        .select(["embedding_model_version", "embedding_dim", "embedding_input_filter"])
        .collect()
        .to_dicts()[0]
    )
    return (
        run_row["embedding_model_version"],
        run_row["embedding_dim"],
        run_row["embedding_input_filter"],
    )


def _current_active_embedding_config(catalog) -> tuple[str, int, str | None] | None:
    """Return the (embedding_model_version, embedding_dim, embedding_input_filter)
    of every currently-active cluster, or None if there are no active clusters
    or (transiently, before feedback_cluster_identity retires the old config)
    more than one config is active at once -- either way, incremental
    placement has no single config to compare against yet.
    """
    if not table_exists(catalog, f"{database_name}.feedback_cluster"):
        return None
    configs_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster"
        )
        .filter(pl.col("cluster_status") == "active")
        .select(["embedding_model_version", "embedding_dim", "embedding_input_filter"])
        .unique()
        .collect()
    )
    if configs_df.height != 1:
        return None
    row = configs_df.to_dicts()[0]
    return (
        row["embedding_model_version"],
        row["embedding_dim"],
        row["embedding_input_filter"],
    )


def _active_clusters(
    catalog,
    embedding_model_version: str,
    embedding_dim: int,
    embedding_input_filter: str | None,
) -> list[dict[str, Any]]:
    """Active feedback_cluster rows scoped to one embedding config, shaped for
    ml.lib.cluster_identity.nearest_active_cluster.
    """
    if not table_exists(catalog, f"{database_name}.feedback_cluster"):
        return []
    clusters_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster"
        )
        .filter(
            (pl.col("cluster_status") == "active")
            & (pl.col("embedding_model_version") == embedding_model_version)
            & (pl.col("embedding_dim") == embedding_dim)
            & (
                pl.col("embedding_input_filter").eq_missing(
                    pl.lit(embedding_input_filter)
                )
            )
        )
        .select(["cluster_key", "centroid", "radius"])
        .collect()
    )
    return [
        {
            "cluster_key": row["cluster_key"],
            "centroid": np.array(row["centroid"]),
            "radius": row["radius"],
        }
        for row in clusters_df.to_dicts()
    ]


def _rewrite_from_run(
    cluster_run_id: str,
    now: datetime,
    catalog,
    embedding_config: tuple[str, int, str | None],
) -> pl.DataFrame:
    """Full membership rewrite for every conversation in cluster_run_id's candidates.

    cluster_id -> cluster_key comes from feedback_cluster_lineage (this run's
    resolved matches); noise (-1) and any cluster_id absent from lineage (i.e.
    retired-only, no successor) resolve to a null cluster_key.
    """
    _embedding_model_version, embedding_dim, _embedding_input_filter = embedding_config
    candidates_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_candidate"
        )
        .filter(pl.col("cluster_run_id") == cluster_run_id)
        .select(["feedback_conversation_pk", "cluster_id"])
        .collect()
    )
    lineage_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_lineage"
        )
        .filter(
            (pl.col("cluster_run_id") == cluster_run_id)
            & pl.col("cluster_id").is_not_null()
        )
        .select(["cluster_id", "cluster_key"])
        .unique()
        .collect()
    )
    cluster_id_to_key = dict(
        zip(lineage_df["cluster_id"], lineage_df["cluster_key"], strict=True)
    )

    centroid_by_key = {
        cluster["cluster_key"]: cluster["centroid"]
        for cluster in _active_clusters(catalog, *embedding_config)
    }
    vector_by_pk = _vector_lookup(
        embedding_dim, candidates_df["feedback_conversation_pk"]
    )

    rows = []
    for pk, cluster_id in zip(
        candidates_df["feedback_conversation_pk"],
        candidates_df["cluster_id"],
        strict=True,
    ):
        cluster_key = (
            cluster_id_to_key.get(cluster_id)
            if cluster_id != NOISE_CLUSTER_ID
            else None
        )
        similarity = None
        if cluster_key is not None and pk in vector_by_pk:
            vector = vector_by_pk[pk]
            norm = np.linalg.norm(vector)
            if norm > 0:
                similarity = float(
                    (vector / norm) @ np.array(centroid_by_key[cluster_key])
                )
        rows.append(
            {
                "feedback_conversation_pk": pk,
                "cluster_key": cluster_key,
                "cluster_similarity": similarity,
                "cluster_assignment_method": "recluster",
                "cluster_run_id": cluster_run_id,
                "assigned_at": now,
            }
        )
    return pl.DataFrame(rows, schema=MEMBERSHIP_SCHEMA)


def _incrementally_place_new_embeddings(
    catalog,
    exclude_pks: set[str],
    cluster_run_id: str | None,
    now: datetime,
    embedding_config: tuple[str, int, str | None],
) -> pl.DataFrame:
    """Place every conversation that needs (re-)placement against the current
    active clusters, scoped to one embedding_config (embedding_model_version,
    embedding_dim, embedding_input_filter).

    "Needs (re-)placement" is: no membership row yet, an embedding newer than
    its current membership row, or a membership row still pointing at a
    cluster_key that isn't active anymore (retired, merged away, or from a
    prior embedding config) -- a conversation the last rewrite/placement never
    revisited otherwise keeps a stale key forever. exclude_pks were just
    handled by _rewrite_from_run in this same execution.

    A conversation whose embedding_input arm doesn't match the active
    clusters' own arm (e.g. a concatenated_turns embedding when the clusters
    were built from summary embeddings) is out of scope for clustering under
    the current configuration and is never compared against them -- it's
    unassigned, not mismatched.
    """
    embedding_model_version, embedding_dim, embedding_input_filter = embedding_config
    active_clusters = _active_clusters(
        catalog, embedding_model_version, embedding_dim, embedding_input_filter
    )
    if not active_clusters:
        return pl.DataFrame(schema=MEMBERSHIP_SCHEMA)
    active_keys = [cluster["cluster_key"] for cluster in active_clusters]

    embeddings_lf = get_dbt_model_as_dataframe(
        database_name=database_name, table_name="feedback_embeddings"
    ).filter(
        (pl.col("embedding_model_version") == embedding_model_version)
        & (pl.col("embedding_dim") == embedding_dim)
        & ~pl.col("feedback_conversation_pk").is_in(list(exclude_pks))
    )
    if embedding_input_filter is not None:
        embeddings_lf = embeddings_lf.filter(
            pl.col("embedding_input") == embedding_input_filter
        )

    membership_lf = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_membership"
        ).select(["feedback_conversation_pk", "cluster_key", "assigned_at"])
        if table_exists(catalog, f"{database_name}.feedback_cluster_membership")
        else pl.LazyFrame(
            schema={
                "feedback_conversation_pk": pl.String,
                "cluster_key": pl.String,
                "assigned_at": pl.Datetime(time_zone="UTC"),
            }
        )
    )

    # Push the join and the "needs (re-)placement" predicate into the lazy plan
    # so only conversations that are new, newly re-embedded, or stale-keyed are
    # ever collected -- an incremental run shouldn't materialize the whole
    # corpus just to find the handful of rows it actually needs to place.
    to_place_df = (
        embeddings_lf.join(membership_lf, on="feedback_conversation_pk", how="left")
        .filter(
            pl.col("assigned_at").is_null()
            | (pl.col("assigned_at") < pl.col("embedded_at"))
            | (
                pl.col("cluster_key").is_not_null()
                & ~pl.col("cluster_key").is_in(active_keys)
            )
        )
        .select(["feedback_conversation_pk", "embedding_vector", "embedded_at"])
        .collect()
    )
    if to_place_df.height == 0:
        return pl.DataFrame(schema=MEMBERSHIP_SCHEMA)

    rows = []
    for pk, vector_list in zip(
        to_place_df["feedback_conversation_pk"],
        to_place_df["embedding_vector"],
        strict=True,
    ):
        cluster_key, similarity = nearest_active_cluster(
            np.array(vector_list), active_clusters
        )
        rows.append(
            {
                "feedback_conversation_pk": pk,
                "cluster_key": cluster_key,
                "cluster_similarity": similarity,
                "cluster_assignment_method": "incremental",
                "cluster_run_id": cluster_run_id,
                "assigned_at": now,
            }
        )
    return pl.DataFrame(rows, schema=MEMBERSHIP_SCHEMA)


@asset(
    code_version="feedback_cluster_assignment_v1",
    group_name="feedback",
    key=AssetKey(["intermediate", "feedback_cluster_membership"]),
    deps=[
        AssetKey(["intermediate", "feedback_embeddings"]),
        AssetKey(["intermediate", "feedback_cluster"]),
        AssetKey(["intermediate", "feedback_cluster_lineage"]),
    ],
    automation_condition=upstream_or_code_changes(),
    io_manager_key="io_manager",
    pool="feedback_cluster_assignment",
    metadata={
        "schema": database_name,
        "write_mode": "upsert",
        "upsert_options": {"join_cols": ["feedback_conversation_pk"]},
        "schema_update_mode": "update",
    },
)
def feedback_cluster_assignment(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Maintain feedback_cluster_membership: the live cluster assignment the fact
    table joins.

    Two paths: after a new clustering run (one feedback_cluster_identity has
    matched but this asset hasn't applied yet), every conversation in that run is
    rewritten from its resolved cluster_key (cluster_assignment_method='recluster').
    Otherwise, every conversation that's new, newly re-embedded, or still
    pointing at a cluster_key that's no longer active is placed by nearest
    active centroid within its radius (cluster_assignment_method='incremental').
    Both paths are scoped to one embedding config (embedding_model_version,
    embedding_dim, embedding_input_filter) -- the just-processed run's config,
    or the config of whatever's currently active if nothing new was processed
    this execution. A conversation embedded under a different arm than the
    active clusters (e.g. concatenated_turns when the clusters are
    summary-based) is out of scope for placement, not mismatched against it.
    No human approves either path.
    """
    catalog = get_glue_catalog()
    now = datetime.now(tz=UTC)

    cluster_run_id = latest_identity_processed_run(catalog, database_name)
    embedding_config = (
        _run_embedding_config(cluster_run_id)
        if cluster_run_id is not None
        else _current_active_embedding_config(catalog)
    )

    rewrite_df = pl.DataFrame(schema=MEMBERSHIP_SCHEMA)
    if (
        cluster_run_id is not None
        and embedding_config is not None
        and not _run_already_applied(catalog, cluster_run_id)
    ):
        rewrite_df = _rewrite_from_run(cluster_run_id, now, catalog, embedding_config)

    incremental_df = (
        _incrementally_place_new_embeddings(
            catalog,
            set(rewrite_df["feedback_conversation_pk"]),
            cluster_run_id,
            now,
            embedding_config,
        )
        if embedding_config is not None
        else pl.DataFrame(schema=MEMBERSHIP_SCHEMA)
    )

    result_df = pl.concat([rewrite_df, incremental_df])
    context.log.info(
        "feedback_cluster_membership: %d rewritten from run %s, %d placed",
        rewrite_df.height,
        cluster_run_id,
        incremental_df.height,
    )
    context.add_output_metadata(
        {
            "rewritten_from_run": MetadataValue.int(rewrite_df.height),
            "placed_incrementally": MetadataValue.int(incremental_df.height),
            "cluster_run_id": MetadataValue.text(cluster_run_id or ""),
        }
    )
    return result_df
