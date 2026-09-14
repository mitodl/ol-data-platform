import os
from datetime import UTC, datetime

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


def _rewrite_from_run(cluster_run_id: str, now: datetime) -> pl.DataFrame:
    """Full membership rewrite for every conversation in cluster_run_id's candidates.

    cluster_id -> cluster_key comes from feedback_cluster_lineage (this run's
    resolved matches); noise (-1) and any cluster_id absent from lineage (i.e.
    retired-only, no successor) resolve to a null cluster_key.
    """
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

    clusters_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster"
        )
        .select(["cluster_key", "centroid", "embedding_dim"])
        .collect()
    )
    centroid_by_key = dict(
        zip(clusters_df["cluster_key"], clusters_df["centroid"], strict=True)
    )
    embedding_dim = clusters_df["embedding_dim"][0] if clusters_df.height else None
    vector_by_pk = (
        _vector_lookup(embedding_dim, candidates_df["feedback_conversation_pk"])
        if embedding_dim
        else {}
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
    catalog, exclude_pks: set[str], cluster_run_id: str | None, now: datetime
) -> pl.DataFrame:
    """Place every conversation whose embedding is newer than its current
    membership row (or that has none), against the current active clusters.

    exclude_pks were just handled by _rewrite_from_run in this same execution.
    """
    clusters_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster"
        )
        .filter(pl.col("cluster_status") == "active")
        .select(
            [
                "cluster_key",
                "centroid",
                "radius",
                "embedding_model_version",
                "embedding_dim",
            ]
        )
        .collect()
        if table_exists(catalog, f"{database_name}.feedback_cluster")
        else pl.DataFrame(
            schema={
                "cluster_key": pl.String,
                "centroid": pl.List(pl.Float32),
                "radius": pl.Float64,
                "embedding_model_version": pl.String,
                "embedding_dim": pl.Int64,
            }
        )
    )
    if clusters_df.height == 0:
        return pl.DataFrame(schema=MEMBERSHIP_SCHEMA)

    embedding_model_version = clusters_df["embedding_model_version"][0]
    embedding_dim = clusters_df["embedding_dim"][0]
    active_clusters = [
        {
            "cluster_key": row["cluster_key"],
            "centroid": np.array(row["centroid"]),
            "radius": row["radius"],
        }
        for row in clusters_df.to_dicts()
    ]

    embeddings_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_embeddings"
        )
        .filter(
            (pl.col("embedding_model_version") == embedding_model_version)
            & (pl.col("embedding_dim") == embedding_dim)
            & ~pl.col("feedback_conversation_pk").is_in(list(exclude_pks))
        )
        .select(["feedback_conversation_pk", "embedding_vector", "embedded_at"])
        .collect()
    )
    if embeddings_df.height == 0:
        return pl.DataFrame(schema=MEMBERSHIP_SCHEMA)

    membership_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_membership"
        )
        .select(["feedback_conversation_pk", "assigned_at"])
        .collect()
        if table_exists(catalog, f"{database_name}.feedback_cluster_membership")
        else pl.DataFrame(
            schema={
                "feedback_conversation_pk": pl.String,
                "assigned_at": pl.Datetime(time_zone="UTC"),
            }
        )
    )
    assigned_at_by_pk = dict(
        zip(
            membership_df["feedback_conversation_pk"],
            membership_df["assigned_at"],
            strict=True,
        )
    )

    rows = []
    for pk, vector_list, embedded_at in zip(
        embeddings_df["feedback_conversation_pk"],
        embeddings_df["embedding_vector"],
        embeddings_df["embedded_at"],
        strict=True,
    ):
        prior_assigned_at = assigned_at_by_pk.get(pk)
        if prior_assigned_at is not None and prior_assigned_at >= embedded_at:
            continue
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
    Otherwise, every embedded conversation whose embedding is newer than its
    current membership row (or that has none) is placed by nearest active
    centroid within its radius (cluster_assignment_method='incremental'). No
    human approves either path.
    """
    catalog = get_glue_catalog()
    now = datetime.now(tz=UTC)

    cluster_run_id = latest_identity_processed_run(catalog, database_name)
    rewrite_df = pl.DataFrame(schema=MEMBERSHIP_SCHEMA)
    if cluster_run_id is not None and not _run_already_applied(catalog, cluster_run_id):
        rewrite_df = _rewrite_from_run(cluster_run_id, now)

    incremental_df = _incrementally_place_new_embeddings(
        catalog, set(rewrite_df["feedback_conversation_pk"]), cluster_run_id, now
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
