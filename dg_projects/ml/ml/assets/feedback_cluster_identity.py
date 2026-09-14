import os
from typing import Any

import numpy as np
import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    Config,
    MetadataValue,
    Output,
    multi_asset,
)
from ml.lib.cluster import NOISE_CLUSTER_ID
from ml.lib.cluster_identity import (
    CLUSTER_LINEAGE_SCHEMA,
    CLUSTER_SCHEMA,
    JACCARD_MATCH_THRESHOLD,
    cluster_lineage_pk,
    compute_cluster_stats,
    match_clusters,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from pydantic import Field
from pyiceberg.exceptions import NoSuchTableError

if DAGSTER_ENV == "dev":
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    database_name = f"ol_warehouse_production_{_schema_suffix}_intermediate"
else:
    database_name = "ol_warehouse_production_intermediate"


class FeedbackClusterIdentityConfig(Config):
    cluster_run_id: str | None = Field(
        default=None,
        description="Process this specific completed run instead of "
        "auto-selecting the most recent completed run with no lineage yet.",
    )
    match_threshold: float = Field(
        default=JACCARD_MATCH_THRESHOLD,
        description="Minimum Jaccard overlap for a new cluster to 'continue' an "
        "existing cluster_key.",
    )


def _table_exists(catalog, table_identifier: str) -> bool:
    try:
        catalog.load_table(table_identifier)
    except NoSuchTableError:
        return False
    return True


def _select_run_to_process(
    catalog, config: FeedbackClusterIdentityConfig
) -> str | None:
    """Return the completed cluster_run_id to match, or None if there's nothing new."""
    if config.cluster_run_id is not None:
        return config.cluster_run_id
    if not _table_exists(catalog, f"{database_name}.feedback_cluster_run"):
        return None
    runs_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_run"
        )
        .filter(pl.col("run_status") == "completed")
        .select(["cluster_run_id", "run_at"])
        .collect()
    )
    if runs_df.height == 0:
        return None
    already_processed: set[str] = set()
    if _table_exists(catalog, f"{database_name}.feedback_cluster_lineage"):
        already_processed = set(
            get_dbt_model_as_dataframe(
                database_name=database_name, table_name="feedback_cluster_lineage"
            )
            .select("cluster_run_id")
            .unique()
            .collect()["cluster_run_id"]
        )
    unprocessed = runs_df.filter(
        ~pl.col("cluster_run_id").is_in(already_processed)
    ).sort("run_at", descending=True)
    if unprocessed.height == 0:
        return None
    return unprocessed["cluster_run_id"][0]


def _active_cluster_members(catalog) -> dict[str, frozenset[str]]:
    """cluster_key -> its live member pks, for every currently-active key.

    Empty if feedback_cluster_membership has no rows, in which case every new
    cluster resolves to 'new'.
    """
    if not _table_exists(
        catalog, f"{database_name}.feedback_cluster_membership"
    ) or not _table_exists(catalog, f"{database_name}.feedback_cluster"):
        return {}
    active_keys = set(
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster"
        )
        .filter(pl.col("cluster_status") == "active")
        .collect()["cluster_key"]
    )
    membership_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_membership"
        )
        .filter(
            pl.col("cluster_key").is_not_null()
            & pl.col("cluster_key").is_in(active_keys)
        )
        .select(["feedback_conversation_pk", "cluster_key"])
        .collect()
    )
    return {
        cluster_key: frozenset(group["feedback_conversation_pk"])
        for cluster_key, group in membership_df.group_by("cluster_key")
    }


def _existing_cluster_rows(catalog) -> dict[str, dict[str, Any]]:
    """cluster_key -> its current feedback_cluster row, for carrying
    first_seen_run_id forward on a continued/merged key.
    """
    if not _table_exists(catalog, f"{database_name}.feedback_cluster"):
        return {}
    return {
        row["cluster_key"]: row
        for row in get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster"
        )
        .collect()
        .to_dicts()
    }


@multi_asset(
    group_name="feedback",
    deps=[
        AssetKey(["intermediate", "feedback_cluster_run"]),
        AssetKey(["intermediate", "feedback_cluster_candidate"]),
    ],
    outs={
        "feedback_cluster": AssetOut(
            key=AssetKey(["intermediate", "feedback_cluster"]),
            io_manager_key="io_manager",
            metadata={
                "schema": database_name,
                "write_mode": "upsert",
                "upsert_options": {"join_cols": ["cluster_key"]},
                "schema_update_mode": "update",
            },
            code_version="feedback_cluster_identity_v1",
            automation_condition=upstream_or_code_changes(),
        ),
        "feedback_cluster_lineage": AssetOut(
            key=AssetKey(["intermediate", "feedback_cluster_lineage"]),
            io_manager_key="io_manager",
            metadata={
                "schema": database_name,
                "write_mode": "append",
                "schema_update_mode": "update",
            },
            code_version="feedback_cluster_identity_v1",
            automation_condition=upstream_or_code_changes(),
            is_required=False,
        ),
    },
    pool="feedback_cluster_identity",
)
def feedback_cluster_identity(
    context: AssetExecutionContext, config: FeedbackClusterIdentityConfig
):
    """
    Match one completed feedback_clusters run onto stable cluster_keys.

    A run's cluster_id is arbitrary and run-local -- it carries no relationship to
    any previous run's numbering. This asset compares the new run's clusters
    against every currently-active cluster_key's live membership (from
    feedback_cluster_membership) by Jaccard overlap, and resolves each new cluster
    to continued/merged/split/new (ml.lib.cluster_identity.match_clusters), writing
    one feedback_cluster row per resolved key and one feedback_cluster_lineage row
    per edge (plus one 'retired' row per active key that didn't survive). No human
    approves this.
    """
    catalog = get_glue_catalog()
    cluster_run_id = _select_run_to_process(catalog, config)
    if cluster_run_id is None:
        context.log.info("No unprocessed completed cluster run found; nothing to do.")
        yield Output(
            pl.DataFrame(schema=CLUSTER_SCHEMA), output_name="feedback_cluster"
        )
        return

    run_row = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_run"
        )
        .filter(pl.col("cluster_run_id") == cluster_run_id)
        .collect()
        .to_dicts()[0]
    )
    embedding_model_version = run_row["embedding_model_version"]
    embedding_dim = run_row["embedding_dim"]

    candidates_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_candidate"
        )
        .filter(
            (pl.col("cluster_run_id") == cluster_run_id)
            & (pl.col("cluster_id") != NOISE_CLUSTER_ID)
        )
        .select(["feedback_conversation_pk", "cluster_id"])
        .collect()
    )
    new_cluster_members = {
        cluster_id: frozenset(group["feedback_conversation_pk"])
        for (cluster_id,), group in candidates_df.group_by("cluster_id")
    }

    active_cluster_members = _active_cluster_members(catalog)
    existing_cluster_rows = _existing_cluster_rows(catalog)

    matches, lineage_rows = match_clusters(
        new_cluster_members, active_cluster_members, config.match_threshold
    )

    embeddings_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_embeddings"
        )
        .filter(
            (pl.col("embedding_model_version") == embedding_model_version)
            & (pl.col("embedding_dim") == embedding_dim)
            & pl.col("feedback_conversation_pk").is_in(
                candidates_df["feedback_conversation_pk"]
            )
        )
        .select(["feedback_conversation_pk", "embedding_vector"])
        .collect()
    )
    vectors_by_pk = dict(
        zip(
            embeddings_df["feedback_conversation_pk"],
            embeddings_df["embedding_vector"].list.to_array(embedding_dim).to_numpy(),
            strict=True,
        )
    )

    cluster_rows = []
    for match in matches:
        members = new_cluster_members[match.new_cluster_id]
        vectors = np.stack([vectors_by_pk[pk] for pk in members])
        centroid, radius = compute_cluster_stats(vectors)
        existing = existing_cluster_rows.get(match.cluster_key)
        cluster_rows.append(
            {
                "cluster_key": match.cluster_key,
                "centroid": centroid.tolist(),
                "radius": radius,
                "embedding_model_version": embedding_model_version,
                "embedding_dim": embedding_dim,
                "member_count": len(members),
                "cluster_status": "active",
                "first_seen_run_id": existing["first_seen_run_id"]
                if existing
                else cluster_run_id,
                "last_seen_run_id": cluster_run_id,
            }
        )

    retired_keys = {
        row["prior_cluster_key"] for row in lineage_rows if row["relation"] == "retired"
    }
    for cluster_key in retired_keys:
        existing = existing_cluster_rows[cluster_key]
        cluster_rows.append(
            {
                **existing,
                "cluster_status": "retired",
                "last_seen_run_id": cluster_run_id,
            }
        )

    lineage_df = pl.DataFrame(
        [
            {
                "cluster_lineage_pk": cluster_lineage_pk(
                    cluster_run_id, row["prior_cluster_key"], row["cluster_key"]
                ),
                "cluster_run_id": cluster_run_id,
                **row,
            }
            for row in lineage_rows
        ],
        schema=CLUSTER_LINEAGE_SCHEMA,
    )

    context.log.info(
        "Cluster identity for run %s: %d resolved (%s), %d retired",
        cluster_run_id,
        len(matches),
        {
            relation: sum(1 for m in matches if m.relation == relation)
            for relation in {m.relation for m in matches}
        },
        len(retired_keys),
    )
    context.add_output_metadata(
        {
            "cluster_run_id": MetadataValue.text(cluster_run_id),
            "clusters_resolved": MetadataValue.int(len(matches)),
            "clusters_retired": MetadataValue.int(len(retired_keys)),
        },
        output_name="feedback_cluster",
    )
    yield Output(
        pl.DataFrame(cluster_rows, schema=CLUSTER_SCHEMA)
        if cluster_rows
        else pl.DataFrame(schema=CLUSTER_SCHEMA),
        output_name="feedback_cluster",
    )
    yield Output(
        lineage_df,
        output_name="feedback_cluster_lineage",
        metadata={"row_count": MetadataValue.int(lineage_df.height)},
    )
