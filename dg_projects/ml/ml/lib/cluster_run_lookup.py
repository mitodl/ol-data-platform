"""Shared lookup for the run feedback_cluster_identity most recently processed --
needed by both feedback_cluster_assignment (to place conversations) and
feedback_category_proposals (to find new/split/merged keys to label).
"""

import polars as pl
from ml.lib.iceberg_helpers import table_exists
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe


def latest_identity_processed_run(catalog, database_name: str) -> str | None:
    """Return the completed cluster_run_id that feedback_cluster_identity processed
    last -- the run whose clusters are live -- or None if none has been processed.

    Ordered by processed_at, not run_at: a run held for review can be promoted after
    a newer run went live, and it then owns the live clusters.
    """
    if not table_exists(
        catalog, f"{database_name}.feedback_cluster_identity_run"
    ) or not table_exists(catalog, f"{database_name}.feedback_cluster_run"):
        return None
    completed_runs_lf = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_run"
        )
        .filter(pl.col("run_status") == "completed")
        .select("cluster_run_id")
    )
    live_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_identity_run"
        )
        .join(completed_runs_lf, on="cluster_run_id", how="semi")
        .sort("processed_at", descending=True)
        .select("cluster_run_id")
        .limit(1)
        .collect()
    )
    return live_df["cluster_run_id"][0] if live_df.height else None


def run_already_applied(catalog, database_name: str, cluster_run_id: str) -> bool:
    """Whether feedback_cluster_assignment has written cluster_run_id's full rewrite
    to feedback_cluster_membership.
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
