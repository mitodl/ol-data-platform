"""Shared lookup for the run feedback_cluster_identity most recently matched --
needed by both feedback_cluster_assignment (to place conversations) and
feedback_category_proposals (to find new/split/merged keys to label).
"""

import polars as pl
from ml.lib.iceberg_helpers import table_exists
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe


def latest_identity_processed_run(catalog, database_name: str) -> str | None:
    """Return the most recent completed cluster_run_id that feedback_cluster_identity
    has matched (has lineage rows), or None if none has been processed yet.
    """
    if not table_exists(
        catalog, f"{database_name}.feedback_cluster_lineage"
    ) or not table_exists(catalog, f"{database_name}.feedback_cluster_run"):
        return None
    processed_run_ids = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_lineage"
        )
        .select("cluster_run_id")
        .unique()
        .collect()["cluster_run_id"]
        .to_list()
    )
    if len(processed_run_ids) == 0:
        return None
    runs_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_run"
        )
        .filter(
            (pl.col("run_status") == "completed")
            & pl.col("cluster_run_id").is_in(processed_run_ids)
        )
        .sort("run_at", descending=True)
        .select("cluster_run_id")
        .limit(1)
        .collect()
    )
    return runs_df["cluster_run_id"][0] if runs_df.height else None
