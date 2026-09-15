import polars as pl
from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from ml.assets.feedback_clusters import database_name as cluster_database_name
from ml.lib.cluster import should_trigger_early_recluster
from ml.lib.embed import EMBEDDING_DIM, EMBEDDING_MODEL_VERSION
from ml.lib.iceberg_helpers import table_exists
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog


@sensor(
    name="feedback_clusters_growth_sensor",
    job_name="feedback_clusters_job",
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Triggers feedback_clusters early when the embedded corpus has grown "
        "enough since the last completed run to be worth reclustering before "
        "feedback_clusters_weekly_schedule. Only checks corpus growth -- an "
        "unplaced-share trigger would also need feedback_cluster_membership."
    ),
)
def feedback_clusters_growth_sensor(_context: SensorEvaluationContext):
    catalog = get_glue_catalog()
    if not table_exists(catalog, f"{cluster_database_name}.feedback_embeddings"):
        return SkipReason("feedback_embeddings hasn't materialized yet")
    # Scoped to match feedback_clusters' own defaults (embedding_input="summary",
    # EMBEDDING_MODEL_VERSION/EMBEDDING_DIM) -- counting every arm/model/dim would
    # inflate this past what a completed run's total_conversations actually
    # measures, and could trigger on growth in rows the next run won't even see.
    embedding_count = (
        get_dbt_model_as_dataframe(
            database_name=cluster_database_name, table_name="feedback_embeddings"
        )
        .filter(
            (pl.col("embedding_input") == "summary")
            & (pl.col("embedding_model_version") == EMBEDDING_MODEL_VERSION)
            & (pl.col("embedding_dim") == EMBEDDING_DIM)
        )
        .select(pl.len())
        .collect()
        .item()
    )

    last_completed_total_conversations = None
    if table_exists(catalog, f"{cluster_database_name}.feedback_cluster_run"):
        last_run_df = (
            get_dbt_model_as_dataframe(
                database_name=cluster_database_name, table_name="feedback_cluster_run"
            )
            .filter(pl.col("run_status") == "completed")
            .sort("run_at", descending=True)
            .select("total_conversations")
            .limit(1)
            .collect()
        )
        if last_run_df.height:
            last_completed_total_conversations = last_run_df.item()

    if should_trigger_early_recluster(
        embedding_count, last_completed_total_conversations
    ):
        return RunRequest()
    return SkipReason(
        "Corpus growth since the last completed run is below the trigger threshold"
    )
