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
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from pyiceberg.exceptions import NoSuchTableError


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
    try:
        catalog.load_table(f"{cluster_database_name}.feedback_embeddings")
    except NoSuchTableError:
        return SkipReason("feedback_embeddings hasn't materialized yet")
    embedding_count = (
        get_dbt_model_as_dataframe(
            database_name=cluster_database_name, table_name="feedback_embeddings"
        )
        .select(pl.len())
        .collect()
        .item()
    )

    last_completed_total_conversations = None
    try:
        catalog.load_table(f"{cluster_database_name}.feedback_cluster_run")
    except NoSuchTableError:
        pass
    else:
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
