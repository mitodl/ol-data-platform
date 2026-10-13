import polars as pl
from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from ml.assets.feedback_clusters import database_name as cluster_database_name
from ml.lib.cluster import (
    DEFAULT_FEEDBACK_SINCE,
    DEFAULT_PLATFORMS,
    filter_conversation_scope,
    should_trigger_early_recluster,
)
from ml.lib.cluster_run_lookup import latest_identity_processed_run
from ml.lib.embed import EMBEDDING_DIM, default_embedding_model_version
from ml.lib.iceberg_helpers import table_exists
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog


@sensor(
    name="feedback_clusters_growth_sensor",
    job_name="feedback_clusters_job",
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Triggers an extend run of feedback_clusters early when the embedded "
        "corpus has grown enough since the live run, before "
        "feedback_clusters_weekly_schedule. Only checks corpus growth -- an "
        "unplaced-share trigger would also need feedback_cluster_membership."
    ),
)
def feedback_clusters_growth_sensor(_context: SensorEvaluationContext):
    catalog = get_glue_catalog()
    if not table_exists(catalog, f"{cluster_database_name}.feedback_embeddings"):
        return SkipReason("feedback_embeddings hasn't materialized yet")
    # Scoped to match feedback_clusters' own defaults (embedding_input="summary",
    # default_embedding_model_version()/EMBEDDING_DIM) -- counting every
    # arm/model/dim would inflate this past what a completed run's
    # total_conversations actually measures, and could trigger on growth in rows
    # the next run won't even see.
    embeddings_lf = get_dbt_model_as_dataframe(
        database_name=cluster_database_name, table_name="feedback_embeddings"
    ).filter(
        (pl.col("embedding_input") == "summary")
        & (pl.col("embedding_model_version") == default_embedding_model_version())
        & (pl.col("embedding_dim") == EMBEDDING_DIM)
    )
    embedding_count = (
        filter_conversation_scope(
            embeddings_lf,
            get_dbt_model_as_dataframe(
                database_name=cluster_database_name,
                table_name="int__feedback__conversation",
            ),
            DEFAULT_FEEDBACK_SINCE,
            DEFAULT_PLATFORMS,
        )
        .select(pl.len())
        .collect()
        .item()
    )

    # The live run, not the latest promoted one: a run promoted by hand through
    # feedback_cluster_identity's cluster_run_id keeps is_promoted=false.
    live_run_id = latest_identity_processed_run(catalog, cluster_database_name)
    if live_run_id is None:
        return SkipReason("No live clusters to extend; run a full recluster first")
    last_completed_total_conversations = (
        get_dbt_model_as_dataframe(
            database_name=cluster_database_name, table_name="feedback_cluster_run"
        )
        .filter(pl.col("cluster_run_id") == live_run_id)
        .select("total_conversations")
        .collect()
        .item()
    )

    if should_trigger_early_recluster(
        embedding_count, last_completed_total_conversations
    ):
        return RunRequest(
            run_config={
                "ops": {
                    "feedback_clusters": {
                        "config": {"is_promoted": True, "mode": "extend"}
                    }
                }
            }
        )
    return SkipReason(
        "Corpus growth since the last completed run is below the trigger threshold"
    )
