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
    platforms_to_run_value,
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
        "Triggers an incremental run of feedback_clusters early when the embedded "
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

    if latest_identity_processed_run(catalog, cluster_database_name) is None:
        return SkipReason("No live clusters to extend; run a full recluster first")
    processed_run_ids = (
        get_dbt_model_as_dataframe(
            database_name=cluster_database_name, table_name="feedback_cluster_lineage"
        )
        .select("cluster_run_id")
        .unique()
        .collect()["cluster_run_id"]
        .to_list()
    )
    # Promoted counts as soon as the run completes, so this sensor's own run is not
    # started again while identity catches up. Processed counts too: a run promoted
    # by hand through identity's cluster_run_id keeps is_promoted=false.
    baseline_df = (
        get_dbt_model_as_dataframe(
            database_name=cluster_database_name, table_name="feedback_cluster_run"
        )
        .filter(
            (pl.col("run_status") == "completed")
            & (
                pl.col("is_promoted")
                | pl.col("cluster_run_id").is_in(processed_run_ids)
            )
            & (pl.col("embedding_model_version") == default_embedding_model_version())
            & (pl.col("embedding_dim") == EMBEDDING_DIM)
            & (pl.col("embedding_input_filter") == "summary")
            & pl.col("feedback_since").eq_missing(
                pl.lit(DEFAULT_FEEDBACK_SINCE, dtype=pl.String)
            )
            & pl.col("platforms").eq_missing(
                pl.lit(platforms_to_run_value(DEFAULT_PLATFORMS), dtype=pl.String)
            )
        )
        .sort("run_at", descending=True)
        .select("total_conversations")
        .limit(1)
        .collect()
    )
    last_completed_total_conversations = (
        baseline_df.item() if baseline_df.height else None
    )

    if should_trigger_early_recluster(
        embedding_count, last_completed_total_conversations
    ):
        return RunRequest(
            run_config={"ops": {"feedback_clusters": {"config": {"is_promoted": True}}}}
        )
    return SkipReason(
        "Corpus growth since the last completed run is below the trigger threshold"
    )
