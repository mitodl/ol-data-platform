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
    DEFAULT_OPENED_SINCE,
    DEFAULT_PLATFORMS,
    filter_conversation_scope,
    platforms_to_run_value,
    should_trigger_early_recluster,
)
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
            DEFAULT_OPENED_SINCE,
            DEFAULT_PLATFORMS,
        )
        .select(pl.len())
        .collect()
        .item()
    )

    last_completed_total_conversations = None
    if table_exists(catalog, f"{cluster_database_name}.feedback_cluster_run"):
        runs_lazy = get_dbt_model_as_dataframe(
            database_name=cluster_database_name, table_name="feedback_cluster_run"
        )
        # is_promoted may not exist yet on a table pre-dating it -- an unpromoted
        # run (a bake-off/hyperparameter sweep) must never become this baseline,
        # so treat a missing column as "nothing eligible" rather than skip the
        # check.
        is_promoted_filter = (
            pl.col("is_promoted")
            if "is_promoted" in runs_lazy.collect_schema().names()
            else pl.lit(False)  # noqa: FBT003
        )
        # A run over a different date range or platform set is not a valid
        # baseline either; a table pre-dating a scope column only holds runs
        # without that filter.
        run_columns = runs_lazy.collect_schema().names()

        def same_scope(column: str, default: str | None) -> pl.Expr:
            run_value = (
                pl.col(column)
                if column in run_columns
                else pl.lit(None, dtype=pl.String)
            )
            return run_value.is_null() if default is None else run_value == default

        same_opened_since = same_scope("opened_since", DEFAULT_OPENED_SINCE)
        same_platforms = same_scope(
            "platforms", platforms_to_run_value(DEFAULT_PLATFORMS)
        )
        # Same model/dim/arm as embedding_count above -- a promoted run from a
        # since-retired production config is not a valid baseline for the
        # current one (e.g. after switching embedding models).
        last_run_df = (
            runs_lazy.filter(
                (pl.col("run_status") == "completed")
                & is_promoted_filter
                & (
                    pl.col("embedding_model_version")
                    == default_embedding_model_version()
                )
                & (pl.col("embedding_dim") == EMBEDDING_DIM)
                & (pl.col("embedding_input_filter") == "summary")
                & same_opened_since
                & same_platforms
            )
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
        return RunRequest(
            run_config={"ops": {"feedback_clusters": {"config": {"is_promoted": True}}}}
        )
    return SkipReason(
        "Corpus growth since the last completed run is below the trigger threshold"
    )
