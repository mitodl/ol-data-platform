import polars as pl
from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    RunsFilter,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from ml.assets.feedback_clusters import database_name as cluster_database_name
from ml.lib.cluster import (
    AUTOMATED_RUN_CONFIG,
    DEFAULT_FEEDBACK_SINCE,
    DEFAULT_PLATFORMS,
    filter_conversation_scope,
    platforms_to_run_value,
    should_trigger_early_recluster,
)
from ml.lib.embed import resolve_embedding_model
from ml.lib.iceberg_helpers import table_exists
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog


@sensor(
    name="feedback_clusters_growth_sensor",
    job_name="feedback_clusters_job",
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Starts an incremental run of feedback_clusters, new themes included, when "
        "the embedded corpus has grown enough since the last completed run. Only "
        "checks corpus growth -- an unplaced-share trigger would also need "
        "feedback_cluster_membership."
    ),
)
def feedback_clusters_growth_sensor(context: SensorEvaluationContext):
    # The baseline counts only completed runs, so a run longer than the sensor
    # interval would otherwise be started again from the same base
    if context.instance.get_run_records(
        filters=RunsFilter(
            job_name="feedback_clusters_job",
            statuses=[
                DagsterRunStatus.QUEUED,
                DagsterRunStatus.NOT_STARTED,
                DagsterRunStatus.STARTING,
                DagsterRunStatus.STARTED,
            ],
        ),
        limit=1,
    ):
        return SkipReason("A feedback_clusters_job run is already queued or running")
    catalog = get_glue_catalog()
    if not table_exists(catalog, f"{cluster_database_name}.feedback_embeddings"):
        return SkipReason("feedback_embeddings hasn't materialized yet")
    # Scoped to match feedback_clusters' own defaults (embedding_input="summary",
    # resolve_embedding_model()'s model and dim) -- counting every
    # arm/model/dim would inflate this past what a completed run's
    # total_conversations actually measures, and could trigger on growth in rows
    # the next run won't even see.
    embedding_model = resolve_embedding_model()
    embeddings_lf = get_dbt_model_as_dataframe(
        database_name=cluster_database_name, table_name="feedback_embeddings"
    ).filter((pl.col("embedding_input") == "summary") & embedding_model.matches())
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

    # Start only a run that can succeed: an incremental run needs an active
    # cluster, and a processed run can have none (an all-noise first run).
    if not table_exists(catalog, f"{cluster_database_name}.feedback_cluster") or (
        get_dbt_model_as_dataframe(
            database_name=cluster_database_name, table_name="feedback_cluster"
        )
        .filter(
            (pl.col("cluster_status") == "active")
            & embedding_model.matches()
            & (pl.col("embedding_input_filter") == "summary")
        )
        .select(pl.len())
        .collect()
        .item()
        == 0
    ):
        return SkipReason("No live clusters to extend; run a full recluster first")
    processed_run_ids = (
        get_dbt_model_as_dataframe(
            database_name=cluster_database_name,
            table_name="feedback_cluster_identity_run",
        )
        .select("cluster_run_id")
        .collect()["cluster_run_id"]
        .to_list()
        if table_exists(
            catalog, f"{cluster_database_name}.feedback_cluster_identity_run"
        )
        else []
    )
    # Promoted counts as soon as the run completes, so this sensor's own run is not
    # started again while identity catches up. Processed counts too: a run promoted
    # by hand through identity's cluster_run_id keeps is_promoted=false.
    runs_lf = get_dbt_model_as_dataframe(
        database_name=cluster_database_name, table_name="feedback_cluster_run"
    )
    run_columns = runs_lf.collect_schema().names()

    # A table written before a column existed reads it as null
    def run_column(name: str, dtype: pl.DataType) -> pl.Expr:
        return pl.col(name) if name in run_columns else pl.lit(None, dtype=dtype)

    baseline_df = (
        runs_lf.filter(
            (pl.col("run_status") == "completed")
            & (
                run_column("is_promoted", pl.Boolean).fill_null(False)  # noqa: FBT003
                | pl.col("cluster_run_id").is_in(processed_run_ids)
            )
            & embedding_model.matches()
            & (pl.col("embedding_input_filter") == "summary")
            & run_column("feedback_since", pl.String).eq_missing(
                pl.lit(DEFAULT_FEEDBACK_SINCE, dtype=pl.String)
            )
            & run_column("platforms", pl.String).eq_missing(
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

    if last_completed_total_conversations is None:
        # The clusters in use have another embedding config or scope, which an
        # incremental run would reject
        return SkipReason(
            "No run with the default embedding config and scope is in use; run a "
            "full recluster first"
        )
    if should_trigger_early_recluster(
        embedding_count, last_completed_total_conversations
    ):
        return RunRequest(
            run_config={"ops": {"feedback_clusters": {"config": AUTOMATED_RUN_CONFIG}}}
        )
    return SkipReason(
        "Corpus growth since the last completed run is below the trigger threshold"
    )
