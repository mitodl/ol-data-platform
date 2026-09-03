import os
from datetime import UTC, datetime

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    Config,
    Failure,
    MetadataValue,
    Output,
    multi_asset,
)
from ml.lib.cluster import (
    CLUSTER_CANDIDATE_SCHEMA,
    CLUSTER_RUN_SCHEMA,
    HDBSCAN_MIN_CLUSTER_SIZE,
    RANDOM_STATE,
    UMAP_N_COMPONENTS,
    UMAP_N_NEIGHBORS,
    cluster_embeddings,
)
from ml.lib.embed import EMBEDDING_DIM, EMBEDDING_MODEL_VERSION
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import (
    get_dbt_model_as_dataframe,
)
from pydantic import Field

if DAGSTER_ENV == "dev":
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    database_name = f"ol_warehouse_production_{_schema_suffix}_intermediate"
else:
    database_name = "ol_warehouse_production_intermediate"


class FeedbackClusteringConfig(Config):
    sample_limit: int | None = Field(
        default=None,
        description="Cap the number of embedded conversations clustered, for fast "
        "local testing.",
    )
    umap_n_components: int = Field(
        default=UMAP_N_COMPONENTS,
        description="UMAP output dimensionality before HDBSCAN (spec: ~5-15).",
    )
    umap_n_neighbors: int = Field(
        default=UMAP_N_NEIGHBORS,
        description="UMAP's n_neighbors -- larger values favor global structure "
        "over local detail.",
    )
    min_cluster_size: int = Field(
        default=HDBSCAN_MIN_CLUSTER_SIZE,
        description="HDBSCAN's min_cluster_size -- how many conversations before "
        "a group counts as systemic rather than a one-off.",
    )
    embedding_input_filter: str | None = Field(
        default="summary",
        description="Restrict to one embedding_input arm ('summary' or "
        "'concatenated_turns')",
    )


@multi_asset(
    group_name="feedback",
    deps=[AssetKey(["intermediate", "feedback_embeddings"])],
    outs={
        "feedback_cluster_run": AssetOut(
            key=AssetKey(["intermediate", "feedback_cluster_run"]),
            io_manager_key="io_manager",
            metadata={"schema": database_name, "write_mode": "append"},
            code_version="feedback_clustering_v1",
            automation_condition=upstream_or_code_changes(),
        ),
        "feedback_cluster_candidate": AssetOut(
            key=AssetKey(["intermediate", "feedback_cluster_candidate"]),
            io_manager_key="io_manager",
            metadata={"schema": database_name, "write_mode": "append"},
            code_version="feedback_clustering_v1",
            automation_condition=upstream_or_code_changes(),
        ),
    },
    pool="feedback_clustering",
)
def feedback_clustering(
    context: AssetExecutionContext, config: FeedbackClusteringConfig
):
    """
    Reduce (UMAP) and cluster (HDBSCAN) feedback conversation embeddings.

    One row lands in feedback_cluster_run describing the run as a whole (params,
    cluster/noise counts, silhouette); one row per clustered conversation lands in
    feedback_cluster_candidate (feedback_ml_approach.md §C). Both are append-only:
    every run gets its own cluster_run_id, so a proposed run can be compared
    against the live one before a human promotes it onto afact_feedback_conversation
    -- that promotion step is not part of this asset.
    """
    embeddings_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name,
            table_name="feedback_embeddings",
        )
        # Mixing vector spaces from different models/dimensions in one run is
        # meaningless -- only cluster the embeddings produced by the model
        # currently configured.
        .filter(
            (pl.col("embedding_model_version") == EMBEDDING_MODEL_VERSION)
            & (pl.col("embedding_dim") == EMBEDDING_DIM)
        )
        .select(
            ["source_slug", "conversation_ref", "embedding_vector", "embedding_input"]
        )
        .collect()
    )
    if config.embedding_input_filter is not None:
        embeddings_df = embeddings_df.filter(
            pl.col("embedding_input") == config.embedding_input_filter
        )
    embeddings_df = embeddings_df.drop("embedding_input")

    if config.sample_limit is not None and config.sample_limit < embeddings_df.height:
        # Random, not head(): the table's row order isn't meaningful.
        embeddings_df = embeddings_df.sample(n=config.sample_limit, seed=RANDOM_STATE)

    if embeddings_df.height < config.min_cluster_size:
        msg = (
            f"Only {embeddings_df.height} embedded conversations available "
            f"(min_cluster_size={config.min_cluster_size}); not enough to form "
            "even one cluster. Run feedback_embeddings first, or lower "
            "min_cluster_size for a small local test."
        )
        raise Failure(msg)

    candidates_df, run_metadata = cluster_embeddings(
        embeddings_df,
        (EMBEDDING_MODEL_VERSION, EMBEDDING_DIM, config.embedding_input_filter),
        umap_params=(config.umap_n_components, config.umap_n_neighbors),
        min_cluster_size=config.min_cluster_size,
    )
    run_metadata["run_at"] = datetime.now(tz=UTC)
    run_df = pl.DataFrame([run_metadata], schema=CLUSTER_RUN_SCHEMA)

    context.log.info(
        "Cluster run %s: %d conversations, %d clusters, %d noise, silhouette=%s",
        run_metadata["cluster_run_id"],
        run_metadata["total_conversations"],
        run_metadata["cluster_count"],
        run_metadata["noise_count"],
        run_metadata["silhouette_score"],
    )

    # Candidates before the run row: the run row is the commit marker for a
    # complete run. Writing it first would let a failed candidate write leave
    # a run marked complete with no matching candidates, and a retry would
    # mint a new cluster_run_id rather than repairing the orphaned one.
    yield Output(
        candidates_df.cast(CLUSTER_CANDIDATE_SCHEMA),
        output_name="feedback_cluster_candidate",
        metadata={"row_count": MetadataValue.int(candidates_df.height)},
    )
    yield Output(
        run_df,
        output_name="feedback_cluster_run",
        metadata={
            "cluster_run_id": MetadataValue.text(run_metadata["cluster_run_id"]),
            "cluster_count": MetadataValue.int(run_metadata["cluster_count"]),
            "noise_count": MetadataValue.int(run_metadata["noise_count"]),
        },
    )
