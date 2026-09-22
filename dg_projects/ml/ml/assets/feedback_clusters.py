import os
from datetime import UTC, datetime

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
from ml.lib.cluster import (
    CLUSTER_CANDIDATE_SCHEMA,
    CLUSTER_RUN_SCHEMA,
    DEFAULT_IS_PROMOTED,
    HDBSCAN_MIN_CLUSTER_SIZE,
    RANDOM_STATE,
    UMAP_N_COMPONENTS,
    UMAP_N_NEIGHBORS,
    cluster_embeddings,
    failed_run_metadata,
)
from ml.lib.embed import EMBEDDING_DIM, default_embedding_model_version
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.failures import permanent_failure
from ol_orchestrate.lib.glue_helper import (
    get_dbt_model_as_dataframe,
)
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from pydantic import Field
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo

if DAGSTER_ENV == "dev":
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    database_name = f"ol_warehouse_production_{_schema_suffix}_intermediate"
else:
    database_name = "ol_warehouse_production_intermediate"


def _clear_partial_run(catalog, table_identifier: str, cluster_run_id: str) -> None:
    """Delete any rows a failed prior attempt at this cluster_run_id already wrote."""
    try:
        table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        return
    table.delete(EqualTo("cluster_run_id", cluster_run_id))


class FeedbackClustersConfig(Config):
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
        "'concatenated_turns'). Set to null to cluster all arms together.",
    )
    embedding_model_version: str | None = Field(
        default=None,
        description=(
            "Override the embedding model id to cluster. Unset uses whichever "
            "model feedback_embeddings' default config would write (see "
            "default_embedding_model_version, ml.lib.embed)."
        ),
    )
    embedding_dim: int | None = Field(
        default=None,
        description=(
            "Override the embedding vector dimension to cluster. Unset uses "
            "EMBEDDING_DIM (ml.lib.embed)."
        ),
    )
    is_promoted: bool = Field(
        default=DEFAULT_IS_PROMOTED,
        description=(
            "Whether feedback_cluster_identity may auto-select this run into "
            "live cluster_membership. Set to false for a bake-off (#2543) or "
            "hyperparameter-sweep run -- embedding model/dim/arm alone can't "
            "tell those apart from a real production run."
        ),
    )


@multi_asset(
    group_name="feedback",
    deps=[AssetKey(["intermediate", "feedback_embeddings"])],
    outs={
        "feedback_cluster_run": AssetOut(
            key=AssetKey(["intermediate", "feedback_cluster_run"]),
            io_manager_key="io_manager",
            metadata={
                "schema": database_name,
                "write_mode": "append",
                "schema_update_mode": "update",
            },
            code_version="feedback_clusters_v1",
        ),
        "feedback_cluster_candidate": AssetOut(
            key=AssetKey(["intermediate", "feedback_cluster_candidate"]),
            io_manager_key="io_manager",
            metadata={
                "schema": database_name,
                "write_mode": "append",
                "schema_update_mode": "update",
            },
            code_version="feedback_clusters_v1",
            # Not required: a failed run writes feedback_cluster_run with no candidates.
            is_required=False,
        ),
    },
    pool="feedback_clusters",
)
def feedback_clusters(context: AssetExecutionContext, config: FeedbackClustersConfig):
    """
    Reduce (UMAP) and cluster (HDBSCAN) feedback conversation embeddings.

    One row lands in feedback_cluster_run describing the run as a whole (params,
    cluster/noise counts, silhouette, run_status='completed'|'failed', is_promoted
    -- whether feedback_cluster_identity may select it); one row per clustered
    conversation lands in
    feedback_cluster_candidate. Both are append-only:
    every run gets its own cluster_run_id. Scheduled/triggered (see
    feedback_clusters_schedule/feedback_clusters_growth_sensor in definitions.py),
    not chained on every feedback_embeddings refresh -- a full re-cluster is
    expensive relative to how often new conversations show up, and
    feedback_cluster_assignment (incremental placement) handles those between
    runs. feedback_cluster_identity matches this run's clusters onto stable
    cluster_keys immediately after; no human approves a run.
    """
    embedding_model_version = (
        config.embedding_model_version or default_embedding_model_version()
    )
    embedding_dim = config.embedding_dim or EMBEDDING_DIM
    embeddings_lazy = (
        get_dbt_model_as_dataframe(
            database_name=database_name,
            table_name="feedback_embeddings",
        )
        # Mixing vector spaces from different models/dimensions in one run is
        # meaningless -- only cluster the embeddings produced by the model
        # currently configured.
        .filter(
            (pl.col("embedding_model_version") == embedding_model_version)
            & (pl.col("embedding_dim") == embedding_dim)
        )
    )
    if config.embedding_input_filter is not None:
        # Filtered before collect(): applied lazily, so the other arm's rows
        # (and their 1024-dim vectors) are never pulled from S3 at all, rather
        # than materialized and then discarded.
        embeddings_lazy = embeddings_lazy.filter(
            pl.col("embedding_input") == config.embedding_input_filter
        )
    embeddings_df = embeddings_lazy.select(
        [
            "feedback_conversation_pk",
            "source_slug",
            "conversation_ref",
            "embedding_input",
            "embedding_vector",
        ]
    ).collect()

    if config.sample_limit is not None and config.sample_limit < embeddings_df.height:
        # Random, not head(): the table's row order isn't meaningful.
        embeddings_df = embeddings_df.sample(n=config.sample_limit, seed=RANDOM_STATE)

    # root_run_id, not run_id: stable across retries, so _clear_partial_run can match.
    cluster_run_id = context.run.root_run_id or context.run.run_id
    embedding_provenance = (
        embedding_model_version,
        embedding_dim,
        config.embedding_input_filter,
    )
    umap_params = (config.umap_n_components, config.umap_n_neighbors)

    # umap_n_components too: UMAP needs n_components < height or it raises TypeError.
    if (
        embeddings_df.height < config.min_cluster_size
        or embeddings_df.height <= config.umap_n_components
    ):
        msg = (
            f"Only {embeddings_df.height} embedded conversations available "
            f"(min_cluster_size={config.min_cluster_size}, "
            f"umap_n_components={config.umap_n_components}); not enough to form "
            "even one cluster. Run feedback_embeddings first, or lower both for "
            "a small local test."
        )
        # A failed row, not silence: run_status='failed' is now distinguishable.
        failed_metadata = failed_run_metadata(
            cluster_run_id,
            embedding_provenance,
            umap_params,
            config.min_cluster_size,
            RANDOM_STATE,
            embeddings_df.height,
            is_promoted=config.is_promoted,
        )
        failed_metadata["run_at"] = datetime.now(tz=UTC)
        yield Output(
            pl.DataFrame([failed_metadata], schema=CLUSTER_RUN_SCHEMA),
            output_name="feedback_cluster_run",
            metadata={"cluster_run_id": MetadataValue.text(cluster_run_id)},
        )
        # Permanent, not Failure: run_retries retries a bare Failure forever.
        raise permanent_failure(msg)

    catalog = get_glue_catalog()
    _clear_partial_run(
        catalog, f"{database_name}.feedback_cluster_candidate", cluster_run_id
    )
    _clear_partial_run(catalog, f"{database_name}.feedback_cluster_run", cluster_run_id)

    candidates_df, run_metadata = cluster_embeddings(
        embeddings_df,
        embedding_provenance,
        umap_params=umap_params,
        min_cluster_size=config.min_cluster_size,
        cluster_run_id=cluster_run_id,
        is_promoted=config.is_promoted,
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
    # a run marked complete with no matching candidates.
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
            "embedding_model_version": MetadataValue.text(embedding_model_version),
            "embedding_dim": MetadataValue.int(embedding_dim),
            "is_promoted": MetadataValue.bool(config.is_promoted),
        },
    )
