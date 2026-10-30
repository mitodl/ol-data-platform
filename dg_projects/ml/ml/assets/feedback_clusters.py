import os
from datetime import UTC, datetime
from typing import Literal

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
from ml.assets.feedback_cluster_identity import _active_cluster_members
from ml.lib.cluster import (
    CLUSTER_CANDIDATE_SCHEMA,
    CLUSTER_RUN_SCHEMA,
    DEFAULT_FEEDBACK_SINCE,
    DEFAULT_IS_PROMOTED,
    DEFAULT_PLATFORMS,
    HDBSCAN_CLUSTER_SELECTION_METHOD,
    HDBSCAN_MIN_CLUSTER_SIZE,
    HDBSCAN_MIN_SAMPLES,
    MAX_CLUSTERS,
    RANDOM_STATE,
    UMAP_N_COMPONENTS,
    UMAP_N_NEIGHBORS,
    cluster_embeddings,
    extend_clusters,
    failed_run_metadata,
    filter_conversation_scope,
    platforms_to_run_value,
)
from ml.lib.cluster_run_lookup import (
    latest_identity_processed_run,
    run_already_applied,
)
from ml.lib.embed import resolve_embedding_model
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


def _check_live_run(
    catalog,
    live_run_id: str | None,
    feedback_since: str | None,
    platforms: list[str] | None,
) -> None:
    if live_run_id is None:
        return
    # Before assignment rewrites membership, a newly live run's new keys have no
    # members, so this run would leave them empty.
    if not run_already_applied(catalog, database_name, live_run_id):
        msg = (
            f"feedback_cluster_assignment has not applied the live run {live_run_id} "
            "yet; run this again after it finishes."
        )
        raise permanent_failure(msg)
    # Another scope would carry no member forward, and identity would then retire
    # every live key without the continuity floor to stop it.
    live_run = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_run"
        )
        .filter(pl.col("cluster_run_id") == live_run_id)
        .collect()
        .to_dicts()
    )
    if live_run and (
        live_run[0].get("feedback_since"),
        live_run[0].get("platforms"),
    ) != (
        feedback_since,
        platforms_to_run_value(platforms),
    ):
        msg = (
            f"feedback_since/platforms differ from the live run {live_run_id}'s; "
            "change the scope only with full_refresh."
        )
        raise permanent_failure(msg)


def _live_cluster_keys(
    catalog,
    embedding_provenance: tuple[str, int, str | None],
    feedback_since: str | None,
    platforms: list[str] | None,
) -> tuple[str | None, dict[str, str]]:
    live_run_id = latest_identity_processed_run(catalog, database_name)
    _check_live_run(catalog, live_run_id, feedback_since, platforms)
    members_by_key = _active_cluster_members(
        catalog, *embedding_provenance, feedback_since, platforms
    )
    cluster_key_by_pk = {pk: key for key, pks in members_by_key.items() for pk in pks}
    if not cluster_key_by_pk:
        msg = (
            "No live cluster has members for this embedding config and scope; run "
            "feedback_clusters with full_refresh."
        )
        raise permanent_failure(msg)
    return live_run_id, cluster_key_by_pk


def _clear_partial_run(catalog, table_identifier: str, cluster_run_id: str) -> None:
    """Delete any rows a failed prior attempt at this cluster_run_id already wrote."""
    try:
        table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        return
    table.delete(EqualTo("cluster_run_id", cluster_run_id))


class FeedbackClustersConfig(Config):
    full_refresh: bool = Field(
        default=False,
        description=(
            "Rebuild all clusters from scratch, which can split, merge, and relabel "
            "existing categories. Run it rarely, and review the labels after. False "
            "keeps the live clusters and adds new conversations that clearly fit "
            "one (ml.lib.cluster.extend_clusters)."
        ),
    )
    add_new_clusters: bool = Field(
        default=False,
        description=(
            "When not a full refresh, also create clusters for new common themes and "
            "move noise groups that sit close to a cluster into it. The growth sensor "
            "sets it. For a manual run held for review (is_promoted false), turn off "
            "feedback_clusters_growth_sensor: identity rejects the run if another run "
            "goes live first. Ignored with full_refresh, which builds every cluster "
            "from scratch."
        ),
    )
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
    hdbscan_min_samples: int | None = Field(
        default=HDBSCAN_MIN_SAMPLES,
        ge=1,
        description=(
            "HDBSCAN's min_samples: how dense an area must be to count as a cluster. "
            "Keep it small, so min_cluster_size alone controls cluster size. Null "
            "reuses min_cluster_size, HDBSCAN's own default."
        ),
    )
    max_clusters: int | None = Field(
        default=MAX_CLUSTERS,
        ge=1,
        description=(
            "Upper bound on clusters, and so on LLM categories. If a run finds more, "
            "min_cluster_size grows until it fits; feedback_cluster_run records the "
            "value used. Null disables the cap."
        ),
    )
    hdbscan_cluster_selection_method: Literal["eom", "leaf"] = Field(
        default=HDBSCAN_CLUSTER_SELECTION_METHOD,
        description=(
            "HDBSCAN's cluster_selection_method. 'leaf' keeps fine-grained clusters; "
            "'eom' tends to merge them into one or two very large clusters."
        ),
    )
    embedding_input_filter: str | None = Field(
        default="summary",
        description="Restrict to one embedding_input arm ('summary' or "
        "'concatenated_turns'). Set to null to cluster all arms together.",
    )
    embedding_model: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9-]+:.+$",
        description=(
            "The embedding model to cluster, in ol-llm's provider:model form, as "
            "feedback_embeddings' embedding_model takes it. Unset uses whichever "
            "model feedback_embeddings' default config writes (EMBEDDING_MODEL, "
            "ml.lib.embed)."
        ),
    )
    embedding_dim: int | None = Field(
        default=None,
        description=(
            "Override the embedding vector dimension to cluster. Unset uses "
            "EMBEDDING_DIM (ml.lib.embed)."
        ),
    )
    feedback_since: str | None = Field(
        default=DEFAULT_FEEDBACK_SINCE,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description=(
            "Only cluster conversations opened on or after this date (YYYY-MM-DD). "
            "Recorded on feedback_cluster_run so feedback_cluster_assignment places "
            "the same range. Defaults to the FEEDBACK_SINCE env var; unset or null "
            "clusters the full history."
        ),
    )
    platforms: list[str] | None = Field(
        default=DEFAULT_PLATFORMS,
        description=(
            "Only cluster conversations whose platform is in this list, e.g. "
            "['mitlearn']. Recorded on feedback_cluster_run so "
            "feedback_cluster_assignment places the same platforms. Set to null to "
            "cluster every platform, and conversations with no platform."
        ),
    )
    is_promoted: bool = Field(
        default=DEFAULT_IS_PROMOTED,
        description=(
            "Whether feedback_cluster_identity may auto-select this run into "
            "live cluster_membership. Defaults false -- set true only for the "
            "production launcher (feedback_clusters_growth_sensor); a manual "
            "run (e.g. a bake-off, "
            "#2543, or hyperparameter sweep) should leave this unset."
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
            code_version="feedback_clusters_v9",
        ),
        "feedback_cluster_candidate": AssetOut(
            key=AssetKey(["intermediate", "feedback_cluster_candidate"]),
            io_manager_key="io_manager",
            metadata={
                "schema": database_name,
                "write_mode": "append",
                "schema_update_mode": "update",
            },
            code_version="feedback_clusters_v9",
            # Not required: a failed run writes feedback_cluster_run with no candidates.
            is_required=False,
        ),
    },
    pool="feedback_clusters",
)
def feedback_clusters(context: AssetExecutionContext, config: FeedbackClustersConfig):
    """
    Add new feedback conversations to the live clusters, or, with full_refresh,
    reduce (UMAP) and cluster (HDBSCAN) every conversation embedding again.

    One row lands in feedback_cluster_run describing the run as a whole (params,
    cluster/noise counts, silhouette, run_status='completed'|'failed', is_promoted
    -- whether feedback_cluster_identity may select it); one row per clustered
    conversation lands in
    feedback_cluster_candidate. Both are append-only:
    every run gets its own cluster_run_id. Triggered by
    feedback_clusters_growth_sensor (see definitions.py), not chained on every
    feedback_embeddings refresh -- a run is expensive relative to how often new
    conversations show up, and feedback_cluster_assignment (incremental
    placement) handles those between runs. feedback_cluster_identity matches this
    run's clusters onto stable cluster_keys. A promoted incremental run goes live
    without a review, new themes included; a full_refresh run waits for one (see
    is_promoted).
    """
    if config.is_promoted and config.full_refresh:
        # The review gate: identity would otherwise take the run live at once
        msg = (
            "full_refresh can split, merge, and relabel every category, so run it "
            "with is_promoted false and promote the run after a review."
        )
        raise permanent_failure(msg)
    embedding_model = resolve_embedding_model(
        config.embedding_model, config.embedding_dim
    )
    embedding_model_version = embedding_model.model_version
    embedding_dim = embedding_model.dim
    embeddings_lazy = (
        get_dbt_model_as_dataframe(
            database_name=database_name,
            table_name="feedback_embeddings",
        )
        # Mixing vector spaces from different models/dimensions in one run is
        # meaningless -- only cluster the embeddings produced by the model
        # currently configured.
        .filter(embedding_model.matches())
    )
    if config.embedding_input_filter is not None:
        # Filtered before collect(): applied lazily, so the other arm's rows
        # (and their 1024-dim vectors) are never pulled from S3 at all, rather
        # than materialized and then discarded.
        embeddings_lazy = embeddings_lazy.filter(
            pl.col("embedding_input") == config.embedding_input_filter
        )
    embeddings_lazy = filter_conversation_scope(
        embeddings_lazy,
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="int__feedback__conversation"
        ),
        config.feedback_since,
        config.platforms,
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
    # UMAP and HDBSCAN depend on row order, and Iceberg's changes as the table is
    # written, so the same data could give different new categories
    embeddings_df = embeddings_df.sort("feedback_conversation_pk")

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
    if config.full_refresh and (
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
        failed_metadata["feedback_since"] = config.feedback_since
        failed_metadata["platforms"] = platforms_to_run_value(config.platforms)
        failed_metadata["hdbscan_min_samples"] = config.hdbscan_min_samples
        failed_metadata["hdbscan_cluster_selection_method"] = (
            config.hdbscan_cluster_selection_method
        )
        failed_metadata["max_clusters"] = config.max_clusters
        failed_metadata["run_type"] = "full_refresh"
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

    step_settings = {
        "min_samples": config.hdbscan_min_samples,
        "cluster_selection_method": config.hdbscan_cluster_selection_method,
        "cluster_run_id": cluster_run_id,
        "is_promoted": config.is_promoted,
    }
    if not config.full_refresh:
        base_cluster_run_id, cluster_key_by_pk = _live_cluster_keys(
            catalog, embedding_provenance, config.feedback_since, config.platforms
        )
        candidates_df, run_metadata = extend_clusters(
            embeddings_df,
            embedding_provenance,
            cluster_key_by_pk,
            umap_params=umap_params,
            add_new_clusters=config.add_new_clusters,
            **step_settings,
        )
        run_metadata["base_cluster_run_id"] = base_cluster_run_id
        if candidates_df["cluster_key"].is_null().all():
            # Missing embeddings, such as an upstream outage
            msg = (
                "No embedded conversation carries a live cluster, so this run would "
                "change nothing; check feedback_embeddings."
            )
            raise permanent_failure(msg)
    else:
        candidates_df, run_metadata = cluster_embeddings(
            embeddings_df,
            embedding_provenance,
            umap_params=umap_params,
            min_cluster_size=config.min_cluster_size,
            max_clusters=config.max_clusters,
            **step_settings,
        )
    run_metadata["run_at"] = datetime.now(tz=UTC)
    run_metadata["feedback_since"] = config.feedback_since
    run_metadata["platforms"] = platforms_to_run_value(config.platforms)
    run_df = pl.DataFrame([run_metadata], schema=CLUSTER_RUN_SCHEMA)

    context.log.info(
        "Cluster run %s (%s): %d conversations, %d clusters, %d noise, "
        "%s noise placed, %s new clusters (%s more held by the cap), "
        "%s noise groups placed, silhouette=%s",
        run_metadata["cluster_run_id"],
        run_metadata["run_type"],
        run_metadata["total_conversations"],
        run_metadata["cluster_count"],
        run_metadata["noise_count"],
        run_metadata.get("placed_noise_count"),
        run_metadata.get("new_cluster_count"),
        run_metadata.get("capped_new_cluster_count"),
        run_metadata.get("placed_group_count"),
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
            "feedback_since": MetadataValue.text(config.feedback_since or ""),
            "platforms": MetadataValue.text(
                platforms_to_run_value(config.platforms) or ""
            ),
        },
    )
