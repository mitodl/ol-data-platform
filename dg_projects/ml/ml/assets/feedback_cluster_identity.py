import os
from datetime import UTC, datetime
from typing import Any

import numpy as np
import polars as pl
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    Config,
    MetadataValue,
    Output,
    multi_asset,
)
from ml.lib.cluster import (
    NOISE_CLUSTER_ID,
    filter_conversation_scope,
    platforms_from_run_value,
)
from ml.lib.cluster_identity import (
    CLUSTER_LINEAGE_SCHEMA,
    CLUSTER_SCHEMA,
    CONTINUITY_FLOOR,
    JACCARD_MATCH_THRESHOLD,
    cluster_lineage_pk,
    compute_cluster_stats,
    compute_continuity,
    match_clusters,
)
from ml.lib.embed import EMBEDDING_DIM, default_embedding_model_version
from ml.lib.iceberg_helpers import table_exists
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.failures import permanent_failure
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from pydantic import Field

if DAGSTER_ENV == "dev":
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    database_name = f"ol_warehouse_production_{_schema_suffix}_intermediate"
else:
    database_name = "ol_warehouse_production_intermediate"


class FeedbackClusterIdentityConfig(Config):
    cluster_run_id: str | None = Field(
        default=None,
        description="Process this specific completed run instead of "
        "auto-selecting the most recent completed run with no lineage yet.",
    )
    match_threshold: float = Field(
        default=JACCARD_MATCH_THRESHOLD,
        description="Minimum Jaccard overlap for a new cluster to 'continue' an "
        "existing cluster_key.",
    )


IDENTITY_RUN_SCHEMA = {
    "cluster_run_id": pl.String,
    "processed_at": pl.Datetime(time_zone="UTC"),
}

# Mirrors FeedbackClustersConfig.embedding_input_filter's default.
PRODUCTION_EMBEDDING_INPUT = "summary"


def _select_run_to_process(
    catalog, config: FeedbackClusterIdentityConfig
) -> str | None:
    """Return the completed cluster_run_id to match, or None if there's nothing new.

    "Already processed" is tracked in feedback_cluster_identity_run rather than by
    the presence of feedback_cluster_lineage rows for the run -- a completed run
    that is all noise while no cluster is active (so nothing is 'new' or 'retired')
    produces zero lineage rows, so lineage-row existence alone can never mark it
    processed and it would be reselected on every tick.
    """
    if config.cluster_run_id is not None:
        return config.cluster_run_id
    if not table_exists(catalog, f"{database_name}.feedback_cluster_run"):
        return None
    runs_lazy = get_dbt_model_as_dataframe(
        database_name=database_name, table_name="feedback_cluster_run"
    )
    # is_promoted may not exist yet on a table pre-dating it -- treat that as
    # "nothing eligible" rather than error or silently skip the check.
    is_promoted_filter = (
        pl.col("is_promoted")
        if "is_promoted" in runs_lazy.collect_schema().names()
        else pl.lit(False)  # noqa: FBT003
    )
    runs_df = (
        runs_lazy.filter(
            (pl.col("run_status") == "completed")
            & is_promoted_filter
            & (pl.col("embedding_model_version") == default_embedding_model_version())
            & (pl.col("embedding_dim") == EMBEDDING_DIM)
            & (pl.col("embedding_input_filter") == PRODUCTION_EMBEDDING_INPUT)
        )
        .select(["cluster_run_id", "run_at"])
        .collect()
    )
    if runs_df.height == 0:
        return None
    already_processed: set[str] = set()
    if table_exists(catalog, f"{database_name}.feedback_cluster_identity_run"):
        already_processed = set(
            get_dbt_model_as_dataframe(
                database_name=database_name,
                table_name="feedback_cluster_identity_run",
            )
            .select("cluster_run_id")
            .unique()
            .collect()["cluster_run_id"]
        )
    unprocessed = runs_df.filter(
        ~pl.col("cluster_run_id").is_in(already_processed)
    ).sort("run_at", descending=True)
    if unprocessed.height == 0:
        return None
    return unprocessed["cluster_run_id"][0]


def _active_cluster_members(  # noqa: PLR0913 -- one filter per scope dimension
    catalog,
    embedding_model_version: str,
    embedding_dim: int,
    embedding_input_filter: str | None,
    opened_since: str | None = None,
    platforms: list[str] | None = None,
) -> dict[str, frozenset[str]]:
    """cluster_key -> its live member pks, for every currently-active key built
    from the same embedding_model_version/embedding_dim/embedding_input_filter
    as the run being matched.

    Empty if feedback_cluster_membership has no rows, in which case every new
    cluster resolves to 'new'. Scoping by the full config -- including which
    arm (summary vs concatenated_turns) was clustered -- keeps a cluster from a
    different config, or conversations incrementally placed from a different
    arm, out of this run's Jaccard comparison.

    opened_since and platforms, the run's own scope, drop members outside it, so
    a date- or platform-limited run is compared only with the part of each
    cluster it could have reproduced.
    """
    if not table_exists(
        catalog, f"{database_name}.feedback_cluster_membership"
    ) or not table_exists(catalog, f"{database_name}.feedback_cluster"):
        return {}
    active_keys = set(
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster"
        )
        .filter(
            (pl.col("cluster_status") == "active")
            & (pl.col("embedding_model_version") == embedding_model_version)
            & (pl.col("embedding_dim") == embedding_dim)
            & (
                pl.col("embedding_input_filter").eq_missing(
                    pl.lit(embedding_input_filter)
                )
            )
        )
        .collect()["cluster_key"]
    )
    membership_lf = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_membership"
        )
        .filter(
            pl.col("cluster_key").is_not_null()
            & pl.col("cluster_key").is_in(active_keys)
        )
        .select(["feedback_conversation_pk", "cluster_key"])
    )
    membership_df = filter_conversation_scope(
        membership_lf,
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="int__feedback__conversation"
        ),
        opened_since,
        platforms,
    ).collect()
    members_by_key = {
        cluster_key: frozenset(group["feedback_conversation_pk"])
        for (cluster_key,), group in membership_df.group_by("cluster_key")
    }
    if opened_since is None and platforms is None:
        return members_by_key
    # Keep a key whose members are all out of scope: an empty set matches nothing,
    # so match_clusters retires it instead of leaving it active forever.
    return {key: members_by_key.get(key, frozenset()) for key in active_keys}


def _other_config_active_keys(
    catalog,
    embedding_model_version: str,
    embedding_dim: int,
    embedding_input_filter: str | None,
) -> set[str]:
    """Active cluster_keys under a different embedding config than this run --
    otherwise an embedding-model/dim/arm change leaves the old config's
    clusters 'active' forever, since no future run can ever match against them.
    """
    if not table_exists(catalog, f"{database_name}.feedback_cluster"):
        return set()
    return set(
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster"
        )
        .filter(
            (pl.col("cluster_status") == "active")
            & (
                (pl.col("embedding_model_version") != embedding_model_version)
                | (pl.col("embedding_dim") != embedding_dim)
                | ~pl.col("embedding_input_filter").eq_missing(
                    pl.lit(embedding_input_filter)
                )
            )
        )
        .collect()["cluster_key"]
    )


def _existing_cluster_rows(catalog) -> dict[str, dict[str, Any]]:
    """cluster_key -> its current feedback_cluster row, for carrying
    first_seen_run_id forward on a continued/merged key.
    """
    if not table_exists(catalog, f"{database_name}.feedback_cluster"):
        return {}
    return {
        row["cluster_key"]: row
        for row in get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster"
        )
        .collect()
        .to_dicts()
    }


@multi_asset(
    group_name="feedback",
    deps=[
        AssetKey(["intermediate", "feedback_cluster_run"]),
        AssetKey(["intermediate", "feedback_cluster_candidate"]),
    ],
    outs={
        "feedback_cluster": AssetOut(
            key=AssetKey(["intermediate", "feedback_cluster"]),
            io_manager_key="io_manager",
            metadata={
                "schema": database_name,
                "write_mode": "upsert",
                "upsert_options": {"join_cols": ["cluster_key"]},
                "schema_update_mode": "update",
            },
            code_version="feedback_cluster_identity_v3",
            automation_condition=upstream_or_code_changes(),
        ),
        "feedback_cluster_lineage": AssetOut(
            key=AssetKey(["intermediate", "feedback_cluster_lineage"]),
            io_manager_key="io_manager",
            metadata={
                "schema": database_name,
                "write_mode": "append",
                "schema_update_mode": "update",
            },
            code_version="feedback_cluster_identity_v3",
            automation_condition=upstream_or_code_changes(),
            is_required=False,
        ),
        "feedback_cluster_identity_run": AssetOut(
            key=AssetKey(["intermediate", "feedback_cluster_identity_run"]),
            io_manager_key="io_manager",
            metadata={
                "schema": database_name,
                "write_mode": "upsert",
                "upsert_options": {"join_cols": ["cluster_run_id"]},
                "schema_update_mode": "update",
            },
            code_version="feedback_cluster_identity_v3",
            automation_condition=upstream_or_code_changes(),
            is_required=False,
        ),
    },
    check_specs=[
        AssetCheckSpec(
            name="continuity_floor",
            asset=AssetKey(["intermediate", "feedback_cluster_lineage"]),
            blocking=True,
        )
    ],
    pool="feedback_cluster_identity",
)
def feedback_cluster_identity(
    context: AssetExecutionContext, config: FeedbackClusterIdentityConfig
):
    """
    Match one completed feedback_clusters run onto stable cluster_keys.

    A run's cluster_id is arbitrary and run-local -- it carries no relationship to
    any previous run's numbering. This asset compares the new run's clusters
    against every currently-active cluster_key's live membership (from
    feedback_cluster_membership) by Jaccard overlap, and resolves each new cluster
    to continued/merged/split/new (ml.lib.cluster_identity.match_clusters), writing
    one feedback_cluster row per resolved key and one feedback_cluster_lineage row
    per edge (plus one 'retired' row per active key that didn't survive, and one
    per active key from a *different* embedding_model_version/embedding_dim/
    embedding_input_filter -- those can never be matched against the run's own
    config, so a model/dim/arm change would otherwise leave them 'active'
    forever). No human approves this;
    a `continuity_floor` asset check blocks the write instead when too few
    conversations kept their cluster_key, since that means the run's
    configuration needs fixing rather than a rerun.
    """
    catalog = get_glue_catalog()
    cluster_run_id = _select_run_to_process(catalog, config)
    if cluster_run_id is None:
        context.log.info("No unprocessed completed cluster run found; nothing to do.")
        yield Output(
            pl.DataFrame(schema=CLUSTER_SCHEMA), output_name="feedback_cluster"
        )
        yield AssetCheckResult(passed=True, check_name="continuity_floor")
        return

    run_row = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_run"
        )
        .filter(pl.col("cluster_run_id") == cluster_run_id)
        .collect()
        .to_dicts()[0]
    )
    embedding_model_version = run_row["embedding_model_version"]
    embedding_dim = run_row["embedding_dim"]
    embedding_input_filter = run_row["embedding_input_filter"]

    candidates_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_cluster_candidate"
        )
        .filter(
            (pl.col("cluster_run_id") == cluster_run_id)
            & (pl.col("cluster_id") != NOISE_CLUSTER_ID)
        )
        .select(["feedback_conversation_pk", "cluster_id"])
        .collect()
    )
    new_cluster_members = {
        cluster_id: frozenset(group["feedback_conversation_pk"])
        for (cluster_id,), group in candidates_df.group_by("cluster_id")
    }

    active_cluster_members = _active_cluster_members(
        catalog,
        embedding_model_version,
        embedding_dim,
        embedding_input_filter,
        # A run table written before a scope column existed has no such key
        run_row.get("opened_since"),
        platforms_from_run_value(run_row.get("platforms")),
    )
    existing_cluster_rows = _existing_cluster_rows(catalog)

    matches, lineage_rows = match_clusters(
        new_cluster_members, active_cluster_members, config.match_threshold
    )
    lineage_rows.extend(
        {
            "prior_cluster_key": other_config_key,
            "cluster_key": None,
            "cluster_id": None,
            "relation": "retired",
            "jaccard": None,
        }
        for other_config_key in _other_config_active_keys(
            catalog, embedding_model_version, embedding_dim, embedding_input_filter
        )
    )

    # A bootstrap run (no active clusters yet) has every match resolve to 'new' by
    # construction -- that's the expected first run, not a bad configuration, so
    # the floor only applies once there's a prior cluster set to compare against.
    # A key emptied by the run's date range counts as nothing to compare against.
    continuity = (
        1.0
        if not any(active_cluster_members.values())
        else compute_continuity(matches, new_cluster_members)
    )
    yield AssetCheckResult(
        passed=continuity >= CONTINUITY_FLOOR,
        check_name="continuity_floor",
        metadata={
            "continuity": MetadataValue.float(continuity),
            "floor": MetadataValue.float(CONTINUITY_FLOOR),
            "cluster_run_id": MetadataValue.text(cluster_run_id),
        },
    )
    if continuity < CONTINUITY_FLOOR:
        # Fail before writing either output -- feedback_cluster_assignment only
        # rewrites from a run that has lineage rows, so leaving this run
        # unresolved keeps it on the prior, still-good cluster_key set.
        msg = (
            f"Run {cluster_run_id} continuity {continuity:.2f} is below the "
            f"floor {CONTINUITY_FLOOR:.2f}; a configuration change is needed, "
            "not a rerun."
        )
        raise permanent_failure(msg)

    embeddings_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_embeddings"
        )
        .filter(
            (pl.col("embedding_model_version") == embedding_model_version)
            & (pl.col("embedding_dim") == embedding_dim)
            & pl.col("feedback_conversation_pk").is_in(
                candidates_df["feedback_conversation_pk"]
            )
        )
        .select(["feedback_conversation_pk", "embedding_vector"])
        .collect()
    )
    vectors_by_pk = dict(
        zip(
            embeddings_df["feedback_conversation_pk"],
            embeddings_df["embedding_vector"].list.to_array(embedding_dim).to_numpy(),
            strict=True,
        )
    )

    cluster_rows = []
    for match in matches:
        members = new_cluster_members[match.new_cluster_id]
        vectors = np.stack([vectors_by_pk[pk] for pk in members])
        centroid, radius = compute_cluster_stats(vectors)
        existing = existing_cluster_rows.get(match.cluster_key)
        cluster_rows.append(
            {
                "cluster_key": match.cluster_key,
                "centroid": centroid.tolist(),
                "radius": radius,
                "embedding_model_version": embedding_model_version,
                "embedding_dim": embedding_dim,
                "embedding_input_filter": embedding_input_filter,
                "member_count": len(members),
                "cluster_status": "active",
                "first_seen_run_id": existing["first_seen_run_id"]
                if existing
                else cluster_run_id,
                "last_seen_run_id": cluster_run_id,
            }
        )

    # A merged-away old key is absorbed into another cluster's key, same as a
    # retired one -- both need cluster_status='retired' so incremental placement
    # stops assigning new conversations to their now-obsolete centroid.
    resolved_keys = {match.cluster_key for match in matches}
    retired_keys = {
        row["prior_cluster_key"]
        for row in lineage_rows
        if row["relation"] in ("retired", "merged")
    } - resolved_keys
    for cluster_key in retired_keys:
        existing = existing_cluster_rows[cluster_key]
        cluster_rows.append(
            {
                **existing,
                "cluster_status": "retired",
                "last_seen_run_id": cluster_run_id,
            }
        )

    lineage_df = pl.DataFrame(
        [
            {
                "cluster_lineage_pk": cluster_lineage_pk(
                    cluster_run_id, row["prior_cluster_key"], row["cluster_key"]
                ),
                "cluster_run_id": cluster_run_id,
                **row,
            }
            for row in lineage_rows
        ],
        schema=CLUSTER_LINEAGE_SCHEMA,
    )

    context.log.info(
        "Cluster identity for run %s: %d resolved (%s), %d retired",
        cluster_run_id,
        len(matches),
        {
            relation: sum(1 for m in matches if m.relation == relation)
            for relation in {m.relation for m in matches}
        },
        len(retired_keys),
    )
    context.add_output_metadata(
        {
            "cluster_run_id": MetadataValue.text(cluster_run_id),
            "clusters_resolved": MetadataValue.int(len(matches)),
            "clusters_retired": MetadataValue.int(len(retired_keys)),
            "continuity": MetadataValue.float(continuity),
        },
        output_name="feedback_cluster",
    )
    yield Output(
        pl.DataFrame(cluster_rows, schema=CLUSTER_SCHEMA)
        if cluster_rows
        else pl.DataFrame(schema=CLUSTER_SCHEMA),
        output_name="feedback_cluster",
    )
    yield Output(
        lineage_df,
        output_name="feedback_cluster_lineage",
        metadata={"row_count": MetadataValue.int(lineage_df.height)},
    )
    yield Output(
        pl.DataFrame(
            [{"cluster_run_id": cluster_run_id, "processed_at": datetime.now(UTC)}],
            schema=IDENTITY_RUN_SCHEMA,
        ),
        output_name="feedback_cluster_identity_run",
    )
