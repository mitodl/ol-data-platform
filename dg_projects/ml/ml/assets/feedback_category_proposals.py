import os
from datetime import UTC, datetime

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MetadataValue,
    asset,
)
from ml.lib.categorize import (
    CATEGORY_PROPOSAL_DOMINANT_TAG_COUNT,
    CATEGORY_PROPOSAL_SAMPLE_SIZE,
    CATEGORY_PROPOSAL_SCHEMA,
    build_category_label_client,
    build_cluster_prompt_inputs,
    propose_categories,
)
from ml.lib.cluster_run_lookup import latest_identity_processed_run
from ml.lib.iceberg_helpers import table_exists
from ml.resources.llm import LLMClientFactory
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from pydantic import Field

if DAGSTER_ENV == "dev":
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    intermediate_database_name = (
        f"ol_warehouse_production_{_schema_suffix}_intermediate"
    )
    dimensional_database_name = f"ol_warehouse_production_{_schema_suffix}_dimensional"
else:
    intermediate_database_name = "ol_warehouse_production_intermediate"
    dimensional_database_name = "ol_warehouse_production_dimensional"


class FeedbackCategoryProposalsConfig(Config):
    sample_size: int | None = Field(
        default=None,
        description=(
            "Representative conversations to sample per cluster for the prompt. "
            "Unset uses CATEGORY_PROPOSAL_SAMPLE_SIZE (ml.lib.categorize)."
        ),
    )
    dominant_tag_count: int | None = Field(
        default=None,
        description=(
            "How many of a cluster's most common existing tags to surface as "
            "context. Unset uses CATEGORY_PROPOSAL_DOMINANT_TAG_COUNT."
        ),
    )
    model_version: str | None = Field(
        default=None,
        description=(
            "Override the model id sent to the anthropic/openai/openai_compatible/"
            "azure_openai client classes. Unset uses CATEGORY_MODEL_VERSION "
            "(ml.lib.categorize). Ignored when the llm resource's client_class is "
            "'bedrock' -- see bedrock_model_version."
        ),
    )
    bedrock_model_version: str | None = Field(
        default=None,
        description=(
            "Same as model_version, but for client_class='bedrock'. Unset uses "
            "BEDROCK_CATEGORY_MODEL_VERSION."
        ),
    )


@asset(
    code_version="feedback_category_proposals_v1",
    group_name="feedback",
    key=AssetKey(["intermediate", "feedback_category_proposal"]),
    deps=[AssetKey(["intermediate", "feedback_cluster_membership"])],
    automation_condition=upstream_or_code_changes(),
    io_manager_key="io_manager",
    pool="feedback_category_proposals",
    metadata={
        "schema": intermediate_database_name,
        # cluster_key alone, not (cluster_run_id, cluster_id): a cluster_key is
        # stable across runs, so re-proposing for it (label drift, retry) replaces
        # its one row rather than accumulating one per run that happened to
        # re-examine it.
        "write_mode": "upsert",
        "upsert_options": {"join_cols": ["cluster_key"]},
        "schema_update_mode": "update",
    },
)
def feedback_category_proposals(
    context: AssetExecutionContext,
    config: FeedbackCategoryProposalsConfig,
    llm: LLMClientFactory,
) -> pl.DataFrame:
    """
    Propose a category label for every active cluster_key that doesn't have one
    yet.

    A cluster_key that already has a proposal row is never re-proposed -- only a
    genuinely new/split/merged key, or one an earlier LLM call failed for, costs a
    call. Samples representative conversation
    text per cluster_key (feedback_cluster_membership + int__feedback__conversation)
    and each cluster's dominant existing tag category (afact_feedback_conversation.
    category_fk, resolved via dim_feedback_category) as prompt context. Output is
    category_source='llm_discovered', category_status='proposed' by construction --
    populated onto afact_feedback_conversation immediately, not gated on human
    approval; approval is a correction a human applies afterward, not a gate
    beforehand.
    """
    catalog = get_glue_catalog()
    cluster_run_id = latest_identity_processed_run(catalog, intermediate_database_name)
    if cluster_run_id is None:
        context.log.info(
            "No identity-matched clustering run found; nothing to propose."
        )
        return pl.DataFrame(schema=dict(CATEGORY_PROPOSAL_SCHEMA))

    # Every currently-active cluster_key without a proposal row needs one --
    # not just the latest run's new/split/merged keys. A key minted by an
    # earlier run whose LLM call failed (propose_categories swallows the
    # failure) never becomes 'new' again once a later run continues it, so
    # scoping to the latest run's lineage would silently strand it on the
    # tag-seed fallback forever. A key still 'active' with no proposal row
    # needs one regardless of which run minted it or last continued it.
    active_cluster_keys = (
        get_dbt_model_as_dataframe(
            database_name=intermediate_database_name, table_name="feedback_cluster"
        )
        .filter(pl.col("cluster_status") == "active")
        .select("cluster_key")
        .unique()
        .collect()["cluster_key"]
        .to_list()
    )
    already_proposed: set[str] = set()
    if table_exists(
        catalog, f"{intermediate_database_name}.feedback_category_proposal"
    ):
        already_proposed = set(
            get_dbt_model_as_dataframe(
                database_name=intermediate_database_name,
                table_name="feedback_category_proposal",
            )
            .select("cluster_key")
            .unique()
            .collect()["cluster_key"]
        )
    cluster_keys_needing_proposal = [
        key for key in active_cluster_keys if key not in already_proposed
    ]
    if not cluster_keys_needing_proposal:
        context.log.info(
            "Every active cluster_key already has a proposal as of run %s.",
            cluster_run_id,
        )
        return pl.DataFrame(schema=dict(CATEGORY_PROPOSAL_SCHEMA))

    membership_df = (
        get_dbt_model_as_dataframe(
            database_name=intermediate_database_name,
            table_name="feedback_cluster_membership",
        )
        .filter(pl.col("cluster_key").is_in(cluster_keys_needing_proposal))
        .select(["feedback_conversation_pk", "cluster_key"])
        .collect()
    )

    category_df = (
        get_dbt_model_as_dataframe(
            database_name=dimensional_database_name,
            table_name="dim_feedback_category",
        )
        .select(["feedback_category_pk", "category_label"])
        .collect()
    )
    member_pks = membership_df["feedback_conversation_pk"]
    # Filtered to just this batch's cluster members before collect() -- the
    # corpus (~198K conversations) is much larger than the handful of clusters
    # needing a proposal here.
    afact_df = (
        get_dbt_model_as_dataframe(
            database_name=dimensional_database_name,
            table_name="afact_feedback_conversation",
        )
        .filter(pl.col("feedback_conversation_pk").is_in(member_pks))
        .select(["feedback_conversation_pk", "category_fk"])
        .collect()
    )
    conversation_df = (
        get_dbt_model_as_dataframe(
            database_name=intermediate_database_name,
            table_name="int__feedback__conversation",
        )
        .filter(pl.col("feedback_conversation_pk").is_in(member_pks))
        .select(["feedback_conversation_pk", "conversation_text"])
        .collect()
    )

    joined = (
        membership_df.join(afact_df, on="feedback_conversation_pk", how="left")
        .join(
            category_df,
            left_on="category_fk",
            right_on="feedback_category_pk",
            how="left",
        )
        .join(conversation_df, on="feedback_conversation_pk", how="left")
        .select(["cluster_key", "conversation_text", "category_label"])
    )

    cluster_prompt_inputs = build_cluster_prompt_inputs(
        joined,
        sample_size=config.sample_size or CATEGORY_PROPOSAL_SAMPLE_SIZE,
        dominant_tag_count=config.dominant_tag_count
        or CATEGORY_PROPOSAL_DOMINANT_TAG_COUNT,
    )

    client = build_category_label_client(
        llm, config.model_version, config.bedrock_model_version
    )
    proposals_df = propose_categories(cluster_prompt_inputs, client, cluster_run_id)

    context.log.info(
        "Proposed %d/%d cluster categories for run %s",
        proposals_df.height,
        len(cluster_prompt_inputs),
        cluster_run_id,
    )
    context.add_output_metadata(
        {
            "cluster_run_id": MetadataValue.text(cluster_run_id),
            "clusters_proposed": MetadataValue.int(proposals_df.height),
            "clusters_needing_proposal": MetadataValue.int(
                len(cluster_keys_needing_proposal)
            ),
            "model_version": MetadataValue.text(client.model_version),
        }
    )
    # The caller stamps wall-clock time, not the pure function -- same convention
    # as feedback_clusters.py's run_at. Unconditional, not gated on height: an
    # empty result still needs every CATEGORY_PROPOSAL_SCHEMA column present for
    # the cast below to succeed.
    proposals_df = proposals_df.with_columns(
        pl.lit(datetime.now(tz=UTC), dtype=pl.Datetime(time_zone="UTC")).alias(
            "proposed_at"
        )
    )
    return proposals_df.cast(CATEGORY_PROPOSAL_SCHEMA)
