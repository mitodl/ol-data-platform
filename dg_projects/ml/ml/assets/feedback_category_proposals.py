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
from ml.resources.llm import LLMClientFactory
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
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
    deps=[AssetKey(["dimensional", "afact_feedback_conversation"])],
    io_manager_key="io_manager",
    pool="feedback_category_proposals",
    metadata={"schema": intermediate_database_name, "write_mode": "append"},
)
def feedback_category_proposals(
    context: AssetExecutionContext,
    config: FeedbackCategoryProposalsConfig,
    llm: LLMClientFactory,
) -> pl.DataFrame:
    """
    Propose a category label per cluster via one LLM call each.

    Reads the promoted clustering run's cluster_id assignments off
    afact_feedback_conversation (int__feedback__cluster_assignment resolves which
    run that is), samples representative conversations + each cluster's dominant
    existing tag per cluster, and asks an LLM to propose a category_label. Output
    is category_source='llm_discovered', category_status='proposed' by
    construction -- a human approves via feedback_category_approval before it's
    ever assigned to a conversation.
    """
    afact_lazy = get_dbt_model_as_dataframe(
        database_name=dimensional_database_name,
        table_name="afact_feedback_conversation",
    ).filter(pl.col("cluster_id").is_not_null())
    afact_df = afact_lazy.select(
        ["feedback_conversation_pk", "cluster_run_id", "cluster_id", "category_fk"]
    ).collect()

    if afact_df.height == 0:
        context.log.info(
            "No promoted cluster assignments on afact_feedback_conversation yet; "
            "nothing to propose categories for."
        )
        return pl.DataFrame(schema=dict(CATEGORY_PROPOSAL_SCHEMA))

    cluster_run_id = afact_df["cluster_run_id"].drop_nulls().unique().to_list()
    if len(cluster_run_id) != 1:
        msg = (
            "Expected exactly one promoted cluster_run_id on "
            f"afact_feedback_conversation, found {cluster_run_id}."
        )
        raise ValueError(msg)
    cluster_run_id = cluster_run_id[0]

    category_df = (
        get_dbt_model_as_dataframe(
            database_name=dimensional_database_name,
            table_name="dim_feedback_category",
        )
        .select(["feedback_category_pk", "category_label"])
        .collect()
    )

    conversation_df = (
        get_dbt_model_as_dataframe(
            database_name=intermediate_database_name,
            table_name="int__feedback__conversation",
        )
        .select(["feedback_conversation_pk", "conversation_text"])
        .collect()
    )

    joined = (
        afact_df.join(
            category_df,
            left_on="category_fk",
            right_on="feedback_category_pk",
            how="left",
        )
        .join(conversation_df, on="feedback_conversation_pk", how="left")
        .select(["cluster_id", "conversation_text", "category_label"])
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
            "clusters_total": MetadataValue.int(len(cluster_prompt_inputs)),
            "model_version": MetadataValue.text(client.model_version),
        }
    )
    if proposals_df.height:
        # The caller stamps wall-clock time, not the pure function -- same
        # convention as feedback_clustering.py's run_at.
        proposals_df = proposals_df.with_columns(
            pl.lit(datetime.now(tz=UTC)).alias("proposed_at")
        )
    return proposals_df.cast(CATEGORY_PROPOSAL_SCHEMA)
