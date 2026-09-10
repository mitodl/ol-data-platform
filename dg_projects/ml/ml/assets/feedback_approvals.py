"""Human approval/promotion decisions, entered via Dagster's Launchpad.

Each asset here has no upstream deps -- it is materialized ad hoc when a human
fills in its Config in the Dagster UI and clicks Materialize, appending one
decision row to an append-only log. dbt resolves the *latest* decision per key
(category_slug / cluster_run_id), so re-materializing to correct a decision is
just another row, not an update.
"""

import os
from datetime import UTC, datetime

import polars as pl
from dagster import AssetExecutionContext, AssetKey, Config, Failure, asset
from ol_orchestrate.lib.constants import DAGSTER_ENV
from pydantic import Field

if DAGSTER_ENV == "dev":
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    database_name = f"ol_warehouse_production_{_schema_suffix}_intermediate"
else:
    database_name = "ol_warehouse_production_intermediate"

CATEGORY_STATUSES = {"approved", "merged", "deprecated"}
RUN_STATUSES = {"promoted", "superseded"}


class CategoryDecision(Config):
    category_slug: str = Field(
        description="The dim_feedback_category.category_slug being decided on."
    )
    category_status: str = Field(
        description="The decision: approved, merged or deprecated."
    )


class CategoryApprovalConfig(Config):
    approved_by: str = Field(
        description="Who is making these decisions -- applies to every entry in "
        "decisions, since one materialization is one person's review pass."
    )
    decisions: list[CategoryDecision] = Field(
        description="One or more (category_slug, category_status) decisions -- "
        "batch a whole review pass into a single materialization rather than "
        "one Launchpad run per category."
    )


class ClusterRunPromotionConfig(Config):
    cluster_run_id: str = Field(
        description="The feedback_cluster_run.cluster_run_id being decided on."
    )
    run_status: str = Field(description="The decision: promoted or superseded.")
    promoted_by: str = Field(description="Who is making this decision.")


@asset(
    key=AssetKey(["intermediate", "feedback_category_approval"]),
    group_name="feedback",
    io_manager_key="io_manager",
    metadata={"schema": database_name, "write_mode": "append"},
)
def feedback_category_approval(
    context: AssetExecutionContext, config: CategoryApprovalConfig
) -> pl.DataFrame:
    if not config.decisions:
        msg = "decisions must have at least one entry."
        raise Failure(msg)
    invalid = sorted({d.category_status for d in config.decisions} - CATEGORY_STATUSES)
    if invalid:
        msg = (
            f"category_status must be one of {sorted(CATEGORY_STATUSES)}, got "
            f"invalid value(s): {invalid}"
        )
        raise Failure(msg)
    approved_at = datetime.now(tz=UTC)
    for decision in config.decisions:
        context.log.info(
            "Recording category_status=%s for category_slug=%s (by %s)",
            decision.category_status,
            decision.category_slug,
            config.approved_by,
        )
    return pl.DataFrame(
        [
            {
                "category_slug": decision.category_slug,
                "category_status": decision.category_status,
                "approved_by": config.approved_by,
                "approved_at": approved_at,
            }
            for decision in config.decisions
        ]
    )


@asset(
    key=AssetKey(["intermediate", "feedback_cluster_run_promotion"]),
    group_name="feedback",
    io_manager_key="io_manager",
    metadata={"schema": database_name, "write_mode": "append"},
)
def feedback_cluster_run_promotion(
    context: AssetExecutionContext, config: ClusterRunPromotionConfig
) -> pl.DataFrame:
    if config.run_status not in RUN_STATUSES:
        msg = (
            f"run_status must be one of {sorted(RUN_STATUSES)}, got "
            f"{config.run_status!r}"
        )
        raise Failure(msg)
    context.log.info(
        "Recording run_status=%s for cluster_run_id=%s (by %s)",
        config.run_status,
        config.cluster_run_id,
        config.promoted_by,
    )
    return pl.DataFrame(
        [
            {
                "cluster_run_id": config.cluster_run_id,
                "run_status": config.run_status,
                "promoted_by": config.promoted_by,
                "promoted_at": datetime.now(tz=UTC),
            }
        ]
    )
