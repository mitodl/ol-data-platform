from b2b_organization.resources.starrocks import StarRocksResource
from dagster import AssetExecutionContext, AssetKey, asset

# Must match the model names in ol_dbt/models/b2b_analytics/.
MV_NAMES = [
    "mv_b2b_contract_utilization",
    "mv_b2b_enrollment_completion_funnel",
    "mv_b2b_monthly_engagement_trend",
    "mv_b2b_program_funnel",
    "mv_b2b_content_engagement_depth",
    "mv_b2b_mit_admin_contract_health",
]


@asset(
    deps=[AssetKey(["reporting", "organization_administration_report"])],
    group_name="b2b_organization",
    key=AssetKey(["b2b_organization", "starrocks_mv_refresh"]),
)
def refresh_starrocks_analytics_mvs(
    context: AssetExecutionContext, starrocks: StarRocksResource
):
    """Manually refresh the b2b_analytics StarRocks materialized views.

    The MVs are created with refresh_method='manual' (see ol_dbt/models/b2b_analytics)
    since external-catalog MVs can't auto-refresh on base-table changes -- this asset
    is what actually triggers the refresh, gated on the upstream dbt model.
    """
    for mv in MV_NAMES:
        context.log.info("Refreshing %s", mv)
        starrocks.execute(f"REFRESH MATERIALIZED VIEW {mv} WITH SYNC MODE")
        context.log.info("Refreshed %s", mv)
