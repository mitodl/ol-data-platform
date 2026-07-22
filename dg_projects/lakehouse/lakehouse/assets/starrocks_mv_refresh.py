import json

from dagster import AssetExecutionContext, AssetKey, asset

from lakehouse.assets.lakehouse.dbt_starrocks import (
    starrocks_dbt_assets,
    starrocks_dbt_project,
)
from lakehouse.lib.starrocks_dbt import materialized_view_relations
from lakehouse.resources.starrocks import StarRocksResource


@asset(
    # Depend on the asset that actually builds these tables against StarRocks,
    # not a Trino-side mart -- the two engines have no materialization
    # relationship to each other.
    deps=[starrocks_dbt_assets],
    group_name="b2b_analytics",
    key=AssetKey(["b2b_analytics", "starrocks_mv_refresh"]),
)
def refresh_starrocks_analytics_mvs(
    context: AssetExecutionContext, starrocks: StarRocksResource
):
    """Manually refresh the b2b_analytics StarRocks materialized views.

    The MVs are created with refresh_method='manual' (see ol_dbt/models/b2b_analytics)
    since external-catalog MVs can't auto-refresh on base-table changes -- this asset
    is what actually triggers the refresh, gated on the upstream dbt model.

    Which MVs exist, and what schema they live in, both come from the same dbt
    manifest that built them. Statements are schema-qualified rather than relying
    on the connection's default database: the resource connects to
    `b2b_analytics`, and an unqualified REFRESH silently means "wherever this
    session happens to point", which is how a dbt-side schema change turned into
    `Can not find materialized view` at runtime.
    """
    manifest = json.loads(starrocks_dbt_project.manifest_path.read_text())
    for relation in materialized_view_relations(manifest):
        context.log.info("Refreshing %s", relation)
        starrocks.execute(f"REFRESH MATERIALIZED VIEW {relation} WITH SYNC MODE")
        context.log.info("Refreshed %s", relation)
