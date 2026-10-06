import json

from dagster import AssetExecutionContext, AssetKey, asset

from lakehouse.assets.lakehouse.dbt_starrocks import (
    starrocks_dbt_assets,
    starrocks_dbt_project,
)
from lakehouse.lib.starrocks_dbt import (
    change_tracked_views,
    materialized_view_relations,
    refresh_materialized_views,
    stamp_change_log,
)
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

    An MV that sets `meta.change_tracking` has its change log stamped straight
    after its own refresh; see `stamp_change_log` for why it has to follow the
    refresh, and why an MV whose refresh failed is left out.
    """
    manifest = json.loads(starrocks_dbt_project.manifest_path.read_text())
    tracked = {view.relation: view for view in change_tracked_views(manifest)}

    def stamp(relation: str) -> None:
        if relation in tracked:
            stamp_change_log(tracked[relation], starrocks.execute, log=context.log)

    refresh_materialized_views(
        materialized_view_relations(manifest),
        starrocks.execute,
        log=context.log,
        after_refresh=stamp,
    )
