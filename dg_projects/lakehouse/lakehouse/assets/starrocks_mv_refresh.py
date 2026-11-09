import json
from collections.abc import Iterator

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
)

from lakehouse.assets.lakehouse.dbt_starrocks import (
    starrocks_dbt_assets,
    starrocks_dbt_project,
)
from lakehouse.lib.starrocks_dbt import (
    MaterializedViewRefreshError,
    change_tracked_views,
    duplicated_grains,
    materialized_view_relations,
    refresh_materialized_views,
    stamp_change_log,
    view_grains,
)
from lakehouse.resources.starrocks import StarRocksResource

REFRESH_ASSET_KEY = AssetKey(["b2b_analytics", "starrocks_mv_refresh"])
GRAIN_CHECK = "grains_are_unique"


@asset(
    # Depend on the asset that actually builds these tables against StarRocks,
    # not a Trino-side mart -- the two engines have no materialization
    # relationship to each other.
    deps=[starrocks_dbt_assets],
    group_name="b2b_analytics",
    key=REFRESH_ASSET_KEY,
    check_specs=[
        AssetCheckSpec(
            GRAIN_CHECK,
            asset=REFRESH_ASSET_KEY,
            description=(
                "No refreshed StarRocks MV holds two rows for one key of its "
                "dbt_utils.unique_combination_of_columns test."
            ),
        )
    ],
)
def refresh_starrocks_analytics_mvs(
    context: AssetExecutionContext, starrocks: StarRocksResource
) -> Iterator[AssetCheckResult | MaterializeResult]:
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

    Each MV's grain test then runs against the rows the refresh loaded (the
    `grains_are_unique` check). The dbt tests on these models stay severity:
    warn: they run inside starrocks_dbt_assets, before the refresh, so they
    read the previous refresh's rows, and an error there would skip the
    refresh of every MV. The check gates nothing; a failure reaches Slack
    through asset_check_failure_sensor, which reports ERROR severity only. It
    is part of this asset rather than a separate asset check because a check
    on a failed asset never runs, and one MV failing its refresh fails the
    asset after the rest have refreshed.
    """
    manifest = json.loads(starrocks_dbt_project.manifest_path.read_text())
    tracked = {view.relation: view for view in change_tracked_views(manifest)}

    def stamp(relation: str) -> None:
        if relation in tracked:
            stamp_change_log(tracked[relation], starrocks.execute, log=context.log)

    refresh_error = None
    try:
        refresh_materialized_views(
            materialized_view_relations(manifest),
            starrocks.execute,
            log=context.log,
            after_refresh=stamp,
        )
    except MaterializedViewRefreshError as exc:
        refresh_error = exc

    failed = refresh_error.failures if refresh_error else {}
    grains = [g for g in view_grains(manifest) if g.relation not in failed]
    duplicates = duplicated_grains(grains, starrocks.fetch)
    for relation, counts in duplicates.items():
        context.log.error("%s has duplicate keys: %s", relation, counts)
    yield AssetCheckResult(
        check_name=GRAIN_CHECK,
        passed=not duplicates,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "checked": MetadataValue.json(sorted({g.relation for g in grains})),
            # A failed change-log stamp lands in the same failures as a failed
            # REFRESH, so an MV listed here may have refreshed.
            "skipped_refresh_or_stamp_failed": MetadataValue.json(sorted(failed)),
            "duplicates": MetadataValue.json(duplicates),
        },
    )
    if refresh_error:
        raise refresh_error
    yield MaterializeResult()
