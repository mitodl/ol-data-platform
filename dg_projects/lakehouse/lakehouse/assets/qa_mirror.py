"""Copy the singleton units' production raw tables into QA -- RFC 12711 step 6.

A singleton (edxorg, emeritus, global_alumni, irx, salesforce, zendesk) has no
QA deployment to ingest from, so the QA lake holds a masked copy of production
instead. One asset per unit; each rebuilds its tables with the CTAS that
``ol_dbt_cli.lib.qa_mirror`` renders from the inventory's ``mirror`` blocks.

These run from production. The QA StarRocks role is explicitly denied every
production Glue resource (ol-infrastructure's cross_environment_glue_denial),
so QA cannot read what it would be copying. The production role can read
production and write QA, which keeps the copy flowing one way: production pushes
an allowlisted subset out and QA never gets to read production.

Refreshed monthly by qa_mirror_monthly_schedule, and by hand whenever someone
needs a newer copy (QA_DATA_TOPOLOGY_SPEC.md §1). Each asset also carries a
check that renders and plans every table's copy against production without
running it, scheduled daily, so a production column dropped or renamed out from
under an allowlist fails within a day rather than at the next refresh. QA's
qa_observation_drift fails on a copy older than its unit's mirror_max_age_days.
"""

from typing import Any

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
)
from ol_dbt_cli.lib.inventory import load_units
from ol_dbt_cli.lib.qa_mirror import (
    MirrorStatement,
    MirrorTable,
    mirror_tables,
    render_mirror,
)

from lakehouse.lib.inventory import INVENTORY_DIR
from lakehouse.resources.starrocks import StarRocksResource


def _render(
    context: AssetExecutionContext | AssetCheckExecutionContext,
    starrocks: StarRocksResource,
    table: MirrorTable,
) -> MirrorStatement:
    # Every identifier here comes from the inventory, quoted by lib.qa_mirror.
    described = starrocks.fetch(f"DESCRIBE {table.source}")
    statement = render_mirror(table, {row["Field"]: row["Type"] for row in described})
    if statement.dropped:
        context.log.info(
            "%s: not copying %s (not in the allowlist)",
            table.name,
            ", ".join(statement.dropped),
        )
    # render_mirror compares `mirror.columns` with DESCRIBE, and rejects a
    # `mirror.where` that is a statement rather than a predicate, but it never
    # resolves the predicate's own columns. So a column production has renamed
    # that the `where` alone names -- including inside its `{source}` subquery
    # -- reached StarRocks for the first time in the CTAS, after this table's
    # DROP, leaving the unit half refreshed. EXPLAIN resolves it here instead,
    # while every QA copy in the unit is still intact.
    #
    # What it resolves is names and function signatures. An expression
    # StarRocks has an implicit cast for is planned, not rejected: varchar
    # arithmetic and `date_add` on a varchar both plan, so a `where` wrong in
    # that way still fails from the CTAS. Measured on QA 2026-09-21, recorded
    # in QA_DATA_TOPOLOGY_SPEC.md §8.
    context.log.info("%s: planning the copy", table.name)
    starrocks.fetch(f"EXPLAIN {statement.select}")
    return statement


def _copy(
    context: AssetExecutionContext,
    starrocks: StarRocksResource,
    table: MirrorTable,
    statement: MirrorStatement,
) -> dict[str, Any]:
    # Drop-then-create rather than a swap: StarRocks cannot rename an Iceberg
    # table. FORCE deletes the old copy's data files, which would otherwise be
    # orphaned in the QA bucket on every refresh.
    starrocks.execute(f"DROP TABLE IF EXISTS {table.target} FORCE")
    starrocks.execute(statement.sql, idempotent=False)
    count_sql = f"SELECT count(*) AS row_count FROM {table.target}"  # noqa: S608
    rows = int(starrocks.fetch(count_sql)[0]["row_count"])
    context.log.info("%s: copied %s rows", table.name, rows)
    return {
        "rows": rows,
        "masked": {
            column: mode for column, mode in table.columns.items() if mode != "copy"
        },
        "dropped": statement.dropped,
        "where": table.where,
    }


def _asset_key(unit: str) -> AssetKey:
    return AssetKey(["qa_mirror", *unit.split("/")])


def _mirror_asset(unit: str, tables: list[MirrorTable]) -> AssetsDefinition:
    deployment, layer = unit.split("/")

    @asset(
        key=_asset_key(unit),
        group_name="qa_mirror",
        description=(
            f"Masked copy of {unit}'s production raw tables into the QA lake. "
            "Refreshed monthly; see RFC 12711 step 6."
        ),
        kinds={"starrocks", "iceberg"},
        # Per unit, because the refresh is DROP-then-CTAS with no swap: two
        # runs of the SAME unit interleave into a dropped table under a live
        # CTAS, or a row count taken over the other run's copy. Different units
        # touch disjoint tables, so they still run in parallel -- a shared pool
        # would queue emeritus behind the 760 GB program_learner_report.
        #
        # As elsewhere in this repo (openedx_course_export), naming the pool
        # only makes the limit *settable*. Until `qa_mirror_<deployment>_<layer>`
        # has a slot limit of 1 on the instance (Deployment -> Concurrency),
        # these runs are still unbounded: the instance config sets no
        # concurrency.pools.default_limit, so an unlimited pool is the default.
        pool=f"qa_mirror_{deployment}_{layer}",
    )
    def _mirror(
        context: AssetExecutionContext, starrocks: StarRocksResource
    ) -> MaterializeResult:
        # Every declaration is checked against production before any QA table
        # is dropped, so a stale one fails the run with the whole unit intact
        # rather than half refreshed.
        statements = [_render(context, starrocks, table) for table in tables]
        copied = {
            table.name: _copy(context, starrocks, table, statement)
            for table, statement in zip(tables, statements, strict=True)
        }
        return MaterializeResult(
            metadata={
                "tables": MetadataValue.json(copied),
                "total_rows": sum(entry["rows"] for entry in copied.values()),
            }
        )

    return _mirror


def _plan_check(unit: str, tables: list[MirrorTable]) -> AssetChecksDefinition:
    @asset_check(
        asset=_asset_key(unit),
        name="copy_plans_against_production",
        description=(
            f"Render and EXPLAIN each of {unit}'s copies against production "
            "without copying anything."
        ),
    )
    def _plans(
        context: AssetCheckExecutionContext, starrocks: StarRocksResource
    ) -> AssetCheckResult:
        # A declaration production no longer fits raises from _render rather
        # than returning passed=False: the Sentry hook fires on a failed step,
        # and a failed check result is only visible to someone looking at it.
        statements = {
            table.name: _render(context, starrocks, table) for table in tables
        }
        return AssetCheckResult(
            passed=True,
            metadata={
                "dropped": MetadataValue.json(
                    {name: statement.dropped for name, statement in statements.items()}
                )
            },
        )

    return _plans


@asset(
    key=AssetKey(["qa_mirror", "inventory_missing"]),
    group_name="qa_mirror",
    description="Stands in for the mirror assets when the inventory is missing.",
)
def qa_mirror_inventory_missing() -> None:
    msg = f"No inventory units found under {INVENTORY_DIR}, so no QA mirror is defined."
    raise Failure(description=msg)


def build_qa_mirror_assets() -> list[AssetsDefinition]:
    """One asset per unit the inventory mirrors that declares at least one table.

    A missing inventory yields one asset that fails naming the path, rather than
    no assets at all, which would look like a healthy code location with every
    mirror gone. It is not raised here: failing at import would take the whole
    code location down (see lakehouse/lib/inventory.py).
    """
    units = load_units(INVENTORY_DIR)
    if not units:
        return [qa_mirror_inventory_missing]
    return [
        _mirror_asset(unit, tables) for unit, tables in mirror_tables(units).items()
    ]


def build_qa_mirror_checks() -> list[AssetChecksDefinition]:
    """One copy-planning check per asset :func:`build_qa_mirror_assets` builds."""
    return [
        _plan_check(unit, tables)
        for unit, tables in mirror_tables(load_units(INVENTORY_DIR)).items()
    ]
