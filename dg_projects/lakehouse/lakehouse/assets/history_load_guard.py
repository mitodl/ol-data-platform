"""The load-side raw history guard: a check on each guarded raw table.

See ``lakehouse.lib.history_load_guard`` for what is guarded and why. One
multi-asset check per Airbyte group, so the group's ``sync_and_stage_*`` job,
whose selection includes the checks on its assets, runs it once after the
sync with a single dbt invocation. The snapshots are not in that job;
dbt_automation_sensor starts each once its check has passed against the
latest load (``raw_load_check_passed``).

A failure is ERROR severity, which asset_check_failure_sensor reports. The
check is not ``blocking``: a blocking ERROR failure would also stop staging in
the sync's run, and since the group's tables share one check step, one short
table would stop staging for the whole connection. Staging is left as it was
before this check.

An error in the step (a missing raw table, a dbt failure) leaves every table
in the group without a result for its latest load, so all of the group's
snapshots wait. That fails closed, and reaches Sentry through the failure hook
rather than asset_check_failure_sensor.

Only the tables lakehouse loads (Airbyte) are checked here. A table loaded in
another code location (dlt, in data_loading) has no check, and its snapshot's
gate passes with nothing to wait for, so a unit's cutover to dlt drops its
load-side guard until data_loading defines one.
"""

import json
from collections.abc import Iterable, Iterator, Mapping

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetKey,
    Config,
    MetadataValue,
    multi_asset_check,
)
from dagster_dbt import DbtCliResource
from pydantic import Field

from lakehouse.assets.lakehouse.dbt import dbt_credentials, dbt_dev_vars
from lakehouse.lib.history_load_guard import (
    CHECK_NAME,
    GuardedTable,
    LoadCount,
    parse_load_counts,
)
from lakehouse.resources.starrocks import StarRocksResource


class HistoryLoadGuardConfig(Config):
    """Run config for a deliberate short load (a purge, a loader cutover)."""

    allow_short: list[str] = Field(
        default_factory=list,
        description=(
            "Raw tables to pass even when short. Compare the missing keys "
            "with the expected delete count first. The snapshot run still needs "
            "raw_history_allow_deletes for its own guard."
        ),
    )


def _result(
    table: GuardedTable, count: LoadCount, *, allowed: bool
) -> AssetCheckResult:
    return AssetCheckResult(
        check_name=CHECK_NAME,
        asset_key=table.raw_key,
        passed=allowed or not count.short,
        severity=AssetCheckSeverity.ERROR,
        description=count.describe() + (" (allowed for this run)" if allowed else ""),
        metadata={
            "loaded_keys": count.loaded_keys,
            "snapshot_open_records": MetadataValue.int(count.snapshot_open_records)
            if count.snapshot_open_records is not None
            else MetadataValue.null(),
            "missing_keys": count.missing_keys,
            "snapshot": count.snapshot,
            "allowed": allowed,
        },
    )


def _group_check(group: str, tables: list[GuardedTable]) -> AssetChecksDefinition:
    by_key = {table.raw_key: table for table in tables}

    @multi_asset_check(
        name=f"history_load_guard_{group}",
        specs=[
            AssetCheckSpec(
                name=CHECK_NAME,
                asset=table.raw_key,
                description=(
                    f"Fails when the load has fewer keys than {table.snapshot} "
                    "holds open, by more than raw_history_delete_guard allows, "
                    "and holds the snapshot until a load passes."
                ),
            )
            for table in tables
        ],
        can_subset=True,
    )
    def _check(
        context: AssetCheckExecutionContext,
        config: HistoryLoadGuardConfig,
        dbt: DbtCliResource,
        starrocks: StarRocksResource,
    ) -> Iterator[AssetCheckResult]:
        selected = [
            by_key[check_key.asset_key]
            for check_key in sorted(
                context.selected_asset_check_keys,
                key=lambda k: k.asset_key.to_user_string(),
            )
        ]
        args = json.dumps({"tables": [table.macro_arg() for table in selected]})
        with dbt_credentials(starrocks):
            invocation = dbt.cli(
                [
                    "run-operation",
                    "raw_history_load_guard",
                    "--args",
                    args,
                    *dbt_dev_vars(),
                ],
                raise_on_error=True,
            )
        counts = parse_load_counts(
            event.raw_event["info"]["msg"] for event in invocation.stream_raw_events()
        )
        if missing := sorted({t.raw_table for t in selected} - counts.keys()):
            msg = f"raw_history_load_guard reported nothing for {', '.join(missing)}"
            raise RuntimeError(msg)
        for table in selected:
            yield _result(
                table,
                counts[table.raw_table],
                allowed=table.raw_table in config.allow_short,
            )

    return _check


def build_history_load_guard_checks(
    tables: Iterable[GuardedTable], group_by_key: Mapping[AssetKey, str]
) -> list[AssetChecksDefinition]:
    """One check per Airbyte group that loads a guarded raw table.

    :param tables: every guarded raw table (``guarded_tables``).
    :param group_by_key: the group of each asset lakehouse loads, from the
        Airbyte asset definitions.
    :returns: the checks, one per group with at least one guarded table.
    :rtype: list[AssetChecksDefinition]
    """
    grouped: dict[str, list[GuardedTable]] = {}
    for table in tables:
        if (group := group_by_key.get(table.raw_key)) is not None:
            grouped.setdefault(group, []).append(table)
    return [_group_check(group, members) for group, members in sorted(grouped.items())]
