"""Airbyte connection assets, with a check that fails an empty full refresh.

A full refresh stream that reads no rows succeeds in Airbyte and materializes in
Dagster. Global Alumni's api_enrollments (full refresh, append) read nothing
every night from 2026-05-04 with every run green, and its raw table kept the
rows it held before. The check here fails that sync the first night it happens.

It fails only when the table held rows when the sync started. A source with
nothing in it (e.g. a disused Open edX feature's table) reads nothing every
night and loses nothing by it, so its stream passes. Reading the table as of the
sync's start rather than now also catches an overwrite stream whose empty sync
emptied its table. Incremental streams are not evaluated: reading nothing new is
what most of their syncs do.
"""

from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from datetime import datetime
from typing import Any

from dagster import (
    AssetCheckKey,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetsDefinition,
    AssetSpec,
    MaterializeResult,
    multi_asset,
)
from dagster_airbyte import DagsterAirbyteTranslator
from dagster_airbyte.translator import AirbyteConnection, AirbyteMetadataSet
from dagster_airbyte.utils import clean_name
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from pyiceberg.exceptions import NoSuchTableError

from lakehouse.resources.airbyte import AirbyteOSSWorkspace, StreamSync

EMPTY_FULL_REFRESH_CHECK = "full_refresh_read_rows"
FULL_REFRESH_SYNC_MODE = "full_refresh"

DestinationRecordCount = Callable[[AssetKey, datetime], int]


def raw_table_record_count(database: str, asset_key: AssetKey, as_of: datetime) -> int:
    """Count the rows a stream's raw Iceberg table held at a point in time.

    The asset key's last part is the table name the destination writes (the
    connection's prefix plus the stream name). A table that did not exist, or
    had no snapshot, held no rows.

    :param database: The Glue database the Airbyte destination writes to.
    :param asset_key: The stream's asset key.
    :param as_of: When to count the rows, e.g. when the sync started.
    :returns: The ``total-records`` of the snapshot current at *as_of*.
    """
    try:
        table = get_glue_catalog().load_table(f"{database}.{asset_key.path[-1]}")
    except NoSuchTableError:
        return 0
    snapshot = table.snapshot_as_of_timestamp(int(as_of.timestamp() * 1000))
    return int(snapshot.summary["total-records"]) if snapshot else 0


def empty_full_refresh_check(
    stream: StreamSync, destination_record_count: DestinationRecordCount
) -> AssetCheckResult | None:
    """Fail a full refresh stream that read no rows into a table that held some.

    :param stream: What the stream read in the sync.
    :param destination_record_count: Counts the rows the stream's table held
        when the sync started. Only called when the stream read nothing.
    :returns: The check result, or None for a stream that is not a full refresh.
    """
    if stream.sync_mode != FULL_REFRESH_SYNC_MODE:
        return None
    if stream.records_emitted:
        return AssetCheckResult(
            asset_key=stream.asset_key,
            check_name=EMPTY_FULL_REFRESH_CHECK,
            passed=True,
            metadata={"records_emitted": stream.records_emitted},
        )
    held = destination_record_count(stream.asset_key, stream.job_started_at)
    return AssetCheckResult(
        asset_key=stream.asset_key,
        check_name=EMPTY_FULL_REFRESH_CHECK,
        passed=held == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"records_emitted": 0, "destination_records": held},
        description=(
            f"Airbyte job {stream.job_id} read no rows for this full refresh "
            f"stream, though its table held {held} rows when the sync started. "
            "The table was not refreshed. Check the source and the connection."
            if held
            else "The stream read no rows, and its table held none."
        ),
    )


def _connection_assets(
    connection_name: str,
    specs: Sequence[AssetSpec],
    destination_record_count: DestinationRecordCount,
) -> AssetsDefinition:
    @multi_asset(
        name=f"airbyte_{clean_name(connection_name)}",
        can_subset=True,
        specs=specs,
        check_specs=[
            AssetCheckSpec(
                EMPTY_FULL_REFRESH_CHECK,
                asset=spec.key,
                description=(
                    "Fails when a full refresh sync reads no rows though the "
                    "table held rows when the sync started. Not evaluated for "
                    "incremental streams."
                ),
            )
            for spec in specs
        ],
    )
    def _assets(
        context: AssetExecutionContext, airbyte: AirbyteOSSWorkspace
    ) -> Iterator[MaterializeResult[Any] | AssetMaterialization | AssetCheckResult]:
        materialized = set()
        for stream in airbyte.sync_streams(context=context):
            if stream.asset_key not in context.selected_asset_keys:
                context.log.warning(
                    "An unexpected asset was materialized: %s. Yielding a "
                    "materialization event.",
                    stream.asset_key,
                )
                yield AssetMaterialization(
                    asset_key=stream.asset_key,
                    description=stream.description,
                    metadata=stream.metadata,
                )
                continue
            yield MaterializeResult(
                asset_key=stream.asset_key, metadata=stream.metadata
            )
            materialized.add(stream.asset_key)
            check_key = AssetCheckKey(stream.asset_key, EMPTY_FULL_REFRESH_CHECK)
            if check_key in context.selected_asset_check_keys and (
                check := empty_full_refresh_check(stream, destination_record_count)
            ):
                yield check
        if unmaterialized := context.selected_asset_keys - materialized:
            context.log.warning("Assets were not materialized: %s", unmaterialized)

    return _assets


def build_airbyte_assets(
    workspace: AirbyteOSSWorkspace,
    dagster_airbyte_translator: DagsterAirbyteTranslator,
    connection_selector_fn: Callable[[AirbyteConnection], bool],
    destination_record_count: DestinationRecordCount,
) -> list[AssetsDefinition]:
    """Build one multi-asset per Airbyte connection, each stream checked.

    This is ``build_airbyte_assets_definitions`` with check specs, which that
    function and the ``airbyte_assets`` decorator have no parameter for. The
    multi-asset names match the library's, so jobs and run history carry over.
    The check is declared on every stream because the sync mode is not in the
    workspace data the asset specs are built from; it is read from the
    connection when the sync finishes.

    :param workspace: The Airbyte workspace to load connections from.
    :param dagster_airbyte_translator: Translates each stream into an asset spec.
    :param connection_selector_fn: Selects the connections to build assets for.
    :param destination_record_count: Counts the rows a stream's table held at a
        point in time.
    :returns: One assets definition per selected connection.
    """
    specs_by_connection: dict[tuple[str, str], list[AssetSpec]] = defaultdict(list)
    for spec in workspace.load_asset_specs(
        dagster_airbyte_translator=dagster_airbyte_translator,
        connection_selector_fn=connection_selector_fn,
    ):
        connection = AirbyteMetadataSet.extract(spec.metadata)
        specs_by_connection[
            connection.connection_id, connection.connection_name
        ].append(spec)
    return [
        _connection_assets(name, specs, destination_record_count)
        for (_, name), specs in specs_by_connection.items()
    ]
