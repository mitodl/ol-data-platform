from functools import partial
from typing import Optional

from dagster import (
    AssetExecutionContext,
    AssetRecordsFilter,
    AssetsDefinition,
    AssetSpec,
    Output,
    PartitionsDefinition,
)


def late_bind_partition_to_spec(
    partition_def: PartitionsDefinition, asset_spec: AssetSpec
) -> AssetSpec:
    return asset_spec.replace_attributes(partitions_def=partition_def)


def late_bind_partition_to_asset(
    asset_def: AssetsDefinition, partition_def: PartitionsDefinition
) -> AssetsDefinition:
    return asset_def.map_asset_specs(
        partial(late_bind_partition_to_spec, partition_def)
    )


def add_prefix_to_asset_keys(
    asset_def: AssetsDefinition, asset_key_prefix: str
) -> AssetsDefinition:
    key_map = {
        dep_key: dep_key.with_prefix(asset_key_prefix)
        for dep_key in asset_def.dependency_keys
    } | {out_key: out_key.with_prefix(asset_key_prefix) for out_key in asset_def.keys}
    return asset_def.with_attributes(
        asset_key_replacements=key_map,
    )


def get_last_materialized_partition(
    context: AssetExecutionContext, course_key: str
) -> Optional[Output]:
    try:
        return (
            context.instance.fetch_materializations(
                records_filter=AssetRecordsFilter(
                    asset_key=context.asset_key,
                    asset_partitions=context.partition_key,
                ),
                limit=1,
            )
            .records[0]
            .asset_materialization
        )
    except IndexError:
        context.log.exception(
            "No previous materialization found for course %s", course_key
        )
        return None
