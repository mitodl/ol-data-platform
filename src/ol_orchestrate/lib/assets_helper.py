from functools import partial

from dagster import AssetsDefinition, AssetSpec, PartitionsDefinition


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
