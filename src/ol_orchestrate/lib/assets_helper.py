from dagster import AssetsDefinition, PartitionsDefinition


def late_bind_partition_to_asset(
    asset_def: AssetsDefinition, partition_def: PartitionsDefinition
) -> AssetsDefinition:
    asset_def._partitions_def = partition_def  # noqa: SLF001
    return asset_def


def add_prefix_to_asset_keys(
    asset_def: AssetsDefinition, asset_key_prefix: str
) -> AssetsDefinition:
    in_key_map = {
        dep_key: dep_key.with_prefix(asset_key_prefix)
        for dep_key in asset_def.dependency_keys
    }
    out_key_map = {
        out_key: out_key.with_prefix(asset_key_prefix) for out_key in asset_def.keys
    }

    return asset_def.with_attributes(
        output_asset_key_replacements=out_key_map,
        input_asset_key_replacements=in_key_map,
    )
