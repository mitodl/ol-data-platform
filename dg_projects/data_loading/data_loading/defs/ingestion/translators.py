"""Dagster dlt translators mapping ol_dlt resources to raw-data asset specs.

These centralize the conventions that used to be spread across six per-source
``defs.yaml`` files: the ``ol_warehouse_raw_data`` key prefix, the ``dlt`` +
storage kinds, and the Glue fully-qualified ``table_name`` metadata.
"""

from typing import Any

from dagster import AssetDep, AssetKey, AssetSpec
from dagster._core.definitions.metadata.metadata_set import TableMetadataSet
from dagster_dlt import DagsterDltTranslator
from dagster_dlt.translator import DltResourceTranslatorData

from ol_dlt import config


def _storage_kind() -> str:
    """Return the storage kind for the active profile (iceberg vs filesystem)."""
    return "iceberg" if config.active_table_format() == "iceberg" else "filesystem"


def _source_system(resource_name: str) -> str:
    """Return the source-system group for a ``raw__<system>__...`` resource.

    Used as the Dagster ``group_name`` so assets are scoped by source system
    (e.g. ``oll``, ``mitpe``, ``edxorg``, ``podcast``). edxorg programs and
    edxorg S3 tables both resolve to ``edxorg``.
    """
    parts = resource_name.split("__")
    return parts[1] if len(parts) > 1 and parts[1] else "ingestion"


class RawDataDltTranslator(DagsterDltTranslator):
    """Shared translator for raw-data dlt assets.

    Sets the ``["ol_warehouse_raw_data", <resource>]`` asset key, the ``dlt`` +
    storage kinds, and the Glue fully-qualified ``table_name`` metadata. dlt does
    not populate ``schema_name`` for filesystem+Iceberg destinations, so the
    fully-qualified name is assembled here from the pipeline ``dataset_name``.
    """

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Return the raw-data asset spec for a dlt resource."""
        default_spec = super().get_asset_spec(data)
        resource_name = data.resource.name
        storage_kind = _storage_kind()

        table_name_meta: dict[str, Any] = {}
        if data.pipeline and data.pipeline.dataset_name:
            table_name_meta = dict(
                TableMetadataSet(
                    table_name=f"{data.pipeline.dataset_name}.{resource_name}"
                )
            )

        return default_spec.replace_attributes(
            key=AssetKey(["ol_warehouse_raw_data", resource_name]),
            # Scope assets by source system (oll, mitpe, edxorg, podcast, ...).
            group_name=_source_system(resource_name),
            # These are root extracts. dagster_dlt's default spec adds a phantom
            # "<source>_<resource>" external dep (keyed off the un-remapped name);
            # drop it so the asset has no orphaned upstream. EdxorgDltTranslator
            # re-adds the real archive deps.
            deps=[],
            # storage_kind defaults to the destination name, not the real storage
            # type; override with the actual kind.
            kinds={"dlt", storage_kind},
            metadata={
                **default_spec.metadata,
                **table_name_meta,
                **dict(TableMetadataSet(storage_kind=storage_kind)),
            },
        )


class EdxorgDltTranslator(RawDataDltTranslator):
    """RawDataDltTranslator plus the upstream edxorg-archive AssetDeps.

    Each ``raw__edxorg__s3__tables__<table>`` asset depends on its corresponding
    partitioned ``edxorg/raw_data/db_table/<table>`` archive asset.
    """

    _PREFIX = "raw__edxorg__s3__tables__"

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Return the raw-data asset spec plus the upstream archive dep."""
        spec = super().get_asset_spec(data)
        resource_name = data.resource.name
        if resource_name.startswith(self._PREFIX):
            short_name = resource_name.removeprefix(self._PREFIX)
            archive_key = AssetKey(["edxorg", "raw_data", "db_table", short_name])
            spec = spec.replace_attributes(deps=[AssetDep(archive_key)])
        return spec
