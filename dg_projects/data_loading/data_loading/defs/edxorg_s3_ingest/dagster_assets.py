"""
Dagster assets that wrap dlt sources with upstream dependencies.

This module creates consolidated, non-partitioned assets that depend on the
upstream partitioned edxorg_archive assets. Each dlt asset consolidates data
from all prod courses into a single table.
"""

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
)
from dagster._core.definitions.metadata.metadata_set import TableMetadataSet
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from ol_orchestrate.lib.constants import EDXORG_DB_TABLES

from .loads import (
    dagster_env,
    edxorg_s3_pipeline,
    edxorg_s3_source,
    edxorg_s3_source_instance,
    table_format,
)

_STORAGE_KIND = "iceberg" if dagster_env in ("qa", "production") else "filesystem"


class EdxorgDltTranslator(DagsterDltTranslator):
    """Custom translator for edxorg dlt assets with upstream dependencies."""

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        default_spec = super().get_asset_spec(data)

        # The resource name is raw__edxorg__s3__tables__{table}
        resource_name = data.resource.name
        asset_key = AssetKey(["ol_warehouse_raw_data", resource_name])

        deps = []
        table_name_meta = {}
        if resource_name.startswith("raw__edxorg__s3__tables__"):
            short_name = resource_name.replace("raw__edxorg__s3__tables__", "")
            deps = [AssetDep(AssetKey(["edxorg", "raw_data", "db_table", short_name]))]
            # Assemble the Glue catalog fully-qualified name.  dlt does not
            # populate schema_name in load_info for filesystem+Iceberg
            # destinations, so extract_resource_metadata leaves table_name
            # as None.  Set it explicitly here from the pipeline dataset_name.
            if data.pipeline and data.pipeline.dataset_name:
                table_name_meta = dict(
                    TableMetadataSet(
                        table_name=f"{data.pipeline.dataset_name}.{resource_name}"
                    )
                )

        return default_spec.replace_attributes(
            key=asset_key,
            deps=deps,
            # storage_kind defaults to the named destination ("production"),
            # not the actual storage type.  Override with the real kind.
            kinds={"dlt", _STORAGE_KIND},
            metadata={
                **default_spec.metadata,
                **table_name_meta,
                **TableMetadataSet(storage_kind=_STORAGE_KIND),
            },
        )


def _asset_key_for_table(table_name: str) -> AssetKey:
    """Return the Dagster AssetKey used for a given edxorg table."""
    return AssetKey(["ol_warehouse_raw_data", f"raw__edxorg__s3__tables__{table_name}"])


# Create dlt assets with upstream dependencies.
# edxorg_s3_source_instance (with all tables) is used here only so that the
# @dlt_assets decorator can discover the full set of asset specs at import
# time.  The execution function below overrides the source per-table.
@dlt_assets(
    dlt_source=edxorg_s3_source_instance,
    dlt_pipeline=edxorg_s3_pipeline,
    name="edxorg_s3_tables",
    group_name="ingestion",
    dagster_dlt_translator=EdxorgDltTranslator(),
)
def edxorg_s3_consolidated_tables(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    """
    Load and consolidate EdX.org database tables from S3.

    Depends on upstream edxorg_archive assets (partitioned by course and source).
    Each downstream table has a one-to-one dependency on its corresponding upstream
    partitioned asset. Filters to only 'prod' source data and consolidates across
    all courses into non-partitioned Iceberg/Parquet tables.

    The dlt source uses incremental loading based on file modification dates,
    so only new or modified files are processed on each run.

    Tables are processed sequentially, one per dlt.run() call, so that:
    * The AWS boto3 credential chain refreshes STS tokens between tables.
    * Large tables (e.g. auth_user) do not exhaust a single token's TTL.
    * dlt's non-thread-safe injectable-context dict is never accessed
      concurrently (see also [extract] workers=1 in .dlt/config.toml).
    """
    # Determine which tables to process, honouring any asset-key sub-selection
    # that Dagster may have applied (e.g. "re-materialise auth_user only").
    if context.is_subset:
        selected_tables = [
            t
            for t in EDXORG_DB_TABLES
            if _asset_key_for_table(t) in context.selected_asset_keys
        ]
    else:
        selected_tables = list(EDXORG_DB_TABLES)

    # Run the dlt pipeline once per table.  This keeps each run short enough
    # that STS credentials issued at the start of the run do not expire before
    # it finishes, and gives the boto3 credential chain a chance to refresh
    # between tables via AssumeRoleWithWebIdentity (IRSA).
    for table_name in selected_tables:
        table_source = edxorg_s3_source(tables=[table_name], table_format=table_format)
        yield from dlt.run(
            context=context,
            dlt_source=table_source,
            loader_file_format="parquet",
        )


__all__ = ["edxorg_s3_consolidated_tables"]
