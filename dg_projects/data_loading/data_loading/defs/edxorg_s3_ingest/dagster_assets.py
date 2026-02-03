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
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from .loads import (
    edxorg_s3_pipeline,
    edxorg_s3_source_instance,
)


class EdxorgDltTranslator(DagsterDltTranslator):
    """Custom translator for edxorg dlt assets with upstream dependencies."""

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """
        Map dlt resource to Dagster asset spec with one-to-one upstream deps.
        """
        # Get the default spec from parent
        default_spec = super().get_asset_spec(data)

        # The resource name is raw__edxorg__s3__tables__{table}
        resource_name = data.resource.name

        # Create asset key with ol_warehouse_raw_data prefix
        asset_key = AssetKey(["ol_warehouse_raw_data", resource_name])

        # Extract table name and create upstream dependency
        deps = []
        if resource_name.startswith("raw__edxorg__s3__tables__"):
            table_name = resource_name.replace("raw__edxorg__s3__tables__", "")
            # Add non-blocking dependency on upstream partitioned asset
            deps = [AssetDep(AssetKey(["edxorg", "raw_data", "db_table", table_name]))]

        return default_spec.replace_attributes(
            key=asset_key,
            deps=deps,
        )


# Create dlt assets with upstream dependencies
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

    The dlt source automatically uses incremental loading based on file modification
    dates, so only new/modified files are processed on each run.
    """
    yield from dlt.run(context=context)


__all__ = ["edxorg_s3_consolidated_tables"]
