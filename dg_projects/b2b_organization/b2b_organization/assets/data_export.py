import hashlib
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    Output,
    asset,
)
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.partitions.b2b_organization import b2b_organization_list_partitions


@asset(
    code_version="b2b_organization_data_export_v1",
    group_name="b2b_organization",
    deps=[AssetKey(["reporting", "organization_administration_report"])],
    partitions_def=b2b_organization_list_partitions,
    io_manager_key="s3file_io_manager",
    key=AssetKey(["b2b_organization", "administration_report_export"]),
)
def export_b2b_organization_data(context: AssetExecutionContext):
    organization_key = context.partition_key

    organization_data_df = get_dbt_model_as_dataframe(
        database_name="ol_warehouse_production_reporting",
        table_name="organization_administration_report",
    )
    organizational_data_df = organization_data_df.filter(
        pl.col("organization").eq(organization_key)
    )
    num_rows = len(organizational_data_df)
    context.log.info(
        "%d rows in organization_administration_report for %s",
        num_rows,
        organization_key,
    )

    organizational_data_version = hashlib.sha256(
        organizational_data_df.write_csv().encode("utf-8")
    ).hexdigest()

    organizational_data_file = Path(
        f"{organization_key}_{organizational_data_version}.csv"
    )
    organizational_data_df.write_csv(organizational_data_file)

    context.log.info(
        "Exported organization_administration_report for %s to %s",
        organization_key,
        organizational_data_file,
    )

    organizational_data_object_key = (
        f"{organization_key}/{organizational_data_version}.csv"
    )

    yield Output(
        (organizational_data_file, organizational_data_object_key),
        data_version=DataVersion(organizational_data_version),
        metadata={
            "b2b_org_key": organization_key,
            "object_key": organizational_data_object_key,
            "row_count": num_rows,
            "file_size_in_bytes": organizational_data_file.stat().st_size,
            "materialization_timestamp": datetime.now(tz=UTC).isoformat(),
        },
    )
