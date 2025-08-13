import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import boto3
import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    ExpectationResult,
    MetadataValue,
    Output,
    asset,
)
from dagster_dbt.asset_utils import get_asset_key_for_model

from ol_orchestrate.assets.lakehouse.dbt import full_dbt_project
from ol_orchestrate.partitions.openedx import UAI_CUSTOMER_PARTITIONS


# Fetch customer course mapping from a table once we have the data
@asset(
    group_name="universal_ai",
    key=AssetKey(["universal_ai", "customer_course_mapping"]),
    description="it contains the mapping of customer to course run IDs",
    deps=[
        get_asset_key_for_model(
            [full_dbt_project], "int__mitxonline__uai_customer_courserun_mapping"
        )
    ],
)
def get_customer_course_mapping(context):
    glue = boto3.client("glue", region_name="us-east-1")
    response = glue.get_table(
        DatabaseName="ol_warehouse_production_intermediate",
        Name="int__mitxonline__uai_customer_courserun_mapping",
    )
    table_metadata = response["Table"]
    storage_descriptor = table_metadata["StorageDescriptor"]
    iceberg_table_location = storage_descriptor.get("Location", None)
    context.log.info("%s iceberg_table_location", iceberg_table_location)
    metadata_location = table_metadata["Parameters"]["metadata_location"]
    context.log.info("%s metadata_location", metadata_location)

    df = pl.scan_iceberg(metadata_location).collect()
    customer_course_mapping = df.to_dicts()
    context.log.info("%s", customer_course_mapping)
    customer_to_course_ids = defaultdict(list)
    for row in customer_course_mapping:
        customer_to_course_ids[row["organization"]].append(row["courserun_readable_id"])

    context.instance.add_dynamic_partitions(
        "uai_customers", partition_keys=list(customer_to_course_ids.keys())
    )

    return dict(customer_to_course_ids)


@asset(
    description="It contains the learner enrollment data for a specific customer",
    key=AssetKey(["universal_ai", "learner_enrollment"]),
    deps=[
        get_asset_key_for_model(
            [full_dbt_project], "marts__combined_course_enrollment_detail"
        )
    ],
    partitions_def=UAI_CUSTOMER_PARTITIONS,
    ins={
        "customer_course_mapping": AssetIn(
            key=AssetKey(["universal_ai", "customer_course_mapping"])
        )
    },
    group_name="universal_ai",
    io_manager_key="s3file_io_manager",
)
def export_learner_enrollment_data(
    context: AssetExecutionContext, customer_course_mapping
):
    customer = context.partition_key
    customer_course_runs = customer_course_mapping.get(customer, [])
    context.log.info(
        "Customer %s has course run mapping: %s", customer, customer_course_runs
    )

    glue = boto3.client("glue", region_name="us-east-1")
    response = glue.get_table(
        DatabaseName="ol_warehouse_production_mart",
        Name="marts__combined_course_enrollment_detail",
    )
    table_metadata = response["Table"]
    storage_descriptor = table_metadata["StorageDescriptor"]
    iceberg_table_location = storage_descriptor.get("Location", None)
    metadata_location = table_metadata["Parameters"]["metadata_location"]

    if iceberg_table_location:
        df = pl.scan_iceberg(metadata_location).collect()
        customer_data_df = df.filter(
            pl.col("courserun_readable_id").is_in(customer_course_runs)
        )
        num_rows = len(customer_data_df)
        context.log.info("%d rows for %s's enrollment data", num_rows, customer)

        # Export the filtered data to a CSV file
        enrollment_data_file = Path(f"{customer}_enrollment_data.csv")
        customer_data_df.write_csv(enrollment_data_file)

        context.log.info(
            "Exported %s's enrollment data to %s", customer, enrollment_data_file
        )

        enrollment_data_version = hashlib.sha256(
            json.dumps(
                {
                    "columns": customer_data_df.columns,
                    "row_count": num_rows,
                }
            ).encode("utf-8")
        ).hexdigest()

        enrollment_data_object_key = (
            f"/universal_ai/{customer}/enrollment_data_{datetime.now(tz=UTC).strftime('%Y-%m-%d')}_"
            f"{enrollment_data_version}.csv"
        )

        yield ExpectationResult(
            success=not customer_data_df.is_empty(),
            label="learner_enrollment_data_not_empty",
            metadata={"number_of_enrollments": MetadataValue.text(text=str(num_rows))},
        )
        yield Output(
            (enrollment_data_file, enrollment_data_object_key),
            metadata={
                "customer": customer,
                "object_key": enrollment_data_object_key,
                "row_count": num_rows,
                "file_size_in_bytes": enrollment_data_file.stat().st_size,
                "materialization_time": datetime.now(tz=UTC).isoformat(),
            },
        )
