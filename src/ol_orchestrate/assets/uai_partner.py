import hashlib
from datetime import UTC, datetime
from pathlib import Path

import boto3
import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    ExpectationResult,
    MetadataValue,
    Output,
    asset,
)
from dagster_dbt.asset_utils import get_asset_key_for_model

from ol_orchestrate.assets.lakehouse.dbt import full_dbt_project
from ol_orchestrate.partitions.openedx import UAI_PARTNER_PARTITIONS

# Sample partner course ids
partner_course_mapping = [
    {"partner_id": "partner1", "course_run_id": "course-v1:TestX+Test101+3T2022"},
    {"partner_id": "partner2", "course_run_id": "course-v1:DEMO+MLx1DEMO+DEMO"},
]


@asset(
    description="It contains the learner enrollment data for a specific partner",
    group_name="universal_ai",
    io_manager_key="s3file_io_manager",
    key=AssetKey(["universal_ai", "learner_enrollment"]),
    deps=[
        get_asset_key_for_model(
            [full_dbt_project], "marts__combined_course_enrollment_detail"
        )
    ],
    partitions_def=UAI_PARTNER_PARTITIONS,
)
def learner_enrollment_data(context: AssetExecutionContext):
    partner_id = context.partition_key

    partner_course_runs = [
        entry["course_run_id"]
        for entry in partner_course_mapping
        if entry["partner_id"] == partner_id
    ]

    glue = boto3.client("glue")
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
        partner_data_df = df.filter(
            pl.col("courserun_readable_id").is_in(partner_course_runs)
        )
        num_rows = len(partner_data_df)
        context.log.info("%d rows for %s's enrollment data", num_rows, partner_id)

        # Export the filtered data to a CSV file
        enrollment_data_file = Path(f"{partner_id}_enrollment_data.csv")
        partner_data_df.write_csv(enrollment_data_file)

        context.log.info(
            "Exported %s's enrollment data to %s", partner_id, enrollment_data_file
        )

        enrollment_data_version = hashlib.sha256(
            partner_data_df.write_csv().encode("utf-8")
        ).hexdigest()

        enrollment_data_object_key = (
            f"/universal_ai/{partner_id}/enrollment_data_{datetime.now(tz=UTC).strftime('%Y-%m-%d')}_"
            f"{enrollment_data_version}.csv"
        )

        yield ExpectationResult(
            success=not partner_data_df.is_empty(),
            label="learner_enrollment_data_not_empty",
            metadata={
                "number_of_enrollments": MetadataValue.text(
                    text=str(partner_data_df.height)
                )
            },
        )
        yield Output(
            (enrollment_data_file, enrollment_data_object_key),
            metadata={
                "partner_id": partner_id,
                "object_key": enrollment_data_object_key,
                "rows": num_rows,
                "file_size_in_bytes": enrollment_data_file.stat().st_size,
                "materialization_time": datetime.now(tz=UTC).isoformat(),
            },
        )
