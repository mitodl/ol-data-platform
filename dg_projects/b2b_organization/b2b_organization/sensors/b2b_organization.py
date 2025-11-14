import json

from dagster import (
    AddDynamicPartitionsRequest,
    AssetKey,
    DefaultSensorStatus,
    RunRequest,
    SensorResult,
    sensor,
)
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe


@sensor(
    name="b2b_organization_list_sensor",
    description="Query a list of B2B organizational customers",
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=60 * 60 * 24,  # daily
    job_name="b2b_organization_data_export_job",
)
def b2b_organization_list_sensor(context):
    organization_df = get_dbt_model_as_dataframe(
        database_name="ol_warehouse_production_intermediate",
        table_name="int__mitxonline__b2b_contract_to_courseruns",
    )
    organization_keys = set(organization_df["organization_key"].unique().to_list())

    # Get existing dynamic partitions
    existing_partitions = set(
        context.instance.get_dynamic_partitions("b2b_organization_list")
    )
    context.log.info("existing b2b_organization_list: %s", existing_partitions)

    new_organization_keys = organization_keys - existing_partitions

    # Create dynamic partition requests if new keys found
    dynamic_requests = []
    if new_organization_keys:
        dynamic_requests.append(
            AddDynamicPartitionsRequest(
                partitions_def_name="b2b_organization_list",
                partition_keys=list(new_organization_keys),
            )
        )
    # Create run requests for each organization
    run_requests = [
        RunRequest(
            asset_selection=[
                AssetKey(["b2b_organization", "administration_report_export"]),
            ],
            partition_key=organization_key,
        )
        for organization_key in organization_keys
    ]

    return SensorResult(
        dynamic_partitions_requests=dynamic_requests,
        run_requests=run_requests,
        cursor=json.dumps(list(organization_keys)),
    )
