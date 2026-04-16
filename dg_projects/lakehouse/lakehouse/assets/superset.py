from datetime import UTC, datetime

import httpx
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Output,
    asset,
)
from dagster_dbt import get_asset_key_for_model
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes

from lakehouse.assets.lakehouse.dbt import full_dbt_project
from lakehouse.resources.superset_api import SupersetApiClientFactory

_DEFAULT_SCHEMA_BASE = "ol_warehouse_production"


def _validate_superset_database_config(database_id: int, database_name: str) -> None:
    """Validate that database_id and database_name are consistent.

    Raises ValueError if a non-trino database_name is paired with the default
    Trino database_id (1), which would silently register the dataset under the
    wrong database.
    """
    if database_name != "trino" and database_id == 1:
        msg = (
            f"database_name={database_name!r} requires an explicit database_id. "
            "Received database_id=1, which is the default reserved for Trino."
        )
        raise ValueError(msg)


def create_superset_asset(
    dbt_asset_group_name: str,
    dbt_model_name: str,
    database_id: int = 1,
    database_name: str = "trino",
    schema_base: str = _DEFAULT_SCHEMA_BASE,
):
    _validate_superset_database_config(database_id, database_name)

    if database_name == "trino":
        asset_key = AssetKey(("superset", "dataset", dbt_model_name))
        group_name = "superset_dataset"
    else:
        asset_key = AssetKey(("superset", database_name, "dataset", dbt_model_name))
        group_name = f"superset_{database_name}_dataset"

    @asset(
        key=asset_key,
        deps=[get_asset_key_for_model([full_dbt_project], dbt_model_name)],
        automation_condition=upstream_or_code_changes(),
        group_name=group_name,
    )
    def _superset_dataset(
        context: AssetExecutionContext,
        superset_api: SupersetApiClientFactory,
    ):
        dataset_id = superset_api.client.get_or_create_dataset(
            schema_suffix=dbt_asset_group_name,
            table_name=dbt_model_name,
            database_id=database_id,
            schema_base=schema_base,
        )

        if dataset_id is None:
            context.log.warning(
                "Dataset ID not found for %s.%s. Skipping refresh.",
                dbt_asset_group_name,
                dbt_model_name,
            )
            return Output(
                value=None,
                metadata={
                    "status": "skipped",
                    "reason": "Superset Dataset ID not found",
                    "dbt_asset_group_name": dbt_asset_group_name,
                    "dbt_model_name": dbt_model_name,
                },
            )

        try:
            # Refresh columns
            superset_api.client.refresh_dataset(int(dataset_id))
            context.log.info(
                "Successfully refreshed dataset: %s.%s",
                dbt_asset_group_name,
                dbt_model_name,
            )

            timestamp = datetime.now(UTC).isoformat()
            # Touch dataset metadata (updates changed_on)
            superset_api.client.update_dataset(
                dataset_id=int(dataset_id),
                payload={"description": f"Auto refreshed at {timestamp}"},
            )

            return Output(
                value={"superset_dataset_id": dataset_id},
                metadata={
                    "status": "refreshed",
                    "superset_dataset_id": dataset_id,
                    "dbt_asset_group_name": dbt_asset_group_name,
                    "dbt_model_name": dbt_model_name,
                },
            )
        except httpx.HTTPStatusError as e:
            context.log.exception(
                "HTTPStatusError while refreshing dataset for %s.%s",
                dbt_asset_group_name,
                dbt_model_name,
            )
            return Output(
                value=None,
                metadata={
                    "status": "error",
                    "error": str(e),
                    "dbt_asset_group_name": dbt_asset_group_name,
                    "dbt_model_name": dbt_model_name,
                },
            )

    return _superset_dataset
