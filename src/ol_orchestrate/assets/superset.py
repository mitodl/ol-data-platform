import httpx
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    asset,
)
from dagster_dbt import get_asset_key_for_model

from ol_orchestrate.assets.lakehouse.dbt import full_dbt_project
from ol_orchestrate.resources.superset_api import SupersetApiClientFactory


def create_superset_asset(dbt_asset_group_name: str, dbt_model_name: str):
    @asset(
        key=AssetKey(("superset", "dataset", dbt_model_name)),
        deps=[get_asset_key_for_model([full_dbt_project], dbt_model_name)],
        automation_condition=AutomationCondition.eager(),
        group_name="superset_dataset",
    )
    def _superset_dataset(
        context: AssetExecutionContext,
        superset_api: SupersetApiClientFactory,
    ):
        dataset_id = superset_api.client.get_or_create_dataset(
            schema_suffix=dbt_asset_group_name,
            table_name=dbt_model_name,
        )

        if dataset_id is None:
            context.log.warning(
                "Dataset ID not found for %s.%s. Skipping refresh.",
                dbt_asset_group_name,
                dbt_model_name,
            )
            return

        try:
            superset_api.client.refresh_dataset(dataset_id)
            context.log.info(
                "Successfully refreshed dataset: %s.%s",
                dbt_asset_group_name,
                dbt_model_name,
            )
        except httpx.HTTPStatusError:
            context.log.exception(
                "HTTPStatusError while refreshing dataset for %s.%s",
                dbt_asset_group_name,
                dbt_model_name,
            )

    return _superset_dataset
