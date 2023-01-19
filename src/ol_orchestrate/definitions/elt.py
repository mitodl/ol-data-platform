import os
from pathlib import Path

from dagster import AssetSelection, Definitions, build_asset_reconciliation_sensor
from dagster_airbyte import airbyte_resource, load_assets_from_airbyte_instance
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from requests.auth import HTTPBasicAuth

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "dev")
configured_airbyte_resource = airbyte_resource.configured(
    {
        "host": {"env": "DAGSTER_AIRBYTE_HOST"},
        "port": {"env": "DAGSTER_AIRBYTE_PORT"},
        "use_https": False,
        "request_additional_params": {
            "auth": HTTPBasicAuth(*os.getenv("DAGSTER_AIRBYTE_AUTH", "").split(":")),
            "verify": False,
        },
    }
)

dbt_repo_dir = str(
    Path(__file__).parent.parent.parent.joinpath("ol_dbt")
    if dagster_deployment == "dev"
    else Path("/opt/dbt")
)

dbt_config = {"project_dir": dbt_repo_dir, "profiles_dir": dbt_repo_dir}
configured_dbt_cli = dbt_cli_resource.configured(dbt_config)

elt = Definitions(
    assets=[
        load_assets_from_airbyte_instance(
            configured_airbyte_resource,
            # This key_prefix is how Dagster knows to map the Airbyte outputs to the dbt
            # sources, since they are defined as ol_warehouse_raw_data in the
            # sources.yml files. (TMM 2023-01-18)
            key_prefix="ol_warehouse_raw_data",
        ),
        *load_assets_from_dbt_project(**dbt_config),
    ],
    resources={"dbt": configured_dbt_cli},
    sensors=[
        build_asset_reconciliation_sensor(
            name="elt_asset_sensor",
            asset_selection=AssetSelection.all(),
            minimum_interval_seconds=60 * 5,
        )
    ],
)
