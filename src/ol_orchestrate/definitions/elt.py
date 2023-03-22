import json
import os
from pathlib import Path

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    ScheduleDefinition,
    build_asset_reconciliation_sensor,
    define_asset_job,
)
from dagster_airbyte import airbyte_resource, load_assets_from_airbyte_instance
from dagster_dbt import (
    dbt_cli_resource,
    load_assets_from_dbt_manifest,
)
from requests.auth import HTTPBasicAuth

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "dev")
configured_airbyte_resource = airbyte_resource.configured(
    {
        "host": {"env": "DAGSTER_AIRBYTE_HOST"},
        "port": {"env": "DAGSTER_AIRBYTE_PORT"},
        "use_https": True,
        "request_additional_params": {
            "auth": HTTPBasicAuth(*os.getenv("DAGSTER_AIRBYTE_AUTH", "").split(":")),
            "verify": False,
        },
    }
)

dbt_repo_dir = (
    Path(__file__).parent.parent.parent.joinpath("ol_dbt")
    if dagster_deployment == "dev"
    else Path("/opt/dbt")
)

dbt_config = {
    "project_dir": str(dbt_repo_dir),
    "profiles_dir": str(dbt_repo_dir),
    "target": dagster_deployment,
}
configured_dbt_cli = dbt_cli_resource.configured(dbt_config)

airbyte_assets = load_assets_from_airbyte_instance(
    configured_airbyte_resource,
    # This key_prefix is how Dagster knows to map the Airbyte outputs to the dbt
    # sources, since they are defined as ol_warehouse_raw_data in the
    # sources.yml files. (TMM 2023-01-18)
    key_prefix="ol_warehouse_raw_data",
    connection_filter=lambda conn: "S3 Glue Data Lake" in conn.name,
    connection_to_group_fn=(
        lambda conn_name: "ol_warehouse_raw"
        if "S3 Glue Data Lake" in conn_name
        else "non_lake_connection"
    ),
)

airbyte_asset_job = define_asset_job(
    name="airbyte_asset_sync",
    selection=AssetSelection.groups("ol_warehouse_raw").downstream(),
)

airbyte_update_schedule = ScheduleDefinition(
    name="daily_airbyte_sync",
    cron_schedule="0 4 * * *",
    job=airbyte_asset_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)

dbt_assets = load_assets_from_dbt_manifest(
    manifest_json=json.loads(
        dbt_repo_dir.joinpath("target", "manifest.json").read_text(),
    ),
    use_build_command=True,
)

elt = Definitions(
    assets=[
        airbyte_assets,
        *dbt_assets,
    ],
    resources={"dbt": configured_dbt_cli},
    sensors=[
        build_asset_reconciliation_sensor(
            name="dbt_asset_sensor",
            asset_selection=AssetSelection.assets(*dbt_assets),
            minimum_interval_seconds=60 * 5,
            default_status=DefaultSensorStatus.RUNNING,
        )
    ],
    jobs=[airbyte_asset_job],
    schedules=[airbyte_update_schedule],
)
