import json  # noqa: INP001
import os
from pathlib import Path

from dagster import (
    AssetSelection,
    AutoMaterializePolicy,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_current_module,
)
from dagster_airbyte import airbyte_resource, load_assets_from_airbyte_instance
from dagster_dbt import (
    dbt_cli_resource,
    load_assets_from_dbt_manifest,
)

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "dev")
configured_airbyte_resource = airbyte_resource.configured(
    {
        "host": {"env": "DAGSTER_AIRBYTE_HOST"},
        "port": {"env": "DAGSTER_AIRBYTE_PORT"},
        "use_https": True,
        "username": os.getenv("DAGSTER_AIRBYTE_AUTH", "").split(":")[0],
        "password": os.getenv("DAGSTER_AIRBYTE_AUTH", "").split(":")[1],
        "request_timeout": 60,  # Allow up to a minute for Airbyte requests
        "request_additional_params": {
            "verify": False,
        },
    }
)

dbt_repo_dir = (
    Path(__file__).parent.parent.parent.parent.joinpath("ol_dbt")
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

dbt_assets = load_assets_from_dbt_manifest(
    manifest=json.loads(
        dbt_repo_dir.joinpath("target", "manifest.json").read_text(),
    ),
)

airbyte_asset_job = define_asset_job(
    name="airbyte_asset_sync",
    selection=AssetSelection.assets(*dbt_assets)
    .upstream()
    .required_multi_asset_neighbors(),
)

airbyte_update_schedule = ScheduleDefinition(
    name="daily_airbyte_sync",
    cron_schedule="0 4 * * *",
    job=airbyte_asset_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)

elt = Definitions(
    assets=load_assets_from_current_module(
        auto_materialize_policy=AutoMaterializePolicy.eager(),
    ),
    resources={"dbt": configured_dbt_cli},
    sensors=[],
    jobs=[airbyte_asset_job],
    schedules=[airbyte_update_schedule],
)
