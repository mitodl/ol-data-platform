import json  # noqa: INP001
import os
import re
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
        # Airbyte uses the unicode "right arrow" (U+2192) in the connection names for
        # separating the source and destination. This selects the source name specifier
        # and converts it to a lowercased, underscore separated string.
        lambda conn_name: re.sub(
            r"[^A-Za-z0-9_]", "", re.sub(r"[-\s]+", "_", conn_name)
        )
        .strip("_")
        .lower()
    ),
)

dbt_assets = load_assets_from_dbt_manifest(
    manifest=json.loads(
        dbt_repo_dir.joinpath("target", "manifest.json").read_text(),
    ),
)

# This section creates a separate job and schedule for each Airbyte connection that will
# materialize the tables for that connection and any associated dbt staging models for
# those tables. The eager auto materialize policy will then take effect for any
# downstream dbt models that are dependent on those staging models being completed.
group_names = set()
for asset in airbyte_assets.compute_cacheable_data():
    group_names.add(asset.group_name)

airbyte_asset_jobs = []
airbyte_update_schedules = []
group_count = len(group_names)
for count, group_name in enumerate(group_names, start=1):
    job = define_asset_job(
        name=f"sync_and_stage_{group_name}",
        selection=AssetSelection.groups(group_name)
        .downstream(depth=1, include_self=True)
        .required_multi_asset_neighbors(),
    )
    airbyte_update_schedules.append(
        ScheduleDefinition(
            name=f"daily_sync_and_stage_{group_name}",
            # Offset schedule starts by an hour for groupings of ~4 connections
            cron_schedule=f"0 {count % (group_count // 4)} * * *",
            job=job,
            execution_timezone="UTC",
            default_status=DefaultScheduleStatus.RUNNING,
        )
    )
    airbyte_asset_jobs.append(job)


elt = Definitions(
    assets=load_assets_from_current_module(
        auto_materialize_policy=AutoMaterializePolicy.eager(),
    ),
    resources={"dbt": configured_dbt_cli},
    sensors=[],
    jobs=airbyte_asset_jobs,
    schedules=airbyte_update_schedules,
)
