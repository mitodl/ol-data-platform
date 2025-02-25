import os
import re

from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    with_source_code_references,
)
from dagster_airbyte import airbyte_resource, load_assets_from_airbyte_instance
from dagster_dbt import (
    DbtCliResource,
)

from ol_orchestrate.assets.lakehouse.dbt import DBT_REPO_DIR, full_dbt_project
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.resources.secrets.vault import Vault

airbyte_host_map = {
    "dev": "api-airbyte-qa.odl.mit.edu",
    "qa": "api-airbyte-qa.odl.mit.edu",
    "production": "api-airbyte.odl.mit.edu",
}

airbyte_host = os.environ.get("DAGSTER_AIRBYTE_HOST", airbyte_host_map[DAGSTER_ENV])
if DAGSTER_ENV == "dev":
    dagster_url = "http://localhost:3000"
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault._auth_github()  # noqa: SLF001
else:
    dagster_url = (
        "https://pipelines.odl.mit.edu"
        if DAGSTER_ENV == "production"
        else "https://pipelines-qa.odl.mit.edu"
    )
    vault = Vault(
        vault_addr=VAULT_ADDRESS, vault_role="dagster-server", aws_auth_mount="aws"
    )
    vault._auth_aws_iam()  # noqa: SLF001

configured_airbyte_resource = airbyte_resource.configured(
    {
        "host": airbyte_host,
        "port": os.environ.get("DAGSTER_AIRBYTE_PORT", "443"),
        "use_https": True,
        "username": "dagster",
        "password": vault.client.secrets.kv.v1.read_secret(
            path="dagster-http-auth-password", mount_point="secret-data"
        )["data"]["dagster_unhashed_password"],
        "request_timeout": 60,  # Allow up to a minute for Airbyte requests
    }
)

dbt_config = {
    "project_dir": str(DBT_REPO_DIR),
    "profiles_dir": str(DBT_REPO_DIR),
    "target": os.environ.get("DAGSTER_DBT_TARGET", DAGSTER_ENV),
}
dbt_cli = DbtCliResource(**dbt_config)

airbyte_assets = load_assets_from_airbyte_instance(
    configured_airbyte_resource,
    # This key_prefix is how Dagster knows to map the Airbyte outputs to the dbt
    # sources, since they are defined as ol_warehouse_raw_data in the
    # sources.yml files. (TMM 2023-01-18)
    key_prefix="ol_warehouse_raw_data",
    connection_filter=lambda conn: re.search(r"S3 (Glue )?Data Lake", conn.name)
    is not None,
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


# This section creates a separate job and schedule for each Airbyte connection that will
# materialize the tables for that connection and any associated dbt staging models for
# those tables. The eager auto materialize policy will then take effect for any
# downstream dbt models that are dependent on those staging models being completed.
group_names = set()
computed_assets = airbyte_assets.compute_cacheable_data()
for asset in computed_assets:
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
    assets=[*with_source_code_references([full_dbt_project]), airbyte_assets],
    resources={"dbt": dbt_cli},
    sensors=[
        AutomationConditionSensorDefinition(
            "dbt_automation_sensor",
            minimum_interval_seconds=3600,
            target=[full_dbt_project],
        )
    ],
    jobs=airbyte_asset_jobs,
    schedules=airbyte_update_schedules,
)
