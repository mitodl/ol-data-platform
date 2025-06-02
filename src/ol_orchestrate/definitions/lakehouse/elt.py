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
from ol_orchestrate.assets.superset import create_superset_asset
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.resources.secrets.vault import Vault
from ol_orchestrate.resources.superset_api import SupersetApiClientFactory

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
    dbt_target = "dev_production"
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
    dbt_target = "production"

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
    "target": dbt_target,
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

# Define a mapping of group_name to interval (6, 12 or 24 hours) on production
group_name_to_interval: dict[str, int] = {}
if DAGSTER_ENV == "production":
    group_name_to_interval = {
        "bootcamps_production_app_db__s3_data_lake": 24,
        "edxorg_production_course_structure__s3_data_lake": 24,
        "edxorg_production_course_tables__s3_data_lake": 24,
        "edxorg_tracking_logs__s3_data_lake": 24,
        "emeritus_bigquery__s3_data_lake": 24,
        "irx_bigquery__s3_data_lake": 24,
        "irx_bigquery_email_opt_in__s3_data_lake": 24,
        "mailgun__s3_data_lake": 24,
        "micromasters_production_app_db__s3_data_lake": 24,
        "mit_learn_production__s3_data_lake": 24,
        "ol_salesforce__s3_data_lake": 24,
        "s3_edxorg_course_and_program__s3_data_lake": 24,
        "s3_edxorg_program_credentials__s3_data_lake": 24,
        "mitx_forum_production__s3_data_lake": 12,
        "mitx_online_open_edx_db__s3_data_lake": 12,
        "mitx_online_production_open_edx_student_module_history__s3_data_lake": 12,
        "mitx_online_tracking_logs__s3_data_lake": 12,
        "mitxonline_forum_production__s3_data_lake": 12,
        "mitx_residential_open_edx_db__s3_data_lake": 12,
        "mitx_residential_open_edx_db_studentmodule_history__s3_data_lake": 12,
        "mitx_tracking_logs__s3_data_lake": 12,
        "s3_mitx_online_open_edx_extracts__s3_data_lake": 12,
        "s3_mitx_open_edx_extracts__s3_data_lake": 12,
        "s3_xpro_open_edx_extracts__s3_data_lake": 12,
        "xpro_forum_production__s3_data_lake": 12,
        "xpro_open_edx_db__s3_data_lake": 12,
        "xpro_tracking_logs__s3_data_lake": 12,
        "xpro_production_app_db__s3_data_lake": 6,
        "mitx_online_production_app_db__s3_data_lake": 6,
        "ocw_studio_app_db__s3_data_lake": 6,
        "odl_video_service__s3_data_lake": 6,
    }

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
    interval = group_name_to_interval.get(group_name, 24)  # default to 24 hours
    start_hour = count % max(1, len(group_names) // 4)

    # Compute explicit run hours (e.g. [1, 13] for 12-hour interval starting at 1)
    hours = [(start_hour + i * interval) % 24 for i in range(24 // interval)]
    hours_str = ",".join(str(h) for h in sorted(hours))

    airbyte_update_schedules.append(
        ScheduleDefinition(
            name=f"daily_sync_and_stage_{group_name}",
            cron_schedule=f"0 {hours_str} * * *",
            job=job,
            execution_timezone="UTC",
            default_status=DefaultScheduleStatus.STOPPED,
        )
    )
    airbyte_asset_jobs.append(job)

dbt_models_for_superset_datasets = {
    "mart",
    "reporting",
}  # relevant dbt models to sync with superset
dbt_model_keys = full_dbt_project.keys

superset_assets = [
    create_superset_asset(dbt_asset_group_name=key.path[0], dbt_model_name=key.path[1])
    for key in dbt_model_keys
    if key.path[0] in dbt_models_for_superset_datasets
]

elt = Definitions(
    assets=[
        *with_source_code_references([full_dbt_project]),
        airbyte_assets,
        *superset_assets,
    ],
    resources={
        "dbt": dbt_cli,
        "vault": vault,
        "superset_api": SupersetApiClientFactory(deployment="superset", vault=vault),
    },
    sensors=[
        AutomationConditionSensorDefinition(
            "dbt_automation_sensor",
            minimum_interval_seconds=3600,
            # exclude staging as they are already handled by "sync_and_stage_" job
            target=(
                AssetSelection.assets(full_dbt_project)
                - AssetSelection.groups("staging")
            )
            | AssetSelection.groups("superset_dataset"),
        ),
    ],
    jobs=[*airbyte_asset_jobs],
    schedules=airbyte_update_schedules,
)
