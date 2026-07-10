"""ELT assets for the data lakehouse."""

import os
import re

from dagster import (
    AssetSelection,
    AssetSpec,
    AutomationConditionSensorDefinition,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    with_source_code_references,
)
from dagster_airbyte import (
    AirbyteConnectionTableProps,
    DagsterAirbyteTranslator,
    build_airbyte_assets_definitions,
)
from dagster_dbt import (
    DbtCliResource,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.utils import authenticate_vault
from ol_orchestrate.resources.github import GithubApiClientFactory
from ol_orchestrate.resources.secrets.vault import Vault
from ol_orchestrate.resources.trino_maintenance import TrinoMaintenanceResource

from lakehouse.assets.iceberg_maintenance import (
    iceberg_dbt_layer_maintenance,
    iceberg_raw_layer_maintenance,
)
from lakehouse.assets.instructor_onboarding import (
    generate_instructor_onboarding_user_list,
    update_access_forge_repo,
)
from lakehouse.assets.lakehouse.dbt import (
    DBT_REPO_DIR,
    DBT_TARGET,
    dbt_docs_artifacts_job,
    full_dbt_project,
)
from lakehouse.assets.lakehouse.dbt_starrocks import b2b_analytics_starrocks_dbt_assets
from lakehouse.assets.starrocks_mv_refresh import refresh_starrocks_analytics_mvs
from lakehouse.assets.superset import create_superset_asset
from lakehouse.resources.airbyte import AirbyteOSSWorkspace
from lakehouse.resources.dbt_s3_artifacts import DbtS3ArtifactsResource
from lakehouse.resources.starrocks import StarRocksResource
from lakehouse.resources.superset_api import SupersetApiClientFactory
from lakehouse.sensors import (
    iceberg_snapshot_pointer_lag_sensor,
    iceberg_snapshot_pointer_repair_job,
)

trino_host_map = {
    "dev": "mitol-ol-data-lake-production.trino.galaxy.starburst.io",
    "ci": "mitol-ol-data-lake-qa-0.trino.galaxy.starburst.io",
    "qa": "mitol-ol-data-lake-qa-0.trino.galaxy.starburst.io",
    "production": "mitol-ol-data-lake-production.trino.galaxy.starburst.io",
}

trino_catalog_map = {
    "dev": "ol_data_lake_production",
    "ci": "ol_data_lake_qa",
    "qa": "ol_data_lake_qa",
    "production": "ol_data_lake_production",
}

# Hosts match the starrocks_qa / starrocks_production target defaults in
# src/ol_dbt/profiles.yml. dev/ci fall back to the QA FE for schema parity.
starrocks_host_map = {
    "dev": "lakehouse.qa.starrocks.ol.mit.edu",
    "ci": "lakehouse.qa.starrocks.ol.mit.edu",
    "qa": "lakehouse.qa.starrocks.ol.mit.edu",
    "production": "lakehouse-starrocks-fe-service.starrocks.svc.cluster.local",
}

# Matches the database-starrocks-{env} mount convention used by
# bin/starrocks-auth and ol_dbt_cli/commands/starrocks.py.
starrocks_vault_mount_map = {
    "dev": "database-starrocks-qa",
    "ci": "database-starrocks-qa",
    "qa": "database-starrocks-qa",
    "production": "database-starrocks-production",
}

airbyte_host_map = {
    "dev": "https://api-airbyte-qa.odl.mit.edu",
    "ci": "https://api-airbyte-qa.odl.mit.edu",
    "qa": "https://api-airbyte-qa.odl.mit.edu",
    "production": "https://api-airbyte.odl.mit.edu",
}

airbyte_host = os.environ.get("DAGSTER_AIRBYTE_HOST", airbyte_host_map[DAGSTER_ENV])

# Allow skipping Airbyte loading for local development
# Set SKIP_AIRBYTE=1 to disable Airbyte connection and asset loading
SKIP_AIRBYTE = os.environ.get("SKIP_AIRBYTE", "").lower() in ("1", "true", "yes")

# Determine dagster URL based on environment. The dbt target is resolved once in
# lakehouse.assets.lakehouse.dbt (DBT_TARGET) and shared by the DbtProject and the
# DbtCliResource so the parsed asset graph matches what executes.
if DAGSTER_ENV == "dev":
    dagster_url = "http://localhost:3000"
elif DAGSTER_ENV == "ci":
    dagster_url = "https://pipelines-ci.odl.mit.edu"
else:
    dagster_url = (
        "https://pipelines.odl.mit.edu"
        if DAGSTER_ENV == "production"
        else "https://pipelines-qa.odl.mit.edu"
    )

# Initialize vault with proper auth
try:
    vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)
    vault_authenticated = True
except Exception as e:  # noqa: BLE001 (resilient loading)
    # If vault auth fails (e.g., in testing without credentials),
    # create a mock vault to allow the code to load
    import warnings

    warnings.warn(
        f"Failed to authenticate with Vault: {e}. Using mock configuration.",
        stacklevel=2,
    )
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault_authenticated = False
    dagster_url = "http://localhost:3000"

airbyte_workspace = (
    AirbyteOSSWorkspace(
        api_server=airbyte_host,
        username="dagster",
        password=(
            vault.client.secrets.kv.v1.read_secret(
                path="dagster-http-auth-password", mount_point="secret-data"
            )["data"]["dagster_unhashed_password"]
            if vault_authenticated
            else "mock_password"
        ),
        request_timeout=60,  # Allow up to a minute for Airbyte requests
    )
    if not SKIP_AIRBYTE
    else None
)

dbt_config = {
    "project_dir": str(DBT_REPO_DIR),
    "profiles_dir": str(DBT_REPO_DIR),
    "target": DBT_TARGET,
}
dbt_cli = DbtCliResource(**dbt_config)


class OLAirbyteTranslator(DagsterAirbyteTranslator):
    """A custom Dagster-Airbyte translator for OL's data platform."""

    def get_asset_spec(self, props: AirbyteConnectionTableProps) -> AssetSpec:
        default_spec = super().get_asset_spec(props)
        return default_spec.replace_attributes(
            # This key_prefix is how Dagster knows to map the Airbyte outputs to the dbt
            # sources, since they are defined as ol_warehouse_raw_data in the
            # sources.yml files. (TMM 2023-01-18)
            key=default_spec.key.with_prefix("ol_warehouse_raw_data"),
            # Airbyte uses the unicode "right arrow" (U+2192) in the connection names
            # for separating the source and destination. This selects the source name
            # specifier and converts it to a lowercased, underscore separated string.
            group_name=re.sub(
                r"[^A-Za-z0-9_]", "", re.sub(r"[-\s]+", "_", props.connection_name)
            )
            .strip("_")
            .lower(),
        )


try:
    if SKIP_AIRBYTE:
        import warnings

        warnings.warn(
            "SKIP_AIRBYTE is set. Airbyte assets will not be loaded.", stacklevel=2
        )
        airbyte_assets = []
    else:
        airbyte_assets = build_airbyte_assets_definitions(
            workspace=airbyte_workspace,
            dagster_airbyte_translator=OLAirbyteTranslator(),
            connection_selector_fn=(
                lambda conn: conn.name.lower().endswith("s3 data lake")
            ),
        )
except Exception as e:  # noqa: BLE001
    # If Airbyte connection fails, create empty list to allow code to load
    import warnings

    warnings.warn(
        f"Failed to load Airbyte assets: {e}. Using empty asset list.", stacklevel=2
    )
    airbyte_assets = []

# This section creates a separate job and schedule for each Airbyte connection that will
# materialize the tables for that connection and any associated dbt staging models for
# those tables. The eager auto materialize policy will then take effect for any
# downstream dbt models that are dependent on those staging models being completed.
group_names: set[str] = set()
for assets_def in airbyte_assets:
    group_names.update(g for g in assets_def.group_names_by_key.values())

# Define a mapping of group_name to interval (6, 12 or 24 hours) on production
group_name_to_interval: dict[str, int] = {}
if DAGSTER_ENV == "production":
    group_name_to_interval = {
        "bootcamps_production_app_db__s3_data_lake": 24,
        "edxorg_production_course_structure_s3_data_lake": 24,
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
        "learn_ai_production__s3_data_lake": 6,
    }

airbyte_asset_jobs = []
airbyte_update_schedules = []
group_count = len(group_names)
for group_name in group_names:
    job = define_asset_job(
        name=f"sync_and_stage_{group_name}",
        selection=AssetSelection.groups(group_name)
        .downstream(depth=1, include_self=True)
        .required_multi_asset_neighbors(),
    )
    interval = group_name_to_interval.get(group_name, 24)  # default to 24 hours
    # No offset needed - K8s autoscaling handles concurrent syncs
    start_hour = 0

    # Compute explicit run hours (e.g. [0, 12] for 12-hour interval starting at 0)
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
    "dimensional",
}  # relevant dbt models to sync with superset
dbt_model_keys = full_dbt_project.keys

TRINO_SUPERSET_DATABASE_ID = int(os.environ.get("TRINO_SUPERSET_DATABASE_ID", "1"))
STARROCKS_SUPERSET_DATABASE_ID = int(
    os.environ.get(
        "STARROCKS_SUPERSET_DATABASE_ID",
        "3" if DAGSTER_ENV == "production" else "4",
    )
)
if TRINO_SUPERSET_DATABASE_ID == STARROCKS_SUPERSET_DATABASE_ID:
    msg = (
        f"TRINO_SUPERSET_DATABASE_ID ({TRINO_SUPERSET_DATABASE_ID}) and "
        f"STARROCKS_SUPERSET_DATABASE_ID ({STARROCKS_SUPERSET_DATABASE_ID}) "
        "must be different values. Check your environment configuration."
    )
    raise ValueError(msg)
_schema_base = "ol_warehouse_production"

superset_assets = [
    create_superset_asset(
        dbt_asset_group_name=key.path[0],
        dbt_model_name=key.path[1],
        database_id=TRINO_SUPERSET_DATABASE_ID,
        database_name="trino",
        schema_base=_schema_base,
    )
    for key in dbt_model_keys
    if key.path[0] in dbt_models_for_superset_datasets
]
superset_starrocks_assets = [
    create_superset_asset(
        dbt_asset_group_name=key.path[0],
        dbt_model_name=key.path[1],
        database_id=STARROCKS_SUPERSET_DATABASE_ID,
        database_name="starrocks",
        schema_base=_schema_base,
    )
    for key in dbt_model_keys
    if key.path[0] in dbt_models_for_superset_datasets
]

# Iceberg maintenance schedules — both default STOPPED; enable in production via
# the Dagster UI or Terraform after verifying the first manual run succeeds.
#
# 02:00 UTC: dbt layer (after nightly Airbyte syncs complete, before business hours)
# 03:00 UTC: raw layer (staggered to avoid concurrent Glue/S3 load with dbt layer)
iceberg_dbt_maintenance_schedule = ScheduleDefinition(
    name="iceberg_dbt_maintenance_nightly",
    job=define_asset_job(
        name="iceberg_dbt_maintenance_job",
        selection=AssetSelection.assets(iceberg_dbt_layer_maintenance),
    ),
    cron_schedule="0 2 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

iceberg_raw_maintenance_schedule = ScheduleDefinition(
    name="iceberg_raw_maintenance_nightly",
    job=define_asset_job(
        name="iceberg_raw_maintenance_job",
        selection=AssetSelection.assets(iceberg_raw_layer_maintenance),
    ),
    cron_schedule="0 3 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

# Regenerate dbt docs artifacts (manifest.json + catalog.json) for OpenMetadata
# once daily. Decoupled from model materialization because catalog generation
# recompiles the whole project and queries every relation. Default STOPPED; enable
# in production via the Dagster UI or Terraform after verifying the first run.
dbt_docs_artifacts_schedule = ScheduleDefinition(
    name="dbt_docs_artifacts_daily",
    job=dbt_docs_artifacts_job,
    cron_schedule="0 4 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

# Builds the b2b_analytics dbt models against StarRocks, then refreshes their
# downstream manual-refresh MVs. STOPPED by default -- enable in production via
# the Dagster UI after verifying the first manual run succeeds.
b2b_analytics_starrocks_schedule = ScheduleDefinition(
    name="b2b_analytics_starrocks_nightly",
    job=define_asset_job(
        name="b2b_analytics_starrocks_job",
        selection=AssetSelection.assets(b2b_analytics_starrocks_dbt_assets).downstream(
            include_self=True
        ),
    ),
    cron_schedule="0 4 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

# Instructor onboarding schedule
instructor_onboarding_schedule = ScheduleDefinition(
    name="instructor_onboarding_daily_schedule",
    job=define_asset_job(
        name="instructor_onboarding_daily_job",
        selection=AssetSelection.assets(
            generate_instructor_onboarding_user_list,
            update_access_forge_repo,
        ),
    ),
    cron_schedule="0 5 * * *",
    execution_timezone="UTC",
)

# Build resources dict, conditionally including airbyte
resources_dict = {
    "dbt": dbt_cli,
    "trino_maintenance": TrinoMaintenanceResource(
        host=os.environ.get("DAGSTER_TRINO_HOST", trino_host_map[DAGSTER_ENV]),
        catalog=os.environ.get("DAGSTER_TRINO_CATALOG", trino_catalog_map[DAGSTER_ENV]),
        vault=vault,
        # Vault KV-v1 path whose secret contains "username" and "password" keys
        # for the Trino service account.  Falls back to DBT_TRINO_USERNAME /
        # DBT_TRINO_PASSWORD env vars when vault_path is empty (local dev).
        vault_path=os.environ.get("DAGSTER_TRINO_VAULT_PATH", ""),
    ),
    "dbt_s3_artifacts": DbtS3ArtifactsResource(
        s3_bucket=os.environ.get("DBT_ARTIFACTS_S3_BUCKET", ""),
        s3_prefix=os.environ.get(
            "DBT_ARTIFACTS_S3_PREFIX", "openmetadata/dbt-artifacts"
        ),
    ),
    "vault": vault,
    "superset_api": SupersetApiClientFactory(deployment="superset", vault=vault),
    "github_api": GithubApiClientFactory(vault=vault),
    "starrocks": StarRocksResource(
        vault=vault,
        vault_mount_point=starrocks_vault_mount_map[DAGSTER_ENV],
        host=starrocks_host_map[DAGSTER_ENV],
        database="b2b_analytics",
    ),
}

if not SKIP_AIRBYTE:
    resources_dict["airbyte"] = airbyte_workspace

defs = Definitions(
    assets=[
        *with_source_code_references([full_dbt_project]),
        *with_source_code_references([b2b_analytics_starrocks_dbt_assets]),
        *airbyte_assets,
        *superset_assets,
        *superset_starrocks_assets,
        generate_instructor_onboarding_user_list,
        update_access_forge_repo,
        iceberg_dbt_layer_maintenance,
        iceberg_raw_layer_maintenance,
        refresh_starrocks_analytics_mvs,
    ],
    resources=resources_dict,
    sensors=[
        iceberg_snapshot_pointer_lag_sensor,
        AutomationConditionSensorDefinition(
            "dbt_automation_sensor",
            minimum_interval_seconds=14400,  # 4 hours - reduced from 1 hour
            # exclude staging as they are already handled by "sync_and_stage_" job
            target=(
                AssetSelection.assets(full_dbt_project)
                - AssetSelection.groups("staging")
            )
            | AssetSelection.groups("superset_dataset")
            | AssetSelection.groups("superset_starrocks_dataset"),
        ),
    ],
    jobs=[
        *airbyte_asset_jobs,
        iceberg_snapshot_pointer_repair_job,
        dbt_docs_artifacts_job,
    ],
    schedules=[
        *airbyte_update_schedules,
        instructor_onboarding_schedule,
        iceberg_dbt_maintenance_schedule,
        iceberg_raw_maintenance_schedule,
        dbt_docs_artifacts_schedule,
        b2b_analytics_starrocks_schedule,
    ],
)
