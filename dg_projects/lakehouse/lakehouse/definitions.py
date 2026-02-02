"""ELT assets for the data lakehouse."""

import os
import re
from pathlib import Path

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
    DbtProject,
)
from dagster_dlt import DagsterDltResource
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.utils import authenticate_vault
from ol_orchestrate.resources.github import GithubApiClientFactory
from ol_orchestrate.resources.secrets.vault import Vault

from lakehouse.assets.instructor_onboarding import (
    generate_instructor_onboarding_user_list,
    update_access_forge_repo,
)
from lakehouse.assets.lakehouse.dbt import DBT_REPO_DIR, full_dbt_project
from lakehouse.assets.superset import create_superset_asset
from lakehouse.resources.airbyte import AirbyteOSSWorkspace
from lakehouse.resources.superset_api import SupersetApiClientFactory

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

# Determine dagster URL and dbt target based on environment
if DAGSTER_ENV == "dev":
    dagster_url = "http://localhost:3000"
    dbt_target = "dev_production"
elif DAGSTER_ENV == "ci":
    dagster_url = "https://pipelines-ci.odl.mit.edu"
    dbt_target = "ci"
else:
    dagster_url = (
        "https://pipelines.odl.mit.edu"
        if DAGSTER_ENV == "production"
        else "https://pipelines-qa.odl.mit.edu"
    )
    dbt_target = "production"

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
    dbt_target = "dev_production"

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
    "target": dbt_target,
}
dbt_cli = DbtCliResource(**dbt_config)

# Initialize dbt project - handle both local dev and Docker paths
if Path("/app/ol_dbt").exists():
    # In Docker container
    DBT_PROJECT_DIR = Path("/app/ol_dbt")
else:
    # Local development
    DBT_PROJECT_DIR = Path(__file__).resolve().parents[3] / "src" / "ol_dbt"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)


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

superset_assets = [
    create_superset_asset(dbt_asset_group_name=key.path[0], dbt_model_name=key.path[1])
    for key in dbt_model_keys
    if key.path[0] in dbt_models_for_superset_datasets
]

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
    "vault": vault,
    "superset_api": SupersetApiClientFactory(deployment="superset", vault=vault),
    "github_api": GithubApiClientFactory(vault=vault),
    "dlt": DagsterDltResource(),  # dlt resource for data ingestion
}

if not SKIP_AIRBYTE:
    resources_dict["airbyte"] = airbyte_workspace

# Load dlt assets
# The dlt source gets config from .dlt/config.toml and
# secrets.toml at runtime
try:
    from dagster import AssetExecutionContext
    from dagster_dlt import dlt_assets

    from lakehouse.defs.edxorg_s3_ingestion.loads import (
        edxorg_s3_pipeline,
        edxorg_s3_source,
    )

    # Create the source with no tables specified - dlt will get config from .dlt files
    # This needs to be called to create the DltSource instance that dlt_assets expects
    edxorg_source_instance = edxorg_s3_source()

    @dlt_assets(
        dlt_source=edxorg_source_instance,
        dlt_pipeline=edxorg_s3_pipeline,
        name="edxorg_s3_ingestion",
        group_name="ingestion",
    )
    def edxorg_s3_dlt_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
        """Load EdX.org database table TSV exports from S3 to lakehouse.

        This asset uses dlt (data load tool) to:
        1. Read TSV files from S3 that are generated by edxorg_archive asset
        2. Process and chunk the data for memory efficiency
        3. Write to Iceberg tables with metadata tracking
        """
        yield from dlt.run(context=context)

    dlt_assets_list = [edxorg_s3_dlt_assets]
except Exception as e:  # noqa: BLE001
    # If dlt assets fail to load (e.g., missing credentials at definition time),
    # continue without them. They can be configured at runtime.
    import warnings

    warnings.warn(
        f"Failed to load dlt assets: {e}. "
        "Configure .dlt/config.toml and .dlt/secrets.toml to enable. "
        "Using empty dlt asset list.",
        stacklevel=2,
    )
    dlt_assets_list = []

defs = Definitions(
    assets=[
        *with_source_code_references([full_dbt_project]),
        *airbyte_assets,
        *superset_assets,
        *dlt_assets_list,
        generate_instructor_onboarding_user_list,
        update_access_forge_repo,
    ],
    resources=resources_dict,
    sensors=[
        AutomationConditionSensorDefinition(
            "dbt_automation_sensor",
            minimum_interval_seconds=14400,  # 4 hours - reduced from 1 hour
            # exclude staging as they are already handled by "sync_and_stage_" job
            target=(
                AssetSelection.assets(full_dbt_project)
                - AssetSelection.groups("staging")
            )
            | AssetSelection.groups("superset_dataset"),
        ),
    ],
    jobs=[*airbyte_asset_jobs],
    schedules=[*airbyte_update_schedules, instructor_onboarding_schedule],
)
