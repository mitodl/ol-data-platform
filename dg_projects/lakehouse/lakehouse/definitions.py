"""ELT assets for the data lakehouse."""

import os
import re
from datetime import timedelta

from dagster import (
    AssetCheckSeverity,
    AssetKey,
    AssetSelection,
    AssetSpec,
    AutomationConditionSensorDefinition,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    ScheduleDefinition,
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
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
from ol_orchestrate.lib.failures import with_failure_hooks
from ol_orchestrate.lib.sentry import init_sentry
from ol_orchestrate.lib.utils import authenticate_vault, unauthenticated_vault
from ol_orchestrate.resources.github import GithubApiClientFactory
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
from lakehouse.assets.lakehouse.dbt_starrocks import (
    starrocks_dbt_assets,
    starrocks_dbt_cli,
)
from lakehouse.assets.starrocks_mv_refresh import refresh_starrocks_analytics_mvs
from lakehouse.assets.superset import create_superset_asset
from lakehouse.lib.dbt_environment import DBT_AUTOMATION_ENABLED
from lakehouse.lib.scheduled_automation import schedules_for_environment
from lakehouse.resources.airbyte import AirbyteOSSWorkspace
from lakehouse.resources.dbt_s3_artifacts import DbtS3ArtifactsResource
from lakehouse.resources.starrocks import StarRocksResource
from lakehouse.resources.superset_api import SupersetApiClientFactory
from lakehouse.sensors import (
    iceberg_snapshot_pointer_lag_sensor,
    iceberg_snapshot_pointer_repair_job,
)

init_sentry("lakehouse")

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

# QA and Production each run their own, entirely separate Vault deployment, so
# the mount name itself doesn't carry an env suffix -- the Vault server (not
# the mount path) is what scopes the environment. Matches the "database-starrocks"
# mount convention used by bin/starrocks-auth and ol_dbt_cli/commands/starrocks.py.
STARROCKS_VAULT_MOUNT = "database-starrocks"

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
    vault = unauthenticated_vault(VAULT_ADDRESS)
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
        # Attach to a sync that is already in flight rather than raising. The
        # automation condition and Airbyte's own scheduler both launch syncs, so
        # a tick landing on top of a running sync is routine, not exceptional --
        # left at the library default of False it raised "Found sync job for
        # connection_id=... already running" across ten connections.
        poll_previous_running_sync=True,
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
#
# That last sentence now holds only in DBT_AUTOMATION_ENVIRONMENTS. Outside it,
# nothing downstream carries an AutomationCondition, so one of these
# runs builds its staging models and stops -- deliberately, since a QA build of a
# union model emits data that looks fine while silently dropping rows. It also
# means starting one of these in QA cannot walk the graph to a full build; RFC
# 12711 step 8 is what adds `qa` to DBT_AUTOMATION_ENVIRONMENTS.
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

# Builds the tag:starrocks dbt models against StarRocks, then refreshes their
# downstream manual-refresh MVs. STOPPED by default -- enable in production via
# the Dagster UI after verifying the first manual run succeeds.
b2b_analytics_starrocks_schedule = ScheduleDefinition(
    name="b2b_analytics_starrocks_nightly",
    job=define_asset_job(
        name="b2b_analytics_starrocks_job",
        selection=AssetSelection.assets(starrocks_dbt_assets).downstream(
            include_self=True
        ),
    ),
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

# MIT Learn delivery chain: refresh the dbt models the mit_learn_delivery
# webhook assets read, between the dlt ingests and the delivery POSTs.
#
# Without this the delivery assets POST whatever was last materialized. The gap
# is not obvious from any one file, so, concretely: the dlt ingest schedules in
# the data_loading location materialize ONLY the raw tables (03:00-04:00 UTC);
# `dbt_automation_sensor` below covers the integrations models but explicitly
# excludes the `staging` group; and the `sync_and_stage_*` jobs that are meant
# to cover staging are built from the Airbyte source groups, so they never touch
# these dlt-sourced staging models. That leaves stg__mit_climate__*,
# stg__mitpe__*, stg__oll__*, stg__edxorg__discovery__api__programs and
# stg__podcast__rss__* with no scheduled materialization at all, and the
# integrations models above them reading yesterday's staging.
#
# These live in the lakehouse location because a Dagster job cannot span code
# locations -- the delivery assets are in `learning_resources` and cannot
# materialize dbt assets defined here, so the refresh has to be driven from
# this side rather than by widening the delivery schedules.
#
# Keys are listed explicitly rather than selected by group: `integrations` and
# `staging` both hold many models unrelated to MIT Learn, and `.upstream()`
# from the integrations models would pull in the whole edxorg lineage.
LEARN_DELIVERY_MODEL_KEYS = [
    # staging
    AssetKey(["staging", "mit_climate", "stg__mit_climate__api__articles"]),
    AssetKey(["staging", "mitpe", "stg__mitpe__api__courses"]),
    AssetKey(["staging", "oll", "stg__oll__google_sheets__courses"]),
    AssetKey(["staging", "edxorg", "stg__edxorg__discovery__api__programs"]),
    AssetKey(["staging", "podcast", "stg__podcast__rss__channels"]),
    AssetKey(["staging", "podcast", "stg__podcast__rss__episodes"]),
    # integrations
    AssetKey(["integrations", "learn", "integrations__learn__mit_climate_articles"]),
    AssetKey(["integrations", "learn", "integrations__learn__mitpe_courses"]),
    AssetKey(["integrations", "learn", "integrations__learn__oll_courses"]),
    AssetKey(["integrations", "learn", "integrations__learn__mit_edx_programs"]),
    AssetKey(["integrations", "learn", "integrations__learn__podcasts"]),
    AssetKey(["integrations", "learn", "integrations__learn__podcast_episodes"]),
]

# 05:00 UTC sits after the last dlt ingest (podcast_rss at 04:00) and before the
# first delivery POST (mit_climate at 06:00). STOPPED by default, matching the
# delivery schedules it feeds -- enabling this without enabling those is safe
# and is the right order to turn them on.
learn_delivery_models_schedule = ScheduleDefinition(
    name="learn_delivery_models_daily",
    job=define_asset_job(
        name="learn_delivery_models_job",
        selection=AssetSelection.assets(*LEARN_DELIVERY_MODEL_KEYS),
    ),
    cron_schedule="0 5 * * *",
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
        vault_mount_point=STARROCKS_VAULT_MOUNT,
        host=starrocks_host_map[DAGSTER_ENV],
        database="b2b_analytics",
    ),
    # Separate from "dbt" (pinned to a Trino target) -- see dbt_starrocks.py.
    "starrocks_dbt": starrocks_dbt_cli,
}

if not SKIP_AIRBYTE:
    resources_dict["airbyte"] = airbyte_workspace

# Freshness checks on the layers the business actually reads. Deliberately not
# applied to every dbt asset: staging and intermediate models are refreshed on
# their own cadences and would generate far more noise than signal.
FRESHNESS_CHECKED_GROUPS = {"mart", "reporting", "dimensional"}
freshness_checked_assets = [
    key for key in dbt_model_keys if key.path[0] in FRESHNESS_CHECKED_GROUPS
]

# 26 hours, against the 24-hour default sync cadence -- a nightly build that
# runs a little late must not page anyone.
dbt_layer_freshness_checks = build_last_update_freshness_checks(
    assets=freshness_checked_assets,
    lower_bound_delta=timedelta(hours=26),
    severity=AssetCheckSeverity.ERROR,
)

dbt_layer_freshness_sensor = build_sensor_for_freshness_checks(
    freshness_checks=dbt_layer_freshness_checks,
    name="dbt_layer_freshness_sensor",
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
)

defs = Definitions(
    assets=with_failure_hooks(
        [
            *with_source_code_references([full_dbt_project]),
            *with_source_code_references([starrocks_dbt_assets]),
            *airbyte_assets,
            *superset_assets,
            *superset_starrocks_assets,
            generate_instructor_onboarding_user_list,
            update_access_forge_repo,
            iceberg_dbt_layer_maintenance,
            iceberg_raw_layer_maintenance,
            refresh_starrocks_analytics_mvs,
        ]
    ),
    asset_checks=dbt_layer_freshness_checks,
    resources=resources_dict,
    sensors=[
        iceberg_snapshot_pointer_lag_sensor,
        dbt_layer_freshness_sensor,
        AutomationConditionSensorDefinition(
            "dbt_automation_sensor",
            minimum_interval_seconds=14400,  # 4 hours - reduced from 1 hour
            # Declared rather than left to the instance, which is how a QA code
            # location came to build the production warehouse unattended. This
            # only seeds the state on first deploy -- what enforces it is that
            # outside DBT_AUTOMATION_ENVIRONMENTS the assets carry no
            # AutomationCondition at all.
            default_status=(
                DefaultSensorStatus.RUNNING
                if DBT_AUTOMATION_ENABLED
                else DefaultSensorStatus.STOPPED
            ),
            # exclude staging as they are already handled by "sync_and_stage_" job
            #
            # Note what that exclusion costs in production, where staging models
            # DO carry a condition: get_default_automation_condition_sensor_target
            # takes every conditioned key this selection does not cover and
            # synthesizes `default_automation_condition_sensor` over
            # AssetSelection.all() minus this one. So staging is automatable there
            # via a sensor no one declared. Pre-existing and STOPPED unless
            # started by hand, but the same invisible instance state
            # DBT_AUTOMATION_ENVIRONMENTS exists to remove -- see the open
            # question recorded beside it.
            #
            # Where automation is off the question does not arise: no asset
            # carries a condition, so there is nothing to synthesize over.
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
    # Registration is the gate. `default_status=DefaultScheduleStatus.STOPPED`
    # on each of these only seeds the instance's instigator state on first
    # deploy; a UI toggle overrides it forever after, so whether one of these
    # ticked in QA was instance state nothing in this file had a say in. A
    # schedule this filter drops is not stopped, it is absent -- there is
    # nothing left to toggle. Note it also drops the job for the four that
    # build one inline; see scheduled_automation for what that does and does
    # not cost.
    schedules=schedules_for_environment(
        [
            *(("daily_sync_and_stage", s) for s in airbyte_update_schedules),
            ("instructor_onboarding_daily_schedule", instructor_onboarding_schedule),
            ("iceberg_dbt_maintenance_nightly", iceberg_dbt_maintenance_schedule),
            ("iceberg_raw_maintenance_nightly", iceberg_raw_maintenance_schedule),
            ("dbt_docs_artifacts_daily", dbt_docs_artifacts_schedule),
            ("b2b_analytics_starrocks_nightly", b2b_analytics_starrocks_schedule),
            ("learn_delivery_models_daily", learn_delivery_models_schedule),
        ]
    ),
)
