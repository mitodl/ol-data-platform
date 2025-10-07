"""OpenEdX data extraction and tracking log normalization definitions."""

import os
from datetime import UTC, datetime
from functools import partial
from typing import Any, Literal

from dagster import (
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    SensorDefinition,
    daily_partitioned_config,
)
from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource

from ol_orchestrate.lib.assets_helper import (
    add_prefix_to_asset_keys,
    late_bind_partition_to_asset,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, OPENEDX_DEPLOYMENTS, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import (
    default_file_object_io_manager,
    default_io_manager,
)
from ol_orchestrate.partitions.openedx import OPENEDX_COURSE_RUN_PARTITIONS
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory
from ol_orchestrate.resources.secrets.vault import Vault
from openedx.assets.openedx import (
    course_structure,
    course_xml,
    extract_courserun_details,
    openedx_live_courseware,
)
from openedx.jobs.normalize_logs import (
    jsonify_tracking_logs,
    normalize_tracking_logs,
)
from openedx.sensors.openedx import course_run_sensor, course_version_sensor

# Initialize vault with resilient loading
try:
    if DAGSTER_ENV == "dev":
        vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
        vault._auth_github()  # noqa: SLF001
        vault_authenticated = True
    else:
        vault = Vault(
            vault_addr=VAULT_ADDRESS, vault_role="dagster-server", aws_auth_mount="aws"
        )
        vault._auth_aws_iam()  # noqa: SLF001
        vault_authenticated = True
except Exception as e:  # noqa: BLE001 (resilient loading)
    import warnings

    warnings.warn(
        f"Failed to authenticate with Vault: {e}. Using mock configuration.",
        stacklevel=2,
    )
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault_authenticated = False

dagster_env: Literal["dev", "qa", "production"] = os.environ.get(  # type: ignore  # noqa: PGH003
    "DAGSTER_ENVIRONMENT", DAGSTER_ENV
)


def s3_uploads_bucket(
    dagster_env: Literal["dev", "qa", "production"],
) -> dict[str, Any]:
    bucket_map = {
        "dev": {"bucket": "ol-devops-sandbox", "prefix": "pipeline-storage"},
        "qa": {"bucket": "ol-data-lake-landing-zone-qa", "prefix": ""},
        "production": {
            "bucket": "ol-data-lake-landing-zone-production",
            "prefix": "",
        },
    }
    return bucket_map[dagster_env]


def earliest_log_date(
    dagster_env: Literal["dev", "qa", "production"],
    deployment_name: Literal["mitx", "mitxonline", "xpro"],
) -> datetime:
    if dagster_env == "dev":
        dagster_env = "qa"
    date_map = {
        "qa": {
            "mitx": datetime(2021, 3, 10, tzinfo=UTC),
            "mitxonline": datetime(2021, 7, 26, tzinfo=UTC),
            "xpro": datetime(2021, 3, 31, tzinfo=UTC),
            "edxorg": datetime(2023, 10, 10, tzinfo=UTC),
        },
        "production": {
            "mitx": datetime(2017, 6, 13, tzinfo=UTC),
            "mitxonline": datetime(2021, 8, 18, tzinfo=UTC),
            "xpro": datetime(2019, 8, 28, tzinfo=UTC),
            "edxorg": datetime(2019, 8, 28, tzinfo=UTC),
        },
    }
    return date_map[dagster_env][deployment_name]


def daily_tracking_log_config(
    deployment, destination, log_date: datetime, _end: datetime
):
    """Generate configuration for daily tracking log jobs."""
    log_bucket = s3_uploads_bucket(dagster_env)["bucket"]
    if dagster_env == "dev":
        return {
            "resources": {
                "duckdb": {
                    "config": {
                        "database": f"/tmp/{deployment}_{dagster_env}_tracking.duckdb"  # noqa: S108
                    }
                }
            },
            "ops": {
                "extract_logs": {
                    "config": {
                        "source_bucket": log_bucket,
                        "source_prefix": f"test_tracking_logs/{deployment}",
                        "log_date": f"{log_date.strftime('%Y-%m-%d')}",
                    }
                },
                f"load_{destination}_logs": {
                    "config": {
                        "destination_bucket": log_bucket,
                        "destination_prefix": f"{deployment}_tracking/{destination}",
                    }
                },
            },
        }
    return {
        "resources": {
            "duckdb": {
                "config": {
                    "database": f"/tmp/{deployment}_{dagster_env}_tracking.duckdb"  # noqa: S108
                }
            }
        },
        "ops": {
            "extract_logs": {
                "config": {
                    "source_bucket": log_bucket,
                    "source_prefix": f"tracking_logs/{deployment}",
                    "log_date": f"{log_date.strftime('%Y-%m-%d')}",
                }
            },
            f"load_{destination}_logs": {
                "config": {
                    "destination_bucket": log_bucket,
                    "destination_prefix": f"{deployment}_tracking/{destination}",
                }
            },
            "fix_json_structure": {
                "inputs": {
                    "log_date": f"{log_date.strftime('%Y-%m-%d')}",
                },
            },
        },
    }


# Build assets for each deployment
all_deployment_assets = []
all_deployment_sensors = []
all_deployment_resources = {}

for deployment_name in OPENEDX_DEPLOYMENTS:
    course_version_asset = late_bind_partition_to_asset(
        add_prefix_to_asset_keys(openedx_live_courseware, deployment_name),
        OPENEDX_COURSE_RUN_PARTITIONS[deployment_name],
    )
    deployment_assets = [
        course_version_asset,
        add_prefix_to_asset_keys(course_structure, deployment_name),
        add_prefix_to_asset_keys(course_xml, deployment_name),
        add_prefix_to_asset_keys(extract_courserun_details, deployment_name),
    ]

    asset_bound_course_version_sensor = SensorDefinition(
        name=f"{deployment_name}_course_version_sensor",
        asset_selection=[course_version_asset],
        job=None,
        default_status=DefaultSensorStatus.STOPPED,
        minimum_interval_seconds=60 * 60,
        evaluation_fn=course_version_sensor,
    )

    deployment_sensors = [
        course_run_sensor,
        asset_bound_course_version_sensor,
        AutomationConditionSensorDefinition(
            f"{deployment_name}_openedx_automation_sensor",
            minimum_interval_seconds=300 if DAGSTER_ENV == "dev" else 60 * 60,
            target=deployment_assets,
        ),
    ]

    all_deployment_assets.extend(deployment_assets)
    all_deployment_sensors.extend(deployment_sensors)

    # Add deployment-specific resources
    all_deployment_resources[f"openedx_{deployment_name}"] = OpenEdxApiClientFactory(
        deployment=deployment_name, vault=vault
    )

# Build normalize/jsonify jobs
normalize_jobs = [
    normalize_tracking_logs.to_job(
        config=daily_partitioned_config(
            start_date=earliest_log_date(dagster_env, deployment)  # type: ignore  # noqa: PGH003
        )(partial(daily_tracking_log_config, deployment, "valid")),
        name=f"normalize_{deployment}_{dagster_env}_tracking_logs",
    )
    for deployment in ["xpro", "mitx", "mitxonline", "edxorg"]
]

jsonify_jobs = [
    jsonify_tracking_logs.to_job(
        config=daily_partitioned_config(
            start_date=earliest_log_date(dagster_env, deployment)  # type: ignore  # noqa: PGH003
        )(partial(daily_tracking_log_config, deployment, "logs")),
        name=f"jsonify_{deployment}_{dagster_env}_tracking_logs",
    )
    for deployment in ["xpro", "mitx", "mitxonline", "edxorg"]
]

# Combine all resources
all_resources = {
    "io_manager": default_io_manager(DAGSTER_ENV),
    "s3file_io_manager": default_file_object_io_manager(
        dagster_env=DAGSTER_ENV,
        bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
        path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
    ),
    "vault": vault,
    "s3": S3Resource(),
    "duckdb": DuckDBResource.configure_at_launch(),
    **all_deployment_resources,
}

# Create unified definitions
defs = Definitions(
    assets=all_deployment_assets,
    resources=all_resources,
    sensors=all_deployment_sensors,
    jobs=normalize_jobs + jsonify_jobs,
)
