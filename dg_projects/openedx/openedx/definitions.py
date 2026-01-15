"""OpenEdX data extraction and tracking log normalization definitions."""

import os
from datetime import UTC, datetime
from functools import partial
from typing import Any, Literal

from dagster import (
    daily_partitioned_config,
)
from dagster._core.definitions.definitions_class import (
    create_repository_using_definitions_args,
)
from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import (
    default_file_object_io_manager,
    default_io_manager,
)
from ol_orchestrate.lib.utils import authenticate_vault
from ol_orchestrate.resources.api_client_factory import ApiClientFactory
from ol_orchestrate.resources.secrets.vault import Vault

from openedx.components import OpenEdxDeploymentComponent
from openedx.jobs.normalize_logs import (
    jsonify_tracking_logs,
    normalize_tracking_logs,
)

# Initialize vault with resilient loading
try:
    vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)
    vault_authenticated = True
except Exception as e:  # noqa: BLE001 (resilient loading)
    import warnings

    warnings.warn(
        f"Failed to authenticate with Vault: {e}. Using mock configuration.",
        stacklevel=2,
    )
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault_authenticated = False

dagster_env: Literal["dev", "ci", "qa", "production"] = os.environ.get(  # type: ignore  # noqa: PGH003
    "DAGSTER_ENVIRONMENT", DAGSTER_ENV
)


def s3_uploads_bucket(
    dagster_env: Literal["dev", "ci", "qa", "production"],
) -> dict[str, Any]:
    bucket_map = {
        "dev": {"bucket": "ol-devops-sandbox", "prefix": "pipeline-storage"},
        "ci": {"bucket": "ol-devops-sandbox", "prefix": "pipeline-storage-ci"},
        "qa": {"bucket": "ol-data-lake-landing-zone-qa", "prefix": ""},
        "production": {
            "bucket": "ol-data-lake-landing-zone-production",
            "prefix": "",
        },
    }
    return bucket_map[dagster_env]


def earliest_log_date(
    dagster_env: Literal["dev", "ci", "qa", "production"],
    deployment_name: Literal["mitx", "mitxonline", "xpro"],
) -> datetime:
    if dagster_env in ("dev", "ci"):
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
    if dagster_env in ("dev", "ci"):
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


# Build shared resources (these will be used across all deployments)
shared_resources = {
    "io_manager": default_io_manager(DAGSTER_ENV),
    "s3file_io_manager": default_file_object_io_manager(
        dagster_env=DAGSTER_ENV,
        bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
        path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
    ),
    "vault": vault,
    "s3": S3Resource(),
    "duckdb": DuckDBResource.configure_at_launch(),
    "learn_api": ApiClientFactory(
        deployment="mit-learn",
        client_class="MITLearnApiClient",
        mount_point="secret-global",
        config_path="shared_hmac",
        kv_version="2",
        vault=vault,
    ),
}

# Create a repository for each OpenEdX deployment
# Each repository has its own isolated resource namespace where "openedx" maps
# to the deployment-specific OpenEdxApiClientFactory

# Build normalize/jsonify jobs (these are shared across deployments)
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


# Helper function to create a repository for a deployment
def _create_deployment_repository(
    deployment_name: Literal["mitx", "mitxonline", "xpro", "edxorg"],
):
    """Create a repository for a specific OpenEdX deployment."""
    component = OpenEdxDeploymentComponent(deployment_name=deployment_name, vault=vault)
    deployment_defs = component.build_definitions(shared_resources=shared_resources)

    return create_repository_using_definitions_args(
        name=f"{deployment_name}_openedx",
        assets=deployment_defs.assets,
        sensors=deployment_defs.sensors,
        resources=deployment_defs.resources,
    )


# Create individual repositories as module-level variables
# Dagster will automatically discover these when loading the code location
mitx_openedx = _create_deployment_repository("mitx")
mitxonline_openedx = _create_deployment_repository("mitxonline")
xpro_openedx = _create_deployment_repository("xpro")

# Create a repository for shared jobs
openedx_shared_jobs = create_repository_using_definitions_args(
    name="openedx_shared_jobs",
    jobs=normalize_jobs + jsonify_jobs,
    resources=shared_resources,
)
