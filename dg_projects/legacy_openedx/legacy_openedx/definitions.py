"""
Legacy Open edX data extraction definitions.

This code location contains the original Open edX course data extraction jobs
that were defined using repository-based patterns. These include:
1. GCS course tarball sync (edx.org courses from Simeon)
2. IRx course data exports for 3 deployments (mitx, xpro, mitxonline)
"""

import os
from typing import Literal

from dagster import Definitions, RunRequest, ScheduleEvaluationContext, schedule
from dagster_aws.s3 import S3Resource
from dagster_aws.s3.resources import s3_resource
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.utils import authenticate_vault
from ol_orchestrate.resources.gcp_gcs import GCSConnection
from ol_orchestrate.resources.openedx import OpenEdxApiClient
from ol_orchestrate.resources.outputs import DailyResultsDir
from ol_orchestrate.resources.secrets.vault import Vault

from legacy_openedx.jobs.open_edx import edx_course_pipeline
from legacy_openedx.resources.healthchecks import HealthchecksIO
from legacy_openedx.resources.mysql_db import VaultMySQLClientFactory
from legacy_openedx.resources.sqlite_db import sqlite_db_resource

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

# Initialize GCS connection with resilient loading
try:
    if vault_authenticated:
        gcs_connection = GCSConnection(
            **vault.client.secrets.kv.v1.read_secret(
                mount_point="secret-data", path="pipelines/edx/org/gcp-oauth-client"
            )["data"]
        )
    else:
        # Mock GCS connection for testing
        gcs_connection = GCSConnection(
            project_id="test-project",
            client_email="test@test.iam.gserviceaccount.com",
            client_id="123456",
            client_x509_cert_url="https://test.com/cert",
            private_key="-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----",  # pragma: allowlist secret  # noqa: E501
            private_key_id="test",
            auth_uri="https://accounts.google.com/o/oauth2/auth",
            token_uri="https://oauth2.googleapis.com/token",  # noqa: S106 (not a password)
        )
except Exception as e:  # noqa: BLE001
    import warnings

    warnings.warn(
        f"Failed to initialize GCS connection: {e}. Using mock.", stacklevel=2
    )
    gcs_connection = GCSConnection(
        project_id="test-project",
        client_email="test@test.iam.gserviceaccount.com",
        client_id="123456",
        client_x509_cert_url="https://test.com/cert",
        private_key="-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----",  # pragma: allowlist secret  # noqa: E501
        private_key_id="test",
        auth_uri="https://accounts.google.com/o/oauth2/auth",
        token_uri="https://oauth2.googleapis.com/token",  # noqa: S106 (not a password)
    )

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "qa")

# ============================================================================
# GCS Course Tarball Sync (from Simeon)
# ============================================================================

course_upload_bucket = {
    "ci": "edxorg-ci-edxapp-courses",
    "qa": "edxorg-qa-edxapp-courses",
    "production": "edxorg-production-edxapp-courses",
}


# ============================================================================
# IRx Open edX Course Data Exports
# ============================================================================


def open_edx_export_irx_job_config(
    deployment: Literal["mitx", "mitxonline", "xpro"],
    dagster_env: Literal["dev", "ci", "qa", "production"],
):
    pipeline_path = "residential" if deployment == "mitx" else deployment
    etl_bucket_map = {
        "mitx": f"mitx-etl-residential-live-mitx-{dagster_env}",
        "xpro": f"mitx-etl-xpro-{dagster_env}-mitxpro-{dagster_env}",
        "mitxonline": f"mitx-etl-mitxonline-{dagster_env}",
    }

    mongo_creds = vault.client.secrets.kv.v1.read_secret(
        mount_point=f"secret-{deployment}", path="mongodb-forum"
    )["data"]
    mongo_config = {
        "edx_mongodb_username": mongo_creds["username"],
        "edx_mongodb_password": mongo_creds["password"],
        "edx_mongodb_auth_db": "admin",
        "edx_mongodb_forum_database_name": "forum",
        "edx_mongodb_uri": mongo_creds["uri"],
    }

    healthcheck_id = vault.client.secrets.kv.v1.read_secret(
        mount_point="secret-data",
        path=f"pipelines/edx/{pipeline_path}/healthchecks-io-check-id",
    )["data"]["value"]

    edx_creds = vault.client.secrets.kv.v1.read_secret(
        mount_point="secret-data",
        path=f"pipelines/edx/{pipeline_path}/edx-oauth-client",
    )["data"]
    edx_resource_config = {
        "client_id": edx_creds["id"],
        "client_secret": edx_creds["secret"],
        "base_url": edx_creds["url"],
        "studio_url": edx_creds["studio_url"],
        "token_type": "JWT",
        "token_url": f"{edx_creds['url']}/oauth2/access_token",
    }

    db_hostname = (
        f"edxapp-db-{deployment}-{dagster_env}"
        f"{'-replica' if dagster_env == 'production' else ''}"
        ".cbnm7ajau6mi.us-east-1.rds.amazonaws.com"
    )
    edx_db_config = {
        "mysql_db_name": "edxapp",
        "mysql_hostname": db_hostname,
        "vault_mount_point": f"mariadb-{deployment}",
        "vault_role": "readonly",
    }

    return {
        "ops": {
            "collect_edx_course_exports": {
                "config": {
                    "edx_course_bucket": f"{deployment}-{dagster_env}-edxapp-courses",
                }
            },
            "edx_upload_daily_extracts": {
                "config": {"edx_etl_results_bucket": etl_bucket_map[deployment]}
            },
            "export_edx_forum_database": {
                "config": mongo_config,
            },
        },
        "resources": {
            "openedx": {"config": edx_resource_config},
            "healthchecks": {"config": {"check_id": healthcheck_id}},
            "results_dir": {
                "config": {"date_format": "%Y%m%d", "dir_prefix": deployment}
            },
            "sqldb": {"config": edx_db_config},
        },
    }


# Resource definitions for IRx jobs
qa_resources = {
    "sqldb": sqlite_db_resource,
    "s3": S3Resource(),
    "results_dir": DailyResultsDir.configure_at_launch(),
    "healthchecks": HealthchecksIO.configure_at_launch(),
    "openedx": OpenEdxApiClient.configure_at_launch(),
}

production_resources = {
    "sqldb": VaultMySQLClientFactory.configure_at_launch(),
    "vault": vault,
    "s3": S3Resource(),
    "results_dir": DailyResultsDir.configure_at_launch(),
    "healthchecks": HealthchecksIO.configure_at_launch(),
    "openedx": OpenEdxApiClient.configure_at_launch(),
}

# Create jobs for each deployment.
# NOTE: config is intentionally omitted here — it is generated at schedule-fire time
# (not at code-location load time) so that Vault dynamic DB credentials are always
# fresh when the job runs.  See the @schedule definitions below.
residential_edx_job = edx_course_pipeline.to_job(
    name="residential_edx_course_pipeline",
    resource_defs=production_resources,
)

xpro_edx_job = edx_course_pipeline.to_job(
    name="xpro_edx_course_pipeline",
    resource_defs=production_resources,
)

mitxonline_edx_job = edx_course_pipeline.to_job(
    name="mitxonline_edx_course_pipeline",
    resource_defs=production_resources,
)


# Schedules
# ---------
# Config (including Vault dynamic DB credentials) is generated inside each schedule
# function so it is fetched at schedule-evaluation time, not at code-location load
# time.  Vault database credentials have a short TTL (~1 h).  Baking them into the
# static job config via `to_job(config=...)` caused "Access denied" failures when the
# Dagster daemon had been running longer than the credential TTL before the daily job
# fired.


@schedule(
    job=residential_edx_job,
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def residential_edx_daily_schedule(_context: ScheduleEvaluationContext) -> RunRequest:
    """Daily schedule for the residential (MITx) edX course pipeline."""
    return RunRequest(
        run_key="residential_edx_course_pipeline",
        run_config=open_edx_export_irx_job_config("mitx", DAGSTER_ENV),
        tags={"business_unit": "residential"},
    )


@schedule(
    job=xpro_edx_job,
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def xpro_edx_daily_schedule(_context: ScheduleEvaluationContext) -> RunRequest:
    """Daily schedule for the xPRO edX course pipeline."""
    return RunRequest(
        run_key="xpro_edx_course_pipeline",
        run_config=open_edx_export_irx_job_config("xpro", DAGSTER_ENV),
        tags={"business_unit": "mitxpro"},
    )


@schedule(
    job=mitxonline_edx_job,
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def mitxonline_edx_daily_schedule(_context: ScheduleEvaluationContext) -> RunRequest:
    """Daily schedule for the MITx Online edX course pipeline."""
    return RunRequest(
        run_key="mitxonline_edx_course_pipeline",
        run_config=open_edx_export_irx_job_config("mitxonline", DAGSTER_ENV),
        tags={"business_unit": "mitxonline"},
    )


# Create unified definitions
defs = Definitions(
    jobs=[residential_edx_job, xpro_edx_job, mitxonline_edx_job],
    schedules=[
        residential_edx_daily_schedule,
        xpro_edx_daily_schedule,
        mitxonline_edx_daily_schedule,
    ],
    resources={
        "gcp_gcs": gcs_connection,
        "s3": s3_resource,
        "results_dir": DailyResultsDir.configure_at_launch(),
    },
)
