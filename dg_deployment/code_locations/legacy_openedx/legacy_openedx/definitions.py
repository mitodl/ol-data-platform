"""
Legacy Open edX data extraction definitions.

This code location contains the original Open edX course data extraction jobs
that were defined using repository-based patterns. These include:
1. GCS course tarball sync (edx.org courses from Simeon)
2. IRx course data exports for 3 deployments (mitx, xpro, mitxonline)
"""

import os
from functools import partial
from typing import Literal

from dagster import DefaultSensorStatus, Definitions, SensorDefinition
from dagster_aws.s3 import S3Resource
from dagster_aws.s3.resources import s3_resource

from legacy_openedx.jobs.edx_gcs_courses import sync_gcs_to_s3
from ol_orchestrate.jobs.open_edx import edx_course_pipeline
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.resources.gcp_gcs import GCSConnection
from ol_orchestrate.resources.healthchecks import HealthchecksIO
from ol_orchestrate.resources.mysql_db import mysql_db_resource
from ol_orchestrate.resources.openedx import OpenEdxApiClient
from ol_orchestrate.resources.outputs import DailyResultsDir
from ol_orchestrate.resources.secrets.vault import Vault
from ol_orchestrate.resources.sqlite_db import sqlite_db_resource
from ol_orchestrate.schedules.open_edx import (
    mitxonline_edx_daily_schedule,
    residential_edx_daily_schedule,
    xpro_edx_daily_schedule,
)
from ol_orchestrate.sensors.object_storage import gcs_multi_file_sensor

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
            service_account_json='{"type": "service_account"}',
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
        service_account_json='{"type": "service_account"}',
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
    "qa": "edxorg-qa-edxapp-courses",
    "production": "edxorg-production-edxapp-courses",
}

gcs_sync_job = sync_gcs_to_s3.to_job(
    name="edx_gcs_course_retrieval",
    config={
        "ops": {
            "edx_upload_gcs_course_tarballs": {
                "config": {
                    "edx_etl_results_bucket": course_upload_bucket[dagster_deployment]
                }
            }
        }
    },
)

gcs_course_sensor = SensorDefinition(
    evaluation_fn=partial(gcs_multi_file_sensor, "simeon-mitx-course-tarballs"),
    name="edxorg_course_bundle_sensor",
    minimum_interval_seconds=86400,
    job=gcs_sync_job,
    default_status=DefaultSensorStatus.STOPPED,
)

# ============================================================================
# IRx Open edX Course Data Exports
# ============================================================================


def open_edx_export_irx_job_config(
    deployment: Literal["mitx", "mitxonline", "xpro"],
    dagster_env: Literal["qa", "production"],
):
    """Generate configuration for Open edX course export jobs."""
    pipeline_path = "residential" if deployment == "mitx" else deployment
    etl_bucket_map = {
        "mitx": f"mitx-etl-residential-live-mitx-{dagster_env}",
        "xpro": f"mitx-etl-xpro-{dagster_env}-mitxpro-{dagster_env}",
        "mitxonline": f"mitx-etl-mitxonline-{dagster_env}",
    }

    # Build configuration based on vault availability
    if not vault_authenticated:
        # Return minimal config for testing
        return {
            "resources": {
                "sqldb": {"config": {"database": ":memory:"}},
                "results_dir": {"config": {"results_dir": "/tmp/results"}},  # noqa: S108
                "healthchecks": {"config": {"healthchecks_io_key": "test"}},
                "openedx": {
                    "config": {
                        "client_id": "test",
                        "client_secret": "test",  # pragma: allowlist secret
                        "base_url": "http://test.com",
                        "studio_url": "http://test.com",
                        "token_type": "JWT",
                        "token_url": "http://test.com/token",
                    }
                },
            },
            "ops": {
                "list_courses": {"config": {"openedx_api_client": "test"}},
                "upload_extracted_data": {
                    "config": {
                        "extract_bucket": "test-bucket",
                        "extract_prefix": f"edxapp-{deployment}-{dagster_env}",
                    }
                },
            },
        }

    # Real configuration with vault
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

    db_creds = vault.client.secrets.database.generate_credentials(
        mount_point=f"mariadb-{deployment}", name="readonly"
    )["data"]
    edx_db_config = {
        "mysql_db_name": "edxapp",
        "mysql_hostname": f"edxapp-db{'-replica' if dagster_env == 'production' else ''}.service.{deployment}-{dagster_env}.consul",  # noqa: E501
        "mysql_username": db_creds["username"],
        "mysql_password": db_creds["password"],
    }

    return {
        "resources": {
            "sqldb": {"config": edx_db_config},
            "results_dir": {
                "config": {"results_dir": f"/dagster_home/tmp/{deployment}"}
            },
            "healthchecks": {"config": {"healthchecks_io_key": healthcheck_id}},
            "openedx": {"config": edx_resource_config},
        },
        "ops": {
            "export_edx_forum_database": {"config": mongo_config},
            "upload_extracted_data": {
                "config": {
                    "extract_bucket": etl_bucket_map[deployment],
                    "extract_prefix": f"edxapp-{deployment}-{dagster_env}",
                }
            },
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
    "sqldb": mysql_db_resource,
    "s3": S3Resource(),
    "results_dir": DailyResultsDir.configure_at_launch(),
    "healthchecks": HealthchecksIO.configure_at_launch(),
    "openedx": OpenEdxApiClient.configure_at_launch(),
}

# Create jobs for each deployment
residential_edx_job = edx_course_pipeline.to_job(
    name="residential_edx_course_pipeline",
    resource_defs=production_resources,
    config=open_edx_export_irx_job_config("mitx", DAGSTER_ENV),
)

xpro_edx_job = edx_course_pipeline.to_job(
    name="xpro_edx_course_pipeline",
    resource_defs=production_resources,
    config=open_edx_export_irx_job_config("xpro", DAGSTER_ENV),
)

mitxonline_edx_job = edx_course_pipeline.to_job(
    name="mitxonline_edx_course_pipeline",
    resource_defs=production_resources,
    config=open_edx_export_irx_job_config("mitxonline", DAGSTER_ENV),
)

# Create unified definitions
defs = Definitions(
    jobs=[gcs_sync_job, residential_edx_job, xpro_edx_job, mitxonline_edx_job],
    sensors=[gcs_course_sensor],
    schedules=[
        residential_edx_daily_schedule.with_updated_job(residential_edx_job),
        xpro_edx_daily_schedule.with_updated_job(xpro_edx_job),
        mitxonline_edx_daily_schedule.with_updated_job(mitxonline_edx_job),
    ],
    resources={
        "gcp_gcs": gcs_connection,
        "s3": s3_resource,
        "results_dir": DailyResultsDir.configure_at_launch(),
    },
)
