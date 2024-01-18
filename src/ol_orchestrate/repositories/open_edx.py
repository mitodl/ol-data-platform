from typing import Literal

from dagster import Definitions
from dagster_aws.s3 import S3Resource

from ol_orchestrate.jobs.open_edx import edx_course_pipeline
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.resources.healthchecks import (
    HealthchecksIO,
)
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

if DAGSTER_ENV == "dev":
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault._auth_github()  # noqa: SLF001
else:
    vault = Vault(
        vault_addr=VAULT_ADDRESS, vault_role="dagster-server", aws_auth_mount="aws"
    )
    vault._auth_aws_iam()  # noqa: SLF001


def open_edx_export_irx_job_config(
    deployment: Literal["mitx", "mitxonline", "xpro"],
    dagster_env: Literal["qa", "production"],
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
        "lms_url": edx_creds["url"],
        "studio_url": edx_creds["studio_url"],
        "token_type": "JWT",
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
        "ops": {
            "edx_export_courses": {
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


dev_resources = {
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

open_edx_irx_extracts = Definitions(
    jobs=[residential_edx_job, xpro_edx_job, mitxonline_edx_job],
    schedules=[
        residential_edx_daily_schedule.with_updated_job(residential_edx_job),
        xpro_edx_daily_schedule.with_updated_job(xpro_edx_job),
        mitxonline_edx_daily_schedule.with_updated_job(mitxonline_edx_job),
    ],
)
