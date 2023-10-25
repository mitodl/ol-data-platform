import os
from typing import Literal

from dagster import Definitions
from dagster_aws.s3 import S3Resource
from pydantic import Field

from ol_orchestrate.jobs.open_edx import edx_course_pipeline
from ol_orchestrate.lib.vault_config_helper import VaultBaseSettings, VaultDBCredentials
from ol_orchestrate.resources.healthchecks import (
    HealthchecksIO,
)
from ol_orchestrate.resources.mysql_db import mysql_db_resource
from ol_orchestrate.resources.openedx import OpenEdxApiClient
from ol_orchestrate.resources.outputs import DailyResultsDir
from ol_orchestrate.resources.sqlite_db import sqlite_db_resource
from ol_orchestrate.schedules.open_edx import (
    mitxonline_edx_daily_schedule,
    residential_edx_daily_schedule,
    xpro_edx_daily_schedule,
)

DAGSTER_ENV: Literal["ci", "qa", "production"] = os.environ.get(  # type: ignore[assignment]
    "DAGSTER_ENVIRONMENT", "ci"
)


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

    class OpenEdxMongoCredentialsConfig(VaultBaseSettings):
        edx_mongodb_username: str = Field(
            ...,
            vault_secret_path=f"secret-{deployment}/mongodb-forum",
            vault_secret_key="username",  # noqa: S106 # pragma: allowlist secret
        )
        edx_mongodb_password: str = Field(
            ...,
            vault_secret_path=f"secret-{deployment}/mongodb-forum",
            vault_secret_key="password",  # noqa: S106 # pragma: allowlist secret
        )
        edx_mongodb_auth_db: str = "admin"
        edx_mongodb_forum_database_name: str = "forum"
        edx_mongodb_uri: str = Field(
            ...,
            vault_secret_path=f"secret-{deployment}/mongodb-forum",
            vault_secret_key="uri",  # noqa: S106 # pragma: allowlist secret
        )

    class HealthcheckConfig(VaultBaseSettings):
        check_id: str = Field(
            ...,
            vault_secret_path=f"secret-data/pipelines/edx/{pipeline_path}/healthchecks-io-check-id",
            vault_secret_key="value",  # noqa: S106 # pragma: allowlist secret
        )

    class OpenEdxResourceRuntimeConfig(VaultBaseSettings):
        client_id: str = Field(
            ...,
            vault_secret_path=f"secret-data/pipelines/edx/{pipeline_path}/edx-oauth-client",
            vault_secret_key="id",  # noqa: S106 # pragma: allowlist secret
        )
        client_secret: str = Field(
            ...,
            vault_secret_path=f"secret-data/pipelines/edx/{pipeline_path}/edx-oauth-client",
            vault_secret_key="secret",  # noqa: S106 # pragma: allowlist secret
        )
        lms_url: str = Field(
            ...,
            vault_secret_path=f"secret-data/pipelines/edx/{pipeline_path}/edx-oauth-client",
            vault_secret_key="url",  # noqa: S106 # pragma: allowlist secret
        )
        studio_url: str = Field(
            ...,
            vault_secret_path=f"secret-data/pipelines/edx/{pipeline_path}/edx-oauth-client",
            vault_secret_key="studio_url",  # noqa: S106 # pragma: allowlist secret
        )
        token_type: str = "JWT"

    class OpenEdxDatabaseConfig(VaultBaseSettings):
        mysql_db_name: str = "edxapp"
        mysql_hostname: str = (
            f"edxapp-db-replica.service.{deployment}-{dagster_env}.consul"
        )
        mysql_credentials: VaultDBCredentials = Field(
            ..., vault_secret_path=f"mariadb-{deployment}/creds/readonly"
        )

        def dict(self):  # noqa: A003
            return {
                "mysql_db_name": self.mysql_db_name,
                "mysql_hostname": self.mysql_hostname,
                "mysql_username": self.mysql_credentials.username,
                "mysql_password": self.mysql_credentials.password.get_secret_value(),
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
                "config": OpenEdxMongoCredentialsConfig().dict()
            },
        },
        "resources": {
            "openedx": {"config": OpenEdxResourceRuntimeConfig().dict()},
            "healthchecks": {"config": HealthcheckConfig().dict()},
            "results_dir": {
                "config": {"date_format": "%Y%m%d", "dir_prefix": deployment}
            },
            "sqldb": {"config": OpenEdxDatabaseConfig().dict()},
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
    config=open_edx_export_irx_job_config("mitx", DAGSTER_ENV),  # type: ignore[arg-type]
)

xpro_edx_job = edx_course_pipeline.to_job(
    name="xpro_edx_course_pipeline",
    resource_defs=production_resources,
    config=open_edx_export_irx_job_config("xpro", DAGSTER_ENV),  # type: ignore[arg-type]
)

mitxonline_edx_job = edx_course_pipeline.to_job(
    name="mitxonline_edx_course_pipeline",
    resource_defs=production_resources,
    config=open_edx_export_irx_job_config("mitxonline", DAGSTER_ENV),  # type: ignore[arg-type]
)

open_edx_irx_extracts = Definitions(
    jobs=[residential_edx_job, xpro_edx_job, mitxonline_edx_job],
    schedules=[
        residential_edx_daily_schedule.with_updated_job(residential_edx_job),
        xpro_edx_daily_schedule.with_updated_job(xpro_edx_job),
        mitxonline_edx_daily_schedule.with_updated_job(mitxonline_edx_job),
    ],
)
