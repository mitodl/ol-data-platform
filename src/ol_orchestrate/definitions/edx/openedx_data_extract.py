import os
from pathlib import Path
from typing import Literal

from dagster import Definitions, ScheduleDefinition
from dagster_aws.s3 import S3Resource
from pydantic import BaseSettings, Field, SecretStr
from pydantic_vault import vault_config_settings_source

from ol_orchestrate.jobs.open_edx import extract_open_edx_data_to_ol_data_platform
from ol_orchestrate.resources.openedx import OpenEdxApiClient
from ol_orchestrate.resources.outputs import DailyResultsDir

dagster_env = os.environ.get("DAGSTER_ENVIRONMENT", "dev")


def open_edx_extract_job_config(
    open_edx_deployment: Literal["residential", "mitxonline", "xpro"],
    dagster_env: Literal["qa", "production"],
):
    class OpenEdxResourceRuntimeConfig(BaseSettings):  # type: ignore[valid-type,misc]
        client_id: str = Field(
            ...,
            vault_secret_path=f"secret-data/pipelines/edx/{open_edx_deployment}/edx-oauth-client",
            vault_secret_key="id",  # noqa: S106 # pragma: allowlist secret
        )
        client_secret: str = Field(
            ...,
            vault_secret_path=f"secret-data/pipelines/edx/{open_edx_deployment}/edx-oauth-client",
            vault_secret_key="secret",  # noqa: S106 # pragma: allowlist secret
        )
        lms_url: str = Field(
            ...,
            vault_secret_path=f"secret-data/pipelines/edx/{open_edx_deployment}/edx-oauth-client",
            vault_secret_key="url",  # noqa: S106 # pragma: allowlist secret
        )
        studio_url: str = Field(
            ...,
            vault_secret_path=f"secret-data/pipelines/edx/{open_edx_deployment}/edx-oauth-client",
            vault_secret_key="studio_url",  # noqa: S106 # pragma: allowlist secret
        )
        token_type: str = "JWT"

        class Config:
            vault_url: str = f"https://vault-{dagster_env}.odl.mit.edu"
            vault_token: SecretStr = os.environ.get(  # type: ignore[assignment]
                "VAULT_TOKEN", Path.home().joinpath(".vault-token").read_text()
            )

            @classmethod
            def customise_sources(
                cls,
                init_settings,
                env_settings,
                file_secret_settings,
            ):
                # This is where you can choose which settings sources to use and their
                # priority
                return (
                    vault_config_settings_source,
                    init_settings,
                    env_settings,
                    file_secret_settings,
                )

    return {
        "ops": {
            "upload_files_to_s3": {
                "config": {
                    "destination_bucket": f"ol-data-lake-landing-zone-{dagster_env}",
                    "destination_prefix": f"{open_edx_deployment}_openedx_extracts",
                }
            },
        },
        "resources": {
            "openedx": {"config": OpenEdxResourceRuntimeConfig().dict()},
        },
    }


ol_extract_jobs = [
    extract_open_edx_data_to_ol_data_platform.to_job(
        resource_defs={
            "results_dir": DailyResultsDir(),
            "s3_upload": S3Resource(),
            "s3": S3Resource(),
            "openedx": OpenEdxApiClient.configure_at_launch(),
        },
        name=f"extract_{deployment}_open_edx_data_to_data_platform",
        config=open_edx_extract_job_config(deployment, dagster_env),  # type: ignore[arg-type]
    )
    for deployment in ("residential", "xpro", "mitxonline")
]

openedx_data_extracts = Definitions(
    jobs=ol_extract_jobs,
    schedules=[
        ScheduleDefinition(
            name=f"{job.name}_nightly", cron_schedule="0 0 * * *", job=job
        )
        for job in ol_extract_jobs
    ],
)
