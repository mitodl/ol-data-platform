import os
from typing import Literal

from dagster import Definitions, ScheduleDefinition
from dagster_aws.s3 import S3Resource

from ol_orchestrate.jobs.open_edx import extract_open_edx_data_to_ol_data_platform
from ol_orchestrate.resources.openedx import OpenEdxApiClient
from ol_orchestrate.resources.outputs import DailyResultsDir
from ol_orchestrate.resources.secrets.vault import Vault

DAGSTER_ENV = os.environ.get("DAGSTER_ENVIRONMENT", "dev")
vault_addr = os.getenv("VAULT_ADDR", "localhost:8200")
if DAGSTER_ENV == "dev":
    vault = Vault(vault_addr=vault_addr, vault_auth_type="github")
    vault._auth_github()  # noqa: SLF001
else:
    vault = Vault(
        vault_addr=vault_addr, vault_role="dagster-server", aws_auth_mount="aws"
    )
    vault._auth_aws_iam()  # noqa: SLF001


def open_edx_extract_job_config(
    open_edx_deployment: Literal["residential", "mitxonline", "xpro"],
    dagster_env: Literal["qa", "production"],
):
    client = vault.client.secrets.kv.v1.read_secret(
        mount_point="secret-data",
        path=f"pipelines/edx/{open_edx_deployment}/edx-oauth-client",
    )["data"]
    client_config = {
        "client_id": client["id"],
        "client_secret": client["secret"],
        "lms_url": client["url"],
        "studio_url": client["studio_url"],
        "token_type": "JWT",
    }

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
            "openedx": {"config": client_config},
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
        config=open_edx_extract_job_config(deployment, DAGSTER_ENV),  # type: ignore[arg-type]
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
