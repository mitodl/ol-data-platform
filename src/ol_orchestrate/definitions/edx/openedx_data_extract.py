from typing import Any, Literal

from dagster import (
    create_repository_using_definitions_args,
)
from dagster_aws.s3 import S3PickleIOManager, S3Resource

from ol_orchestrate.assets.openedx import course_structure, openedx_live_courseware
from ol_orchestrate.io_managers.filepath import (
    S3FileObjectIOManager,
)
from ol_orchestrate.lib.assets_helper import (
    add_prefix_to_asset_keys,
    late_bind_partition_to_asset,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, OPENEDX_DEPLOYMENTS, VAULT_ADDRESS
from ol_orchestrate.partitions.openedx import OPENEDX_COURSE_RUN_PARTITIONS
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory
from ol_orchestrate.resources.secrets.vault import Vault
from ol_orchestrate.sensors.openedx import course_run_sensor

if DAGSTER_ENV == "dev":
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault._auth_github()  # noqa: SLF001
else:
    vault = Vault(
        vault_addr=VAULT_ADDRESS, vault_role="dagster-server", aws_auth_mount="aws"
    )
    vault._auth_aws_iam()  # noqa: SLF001


def open_edx_extract_job_config(
    open_edx_deployment: OPENEDX_DEPLOYMENTS,
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


def edxorg_data_archive_config(dagster_env):
    return {
        "ops": {
            "process_edxorg_archive_bundle": {
                "config": {
                    "s3_bucket": s3_uploads_bucket(dagster_env)["bucket"],
                    "s3_prefix": s3_uploads_bucket(dagster_env)["prefix"],
                }
            },
        }
    }


for deployment_name in OPENEDX_DEPLOYMENTS:
    locals()[f"{deployment_name}_openedx_assets_definition"] = (
        create_repository_using_definitions_args(
            name=f"{deployment_name}_openedx_assets",
            resources={
                "io_manager": S3PickleIOManager(S3Resource()),
                "s3file_io_manager": S3FileObjectIOManager(
                    bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
                    path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
                ),
                "vault": vault,
                "openedx": OpenEdxApiClientFactory(
                    deployment=deployment_name, vault=vault
                ),
            },
            assets=[
                late_bind_partition_to_asset(
                    add_prefix_to_asset_keys(openedx_live_courseware, deployment_name),
                    OPENEDX_COURSE_RUN_PARTITIONS[deployment_name],
                ),
                late_bind_partition_to_asset(
                    add_prefix_to_asset_keys(course_structure, deployment_name),
                    OPENEDX_COURSE_RUN_PARTITIONS[deployment_name],
                ),
            ],
            sensors=[course_run_sensor],
        )
    )
