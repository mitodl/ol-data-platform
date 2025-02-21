from dagster import create_repository_using_definitions_args
from dagster_aws.s3 import S3Resource

from ol_orchestrate.assets.openmetadata import trino_metadata
from ol_orchestrate.lib.assets_helper import (
    add_prefix_to_asset_keys,
    late_bind_partition_to_asset,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.partitions.openmetadata import TRINO_DAILY_PARTITIONS
from ol_orchestrate.resources.secrets.vault import Vault

if DAGSTER_ENV == "dev":
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault._auth_github()  # noqa: SLF001
else:
    vault = Vault(
        vault_addr=VAULT_ADDRESS, vault_role="dagster-server", aws_auth_mount="aws"
    )
    vault._auth_aws_iam()  # noqa: SLF001


deployment_name = "openmetadata"
deployment_assets = [
    late_bind_partition_to_asset(
        add_prefix_to_asset_keys(trino_metadata, deployment_name),
        TRINO_DAILY_PARTITIONS,
    ),
]

create_repository_using_definitions_args(
    name=f"{deployment_name}_assets",
    resources={
        "vault": vault,
        "s3": S3Resource(),
    },
    assets=deployment_assets,
)
