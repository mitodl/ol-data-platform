from dagster import create_repository_using_definitions_args
from dagster_aws.s3 import S3Resource

from ol_orchestrate.assets.platform.openmetadata import trino_metadata
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
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
    trino_metadata,
]

create_repository_using_definitions_args(
    name=f"{deployment_name}_assets",
    resources={
        "vault": vault,
        "s3": S3Resource(),
    },
    assets=deployment_assets,
)
