import os

from dagster import Definitions
from dagster_slack import make_slack_on_run_failure_sensor

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

notifications = Definitions(
    sensors=[
        make_slack_on_run_failure_sensor(
            channel="#data-platform-alerts",
            webserver_base_url=os.getenv("DAGSTER_HOSTNAME", "localhost:3000"),
            monitor_all_repositories=True,
            slack_token=vault.client.secrets.kv.v2.read_secret(
                path="slack", mount_point="secret-data"
            )["data"]["data"]["token"],
        )
    ]
)
