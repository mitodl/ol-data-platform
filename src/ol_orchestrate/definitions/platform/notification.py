from dagster import DefaultSensorStatus, Definitions, RunFailureSensorContext
from dagster_slack import make_slack_on_run_failure_sensor

from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.resources.secrets.vault import Vault

if DAGSTER_ENV == "dev":
    dagster_url = "http://localhost:3000"
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault._auth_github()  # noqa: SLF001
else:
    dagster_url = (
        "https://pipelines.odl.mit.edu"
        if DAGSTER_ENV == "production"
        else "https://pipelines-qa.odl.mit.edu"
    )
    vault = Vault(
        vault_addr=VAULT_ADDRESS, vault_role="dagster-server", aws_auth_mount="aws"
    )
    vault._auth_aws_iam()  # noqa: SLF001


def error_message(context: RunFailureSensorContext) -> str:
    error_strings_by_step_key = {
        # includes the stack trace
        event.step_key: event.event_specific_data.error.to_string()
        # Get details on a failure due to an error inside a step
        for event in context.get_step_failure_events()
    }
    return (
        f"Job {context.dagster_run.job_name} failed!"
        f"Run ID: {context.dagster_run.run_id}"
        f"Error: {context.failure_event.message}"
        f"Error details: {error_strings_by_step_key}"
    )


notifications = Definitions(
    sensors=[
        make_slack_on_run_failure_sensor(
            channel="#data-platform-notifications",
            webserver_base_url=dagster_url,
            monitor_all_repositories=True,
            slack_token=vault.client.secrets.kv.v1.read_secret(
                path="dagster/slack", mount_point="secret-data"
            )["data"]["token"],
            default_status=DefaultSensorStatus.STOPPED,
            text_fn=error_message,
        )
    ]
)
