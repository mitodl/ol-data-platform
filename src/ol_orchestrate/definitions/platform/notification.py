from typing import Any

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

MAX_SLACK_TEXT_LENGTH = 3000


def truncate_text(text: str, max_length: int = MAX_SLACK_TEXT_LENGTH) -> str:
    if len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text


def error_message(context: RunFailureSensorContext) -> list[dict[str, Any]]:
    error_details = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": truncate_text(
                    f"Step: {event.step_key}"
                    f"\nError: {event.event_specific_data.error.to_string()}"
                ),
            },
            "expand": True,
        }
        for event in context.get_step_failure_events()
    ]
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": truncate_text(
                    f'*Job "{context.dagster_run.job_name}" failed.'
                    f"\n`{context.dagster_run.run_id.split('-')[0]}`*"
                ),
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": truncate_text(f"*Error:*\n{context.failure_event.message}"),
            },
        },
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Step Failure Events",
            },
        },
        *error_details,
    ]


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
            blocks_fn=error_message,
        )
    ]
)
