"""Platform-level failure notification.

This code location owns cross-code-location monitoring. It runs a single run
failure sensor that watches every code location in the deployment and reports
each failure to both Sentry and Slack.

The two live in one sensor on purpose: Sentry is captured first so the Slack
message can carry the resulting event ID, which is what turns a notification
into something you can actually go and look up.
"""

from collections.abc import Sequence
from typing import Any

import sentry_sdk
from dagster import (
    AssetCheckSeverity,
    DagsterEventType,
    DagsterRun,
    DefaultSensorStatus,
    Definitions,
    EventRecordsFilter,
    RunFailureSensorContext,
    SensorEvaluationContext,
    run_failure_sensor,
    sensor,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.sentry import init_sentry
from ol_orchestrate.lib.utils import authenticate_vault
from slack_sdk import WebClient

init_sentry("data_platform")

# Dagster's own run tags. Hardcoded rather than imported: the constants live in
# dagster._core.storage.tags, which is private, but the tag strings themselves
# are stable and documented.
WILL_RETRY_TAG = "dagster/will_retry"
RETRY_NUMBER_TAG = "dagster/retry_number"
MAX_RETRIES_TAG = "dagster/max_retries"

DAGSTER_URL_BY_ENV = {
    "dev": "http://localhost:3000",
    "ci": "https://pipelines-ci.odl.mit.edu",
    "qa": "https://pipelines-qa.odl.mit.edu",
    "production": "https://pipelines.odl.mit.edu",
}
dagster_url = DAGSTER_URL_BY_ENV[DAGSTER_ENV]

# QA and CI failures are noise in the channel the team watches for production
# breakage, so each environment gets its own.
SLACK_CHANNEL_BY_ENV = {
    "dev": "#notifications-data-platform-dev",
    "ci": "#notifications-data-platform-ci",
    "qa": "#notifications-data-platform-qa",
    "production": "#notifications-data-platform",
}
slack_channel = SLACK_CHANNEL_BY_ENV[DAGSTER_ENV]

# Bounds a single tick's Slack payload and its event-log scan. A backlog larger
# than this is drained over subsequent ticks via the cursor.
MAX_CHECK_EVALUATIONS_PER_TICK = 50

MAX_SLACK_TEXT_LENGTH = 3000
# Leaves room for the surrounding header and step key within Slack's limit.
MAX_SLACK_ERROR_LENGTH = 2900


def truncate_text(text: str, max_length: int = MAX_SLACK_TEXT_LENGTH) -> str:
    """Truncate text to maximum length with ellipsis."""
    if len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text


DBT_ERROR_MARKER = "dagster_dbt.errors.DagsterDbtCliRuntimeError"
DBT_LOG_PREAMBLE = "Errors parsed from dbt logs:\n\n"


def get_exception(text: str, substring: str = "\n\nStack Trace:") -> str:
    """Extract and format exception from error text."""
    index = text.find(substring)
    # Pull out dbt errors
    dbt_error = text.find(DBT_ERROR_MARKER)
    if dbt_error != -1:
        # Skip exactly the preamble. This used to add a hardcoded 34 against a
        # 30-character marker, which ate the first four characters of every dbt
        # error -- "Model x failed" arrived as "l x failed".
        dbt_log_index = text.find(DBT_LOG_PREAMBLE) + len(DBT_LOG_PREAMBLE)
        dbt_log_msg = text[dbt_log_index:index] if index != -1 else text[dbt_log_index:]
        body = truncate_text(dbt_log_msg, MAX_SLACK_ERROR_LENGTH)
        return f"*DBT Error:*\n```{body}```"
    else:
        # Return full text if substring not found
        body = truncate_text(
            text[:index] if index != -1 else text, MAX_SLACK_ERROR_LENGTH
        )
        return f"*Error:*\n```{body}```"


def will_be_retried(run: DagsterRun, instance: Any) -> bool:
    """Whether Dagster is going to automatically retry this failed run.

    Without this check the sensor fires once per attempt, so a single broken
    model becomes four identical alerts under the deployment's
    ``run_retries.max_retries: 3``.

    Prefers the daemon's own recorded decision. The tag is written by the
    auto-reexecution daemon, which can lag this sensor's tick, so fall back to
    the same arithmetic the daemon uses.
    """
    will_retry_tag = run.tags.get(WILL_RETRY_TAG)
    if will_retry_tag is not None:
        return will_retry_tag.lower() == "true"

    if not instance.run_retries_enabled:
        return False
    retry_number = int(run.tags.get(RETRY_NUMBER_TAG, 0))
    max_retries = int(run.tags.get(MAX_RETRIES_TAG, instance.run_retries_max_retries))
    return retry_number < max_retries


def capture_run_failure_to_sentry(context: RunFailureSensorContext) -> str | None:
    """Report a run failure to Sentry and return the event ID.

    Complements the in-process hook in ol_orchestrate.lib.sentry. That hook has
    better fidelity but cannot run when the process itself dies -- an OOM-killed
    run worker, a pod reaped by run monitoring, or a failure during run startup
    before any step executed. Those only ever surface here.

    Both paths fingerprint on the same values, so a failure reported by both
    collapses into one Sentry issue rather than two.
    """
    if not sentry_sdk.get_client().is_active():
        return None

    run = context.dagster_run
    step_failures = context.get_step_failure_events()
    step_key = step_failures[0].step_key if step_failures else "run"

    with sentry_sdk.new_scope() as scope:
        scope.set_tag("dagster_job", run.job_name)
        scope.set_tag("dagster_step", step_key)
        scope.set_tag("dagster_run_id", run.run_id)
        scope.set_tag("dagster_code_location", "data_platform")
        scope.set_tag("captured_by", "sensor")
        scope.set_context(
            "dagster_run",
            {
                "run_id": run.run_id,
                "job_name": run.job_name,
                "run_url": f"{dagster_url}/runs/{run.run_id}",
                "tags": dict(run.tags),
            },
        )
        scope.fingerprint = [run.job_name, step_key, "run_failure"]
        event_id = sentry_sdk.capture_message(
            f"Dagster run failure: {run.job_name}",
            level="error",
        )

    sentry_sdk.flush(timeout=5.0)
    return event_id


def error_message(
    context: RunFailureSensorContext,
    sentry_event_id: str | None = None,
) -> list[dict[str, Any]]:
    """Format error message for Slack notification."""

    def format_error(error_event):
        return (
            get_exception(error_event.event_specific_data.error.to_string())
            if hasattr(error_event.event_specific_data, "error")
            and error_event.event_specific_data.error
            else "Unknown error"
        )

    step_failure_events = context.get_step_failure_events()
    error_details = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (f"*Step:* {event.step_key}\n{format_error(event)}"),
            },
            "expand": False,
        }
        for event in step_failure_events
    ]

    # A run killed by OOM or reaped by run monitoring produces no step failure
    # events at all. Without this the message would be a bare header and job
    # name, which says nothing about what went wrong.
    if not error_details:
        failure_detail = truncate_text(
            context.failure_event.message or "No detail available",
            MAX_SLACK_ERROR_LENGTH,
        )
        error_details = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*No step failure recorded* -- the run died before or "
                        "between steps (OOM, eviction, or run monitoring "
                        "timeout).\n"
                        f"```{failure_detail}```"
                    ),
                },
            }
        ]

    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Dagster {DAGSTER_ENV.capitalize()} Run Failure",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Job Name:* {context.dagster_run.job_name}"
                    f"\n*Run ID:* `{context.dagster_run.run_id.split('-')[0]}`"
                ),
            },
        },
        *error_details,
    ]

    if sentry_event_id:
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"Sentry event `{sentry_event_id}`"}
                ],
            }
        )

    blocks.append(
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View in Dagster"},
                    "url": f"{dagster_url}/runs/{context.dagster_run.run_id}",
                }
            ],
        }
    )
    return blocks


def get_slack_token() -> str:
    """Read the Slack bot token from Vault.

    Resolved per tick rather than at module import. Reading it at import meant a
    transient Vault failure silently dropped the sensor from the definitions and
    alerting vanished with nothing but a warning.
    """
    vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)
    return vault.client.secrets.kv.v1.read_secret(
        path="dagster/slack", mount_point="secret-data"
    )["data"]["token"]


@run_failure_sensor(
    name="run_failure_notification_sensor",
    monitor_all_code_locations=True,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Reports run failures across all code locations to Sentry and Slack. "
        "Suppresses attempts that Dagster is going to retry automatically."
    ),
)
def run_failure_notification_sensor(context: RunFailureSensorContext) -> None:
    """Report a failed run to Sentry, then announce it in Slack."""
    run = context.dagster_run

    if will_be_retried(run, context.instance):
        context.log.info(
            "Skipping notification for run %s: Dagster will retry it "
            "automatically. The final attempt will be reported.",
            run.run_id,
        )
        return

    sentry_event_id = capture_run_failure_to_sentry(context)

    # Deliberately unguarded. A failure here fails the sensor tick, which
    # surfaces in the Dagster UI and in Sentry -- the alternative is alerting
    # that has quietly stopped working and nobody knowing.
    client = WebClient(token=get_slack_token())
    client.chat_postMessage(
        channel=slack_channel,
        blocks=error_message(context, sentry_event_id),
        text=f"Dagster {DAGSTER_ENV} run failure: {run.job_name}",
    )


def asset_check_failure_message(
    records: Sequence[Any],
) -> list[dict[str, Any]]:
    """Format a batch of failed asset check evaluations for Slack."""
    detail_blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{evaluation.asset_key.to_user_string()}* -- "
                    f"`{evaluation.check_name}`"
                ),
            },
        }
        for evaluation in records
    ]
    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": (
                    f"Dagster {DAGSTER_ENV.capitalize()} Asset Check Failure "
                    f"({len(records)})"
                ),
            },
        },
        *detail_blocks,
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View asset checks"},
                    "url": f"{dagster_url}/asset-groups",
                }
            ],
        },
    ]


def collect_new_check_failures(
    instance: Any, cursor: int | None
) -> tuple[list[Any], str | None]:
    """Read the next batch of asset check evaluations above ``cursor``.

    Returns the ERROR-severity failures in that batch and the cursor to store,
    or an empty list and None when there is nothing new.

    ``ascending=True`` is load-bearing. The storage layer applies the LIMIT
    after ordering, so descending order returns the *newest* batch above the
    cursor; advancing the cursor past it would permanently skip any older
    backlog beyond MAX_CHECK_EVALUATIONS_PER_TICK. Ascending returns the oldest
    events above the cursor, so a backlog drains in order, one batch per tick,
    with no gaps and no repeats.
    """
    records = instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_CHECK_EVALUATION,
            after_cursor=cursor,
        ),
        limit=MAX_CHECK_EVALUATIONS_PER_TICK,
        ascending=True,
    )
    if not records:
        return [], None

    evaluations = [
        record.event_log_entry.dagster_event.event_specific_data for record in records
    ]
    # WARN-severity checks are advisory; only ERROR is worth a notification.
    failures = [
        evaluation
        for evaluation in evaluations
        if not evaluation.passed and evaluation.severity == AssetCheckSeverity.ERROR
    ]
    # The newest record of an ascending batch: nothing older is left behind,
    # nothing newer is re-delivered.
    return failures, str(records[-1].storage_id)


@sensor(
    name="asset_check_failure_sensor",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Announces ERROR-severity asset check failures, including the freshness "
        "checks, in Slack. Asset checks do not fail their run, so the run "
        "failure sensor never sees them."
    ),
)
def asset_check_failure_sensor(context: SensorEvaluationContext) -> None:
    """Post newly-failed ERROR-severity asset checks to Slack."""
    cursor = int(context.cursor) if context.cursor else None

    failures, next_cursor = collect_new_check_failures(context.instance, cursor)
    if next_cursor is None:
        return

    context.update_cursor(next_cursor)

    if not failures:
        return

    client = WebClient(token=get_slack_token())
    client.chat_postMessage(
        channel=slack_channel,
        blocks=asset_check_failure_message(failures),
        text=(
            f"Dagster {DAGSTER_ENV}: {len(failures)} asset check "
            f"failure{'s' if len(failures) > 1 else ''}"
        ),
    )


defs = Definitions(
    sensors=[run_failure_notification_sensor, asset_check_failure_sensor],
)
