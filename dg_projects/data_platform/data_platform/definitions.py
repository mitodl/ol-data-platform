"""Platform-level failure notification.

This code location owns cross-code-location monitoring. It runs a single run
failure sensor that watches every code location in the deployment and reports
each failure to both Sentry and Slack.

The two live in one sensor on purpose: Sentry is captured first so the Slack
message can carry the resulting event ID, which is what turns a notification
into something you can actually go and look up.
"""

import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import sentry_sdk
from dagster import (
    AssetCheckSeverity,
    BoolMetadataValue,
    DagsterEventType,
    DagsterRun,
    DefaultSensorStatus,
    Definitions,
    EventRecordsFilter,
    FloatMetadataValue,
    IntMetadataValue,
    MarkdownMetadataValue,
    MetadataValue,
    RunFailureSensorContext,
    SensorEvaluationContext,
    TextMetadataValue,
    TimestampMetadataValue,
    UrlMetadataValue,
    run_failure_sensor,
    sensor,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.sentry import (
    INTERRUPTION_ERRORS,
    PARTITION_NAME_TAG,
    failure_fingerprint,
    init_sentry,
)
from ol_orchestrate.lib.utils import authenticate_vault
from slack_sdk import WebClient

init_sentry("data_platform")

# Dagster's own run tag. Hardcoded rather than imported: the constant lives in
# dagster._core.storage.tags, which is private, but the tag string itself is
# stable and documented.
RETRY_NUMBER_TAG = "dagster/retry_number"

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
# than this is drained over subsequent ticks via the cursor. Capped at 49, not
# 50: asset_check_failure_message adds one header block on top of one detail
# block per failure, and Slack rejects a chat.postMessage over 50 blocks.
MAX_CHECK_EVALUATIONS_PER_TICK = 49

# Metadata dagster-dbt stamps on every check evaluation it builds from a dbt
# result. Matched as a set: any one of these keys can occur on a hand-written
# check, but the three together identify the evaluation as dbt's.
DBT_PROVENANCE_KEYS = frozenset({"unique_id", "invocation_id", "status"})

MAX_SLACK_TEXT_LENGTH = 3000
# Leaves room for the surrounding header and step key within Slack's limit.
MAX_SLACK_ERROR_LENGTH = 2900

# How much of the serialized step error travels on a sensor-reported Sentry
# event. That path reports through capture_message, which carries no exception
# and therefore no traceback, so without this the issue names the job and
# nothing else. Bounded because Sentry drops an oversized context silently.
MAX_SENTRY_ERROR_LENGTH = 8192


def truncate_text(text: str, max_length: int = MAX_SLACK_TEXT_LENGTH) -> str:
    """Truncate text to maximum length with ellipsis."""
    if len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text


DBT_ERROR_MARKER = "dagster_dbt.errors.DagsterDbtCliRuntimeError"
DBT_LOG_PREAMBLE = "Errors parsed from dbt logs:\n\n"
# dbt's per-node result line. Both terminal states count: a test that could not
# run logs ERROR ("46 of 65 ERROR accepted_values_foo"), a test whose assertion
# was violated logs FAIL with its row count ("46 of 65 FAIL 12 not_null_foo").
DBT_FAILED_NODE_RE = re.compile(r"^\d+ of \d+ (?:ERROR|FAIL)\b", re.MULTILINE)


def count_dbt_failures(dbt_log_msg: str) -> int:
    """How many dbt nodes failed, counted before any truncation.

    The log is cut at MAX_SLACK_ERROR_LENGTH, so a run failing more nodes than
    fit would otherwise read as though the ones that fit were all of them.
    """
    return len(DBT_FAILED_NODE_RE.findall(dbt_log_msg))


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
        failed = count_dbt_failures(dbt_log_msg)
        header = f"*DBT Error* ({failed} failed):" if failed else "*DBT Error:*"
        body = truncate_text(dbt_log_msg, MAX_SLACK_ERROR_LENGTH)
        return f"{header}\n```{body}```"
    else:
        # Return full text if substring not found
        body = truncate_text(
            text[:index] if index != -1 else text, MAX_SLACK_ERROR_LENGTH
        )
        return f"*Error:*\n```{body}```"


def is_retry_of_a_reported_failure(run: DagsterRun) -> bool:
    """Whether an earlier attempt of this run already raised the alert.

    The deployment runs ``run_retries.max_retries: 3``, and the sensor fires per
    attempt, so a broken model would otherwise produce four identical alerts.

    Reporting the first attempt and skipping its retries is what bounds that to
    one. Skipping every attempt Dagster *might* retry -- what this used to do --
    bounded it to one only when the run kept failing: a transient failure that
    passed on retry had every attempt suppressed and was never reported at all.
    That is the common case for infrastructure races, which are exactly the
    failures worth seeing once.
    """
    return int(run.tags.get(RETRY_NUMBER_TAG, 0)) > 0


# Dagster wraps anything raised by user code in one of these before putting it
# on the step failure event, so the serialized error's cls_name is the wrapper
# rather than the exception the in-process hook saw. Sourced from the concrete
# subclasses of DagsterUserCodeExecutionError, plus RetryRequestedFromPolicy,
# which is not one of those but wraps the same way: an op with a RetryPolicy
# that exhausts its attempts serializes as
# ``RetryRequestedFromPolicy -> <the user's exception>`` while the hook still
# reports the user's exception. Several learning_resources assets carry a
# RetryPolicy, so this is a live path, not a hypothetical one.
DAGSTER_USER_CODE_WRAPPERS = frozenset(
    {
        "DagsterConfigMappingFunctionError",
        "DagsterExecutionHandleOutputError",
        "DagsterExecutionLoadInputError",
        "DagsterExecutionStepExecutionError",
        "DagsterResourceFunctionError",
        "DagsterTypeCheckError",
        "DagsterTypeLoadingError",
        "DagsterUserCodeExecutionError",
        "DagsterUserCodeLoadError",
        "RetryRequestedFromPolicy",
    }
)

# The wrapping is one level deep in practice. The bound only stops a malformed
# or self-referential cause chain from spinning.
MAX_ERROR_CAUSE_DEPTH = 5


def user_code_error_type(error: Any) -> str:
    """Name the exception the user's code raised, not Dagster's wrapper.

    A step that raises FileNotFoundError serializes as

        cls_name = "DagsterExecutionStepExecutionError"
        cause.cls_name = "FileNotFoundError"

    while the in-process hook records ``type(exception).__name__`` -- the
    FileNotFoundError. Fingerprinting on the un-unwrapped cls_name therefore
    never matched the hook, and every step failure raised two Sentry issues:
    one from the hook and one from this sensor. Unwrapping the wrapper
    recovers the name the hook used.
    """
    for _ in range(MAX_ERROR_CAUSE_DEPTH):
        cause = getattr(error, "cause", None)
        if cause is None or error.cls_name not in DAGSTER_USER_CODE_WRAPPERS:
            break
        error = cause
    return error.cls_name


def code_location_of(run: DagsterRun) -> str | None:
    """Which code location launched ``run``.

    This sensor monitors every code location, so the location cannot be a
    constant here the way it can inside a run worker. It used to be tagged as
    "data_platform" for every run regardless of origin, which named the sensor's
    own location rather than the one that broke.
    """
    if run.remote_job_origin is None:
        return None
    return run.remote_job_origin.repository_origin.code_location_origin.location_name


def first_step_failure(context: RunFailureSensorContext) -> Any | None:
    """Return the step failure this run's report is built from, or None.

    A run that died without any step failure event -- OOM, eviction, a run
    monitoring timeout -- has none, and the in-process hook never ran for it
    either.
    """
    step_failures = context.get_step_failure_events()
    return step_failures[0] if step_failures else None


def step_failure_error(context: RunFailureSensorContext) -> Any | None:
    """Return the serialized error that step failure carried, or None."""
    failure = first_step_failure(context)
    if failure is None:
        return None
    return getattr(failure.event_specific_data, "error", None)


def step_failure_error_type(context: RunFailureSensorContext) -> str | None:
    """Name the exception the step raised, Dagster's wrapper unwrapped.

    One reader for the three callers below, so the fingerprint, the interrupt
    filter and the reported exception type cannot disagree about what broke.
    """
    error = step_failure_error(context)
    return user_code_error_type(error) if error is not None else None


def sentry_fingerprint(context: RunFailureSensorContext) -> list[str]:
    """Build a fingerprint matching the one the in-process hook uses.

    Both paths delegate to ol_orchestrate.lib.sentry.failure_fingerprint. They
    have to produce the identical list for the same failure, or the two
    reporting paths raise two Sentry issues for every failure instead of
    collapsing into one.

    A run with no step failure has no exception class and no step, and the hook
    never ran for it, so there is nothing to align with. Those get their own
    grouping instead.
    """
    location = code_location_of(context.dagster_run)
    failure = first_step_failure(context)
    if failure is None:
        return failure_fingerprint(location, "run", "run_failure")
    error_type = step_failure_error_type(context) or "run_failure"
    return failure_fingerprint(location, failure.step_key, error_type)


def sentry_message(context: RunFailureSensorContext) -> str:
    """Title the issue after the defect rather than the launch path.

    Sentry titles a message event with its message, and this one used to be
    ``Dagster run failure: {job_name}``. Everything the automation sensor
    materializes runs as ``__ASSET_JOB``, so every such issue in the list read
    identically -- DAGSTER-2J reached 27,000 events without naming what broke.
    The exception type lived only inside the fingerprint, which the UI does not
    render.

    Formatted like an exception issue's title so a failure reads the same
    whichever path reported it.
    """
    failure = first_step_failure(context)
    if failure is None:
        return (
            f"Dagster run failed with no step failure: {context.dagster_run.job_name}"
        )
    error_type = step_failure_error_type(context) or "run_failure"
    return f"{error_type}: {failure.step_key}"


def sentry_failure_detail(context: RunFailureSensorContext) -> str:
    """Return the failure text a reader needs, trimmed to what Sentry will keep.

    The serialized step error carries the traceback the hook path would have
    attached as real frames. A run with no step failure has only the run's own
    failure message, which is what the Slack path falls back to as well.
    """
    error = step_failure_error(context)
    detail = (
        error.to_string()
        if error is not None
        else (context.failure_event.message or "No detail available")
    )
    return truncate_text(detail, MAX_SENTRY_ERROR_LENGTH)


def is_an_interrupted_worker(context: RunFailureSensorContext) -> bool:
    """Whether this run died because its process was taken away.

    The ``drop_interruptions`` before_send in ol_orchestrate.lib.sentry reads
    ``exception.values``, which only an exception event carries. This sensor
    reports through ``capture_message``, so its events have no exception at all
    and sail straight past that filter -- meaning a SIGTERM'd worker was
    dropped on the hook path and reported on this one.

    The serialized step error is where the type survives, so the check has to
    happen here rather than in the shared filter.
    """
    return step_failure_error_type(context) in INTERRUPTION_ERRORS


def capture_run_failure_to_sentry(context: RunFailureSensorContext) -> str | None:
    """Report a run failure to Sentry and return the event ID.

    Complements the in-process hook in ol_orchestrate.lib.sentry. That hook has
    better fidelity but cannot run when the process itself dies -- an OOM-killed
    run worker, a pod reaped by run monitoring, or a failure during run startup
    before any step executed. Those only ever surface here.
    """
    if not sentry_sdk.get_client().is_active():
        return None

    run = context.dagster_run
    failure = first_step_failure(context)
    step_key = failure.step_key if failure else "run"
    error_type = step_failure_error_type(context) or "run_failure"

    with sentry_sdk.new_scope() as scope:
        scope.set_tag("dagster_job", run.job_name)
        scope.set_tag("dagster_step", step_key)
        scope.set_tag("dagster_run_id", run.run_id)
        scope.set_tag("dagster_code_location", code_location_of(run) or "unknown")
        # Matches the hook's tag, which reads the same run tag. Without it a
        # partitioned asset's issue names everything except the one identifier
        # you need to go and look at the failure.
        scope.set_tag("dagster_partition", run.tags.get(PARTITION_NAME_TAG, "none"))
        scope.set_tag("captured_by", "sensor")
        # The hook path puts the exception type in the event itself. This one
        # reports through capture_message, which has no exception, so the type
        # has to be carried explicitly or it exists only inside the fingerprint
        # -- where nothing in the Sentry UI will show it, and no search can
        # filter on it.
        scope.set_tag("dagster_exception_type", error_type)
        scope.set_context(
            "dagster_step_failure",
            {
                "step_key": step_key,
                "exception_type": error_type,
                "error": sentry_failure_detail(context),
            },
        )
        scope.set_context(
            "dagster_run",
            {
                "run_id": run.run_id,
                "job_name": run.job_name,
                "run_url": f"{dagster_url}/runs/{run.run_id}",
                "tags": dict(run.tags),
            },
        )
        scope.fingerprint = sentry_fingerprint(context)
        event_id = sentry_sdk.capture_message(
            sentry_message(context),
            level="error",
        )

    sentry_sdk.flush(timeout=5.0)
    return event_id


def error_message(
    context: RunFailureSensorContext,
    sentry_event_id: str | None = None,
    suppressed_repeats: int = 0,
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

    # The count is the difference between "an asset broke" and "an asset has
    # been breaking on a loop since the last message", which the unbatched
    # stream could not express except by posting thousands of times.
    if suppressed_repeats:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f":repeat: *{suppressed_repeats} further failures* of "
                        "this same step were suppressed since the last message."
                    ),
                },
            }
        )

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


# How long one distinct failure stays announced before it is announced again.
# The sensor fires per failed run with no ceiling, so a single broken asset on
# the automation treadmill posted ~27,900 messages in fourteen days -- past the
# first few, every one of them said exactly what the one before it said.
SLACK_REPEAT_WINDOW_SECONDS = 30 * 60

# Namespaced so these cannot collide with Dagster's own cursor keys in the same
# table.
SLACK_ANNOUNCEMENT_KEY_PREFIX = "ol/slack_failure_announced/"


def slack_announcement_key(fingerprint: Sequence[str]) -> str:
    """Build the rate-limit key for a failure, keyed the way Sentry groups it."""
    return SLACK_ANNOUNCEMENT_KEY_PREFIX + "|".join(fingerprint)


def repeats_to_announce(
    instance: Any,
    key: str,
    now: float,
) -> int | None:
    """Whether to post, and how many repeats were swallowed since last time.

    Returns None to stay quiet, or the number of occurrences suppressed since
    the previous message -- which is the part worth saying, because "this failed
    once" and "this failed four thousand times" are different situations and the
    unbatched stream rendered them identically.

    Deliberately does NOT open a new window; ``record_announcement`` does that,
    after Slack has accepted the message. Opening it here would mean a Vault
    outage or a Slack 5xx -- which fails the tick on purpose -- left a record
    behind saying the failure had been announced, so the retry suppressed it and
    every later occurrence stayed hidden for the rest of the window.

    Suppressions *are* recorded here, because that path posts nothing and so has
    nothing to fail.

    State lives in the run storage KV table rather than the sensor cursor:
    ``RunFailureSensorContext`` is invoked once per failed run and exposes no
    cursor to carry counts between those invocations.
    """
    record = instance.run_storage.get_cursor_values({key}).get(key)
    if not record:
        return 0

    announced_at, _, count = record.partition(":")
    last_announced, suppressed = float(announced_at), int(count)

    if now - last_announced < SLACK_REPEAT_WINDOW_SECONDS:
        instance.run_storage.set_cursor_values(
            {key: f"{last_announced}:{suppressed + 1}"}
        )
        return None

    return suppressed


def record_announcement(instance: Any, key: str, now: float) -> None:
    """Open a fresh rate-limit window, having actually announced something.

    Separate from ``repeats_to_announce`` so the window only starts once the
    message is out. Also clears the suppressed count, which the message just
    reported.
    """
    instance.run_storage.set_cursor_values({key: f"{now}:0"})


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
        "Reports the first failed attempt and suppresses its automatic retries, "
        "so a failure is announced once whether or not a retry clears it. "
        "Slack is rate-limited per distinct failure; Sentry gets every one."
    ),
)
def run_failure_notification_sensor(context: RunFailureSensorContext) -> None:
    """Report a failed run to Sentry, then announce it in Slack."""
    run = context.dagster_run

    if is_retry_of_a_reported_failure(run):
        context.log.info(
            "Skipping notification for run %s: it is retry %s of a failure "
            "already reported on the first attempt.",
            run.run_id,
            run.tags.get(RETRY_NUMBER_TAG),
        )
        return

    if is_an_interrupted_worker(context):
        context.log.info(
            "Skipping notification for run %s: its worker was terminated "
            "(deploy, eviction or drain) rather than failing.",
            run.run_id,
        )
        return

    fingerprint = sentry_fingerprint(context)
    sentry_event_id = capture_run_failure_to_sentry(context)

    # Sentry still receives every failure -- it aggregates, and the count is the
    # signal. Slack does not aggregate, so the same failure repeating is rate
    # limited to one message per window with a count of what it swallowed.
    #
    # The window is measured in wall-clock time rather than the run's creation
    # time: a backlog of old failures drained in one tick carries creation
    # timestamps hours apart and out of order, which would both let several
    # messages through at once and suppress newer failures behind older ones.
    announced_at = datetime.now(tz=UTC).timestamp()
    announcement_key = slack_announcement_key(fingerprint)
    repeats = repeats_to_announce(context.instance, announcement_key, announced_at)
    if repeats is None:
        context.log.info(
            "Suppressing Slack notification for run %s: %s was already "
            "announced within the last %d minutes.",
            run.run_id,
            "|".join(fingerprint),
            SLACK_REPEAT_WINDOW_SECONDS // 60,
        )
        return

    # Deliberately unguarded. A failure here fails the sensor tick, which
    # surfaces in the Dagster UI and in Sentry -- the alternative is alerting
    # that has quietly stopped working and nobody knowing. The window is opened
    # only after the post succeeds, so a failed tick retries rather than
    # silencing the next thirty minutes.
    client = WebClient(token=get_slack_token())
    client.chat_postMessage(
        channel=slack_channel,
        blocks=error_message(context, sentry_event_id, repeats),
        text=f"Dagster {DAGSTER_ENV} run failure: {run.job_name}",
    )
    record_announcement(context.instance, announcement_key, announced_at)


def is_reported_by_the_run_failure_sensor(evaluation: Any) -> bool:
    """Whether a failed check's run already reported this with full detail.

    A failing dbt test makes ``dbt build`` exit non-zero, which fails the step
    and the run, so the run failure sensor announces it with dbt's own error
    text -- naming every failed test and the database error underneath. Repeating
    it here added a second message that named the check but not the cause, and
    described a test that errored before evaluating anything in the same words
    as a violated assertion.

    Identified by dagster-dbt's full provenance, not by ``status`` alone:
    ``status`` is caller-defined metadata that a native check is free to record
    (``{"status": "unhealthy"}``), and dropping those would silence a check
    nothing else reports. All three keys together are stamped only by
    dagster-dbt's own result-to-evaluation conversion.

    Freshness checks and native Dagster asset checks carry no such signature.
    Those have no failed run behind them, so this sensor is the only thing that
    will ever report them.
    """
    return set(evaluation.metadata or {}) >= DBT_PROVENANCE_KEYS


# Bounds on the substance appended to each failure's detail block. Kept small
# on purpose: this appends to the block's existing text rather than adding a
# block per failure, which is what keeps the 1-header-plus-1-block-per-failure
# arithmetic behind MAX_CHECK_EVALUATIONS_PER_TICK intact.
MAX_METADATA_ENTRIES_PER_FAILURE = 3
MAX_METADATA_VALUE_LENGTH = 200
MAX_DESCRIPTION_LENGTH = 300

# Metadata types rendered inline. All are scalars a human can read at a
# glance -- JSON, tables and other structured metadata are left for the
# Dagster UI, which is one click away via the "View run" button, rather than
# dumped into the notification unbounded.
_RENDERABLE_METADATA_TYPES = (
    BoolMetadataValue,
    FloatMetadataValue,
    IntMetadataValue,
    MarkdownMetadataValue,
    TextMetadataValue,
    TimestampMetadataValue,
    UrlMetadataValue,
)


def _render_metadata_value(value: MetadataValue) -> str:
    if isinstance(value, TimestampMetadataValue):
        return datetime.fromtimestamp(value.value, tz=UTC).isoformat(timespec="seconds")
    return str(value.value)


def format_check_metadata(metadata: Mapping[str, Any] | None) -> str:
    """Render a bounded, human-readable subset of a check's metadata.

    Only the first MAX_METADATA_ENTRIES_PER_FAILURE scalar-valued keys render,
    in metadata's own (insertion) order -- a check author who wants something
    seen puts it first. Everything else, structured or overflow, stays in the
    UI rather than turning the notification into a dump of the mapping.
    """
    lines = []
    for key, value in (metadata or {}).items():
        if not isinstance(value, _RENDERABLE_METADATA_TYPES):
            continue
        rendered = truncate_text(
            _render_metadata_value(value), MAX_METADATA_VALUE_LENGTH
        )
        lines.append(f"• *{key}:* {rendered}")
        if len(lines) == MAX_METADATA_ENTRIES_PER_FAILURE:
            break
    return "\n".join(lines)


def asset_check_failure_message(
    failures: Sequence[tuple[str, Any]],
) -> list[dict[str, Any]]:
    """Format a batch of failed asset check evaluations for Slack.

    Each failure links to the run that produced it rather than a shared link,
    since a single batch can span multiple runs. Beneath the check name, each
    detail block also carries the check's own description (freshness checks
    write one) and a bounded slice of its metadata -- see
    format_check_metadata -- so the substance a check computed does not
    require a click into the UI to see.
    """

    def _detail_text(evaluation: Any) -> str:
        lines = [
            f"*{evaluation.asset_key.to_user_string()}* -- `{evaluation.check_name}`"
        ]
        description = getattr(evaluation, "description", None)
        if description:
            lines.append(truncate_text(description, MAX_DESCRIPTION_LENGTH))
        metadata_text = format_check_metadata(getattr(evaluation, "metadata", None))
        if metadata_text:
            lines.append(metadata_text)
        # Per-field limits (MAX_DESCRIPTION_LENGTH, MAX_METADATA_VALUE_LENGTH)
        # bound each piece, but not an unbounded metadata *key* or the sum of
        # several fields together. A section block over Slack's 3000-character
        # text limit fails the whole chat.postMessage call -- and by then the
        # sensor has already advanced its cursor past this batch, permanently
        # dropping it. This is the backstop for that.
        return truncate_text("\n".join(lines))

    detail_blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _detail_text(evaluation),
            },
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": "View run"},
                "url": f"{dagster_url}/runs/{run_id}",
            },
        }
        for run_id, evaluation in failures
    ]
    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": (
                    f"Dagster {DAGSTER_ENV.capitalize()} Asset Check Failure "
                    f"({len(failures)})"
                ),
            },
        },
        *detail_blocks,
    ]


def collect_new_check_failures(
    instance: Any, cursor: int | None
) -> tuple[list[tuple[str, Any]], str | None]:
    """Read the next batch of asset check evaluations above ``cursor``.

    Returns (run_id, evaluation) pairs for the ERROR-severity failures in that
    batch and the cursor to store, or an empty list and None when there is
    nothing new. The run_id travels with each evaluation because
    ``AssetCheckEvaluation`` itself carries none -- it is only on the
    surrounding event log record -- and the Slack message needs it to link
    back to the run that raised the failure.

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
        (
            record.event_log_entry.run_id,
            record.event_log_entry.dagster_event.event_specific_data,
        )
        for record in records
    ]
    # WARN-severity checks are advisory; only ERROR is worth a notification. dbt
    # tests are dropped because their run failure already reports them in full.
    failures = [
        (run_id, evaluation)
        for run_id, evaluation in evaluations
        if not evaluation.passed
        and evaluation.severity == AssetCheckSeverity.ERROR
        and not is_reported_by_the_run_failure_sensor(evaluation)
    ]
    # The newest record of an ascending batch: nothing older is left behind,
    # nothing newer is re-delivered.
    return failures, str(records[-1].storage_id)


@sensor(
    name="asset_check_failure_sensor",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Announces ERROR-severity failures of checks that have no failed run "
        "behind them -- the freshness checks and any native Dagster asset check "
        "-- in Slack. A failing dbt test does fail its run, so it is left to the "
        "run failure sensor, which reports it with dbt's own error text."
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
