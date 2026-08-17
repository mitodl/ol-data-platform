"""Sentry error tracking for Dagster code locations.

Wiring a code location up takes two calls:

    from ol_orchestrate.lib.sentry import init_sentry, with_sentry_hooks

    init_sentry("lakehouse")

    defs = Definitions(assets=with_sentry_hooks([...]), ...)

``init_sentry`` must be called at *module* scope. The default multiprocess
executor re-imports the definitions module in every step subprocess, and
``sentry_sdk`` state does not survive the fork, so a module-scope call is what
gives each subprocess its own initialized client. Initializing from inside a
resource or a hook would leave the step subprocesses unreported.
"""

import logging
import os
from collections.abc import Sequence
from typing import Any

import sentry_sdk
from dagster import AssetsDefinition, HookContext, failure_hook
from sentry_sdk.integrations.logging import LoggingIntegration, ignore_logger

from ol_orchestrate.lib.constants import DAGSTER_ENV

# Give the transport a bounded window to drain before a step subprocess exits.
# Run workers terminate promptly after a failure, so without an explicit flush
# the event we just captured can die with the process.
SENTRY_FLUSH_TIMEOUT_SECONDS = 5.0

# The code location this process has been initialized for, or None. Tracks the
# name rather than a bare bool so a second call naming a *different* location
# still re-tags -- see init_sentry.
_initialized_location: str | None = None

# Dagster's own run tag. Hardcoded rather than imported: the constant lives in
# the private dagster._core.storage.tags, but the tag string is stable and
# documented.
PARTITION_NAME_TAG = "dagster/partition"

# A run worker that receives SIGTERM -- a deploy rolling the deployment, a node
# draining, a pod evicted -- raises this on its way out. It is the process being
# taken away, not the code being wrong, and there is nothing in the traceback
# for anyone to act on.
INTERRUPTION_ERRORS = frozenset(
    {
        "DagsterExecutionInterruptedError",
        "KeyboardInterrupt",
    }
)


def exception_type_of(event: dict[str, Any]) -> str | None:
    """Return the type name of the exception an event carries, if it has one."""
    values = (event.get("exception") or {}).get("values") or []
    return values[-1].get("type") if values else None


def drop_interruptions(
    event: dict[str, Any],
    hint: dict[str, Any],  # noqa: ARG001
) -> dict[str, Any] | None:
    """``before_send``: suppress events that only report a terminated process.

    Deliberately narrow. Connection-class transients -- pgbouncer churn, httpx
    timeouts, DNS -- are handled by giving the assets that touch the database
    and external HTTP a ``RetryPolicy`` instead of by filtering here: a step
    failure hook fires only once a RetryPolicy is exhausted, so a connection
    error that still reaches Sentry is one that survived its retries and is
    worth reading. Filtering those at the transport would hide the sustained
    outage along with the blip.
    """
    if exception_type_of(event) in INTERRUPTION_ERRORS:
        return None
    return event


def current_code_location() -> str | None:
    """Return the code location this process was initialized for, if any."""
    return _initialized_location


def partition_key_of(context: HookContext) -> str:
    """Return the partition this step ran for, or ``none``.

    Deliberately a tag rather than context: for a partitioned asset "which
    partitions are broken" is the whole question, and an issue naming the job,
    the step and the run but not the partition cannot answer it without opening
    runs one at a time.

    ``HookContext`` exposes no partition of its own, so this reads the run tag
    Dagster stamps on every partitioned run -- one extra instance read on a path
    that is already about to write to Sentry and block on a flush.
    """
    run = context.instance.get_run_by_id(context.run_id)
    if run is None:
        return "unknown"
    return run.tags.get(PARTITION_NAME_TAG, "none")


def init_sentry(code_location: str) -> bool:
    """Initialize the Sentry SDK for a Dagster code location.

    Returns True when Sentry was configured, False when it was skipped because
    no DSN is set. An unset DSN is the normal case for local ``dagster dev``
    and for test collection, so it is a no-op rather than an error.

    Repeat calls naming the same location are a no-op. A call naming a
    different one re-tags without re-initializing the client, so the tag names
    whichever location was loaded most recently. In this deployment every code
    location gets its own container and its own process, so that case only
    arises locally or in tests; note that a single process genuinely emitting
    from two locations at once would need the tag set per-event instead.
    """
    global _initialized_location  # noqa: PLW0603

    dsn = os.environ.get("SENTRY_DSN")
    if not dsn:
        return False

    if _initialized_location is None:
        sentry_sdk.init(
            dsn=dsn,
            environment=DAGSTER_ENV,
            release=os.environ.get("SENTRY_RELEASE"),
            # Log records become breadcrumbs but never events. Every event this
            # deployment sends comes from an explicit capture below or from the
            # run failure sensor, so a step failure is reported once rather
            # than once per logger that happens to shout about it.
            integrations=[LoggingIntegration(level=logging.INFO, event_level=None)],
            before_send=drop_interruptions,
        )
        # Dagster logs the full failure of every step through its own logger.
        # Left alone it floods the breadcrumb trail with the same traceback we
        # are already attaching to the event.
        ignore_logger("dagster")
    elif _initialized_location == code_location:
        return True

    # Deliberately not re-running sentry_sdk.init() on a location change:
    # rebuilding the client would drop anything its transport still has
    # buffered.
    sentry_sdk.set_tag("dagster_code_location", code_location)
    _initialized_location = code_location
    return True


def failure_fingerprint(
    code_location: str | None,
    step_key: str | None,
    exception_type: str,
) -> list[str]:
    """Build the grouping key for a step failure, shared by both capture paths.

    ``step_key`` identifies the asset that broke. ``job_name`` -- which this
    triple used to lead with -- identifies how the run was *launched*, which is
    not a property of the defect: the same OVS webhook failure arrived as two
    separate issues because one run came from the automation sensor's
    ``__ASSET_JOB`` and the other from ``ovs_videos_webhook_job``.

    The environment leads instead. QA and production share one Sentry project,
    so without it a QA-only defect and a production outage merge into a single
    issue and become indistinguishable in the list.

    Both capture paths must build this identically or every failure raises two
    issues rather than one, which is why they call this rather than each
    assembling their own list.
    """
    return [DAGSTER_ENV, code_location or "unknown", step_key or "run", exception_type]


@failure_hook(name="capture_exception_to_sentry")
def capture_exception_to_sentry(context: HookContext) -> None:
    """Report a failed step to Sentry from inside the run worker.

    Runs in the process where the failure happened, so ``op_exception`` is the
    live exception object and Sentry gets real frames and locals instead of a
    traceback that has been flattened to a string.
    """
    exception = context.op_exception
    if exception is None:
        return

    with sentry_sdk.new_scope() as scope:
        scope.set_tag("dagster_job", context.job_name)
        scope.set_tag("dagster_step", context.step_key)
        scope.set_tag("dagster_run_id", context.run_id)
        scope.set_tag("dagster_partition", partition_key_of(context))
        scope.set_tag("captured_by", "hook")
        # Group by the step that broke rather than by traceback text, so a
        # recurring failure of one dbt model stays a single issue.
        scope.fingerprint = failure_fingerprint(
            current_code_location(),
            context.step_key,
            type(exception).__name__,
        )
        sentry_sdk.capture_exception(exception)

    sentry_sdk.flush(timeout=SENTRY_FLUSH_TIMEOUT_SECONDS)


def with_sentry_hooks(assets: Sequence[Any]) -> list[Any]:
    """Attach only the Sentry failure hook to every AssetsDefinition.

    Prefer ``ol_orchestrate.lib.failures.with_failure_hooks``, which attaches
    this alongside the hook that stops ``run_retries`` re-running a permanent
    failure. This narrower form exists for assets that deliberately want
    reporting without that retry behaviour.

    Hooks are attached to the asset rather than to a job on purpose. Job-level
    hooks only fire for runs launched from a job, which would miss everything
    materialized by an AutomationConditionSensorDefinition -- that is how the
    whole dbt project runs.

    Entries that are not AssetsDefinitions (AssetSpec, for instance) are passed
    through untouched; they have no ops to hook.
    """
    return [
        asset.with_hooks({capture_exception_to_sentry})
        if isinstance(asset, AssetsDefinition)
        else asset
        for asset in assets
    ]
