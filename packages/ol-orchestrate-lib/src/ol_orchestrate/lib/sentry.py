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

_initialized = False


def init_sentry(code_location: str) -> bool:
    """Initialize the Sentry SDK for a Dagster code location.

    Returns True when Sentry was configured, False when it was skipped because
    no DSN is set. An unset DSN is the normal case for local ``dagster dev``
    and for test collection, so it is a no-op rather than an error.
    """
    global _initialized  # noqa: PLW0603

    dsn = os.environ.get("SENTRY_DSN")
    if not dsn or _initialized:
        return _initialized

    sentry_sdk.init(
        dsn=dsn,
        environment=DAGSTER_ENV,
        release=os.environ.get("SENTRY_RELEASE"),
        # Log records become breadcrumbs but never events. Every event this
        # deployment sends comes from an explicit capture below or from the
        # run failure sensor, so a step failure is reported once rather than
        # once per logger that happens to shout about it.
        integrations=[LoggingIntegration(level=logging.INFO, event_level=None)],
    )
    # Dagster logs the full failure of every step through its own logger. Left
    # alone it floods the breadcrumb trail with the same traceback we are
    # already attaching to the event.
    ignore_logger("dagster")

    sentry_sdk.set_tag("dagster_code_location", code_location)
    _initialized = True
    return True


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
        scope.set_tag("captured_by", "hook")
        # Group by the step that broke rather than by traceback text, so a
        # recurring failure of one dbt model stays a single issue.
        scope.fingerprint = [
            context.job_name,
            context.step_key,
            type(exception).__name__,
        ]
        sentry_sdk.capture_exception(exception)

    sentry_sdk.flush(timeout=SENTRY_FLUSH_TIMEOUT_SECONDS)


def with_sentry_hooks(assets: Sequence[Any]) -> list[Any]:
    """Attach the Sentry failure hook to every AssetsDefinition in ``assets``.

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
