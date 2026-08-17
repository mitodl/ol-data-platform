"""Failures a rerun cannot clear, and the wiring that makes that mean something.

Dagster has three independent retry mechanisms and ``Failure(allow_retries=False)``
only speaks to one of them:

- an op's ``RetryPolicy`` -- honours ``allow_retries`` (see
  ``dagster._core.execution.plan.utils.op_execution_error_boundary``);
- the run-level auto-reexecution daemon -- decides purely from run status, the
  ``dagster/max_retries`` and ``dagster/retry_on_asset_or_op_failure`` run tags,
  and the run group size. It never looks at the exception;
- an ``AutomationCondition`` -- sees only that the latest execution failed.

So a failure raised as permanent was still re-run by ``run_retries`` and still
re-requested by the automation condition. The Learn API 405 on Canvas course
2198 was correctly classified as unretryable and then retried 5,363 times over
six days.

``stop_run_retries`` closes the second gap: it stamps the run so the daemon
declines to retry it. Attach it, with the Sentry hook, via ``with_failure_hooks``.

The third gap stays open by construction. Declarative automation exposes no
primitive for "the failure was permanent" -- ``execution_failed()`` is the only
failure signal and it is untyped. ``upstream_or_code_changes()`` bounds it to one
re-request per failure edge instead (see ``automation_policies``), so a permanent
failure now costs two runs rather than an unbounded stream of them.
"""

from collections.abc import Sequence
from typing import Any

from dagster import AssetsDefinition, Failure, HookContext, failure_hook

from ol_orchestrate.lib.sentry import capture_exception_to_sentry

# Dagster's own run tag, read by the auto-reexecution daemon in
# dagster._daemon.auto_run_reexecution. Hardcoded rather than imported: the
# constant lives in the private dagster._core.storage.tags, but the tag string
# is stable and documented.
RETRY_ON_ASSET_OR_OP_FAILURE_TAG = "dagster/retry_on_asset_or_op_failure"


class PermanentFailure(Failure):
    """A failure a rerun cannot change.

    Subclass it rather than raising it directly when the *kind* of permanence is
    worth its own Sentry issue -- both capture paths fingerprint on the
    exception's class name, so everything raised as a bare ``PermanentFailure``
    from one step collapses into a single issue no matter what went wrong.
    """


def permanent_failure(
    description: str,
    metadata: dict[str, Any] | None = None,
) -> PermanentFailure:
    """Build a failure for something a rerun cannot fix, outside HTTP.

    ``description`` should say what the caller was trying to do and why another
    attempt will not help, in terms a reader of the Sentry issue can act on.

    Returned rather than raised so the call site keeps ``raise ... from error``
    where there is a cause, and the original traceback survives. Mirrors
    ``http_errors.http_failure``.
    """
    return PermanentFailure(
        description=description,
        metadata={
            "retryable": False,
            **(metadata or {}),
        },
        allow_retries=False,
    )


@failure_hook(name="stop_run_retries")
def stop_run_retries(context: HookContext) -> None:
    """Stop ``run_retries`` re-running a step that failed permanently.

    The daemon reads ``dagster/retry_on_asset_or_op_failure`` off the run when it
    processes the failure, which is after this hook has run in the run worker, so
    stamping it here is in time to be seen.

    A hook rather than a call at each raise site: the tag has to be set for every
    permanent failure or the guarantee is only as good as whoever remembered, and
    the raise sites do not all have an op context to hand.
    """
    if not isinstance(context.op_exception, PermanentFailure):
        return

    context.instance.add_run_tags(
        context.run_id, {RETRY_ON_ASSET_OR_OP_FAILURE_TAG: "false"}
    )


def with_failure_hooks(assets: Sequence[Any]) -> list[Any]:
    """Attach the failure hooks to every AssetsDefinition in ``assets``.

    Hooks are attached to the asset rather than to a job on purpose. Job-level
    hooks only fire for runs launched from a job, which would miss everything
    materialized by an AutomationConditionSensorDefinition -- that is how the
    whole dbt project runs.

    Entries that are not AssetsDefinitions (AssetSpec, for instance) are passed
    through untouched; they have no ops to hook.
    """
    return [
        asset.with_hooks({capture_exception_to_sentry, stop_run_retries})
        if isinstance(asset, AssetsDefinition)
        else asset
        for asset in assets
    ]
