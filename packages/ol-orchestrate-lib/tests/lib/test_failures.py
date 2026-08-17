"""Tests for ol_orchestrate.lib.failures.

The thing under test is not the exception classes, it is whether calling
something "permanent" actually stops it being re-run. `Failure(allow_retries=
False)` binds only an op's RetryPolicy; the run-level auto-reexecution daemon
reads run tags and never looks at the exception. So these drive real runs and
assert on the tag the daemon reads.
"""

import dagster as dg
import pytest
from dagster import AssetSpec, Failure, asset
from ol_orchestrate.lib.failures import (
    RETRY_ON_ASSET_OR_OP_FAILURE_TAG,
    PermanentFailure,
    permanent_failure,
    with_failure_hooks,
)
from ol_orchestrate.lib.http_errors import PermanentHTTPFailure, TransientHTTPFailure


def test_a_permanent_failure_refuses_op_level_retries() -> None:
    failure = permanent_failure("the course is gone")

    assert failure.allow_retries is False
    assert failure.metadata["retryable"].value is False


def test_permanent_http_failure_is_a_permanent_failure() -> None:
    """Otherwise http_failure's classification never reaches the retry hook."""
    assert issubclass(PermanentHTTPFailure, PermanentFailure)


def test_a_transient_http_failure_is_not_permanent() -> None:
    """A 502 must stay retryable -- the hook keys off the class."""
    assert not issubclass(TransientHTTPFailure, PermanentFailure)


def _run_failing_asset(exception: BaseException) -> dg.DagsterInstance:
    """Materialize an asset that raises ``exception``, hooks attached."""

    @asset(name="doomed")
    def doomed() -> None:
        raise exception

    (hooked,) = with_failure_hooks([doomed])
    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([hooked], instance=instance, raise_on_error=False)
    return instance


def _tags_of_the_only_run(instance: dg.DagsterInstance) -> dict[str, str]:
    (run,) = instance.get_runs()
    return dict(run.tags)


def test_a_permanent_failure_stops_run_retries() -> None:
    """The whole point of the task.

    Dagster's auto-reexecution daemon decides purely from run status and the
    dagster/retry_on_asset_or_op_failure tag -- it never inspects the exception.
    A Learn API 405 was correctly classified as unretryable and then retried
    5,363 times over six days because nothing wrote that tag.
    """
    instance = _run_failing_asset(permanent_failure("the endpoint is gone"))

    tags = _tags_of_the_only_run(instance)
    assert tags[RETRY_ON_ASSET_OR_OP_FAILURE_TAG] == "false"


def test_a_permanent_http_failure_stops_run_retries() -> None:
    """The subclass has to carry the behaviour, not just the name."""
    instance = _run_failing_asset(
        PermanentHTTPFailure(description="HTTP 405", allow_retries=False)
    )

    assert _tags_of_the_only_run(instance)[RETRY_ON_ASSET_OR_OP_FAILURE_TAG] == "false"


@pytest.mark.parametrize(
    "exception",
    [
        Failure(description="a plain failure"),
        TransientHTTPFailure(description="HTTP 502"),
        RuntimeError("something transient"),
    ],
    ids=["failure", "transient_http", "runtime_error"],
)
def test_a_failure_that_is_not_permanent_leaves_run_retries_alone(
    exception: BaseException,
) -> None:
    """Over-tagging would silently disable retries for every ordinary failure."""
    instance = _run_failing_asset(exception)

    assert RETRY_ON_ASSET_OR_OP_FAILURE_TAG not in _tags_of_the_only_run(instance)


def test_a_successful_run_is_not_tagged() -> None:
    @asset(name="calm")
    def calm() -> int:
        return 1

    (hooked,) = with_failure_hooks([calm])
    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize([hooked], instance=instance)

    assert result.success
    assert RETRY_ON_ASSET_OR_OP_FAILURE_TAG not in _tags_of_the_only_run(instance)


def test_with_failure_hooks_attaches_both_hooks() -> None:
    @asset
    def some_asset() -> int:
        return 1

    (hooked,) = with_failure_hooks([some_asset])

    assert {hook.name for hook in hooked.hook_defs} == {
        "capture_exception_to_sentry",
        "stop_run_retries",
    }


def test_with_failure_hooks_passes_through_non_assets_definitions() -> None:
    """AssetSpec has no ops to hook and must survive untouched."""
    spec = AssetSpec("an_external_asset")

    (passed_through,) = with_failure_hooks([spec])

    assert passed_through is spec
