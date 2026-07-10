"""End-to-end tests for edxorg_upstream_changes_sensor against a real (ephemeral)
Dagster instance -- not mocked -- so the retry state machine is validated
against Dagster's own run-records/cursor semantics, not our assumptions about
them.
"""

import dagster as dg
import pytest
from dagster._core.test_utils import create_run_for_test
from data_loading.defs.ingestion.sensor import (
    _BATCH_ID_TAG,
    _EDXORG_UPSTREAM_ASSET_KEYS,
    edxorg_s3_ingest_job,
    edxorg_upstream_changes_sensor,
)

_TABLE_A_KEY = dg.AssetKey(["edxorg", "raw_data", "db_table", "auth_user"])
_TABLE_B_KEY = dg.AssetKey(
    ["edxorg", "raw_data", "db_table", "student_courseenrollment"]
)


@dg.asset(key=_TABLE_A_KEY)
def _archive_table_a() -> None: ...


@dg.asset(key=_TABLE_B_KEY)
def _archive_table_b() -> None: ...


_TEST_DEFINITIONS = dg.Definitions(
    assets=[_archive_table_a, _archive_table_b],
    jobs=[edxorg_s3_ingest_job],
    sensors=[edxorg_upstream_changes_sensor],
)


@pytest.fixture
def instance() -> dg.DagsterInstance:
    return dg.DagsterInstance.ephemeral()


def _evaluate(
    instance: dg.DagsterInstance, cursor: str | None
) -> tuple[dg.RunRequest | dg.SkipReason, str]:
    """Run one sensor tick and return (result, cursor to pass to the next tick)."""
    context = dg.build_multi_asset_sensor_context(
        monitored_assets=_EDXORG_UPSTREAM_ASSET_KEYS,
        instance=instance,
        cursor=cursor,
        definitions=_TEST_DEFINITIONS,
    )
    result = edxorg_upstream_changes_sensor(context)
    context.update_cursor_after_evaluation()
    return result, context.cursor


def _create_attempt(
    instance: dg.DagsterInstance,
    run_request: dg.RunRequest,
    status: dg.DagsterRunStatus,
) -> None:
    """Simulate the daemon launching run_request's run and it reaching status."""
    create_run_for_test(
        instance,
        job_name=edxorg_s3_ingest_job.name,
        tags=dict(run_request.tags),
        status=status,
    )


def test_no_new_materializations_skips(instance: dg.DagsterInstance) -> None:
    result, _ = _evaluate(instance, cursor=None)
    assert isinstance(result, dg.SkipReason)


def test_new_materialization_fires_run_request(instance: dg.DagsterInstance) -> None:
    dg.materialize([_archive_table_a], instance=instance)
    result, _ = _evaluate(instance, cursor=None)
    assert isinstance(result, dg.RunRequest)
    assert result.run_key is not None
    assert result.run_key.endswith("__attempt0")


def test_no_run_yet_launched_re_requests_the_same_key(
    instance: dg.DagsterInstance,
) -> None:
    """The bug this fixes: cursors used to advance before the run was known to
    succeed. If the daemon hasn't actually launched a run for the first
    request yet (no run record exists), the next tick must not invent a new
    attempt number or lose track of the batch -- it re-requests the same key.
    """
    dg.materialize([_archive_table_a], instance=instance)
    first, cursor = _evaluate(instance, cursor=None)
    assert isinstance(first, dg.RunRequest)

    second, _ = _evaluate(instance, cursor=cursor)
    assert isinstance(second, dg.RunRequest)
    assert second.run_key == first.run_key


def test_failed_run_is_retried_with_a_fresh_run_key(
    instance: dg.DagsterInstance,
) -> None:
    dg.materialize([_archive_table_a], instance=instance)
    first, cursor = _evaluate(instance, cursor=None)
    assert isinstance(first, dg.RunRequest)
    _create_attempt(instance, first, dg.DagsterRunStatus.FAILURE)

    second, cursor = _evaluate(instance, cursor=cursor)
    assert isinstance(second, dg.RunRequest)
    assert second.run_key != first.run_key
    assert second.run_key.endswith("__attempt1")
    # Same batch (same upstream materialization), not a coincidentally-new one.
    assert second.tags[_BATCH_ID_TAG] == first.tags[_BATCH_ID_TAG]

    # And it keeps retrying on repeated failure.
    _create_attempt(instance, second, dg.DagsterRunStatus.FAILURE)
    third, _ = _evaluate(instance, cursor=cursor)
    assert isinstance(third, dg.RunRequest)
    assert third.run_key.endswith("__attempt2")


def test_successful_run_advances_cursor_and_stops_retrying(
    instance: dg.DagsterInstance,
) -> None:
    dg.materialize([_archive_table_a], instance=instance)
    first, cursor = _evaluate(instance, cursor=None)
    assert isinstance(first, dg.RunRequest)
    _create_attempt(instance, first, dg.DagsterRunStatus.SUCCESS)

    second, cursor = _evaluate(instance, cursor=cursor)
    assert isinstance(second, dg.SkipReason)

    # Cursor genuinely advanced -- a third tick with no new data also skips.
    third, _ = _evaluate(instance, cursor=cursor)
    assert isinstance(third, dg.SkipReason)


def test_in_flight_run_is_not_retried(instance: dg.DagsterInstance) -> None:
    dg.materialize([_archive_table_a], instance=instance)
    first, cursor = _evaluate(instance, cursor=None)
    assert isinstance(first, dg.RunRequest)
    _create_attempt(instance, first, dg.DagsterRunStatus.STARTED)

    second, _ = _evaluate(instance, cursor=cursor)
    assert isinstance(second, dg.SkipReason)


def test_new_materialization_during_a_failure_retries_the_combined_batch(
    instance: dg.DagsterInstance,
) -> None:
    """A second table materializing while the first is being retried should
    fold into one combined-batch retry, not race/duplicate.
    """
    dg.materialize([_archive_table_a], instance=instance)
    first, cursor = _evaluate(instance, cursor=None)
    assert isinstance(first, dg.RunRequest)
    _create_attempt(instance, first, dg.DagsterRunStatus.FAILURE)

    dg.materialize([_archive_table_b], instance=instance)
    second, _ = _evaluate(instance, cursor=cursor)
    assert isinstance(second, dg.RunRequest)
    # A fresh batch (now includes table B too) -- first attempt at THIS batch.
    assert second.run_key.endswith("__attempt0")
    assert second.tags[_BATCH_ID_TAG] != first.tags[_BATCH_ID_TAG]
