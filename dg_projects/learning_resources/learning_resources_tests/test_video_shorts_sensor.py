"""Tests for video shorts sensors."""

from unittest.mock import MagicMock

import pytest
from dagster import (
    AddDynamicPartitionsRequest,
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    DeleteDynamicPartitionsRequest,
    MetadataValue,
    SensorResult,
    SkipReason,
    build_run_status_sensor_context,
    build_sensor_context,
    job,
    op,
)
from learning_resources.sensors.video_shorts import (
    MAX_STALE_DELETIONS,
    video_shorts_delete_partition_cleanup_sensor,
    video_shorts_discovery_sensor,
    video_shorts_stale_cleanup_sensor,
)

EXPECTED_TWO_RUNS = 2


@pytest.fixture
def instance() -> DagsterInstance:
    """Create an ephemeral Dagster instance for sensor tests."""
    return DagsterInstance.ephemeral()


@pytest.fixture
def make_materialization_event():
    """Build a sheets_api materialization event with partition metadata."""

    def _builder(
        processing_partition_keys: list[str],
        sheet_partition_keys: list[str] | None = None,
    ) -> MagicMock:
        sheet_keys = (
            sheet_partition_keys
            if sheet_partition_keys is not None
            else processing_partition_keys
        )
        event = MagicMock()
        event.run_id = "test-run-id"
        event.asset_materialization = AssetMaterialization(
            asset_key=AssetKey(["video_shorts", "sheets_api"]),
            metadata={
                "partition_keys": MetadataValue.json(processing_partition_keys),
                "processing_partition_keys": MetadataValue.json(
                    processing_partition_keys
                ),
                "sheet_partition_keys": MetadataValue.json(sheet_keys),
            },
        )
        return event

    return _builder


@pytest.fixture
def build_context(instance: DagsterInstance):
    """Build a sensor context with mocked instance methods."""

    def _builder(
        *,
        existing_partitions: list[str] | None = None,
        materialization_event: MagicMock | None = None,
    ):
        context = build_sensor_context(instance=instance)
        context.instance.get_latest_materialization_event = MagicMock(
            return_value=materialization_event
        )
        context.instance.get_dynamic_partitions = MagicMock(
            return_value=existing_partitions or []
        )
        return context

    return _builder


@pytest.fixture
def build_run_status_context(instance: DagsterInstance):
    """Build a Dagster run-status sensor context for direct invocation tests."""

    @op
    def _noop_op() -> None:
        return None

    @job
    def _noop_job() -> None:
        _noop_op()

    def _builder(partition_tag: str | None, *, stale_cleanup: bool = True):
        tags = {}
        if partition_tag:
            tags["dagster/partition"] = partition_tag
        if stale_cleanup:
            tags["video_shorts_stale_cleanup"] = "true"
        result = _noop_job.execute_in_process(instance=instance, tags=tags or None)
        return build_run_status_sensor_context(
            sensor_name="video_shorts_delete_partition_cleanup_sensor",
            dagster_instance=instance,
            dagster_run=result.dagster_run,
            dagster_event=result.get_run_success_event(),
        )

    return _builder


@pytest.mark.parametrize(
    ("materialization_keys", "existing_partitions", "expected_message"),
    [
        (None, [], "Waiting for initial sheets_api materialization"),
        ([], [], "No videos to process found in Google Sheets"),
        (["abc123"], ["abc123"], "No new videos found"),
    ],
)
def test_discovery_sensor_skip_cases(
    build_context,
    make_materialization_event,
    materialization_keys,
    existing_partitions,
    expected_message,
):
    """Discovery sensor should skip for no materialization, no keys, or no new keys."""
    event = (
        None
        if materialization_keys is None
        else make_materialization_event(materialization_keys)
    )
    context = build_context(
        materialization_event=event,
        existing_partitions=existing_partitions,
    )

    result = video_shorts_discovery_sensor(context)
    assert isinstance(result, SkipReason)
    assert result.skip_message == expected_message


def test_discovery_sensor_new_partitions_only(
    build_context, make_materialization_event
):
    """Discovery sensor should add and launch runs for only new partitions."""
    context = build_context(
        materialization_event=make_materialization_event(["abc123", "def456"]),
        existing_partitions=[],
    )

    result = video_shorts_discovery_sensor(context)
    assert isinstance(result, SensorResult)

    assert len(result.dynamic_partitions_requests) == 1
    request = result.dynamic_partitions_requests[0]
    assert isinstance(request, AddDynamicPartitionsRequest)
    assert set(request.partition_keys) == {"abc123", "def456"}

    assert len(result.run_requests) == EXPECTED_TWO_RUNS
    assert {run.partition_key for run in result.run_requests} == {"abc123", "def456"}


@pytest.mark.parametrize(
    ("materialization_keys", "existing_partitions", "expected_message"),
    [
        (None, [], "Waiting for initial sheets_api materialization"),
        ([], [], "No videos to process found in Google Sheets"),
        (["current"], ["current"], "No stale videos found"),
    ],
)
def test_stale_cleanup_sensor_skip_cases(
    build_context,
    make_materialization_event,
    materialization_keys,
    existing_partitions,
    expected_message,
):
    """Stale cleanup sensor should skip when there is nothing actionable."""
    event = (
        None
        if materialization_keys is None
        else make_materialization_event(materialization_keys)
    )
    context = build_context(
        materialization_event=event,
        existing_partitions=existing_partitions,
    )

    result = video_shorts_stale_cleanup_sensor(context)
    assert isinstance(result, SkipReason)
    assert result.skip_message == expected_message


def test_stale_cleanup_sensor_launches_delete_runs(
    build_context,
    make_materialization_event,
):
    """Stale cleanup sensor should launch delete runs for stale partitions."""
    context = build_context(
        materialization_event=make_materialization_event(["current"]),
        existing_partitions=["current", "stale_a", "stale_b"],
    )

    result = video_shorts_stale_cleanup_sensor(context)
    assert isinstance(result, SensorResult)
    assert len(result.run_requests) == EXPECTED_TWO_RUNS
    assert {run.partition_key for run in result.run_requests} == {"stale_a", "stale_b"}


def test_stale_cleanup_sensor_threshold_skip(
    build_context,
    make_materialization_event,
):
    """Stale cleanup sensor should skip if stale keys exceed safety threshold."""
    stale_keys = [f"stale_{idx}" for idx in range(MAX_STALE_DELETIONS + 1)]
    context = build_context(
        materialization_event=make_materialization_event(["current"]),
        existing_partitions=["current", *stale_keys],
    )

    result = video_shorts_stale_cleanup_sensor(context)
    assert isinstance(result, SkipReason)
    assert result.skip_message == "Stale partition count exceeds safety threshold"


def test_stale_cleanup_sensor_uses_full_sheet_membership(
    build_context,
    make_materialization_event,
):
    """Partitions omitted from processing window are not stale if still in sheet."""
    context = build_context(
        materialization_event=make_materialization_event(
            ["latest_a", "latest_b"],
            ["latest_a", "latest_b", "older_kept"],
        ),
        existing_partitions=["latest_a", "latest_b", "older_kept"],
    )

    result = video_shorts_stale_cleanup_sensor(context)
    assert isinstance(result, SkipReason)
    assert result.skip_message == "No stale videos found"


@pytest.mark.parametrize(
    (
        "partition_tag",
        "stale_cleanup",
        "has_partition",
        "expected_message",
        "should_delete",
    ),
    [
        (
            "stale_a",
            False,
            True,
            "Run was not triggered by stale cleanup sensor",
            False,
        ),
        (
            None,
            True,
            True,
            "Delete workflow run missing partition tag",
            False,
        ),
        (
            "stale_a",
            True,
            False,
            "Partition already removed or not found: stale_a",
            False,
        ),
        (
            "stale_a",
            True,
            True,
            None,
            True,
        ),
    ],
)
def test_delete_partition_cleanup_sensor(  # noqa: PLR0913
    build_run_status_context,
    partition_tag,
    stale_cleanup,
    has_partition,
    expected_message,
    should_delete,
):
    """Cleanup sensor deletes partitions only for sensor-triggered runs."""
    context = build_run_status_context(partition_tag, stale_cleanup=stale_cleanup)
    context.instance.has_dynamic_partition = MagicMock(return_value=has_partition)
    context.instance.delete_dynamic_partition = MagicMock()

    result = video_shorts_delete_partition_cleanup_sensor(context)

    if should_delete:
        assert isinstance(result, SensorResult)
        assert len(result.dynamic_partitions_requests) == 1
        request = result.dynamic_partitions_requests[0]
        assert isinstance(request, DeleteDynamicPartitionsRequest)
        assert request.partition_keys == [partition_tag]
    else:
        assert isinstance(result, SkipReason)
        assert result.skip_message == expected_message
