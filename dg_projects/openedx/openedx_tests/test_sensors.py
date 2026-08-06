"""Tests for openedx.sensors.openedx."""

from collections.abc import Iterator

import pytest
from dagster import AssetKey, AssetMaterialization, DagsterInstance
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.storage.tags import PARTITION_NAME_TAG, SENSOR_NAME_TAG
from dagster._core.test_utils import create_run_for_test
from openedx.sensors.openedx import in_flight_partitions, last_exported_version

COURSE_XML_KEY = AssetKey(("mitxonline", "openedx", "raw_data", "course_xml"))
SENSOR_NAME = "mitxonline_course_version_sensor"


@pytest.fixture
def instance() -> Iterator[DagsterInstance]:
    """Return a throwaway Dagster instance for event-log assertions."""
    with DagsterInstance.ephemeral() as ephemeral_instance:
        yield ephemeral_instance


def _record_export(
    instance: DagsterInstance, partition_key: str, version: str | None
) -> None:
    """Record a course_xml materialization, optionally without the version key."""
    metadata = {"courseware_published_version": version} if version else {}
    instance.report_runless_asset_event(
        AssetMaterialization(
            asset_key=COURSE_XML_KEY, partition=partition_key, metadata=metadata
        )
    )


def test_last_exported_version_reads_the_latest_materialization(
    instance: DagsterInstance,
) -> None:
    """The most recent materialization's recorded version wins."""
    _record_export(instance, "course-v1:org+num+run", "version-one")
    _record_export(instance, "course-v1:org+num+run", "version-two")

    result = last_exported_version(instance, COURSE_XML_KEY, "course-v1:org+num+run")

    assert result == "version-two"


def test_last_exported_version_is_none_when_never_exported(
    instance: DagsterInstance,
) -> None:
    """A partition with no materialization has no exported version."""
    result = last_exported_version(instance, COURSE_XML_KEY, "course-v1:org+num+run")

    assert result is None


def test_last_exported_version_is_none_for_pre_existing_archives(
    instance: DagsterInstance,
) -> None:
    """Archives materialized before the metadata key existed count as unknown."""
    _record_export(instance, "course-v1:org+num+run", None)

    result = last_exported_version(instance, COURSE_XML_KEY, "course-v1:org+num+run")

    assert result is None


def _record_run(
    instance: DagsterInstance,
    partition_key: str,
    status: DagsterRunStatus,
    sensor_name: str = SENSOR_NAME,
) -> None:
    """Create a run tagged the way a sensor-launched partitioned run is tagged."""
    create_run_for_test(
        instance,
        job_name="openedx_course_export",
        status=status,
        tags={SENSOR_NAME_TAG: sensor_name, PARTITION_NAME_TAG: partition_key},
    )


def test_in_flight_partitions_includes_runs_not_yet_started(
    instance: DagsterInstance,
) -> None:
    """A run waiting in the queue counts as in flight.

    NOT_STARTED and QUEUED are excluded from IN_PROGRESS_RUN_STATUSES, so this
    fails if the implementation reaches for that constant instead of
    NOT_FINISHED_STATUSES.
    """
    _record_run(instance, "course-v1:org+num+queued", DagsterRunStatus.NOT_STARTED)

    assert in_flight_partitions(instance, SENSOR_NAME) == {"course-v1:org+num+queued"}


def test_in_flight_partitions_includes_started_runs(
    instance: DagsterInstance,
) -> None:
    """A running export counts as in flight."""
    _record_run(instance, "course-v1:org+num+running", DagsterRunStatus.STARTED)

    assert in_flight_partitions(instance, SENSOR_NAME) == {"course-v1:org+num+running"}


def test_in_flight_partitions_excludes_finished_runs(
    instance: DagsterInstance,
) -> None:
    """Failed and successful runs are finished, so they do not suppress a retry."""
    _record_run(instance, "course-v1:org+num+failed", DagsterRunStatus.FAILURE)
    _record_run(instance, "course-v1:org+num+ok", DagsterRunStatus.SUCCESS)

    assert in_flight_partitions(instance, SENSOR_NAME) == set()


def test_in_flight_partitions_ignores_other_sensors(
    instance: DagsterInstance,
) -> None:
    """Runs launched by a different sensor are not this sensor's business."""
    _record_run(
        instance,
        "course-v1:org+num+other",
        DagsterRunStatus.STARTED,
        sensor_name="some_other_sensor",
    )

    assert in_flight_partitions(instance, SENSOR_NAME) == set()
