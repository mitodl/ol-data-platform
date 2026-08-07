"""Tests for openedx.sensors.openedx."""

from collections.abc import Iterator

import pytest
from dagster import DagsterInstance, build_sensor_context
from openedx.partitions.openedx import OPENEDX_COURSE_RUN_PARTITIONS
from openedx.sensors.openedx import course_run_sensor

COURSEWARE_SENSOR_NAME = "mitxonline_courseware_sensor"


@pytest.fixture
def instance() -> Iterator[DagsterInstance]:
    """Return a throwaway Dagster instance for event-log assertions."""
    with DagsterInstance.ephemeral() as ephemeral_instance:
        yield ephemeral_instance


class _CatalogClient:
    """Stand-in for OpenEdxApiClient that serves a course catalog."""

    def __init__(self, course_run_ids: list[str]) -> None:
        self.course_run_ids = course_run_ids

    def get_edx_course_ids(self) -> Iterator[list[dict[str, str]]]:
        yield [{"id": course_run_id} for course_run_id in self.course_run_ids]


class _FakeFactory:
    """Stand-in for OpenEdxApiClientFactory."""

    def __init__(self, client: _CatalogClient, deployment: str = "mitxonline") -> None:
        self.client = client
        self.deployment = deployment


def _seed_partitions(instance: DagsterInstance, keys: list[str]) -> None:
    """Register dynamic partitions for the mitxonline deployment."""
    instance.add_dynamic_partitions(
        OPENEDX_COURSE_RUN_PARTITIONS["mitxonline"].name, keys
    )


def _added_partitions(result) -> set[str]:
    """Collect the partition keys a SensorResult asks to register."""
    return {
        key
        for request in result.dynamic_partitions_requests
        for key in request.partition_keys
    }


def test_course_run_sensor_registers_unseen_course_runs(
    instance: DagsterInstance,
) -> None:
    """Course runs the LMS reports but Dagster has not seen become partitions."""
    _seed_partitions(instance, ["course-v1:org+num+known"])
    factory = _FakeFactory(
        _CatalogClient(["course-v1:org+num+known", "course-v1:org+num+new"])
    )

    result = course_run_sensor(
        build_sensor_context(instance=instance, sensor_name=COURSEWARE_SENSOR_NAME),
        factory,
    )

    assert _added_partitions(result) == {"course-v1:org+num+new"}


def test_course_run_sensor_requests_no_runs(instance: DagsterInstance) -> None:
    """Discovery only: launching exports belongs to the asset graph alone.

    Requesting runs here as well double-exported every new course and bypassed
    every throttle, because a sensor-launched run is invisible to the
    reconciliation that decides whether an export is still needed.
    """
    factory = _FakeFactory(_CatalogClient(["course-v1:org+num+new"]))

    result = course_run_sensor(
        build_sensor_context(instance=instance, sensor_name=COURSEWARE_SENSOR_NAME),
        factory,
    )

    assert not result.run_requests


def test_invalid_partition_strings_are_not_registered(
    instance: DagsterInstance,
) -> None:
    """A course id Dagster cannot use as a partition key is dropped, not raised."""
    factory = _FakeFactory(
        _CatalogClient(["course-v1:org+num+ok", "course-v1:org+num+ba\nd"])
    )

    result = course_run_sensor(
        build_sensor_context(instance=instance, sensor_name=COURSEWARE_SENSOR_NAME),
        factory,
    )

    assert _added_partitions(result) == {"course-v1:org+num+ok"}
