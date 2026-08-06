"""Tests for openedx.sensors.openedx."""

from collections.abc import Iterator
from datetime import timedelta

import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    build_sensor_context,
)
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.storage.tags import PARTITION_NAME_TAG, SENSOR_NAME_TAG
from dagster._core.test_utils import create_run_for_test
from openedx.partitions.openedx import OPENEDX_COURSE_RUN_PARTITIONS
from openedx.sensors.openedx import (
    course_version_sensor,
    in_flight_partitions,
    last_exported_version,
)

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


class _FakeClient:
    """Stand-in for OpenEdxApiClient that serves canned outlines."""

    def __init__(
        self, versions: dict[str, str], raises: set[str] | None = None
    ) -> None:
        self.versions = versions
        self.raises = raises or set()
        self.calls: list[str] = []

    def get_course_outline(self, course_id: str) -> dict[str, str]:
        self.calls.append(course_id)
        if course_id in self.raises:
            msg = f"boom for {course_id}"
            raise ValueError(msg)
        return {"published_version": self.versions[course_id]}


class _FakeFactory:
    """Stand-in for OpenEdxApiClientFactory."""

    def __init__(self, client: _FakeClient, deployment: str = "mitxonline") -> None:
        self.client = client
        self.deployment = deployment


def _seed_partitions(instance: DagsterInstance, keys: list[str]) -> None:
    """Register dynamic partitions for the mitxonline deployment."""
    instance.add_dynamic_partitions(
        OPENEDX_COURSE_RUN_PARTITIONS["mitxonline"].name, keys
    )


def _requested_partitions(result) -> set[str]:
    """Collect the partition keys a SensorResult asks to run."""
    return {request.partition_key for request in result.run_requests}


def test_only_changed_courses_are_requested(instance: DagsterInstance) -> None:
    """A partition whose recorded version matches the live one is left alone."""
    _seed_partitions(instance, ["course-v1:org+num+same", "course-v1:org+num+changed"])
    _record_export(instance, "course-v1:org+num+same", "version-one")
    _record_export(instance, "course-v1:org+num+changed", "version-one")
    factory = _FakeFactory(
        _FakeClient(
            {
                "course-v1:org+num+same": "version-one",
                "course-v1:org+num+changed": "version-two",
            }
        )
    )

    result = course_version_sensor(
        build_sensor_context(instance=instance, sensor_name=SENSOR_NAME), factory
    )

    assert _requested_partitions(result) == {"course-v1:org+num+changed"}


def test_never_exported_course_is_requested(instance: DagsterInstance) -> None:
    """No materialization means we do not know what is in S3, so re-export."""
    _seed_partitions(instance, ["course-v1:org+num+new"])
    factory = _FakeFactory(_FakeClient({"course-v1:org+num+new": "version-one"}))

    result = course_version_sensor(
        build_sensor_context(instance=instance, sensor_name=SENSOR_NAME), factory
    )

    assert _requested_partitions(result) == {"course-v1:org+num+new"}


def test_requests_carry_the_published_version_tag(instance: DagsterInstance) -> None:
    """Run tags record which version triggered the export."""
    _seed_partitions(instance, ["course-v1:org+num+new"])
    factory = _FakeFactory(_FakeClient({"course-v1:org+num+new": "version-one"}))

    result = course_version_sensor(
        build_sensor_context(instance=instance, sensor_name=SENSOR_NAME), factory
    )

    assert result.run_requests[0].tags["published_version"] == "version-one"


def test_in_flight_partitions_are_not_requested_again(
    instance: DagsterInstance,
) -> None:
    """A partition with an unfinished run is skipped even though it mismatches."""
    _seed_partitions(instance, ["course-v1:org+num+busy"])
    _record_run(instance, "course-v1:org+num+busy", DagsterRunStatus.NOT_STARTED)
    factory = _FakeFactory(_FakeClient({"course-v1:org+num+busy": "version-two"}))

    result = course_version_sensor(
        build_sensor_context(instance=instance, sensor_name=SENSOR_NAME), factory
    )

    assert result.run_requests == []


def test_failed_run_does_not_block_a_retry(instance: DagsterInstance) -> None:
    """A failed export is finished, so the mismatch is retried."""
    _seed_partitions(instance, ["course-v1:org+num+retry"])
    _record_run(instance, "course-v1:org+num+retry", DagsterRunStatus.FAILURE)
    factory = _FakeFactory(_FakeClient({"course-v1:org+num+retry": "version-two"}))

    result = course_version_sensor(
        build_sensor_context(instance=instance, sensor_name=SENSOR_NAME), factory
    )

    assert _requested_partitions(result) == {"course-v1:org+num+retry"}


def test_run_requests_are_capped(
    instance: DagsterInstance, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The cap bounds how many exports one tick can launch."""
    monkeypatch.setattr("openedx.sensors.openedx.MAX_RUN_REQUESTS_PER_TICK", 2)
    keys = [f"course-v1:org+num+{index}" for index in range(6)]
    _seed_partitions(instance, keys)
    factory = _FakeFactory(_FakeClient(dict.fromkeys(keys, "version-one")))

    result = course_version_sensor(
        build_sensor_context(instance=instance, sensor_name=SENSOR_NAME), factory
    )

    assert len(result.run_requests) == 2


def test_a_failing_outline_lookup_does_not_stop_the_sweep(
    instance: DagsterInstance,
) -> None:
    """One broken course is skipped; the rest of the sweep still reports."""
    _seed_partitions(instance, ["course-v1:org+num+bad", "course-v1:org+num+good"])
    factory = _FakeFactory(
        _FakeClient(
            {"course-v1:org+num+bad": "x", "course-v1:org+num+good": "version-one"},
            raises={"course-v1:org+num+bad"},
        )
    )

    result = course_version_sensor(
        build_sensor_context(instance=instance, sensor_name=SENSOR_NAME), factory
    )

    assert _requested_partitions(result) == {"course-v1:org+num+good"}


def test_exhausted_time_budget_returns_what_it_collected(
    instance: DagsterInstance, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A budget of zero ends the tick cleanly instead of raising."""
    monkeypatch.setattr(
        "openedx.sensors.openedx.SWEEP_TIME_BUDGET", timedelta(seconds=0)
    )
    keys = [f"course-v1:org+num+{index}" for index in range(4)]
    _seed_partitions(instance, keys)
    factory = _FakeFactory(_FakeClient(dict.fromkeys(keys, "version-one")))

    result = course_version_sensor(
        build_sensor_context(instance=instance, sensor_name=SENSOR_NAME), factory
    )

    assert result.run_requests == []
