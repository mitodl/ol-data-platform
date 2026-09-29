"""Tests for openedx.sensors.openedx."""

import threading
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from dagster import AssetKey, DagsterInstance, build_sensor_context
from openedx.assets.openedx import courseware_data_version
from openedx.partitions.openedx import OPENEDX_COURSE_RUN_PARTITIONS
from openedx.sensors.openedx import (
    course_run_sensor,
    courseware_observation_sensor,
    cursor_offset,
    next_offset,
    resume_order,
)

COURSEWARE_SENSOR_NAME = "mitxonline_courseware_sensor"
OBSERVATION_SENSOR_NAME = "mitxonline_courseware_observation_sensor"
COURSEWARE_KEY = AssetKey(["mitxonline", "openedx", "courseware"])


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
    """Stand-in for OpenEdxApiClientFactory.

    Carries whichever client the sensor under test calls: discovery reads the
    catalog, observation reads content versions.
    """

    def __init__(
        self,
        client: "_CatalogClient | _VersionsClient",
        deployment: str = "mitxonline",
    ) -> None:
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


def _facts(
    published: str,
    files: int = 0,
    latest_upload: str | None = None,
    transcripts: int = 0,
    latest_transcript: str | None = None,
) -> dict[str, Any]:
    """Build one course's entry in a content versions response."""
    return {
        "published_version": published,
        "static_assets": {
            "count": files,
            "latest_upload": latest_upload,
            "latest_asset": None,
        },
        "transcripts": {"count": transcripts, "latest_modified": latest_transcript},
    }


class _VersionsClient:
    """Serves canned content versions, optionally failing or blocking a batch.

    A batch that holds any course in ``raises`` or ``blocks`` fails or blocks as
    a whole, as a real request would.
    """

    def __init__(
        self,
        versions: dict[str, str],
        missing: set[str] | None = None,
        raises: set[str] | None = None,
        blocks: set[str] | None = None,
        malformed: set[str] | None = None,
        *,
        available: bool = True,
    ) -> None:
        self.available = available
        self.versions = versions
        self.missing = missing or set()
        self.raises = raises or set()
        self.blocks = blocks or set()
        self.malformed = malformed or set()
        self.released = threading.Event()
        self.batches: list[list[str]] = []

    def course_content_versions_available(self) -> bool:
        return self.available

    def get_course_content_versions(self, course_ids: list[str]) -> dict[str, Any]:
        self.batches.append(course_ids)
        if self.blocks.intersection(course_ids):
            # Bounded so a hung test fails on its assertion rather than its
            # timeout, and long enough that the sweep budget always wins.
            self.released.wait(timeout=30)
        if self.raises.intersection(course_ids):
            msg = f"boom for {course_ids}"
            raise ValueError(msg)
        if self.malformed.intersection(course_ids):
            return {"versions": {course_id: {} for course_id in course_ids}}
        return {
            "versions": {
                course_id: _facts(self.versions[course_id])
                for course_id in course_ids
                if course_id in self.versions
            },
            "missing": [c for c in course_ids if c in self.missing],
        }


@pytest.fixture
def one_course_per_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Put each course in its own request, so batch failures isolate one course."""
    monkeypatch.setattr("openedx.assets.openedx.VERSIONS_BATCH_SIZE", 1)


def _observations(result) -> dict[str, str]:
    """Map partition key to the data version the sensor reported for it."""
    return {
        event.partition: event.tags["dagster/data_version"]
        for event in result.asset_events
        if event.partition is not None
    }


def test_observation_sensor_reports_a_version_for_every_partition(
    instance: DagsterInstance,
) -> None:
    """One tick observes the whole deployment, and asks for no runs at all.

    The run count is the point: the automation condition this replaced asked for
    one observation run per partition, each of which swept every course anyway.
    """
    _seed_partitions(instance, ["course-a", "course-b"])
    client = _VersionsClient({"course-a": "v1", "course-b": "v2"})

    result = courseware_observation_sensor(
        build_sensor_context(instance=instance, sensor_name=OBSERVATION_SENSOR_NAME),
        _FakeFactory(client),
    )

    assert _observations(result) == {
        "course-a": courseware_data_version(_facts("v1")),
        "course-b": courseware_data_version(_facts("v2")),
    }
    assert not result.run_requests
    assert all(event.asset_key == COURSEWARE_KEY for event in result.asset_events), (
        "observations must land on the courseware source asset"
    )


def test_observation_sensor_asks_for_courses_in_batches(
    instance: DagsterInstance, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Courses are fetched a batch per request, never one request per course."""
    monkeypatch.setattr("openedx.assets.openedx.VERSIONS_BATCH_SIZE", 2)
    keys = ["course-a", "course-b", "course-c", "course-d", "course-e"]
    _seed_partitions(instance, keys)
    client = _VersionsClient(dict.fromkeys(keys, "v1"))

    result = courseware_observation_sensor(
        build_sensor_context(instance=instance, sensor_name=OBSERVATION_SENSOR_NAME),
        _FakeFactory(client),
    )

    assert sorted(len(batch) for batch in client.batches) == [1, 2, 2]
    assert set(_observations(result)) == set(keys)


def test_observation_sensor_omits_a_course_missing_from_the_instance(
    instance: DagsterInstance,
) -> None:
    """A course the instance no longer has reports nothing.

    Its last known version stands. Inventing one for a course that no longer
    exists would read as a change and ask for an export of something that cannot
    be exported.
    """
    _seed_partitions(instance, ["course-a", "course-gone"])
    client = _VersionsClient({"course-a": "v1"}, missing={"course-gone"})

    result = courseware_observation_sensor(
        build_sensor_context(instance=instance, sensor_name=OBSERVATION_SENSOR_NAME),
        _FakeFactory(client),
    )

    assert set(_observations(result)) == {"course-a"}


def test_the_cursor_steps_past_courses_reported_missing(
    instance: DagsterInstance,
) -> None:
    """A missing course was finished, so the next tick must not sweep it again."""
    _seed_partitions(instance, ["course-a", "course-b", "course-gone"])
    client = _VersionsClient(
        {"course-a": "v1", "course-b": "v1"}, missing={"course-gone"}
    )

    result = courseware_observation_sensor(
        build_sensor_context(
            instance=instance, sensor_name=OBSERVATION_SENSOR_NAME, cursor="1"
        ),
        _FakeFactory(client),
    )

    assert result.cursor == "1", "a full pass of three returns to where it began"


@pytest.mark.usefixtures("one_course_per_batch")
def test_observation_sensor_survives_one_failing_batch(
    instance: DagsterInstance,
) -> None:
    """One bad request does not cost the deployment its whole sweep."""
    _seed_partitions(instance, ["course-a", "course-bad"])
    client = _VersionsClient({"course-a": "v1"}, raises={"course-bad"})

    result = courseware_observation_sensor(
        build_sensor_context(instance=instance, sensor_name=OBSERVATION_SENSOR_NAME),
        _FakeFactory(client),
    )

    assert set(_observations(result)) == {"course-a"}


@pytest.mark.usefixtures("one_course_per_batch")
def test_a_malformed_response_fails_its_batch_not_the_sweep(
    instance: DagsterInstance,
) -> None:
    """A 200 whose body can't be read counts against its batch like a 500."""
    _seed_partitions(instance, ["course-a", "course-odd"])
    client = _VersionsClient({"course-a": "v1"}, malformed={"course-odd"})

    result = courseware_observation_sensor(
        build_sensor_context(instance=instance, sensor_name=OBSERVATION_SENSOR_NAME),
        _FakeFactory(client),
    )

    assert set(_observations(result)) == {"course-a"}


def test_nothing_is_posted_to_a_studio_without_the_versions_endpoint(
    instance: DagsterInstance,
) -> None:
    """An older plugin would turn the batch POST into an export of every course.

    So the sweep fails the tick before sending a single batch, rather than
    queuing thousands of exports an hour.
    """
    _seed_partitions(instance, ["course-a", "course-b"])
    client = _VersionsClient({"course-a": "v1", "course-b": "v1"}, available=False)

    with pytest.raises(RuntimeError, match="does not serve"):
        courseware_observation_sensor(
            build_sensor_context(
                instance=instance, sensor_name=OBSERVATION_SENSOR_NAME
            ),
            _FakeFactory(client),
        )
    assert client.batches == []


def test_observation_sensor_fails_when_every_lookup_fails(
    instance: DagsterInstance,
) -> None:
    """A total failure is a bad token or a down Studio, not a quiet deployment.

    Reporting it as a clean tick would leave every downstream silent, hourly,
    forever -- with nothing in the logs that looks like a problem. A Studio
    without the course export plugin's versions endpoint fails this way too.
    """
    _seed_partitions(instance, ["course-a", "course-b"])
    client = _VersionsClient({}, raises={"course-a", "course-b"})

    with pytest.raises(RuntimeError, match="failed for all"):
        courseware_observation_sensor(
            build_sensor_context(
                instance=instance, sensor_name=OBSERVATION_SENSOR_NAME
            ),
            _FakeFactory(client),
        )


def test_observation_sensor_is_quiet_with_no_partitions(
    instance: DagsterInstance,
) -> None:
    """A deployment whose courses have not been discovered yet is not an error."""
    result = courseware_observation_sensor(
        build_sensor_context(instance=instance, sensor_name=OBSERVATION_SENSOR_NAME),
        _FakeFactory(_VersionsClient({})),
    )

    assert not result.asset_events


def test_each_kind_of_change_moves_its_own_part_of_the_data_version() -> None:
    """A publish, an upload and a transcript change each move the version.

    A version built from the published version alone left course_xml serving
    archives missing every transcript and file uploaded since, because neither
    kind of change ever moves it.
    """
    base = courseware_data_version(_facts("v1")).split("/")
    published = courseware_data_version(_facts("v2")).split("/")
    uploaded = courseware_data_version(
        _facts("v1", files=1, latest_upload="2026-09-01T00:00:00+00:00")
    ).split("/")
    transcribed = courseware_data_version(
        _facts("v1", transcripts=2, latest_transcript="2026-09-05T00:00:00+00:00")
    ).split("/")

    assert courseware_data_version(_facts("v1")).split("/") == base, "stable"
    assert base[0] == "v1", "the published version stays readable"
    assert [published[0] != base[0], published[1:] == base[1:]] == [True, True]
    assert [uploaded[1] != base[1], uploaded[::2] == base[::2]] == [True, True]
    assert [transcribed[2] != base[2], transcribed[:2] == base[:2]] == [True, True]


def test_a_malformed_cursor_restarts_from_the_top() -> None:
    """Losing the cursor costs one pass over the head, not the whole sweep."""
    assert cursor_offset(None) == 0
    assert cursor_offset("") == 0
    assert cursor_offset("not-a-number") == 0
    assert cursor_offset("-4") == 0
    assert cursor_offset("7") == 7


def test_resume_order_rotates_and_wraps() -> None:
    """Every course is reachable from any offset, including one past the end."""
    keys = ["a", "b", "c", "d"]

    assert resume_order(keys, 0) == ["a", "b", "c", "d"]
    assert resume_order(keys, 2) == ["c", "d", "a", "b"]
    assert resume_order(keys, 6) == ["c", "d", "a", "b"], "offset wraps"
    assert sorted(resume_order(keys, 3)) == keys, "rotation drops nothing"
    assert resume_order([], 3) == []


def test_next_offset_steps_past_a_course_that_never_finishes() -> None:
    """A blocker must not pin the cursor to its own index.

    Resuming at the earliest miss looks fairer, but a course that blocks every
    tick would hold the cursor at its index forever: the same prefix is swept
    again and again and the tail behind it is never reached.
    """
    assert next_offset(0, 3, 4) == 3, "steps past what finished"
    assert next_offset(3, 3, 4) == 2, "wraps"
    assert next_offset(0, 0, 4) == 1, "a pass that finished nothing still moves"
    assert next_offset(0, 4, 4) == 0, "a full pass returns to the top"
    assert next_offset(0, 1, 0) == 0, "no partitions, nowhere to go"


@pytest.mark.usefixtures("one_course_per_batch")
def test_a_blocked_course_does_not_stop_the_rest_being_reached(
    instance: DagsterInstance, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Successive partial ticks still reach every course behind the blocker.

    The failure this guards is silent: a course that hangs on every tick used
    to pin the cursor to its index, so the sweep re-fetched the same prefix
    forever and the courses behind it were simply never observed -- their
    exports would stop, with a green tick every hour and nothing in the logs
    that looks like a problem.
    """
    monkeypatch.setattr(
        "openedx.sensors.openedx.COURSEWARE_SWEEP_BUDGET", timedelta(seconds=1)
    )
    keys = ["course-a", "course-b", "course-c", "course-d"]
    _seed_partitions(instance, keys)
    # course-a never answers, on this tick or any other.
    client = _VersionsClient(dict.fromkeys(keys, "v1"), blocks={"course-a"})

    observed: set[str] = set()
    cursors: list[str | None] = []
    cursor: str | None = None
    for _ in range(4):
        result = courseware_observation_sensor(
            build_sensor_context(
                instance=instance,
                sensor_name=OBSERVATION_SENSOR_NAME,
                cursor=cursor,
            ),
            _FakeFactory(client),
        )
        observed |= set(_observations(result))
        cursor = result.cursor
        cursors.append(cursor)
    client.released.set()

    assert observed == {"course-b", "course-c", "course-d"}, (
        "every course except the blocked one must be reachable"
    )
    assert len(set(cursors)) > 1, "the cursor must move rather than pin on the blocker"
