"""Tests for ocw_content_publish_sensor against an ephemeral Dagster instance."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import dagster as dg
import pytest
from dagster._core.test_utils import create_run_for_test
from data_loading.definitions import defs
from data_loading.defs.ingestion import ocw_content_sensor
from data_loading.defs.ingestion.assets import (
    OCW_COURSE_LOAD_KEY,
    OCW_COURSE_SLUGS_NAME,
)
from data_loading.defs.ingestion.ocw_content_sensor import (
    MAX_COURSES_PER_POLL,
    POLL_INTERVAL_SECONDS,
    SETTLE_SECONDS,
    courses_published_between,
    ocw_content_publish_sensor,
)
from data_loading.defs.ingestion.schedules import (
    OCW_CONTENT_RUN_TAGS,
    OCW_CONTENT_SCHEDULE_NAME,
    OCW_CONTENT_SENSOR_NAME,
    ocw_content_sweep_due,
)
from ol_dlt.sources import ocw_content

# The previous tick's window ended this long before the tick under test.
_SINCE_AGO = timedelta(minutes=10)


@pytest.fixture
def instance() -> dg.DagsterInstance:
    return dg.DagsterInstance.ephemeral()


@pytest.fixture
def persistent_instance() -> Iterator[dg.DagsterInstance]:
    """Yield an instance on disk, which a schedule context needs."""
    with dg.instance_for_test() as instance:
        yield instance


@pytest.fixture
def published(monkeypatch: pytest.MonkeyPatch) -> dict[str, datetime]:
    """Stand in for the bucket's publish times, read by ``published_at``."""
    times: dict[str, datetime] = {}
    monkeypatch.setattr(
        ocw_content_sensor.s3fs, "S3FileSystem", lambda **_kwargs: object()
    )
    monkeypatch.setattr(
        ocw_content_sensor.ocw_content,
        "published_at",
        lambda _fs, bucket: times if bucket == ocw_content.BUCKET else {},
    )
    monkeypatch.setattr(
        ocw_content_sensor,
        "course_digest",
        lambda _fs, slug: f"digest-of-{slug}",
    )
    return times


def _tick(instance: dg.DagsterInstance, cursor: str | None) -> dg.SensorResult:
    context = dg.build_sensor_context(instance=instance, cursor=cursor)
    return ocw_content_publish_sensor(context)


def test_window_is_open_at_its_start_and_closed_at_its_end() -> None:
    since = datetime(2026, 10, 19, 18, 30, tzinfo=UTC)
    until = since + timedelta(minutes=5)
    published = {
        "at-since": since,
        "inside": since + timedelta(minutes=1),
        "at-until": until,
        "settling": until + timedelta(seconds=1),
    }

    assert courses_published_between(published, since, until) == [
        "at-until",
        "inside",
    ]


def test_first_tick_starts_watching_without_a_run(
    instance: dg.DagsterInstance, published: dict[str, datetime]
) -> None:
    now = datetime.now(tz=UTC)
    published["old-course"] = now - timedelta(days=30)

    result = _tick(instance, None)

    assert not result.run_requests
    assert result.cursor is not None
    assert datetime.fromisoformat(result.cursor) <= now


def test_requests_each_course_published_since_the_last_tick(
    instance: dg.DagsterInstance, published: dict[str, datetime]
) -> None:
    now = datetime.now(tz=UTC)
    since = now - _SINCE_AGO
    published.update(
        {
            "b-course": since + timedelta(minutes=1),
            "a-course": since + timedelta(minutes=2),
            "earlier": since - timedelta(seconds=1),
            # Still landing: picked up by a later tick.
            "settling": now,
        }
    )

    result = _tick(instance, since.isoformat())

    requests = result.run_requests or []
    assert {request.partition_key: request.run_key for request in requests} == {
        "a-course": "a-course:digest-of-a-course",
        "b-course": "b-course:digest-of-b-course",
    }
    assert result.cursor is not None
    cursor = datetime.fromisoformat(result.cursor)
    assert published["a-course"] < cursor < published["settling"]

    later = _tick(instance, result.cursor)
    assert not later.run_requests


def test_every_course_gets_a_partition_once(
    instance: dg.DagsterInstance, published: dict[str, datetime]
) -> None:
    since = datetime.now(tz=UTC) - _SINCE_AGO
    published.update({"old-course": since - timedelta(days=30)})

    requests = _tick(instance, since.isoformat()).dynamic_partitions_requests or []

    (request,) = requests
    assert isinstance(request, dg.AddDynamicPartitionsRequest)
    assert request.partitions_def_name == OCW_COURSE_SLUGS_NAME
    assert list(request.partition_keys) == ["old-course"]

    instance.add_dynamic_partitions(OCW_COURSE_SLUGS_NAME, ["old-course"])
    assert not _tick(instance, since.isoformat()).dynamic_partitions_requests


def test_the_sensor_targets_the_course_load_and_not_the_table() -> None:
    repository = defs.get_repository_def()
    sensor = repository.get_sensor_def(OCW_CONTENT_SENSOR_NAME)
    job = repository.get_job(sensor.job_name)
    assert job.asset_layer.executable_asset_keys == {OCW_COURSE_LOAD_KEY}


def test_mass_rebuild_is_left_to_the_sweep(
    instance: dg.DagsterInstance, published: dict[str, datetime]
) -> None:
    # One poll interval's worth of window.
    since = datetime.now(tz=UTC) - timedelta(
        seconds=SETTLE_SECONDS + POLL_INTERVAL_SECONDS
    )
    for number in range(1, MAX_COURSES_PER_POLL + 2):
        published[f"course-{number}"] = since + timedelta(seconds=number)

    result = _tick(instance, since.isoformat())

    assert not result.run_requests
    assert result.skip_reason is not None
    assert "rebuild" in str(result.skip_reason)
    assert result.cursor is not None
    assert datetime.fromisoformat(result.cursor) > since


def test_window_held_open_by_a_long_load_is_not_taken_for_a_rebuild(
    instance: dg.DagsterInstance, published: dict[str, datetime]
) -> None:
    since = datetime.now(tz=UTC) - timedelta(hours=2)
    for number in range(1, MAX_COURSES_PER_POLL + 2):
        published[f"course-{number}"] = since + timedelta(minutes=number)

    requests = _tick(instance, since.isoformat()).run_requests or []

    assert len(requests) == MAX_COURSES_PER_POLL + 1


@pytest.mark.parametrize(
    "tags",
    [
        {"dagster/schedule_name": OCW_CONTENT_SCHEDULE_NAME, **OCW_CONTENT_RUN_TAGS},
        {"dagster/sensor_name": OCW_CONTENT_SENSOR_NAME, **OCW_CONTENT_RUN_TAGS},
        # A load started from the launchpad with the tag.
        OCW_CONTENT_RUN_TAGS,
        # A sweep started before the tag existed.
        {"dagster/schedule_name": OCW_CONTENT_SCHEDULE_NAME},
    ],
)
def test_waits_while_a_load_is_in_flight(
    instance: dg.DagsterInstance,
    published: dict[str, datetime],
    tags: dict[str, str],
) -> None:
    since = datetime.now(tz=UTC) - _SINCE_AGO
    published["a-course"] = since + timedelta(minutes=1)
    run = create_run_for_test(
        instance, job_name="__ASSET_JOB", status=dg.DagsterRunStatus.STARTED, tags=tags
    )

    result = _tick(instance, since.isoformat())

    assert not result.run_requests
    # The window is kept, so the course is requested once the load ends.
    assert result.cursor is None
    instance.report_run_failed(run)
    (request,) = _tick(instance, since.isoformat()).run_requests or []
    assert request.partition_key == "a-course"


def test_scoped_loads_and_the_sweep_carry_the_queue_tag(
    instance: dg.DagsterInstance, published: dict[str, datetime]
) -> None:
    since = datetime.now(tz=UTC) - _SINCE_AGO
    published["a-course"] = since + timedelta(minutes=1)

    (request,) = _tick(instance, since.isoformat()).run_requests or []

    assert request.tags.items() >= OCW_CONTENT_RUN_TAGS.items()
    schedule = defs.get_schedule_def(OCW_CONTENT_SCHEDULE_NAME)
    assert schedule.tags.items() >= OCW_CONTENT_RUN_TAGS.items()


def test_a_run_without_the_tag_is_not_waited_for(
    instance: dg.DagsterInstance, published: dict[str, datetime]
) -> None:
    since = datetime.now(tz=UTC) - _SINCE_AGO
    published["a-course"] = since + timedelta(minutes=1)
    create_run_for_test(
        instance, job_name="__ASSET_JOB", status=dg.DagsterRunStatus.STARTED
    )

    assert _tick(instance, since.isoformat()).run_requests


def test_sensor_is_registered_stopped() -> None:
    sensor = defs.get_sensor_def(OCW_CONTENT_SENSOR_NAME)

    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED


def _sweep_due(instance: dg.DagsterInstance, tick: datetime) -> bool:
    context = dg.build_schedule_context(
        instance=instance, scheduled_execution_time=tick
    )
    return ocw_content_sweep_due(context)


def test_sweep_waits_for_a_scoped_load_and_runs_once_a_day(
    persistent_instance: dg.DagsterInstance,
) -> None:
    instance = persistent_instance
    # Runs are created at the real time, so the ticks are today's.
    today = datetime.now(tz=UTC)
    scoped = create_run_for_test(
        instance,
        job_name="__ASSET_JOB",
        status=dg.DagsterRunStatus.STARTED,
        tags={"dagster/sensor_name": OCW_CONTENT_SENSOR_NAME, **OCW_CONTENT_RUN_TAGS},
    )

    assert not _sweep_due(instance, today)

    instance.report_run_failed(scoped)
    assert _sweep_due(instance, today)
    create_run_for_test(
        instance,
        job_name="__ASSET_JOB",
        status=dg.DagsterRunStatus.SUCCESS,
        tags={
            "dagster/schedule_name": OCW_CONTENT_SCHEDULE_NAME,
            **OCW_CONTENT_RUN_TAGS,
        },
    )
    assert not _sweep_due(instance, today)
    assert _sweep_due(instance, today + timedelta(days=1))


def test_sweep_waits_for_the_previous_sweep(
    persistent_instance: dg.DagsterInstance,
) -> None:
    instance = persistent_instance
    create_run_for_test(
        instance,
        job_name="__ASSET_JOB",
        status=dg.DagsterRunStatus.STARTED,
        tags={
            "dagster/schedule_name": OCW_CONTENT_SCHEDULE_NAME,
            **OCW_CONTENT_RUN_TAGS,
        },
    )

    assert not _sweep_due(instance, datetime.now(tz=UTC) + timedelta(days=1))
