"""Tests for ocw_content_publish_sensor against an ephemeral Dagster instance."""

from datetime import UTC, datetime, timedelta

import dagster as dg
import pytest
from dagster._core.test_utils import create_run_for_test
from data_loading.definitions import defs
from data_loading.defs.ingestion import ocw_content_sensor
from data_loading.defs.ingestion.assets import ocw_content_assets
from data_loading.defs.ingestion.ocw_content_sensor import (
    MAX_COURSES_PER_RUN,
    OCW_CONTENT_SENSOR_NAME,
    courses_published_between,
    ocw_content_publish_sensor,
)
from data_loading.defs.ingestion.schedules import OCW_CONTENT_SCHEDULE_NAME
from ol_dlt.sources import ocw_content

# The previous tick's window ended this long before the tick under test.
_SINCE_AGO = timedelta(minutes=10)


@pytest.fixture
def instance() -> dg.DagsterInstance:
    return dg.DagsterInstance.ephemeral()


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


def test_requests_one_load_of_the_courses_published_since_the_last_tick(
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

    (request,) = result.run_requests or []
    config = request.run_config["ops"][ocw_content_assets.op.name]["config"]
    assert config == {"courses": ["a-course", "b-course"]}
    assert result.cursor is not None
    cursor = datetime.fromisoformat(result.cursor)
    assert published["a-course"] < cursor < published["settling"]

    later = _tick(instance, result.cursor)
    assert not later.run_requests


def test_requested_run_config_is_valid_for_the_asset() -> None:
    run_config = dg.RunConfig(
        ops={
            ocw_content_assets.op.name: ocw_content_sensor.OcwContentLoadConfig(
                courses=["a-course"]
            )
        }
    )

    assert dg.validate_run_config(
        defs.resolve_implicit_global_asset_job_def(), run_config
    )


def test_mass_rebuild_is_left_to_the_sweep(
    instance: dg.DagsterInstance, published: dict[str, datetime]
) -> None:
    since = datetime.now(tz=UTC) - _SINCE_AGO
    for number in range(1, MAX_COURSES_PER_RUN + 2):
        published[f"course-{number}"] = since + timedelta(seconds=number)

    result = _tick(instance, since.isoformat())

    assert not result.run_requests
    assert result.skip_reason is not None
    assert "rebuild" in str(result.skip_reason)
    assert result.cursor is not None
    assert datetime.fromisoformat(result.cursor) > since


@pytest.mark.parametrize(
    "tags",
    [
        {"dagster/schedule_name": OCW_CONTENT_SCHEDULE_NAME},
        {"dagster/sensor_name": OCW_CONTENT_SENSOR_NAME},
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
    assert request.run_config["ops"][ocw_content_assets.op.name]["config"] == {
        "courses": ["a-course"]
    }


def test_sensor_is_registered_stopped() -> None:
    sensor = defs.get_sensor_def(OCW_CONTENT_SENSOR_NAME)

    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED
