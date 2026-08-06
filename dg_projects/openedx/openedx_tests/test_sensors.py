"""Tests for course_version_sensor's batching, cursor, and skip behaviour.

The failure these guard against (mitodl/hq#12739) was a tick that never finished:
because the run requests and the cursor write both happened after a loop over every
partition in the deployment, a timeout mid-loop discarded the whole tick's work and the
next tick started over from the same place.
"""

import json
from datetime import UTC, datetime, timedelta

import dagster as dg
import httpx2 as httpx
import pytest
from openedx.partitions.openedx import OPENEDX_COURSE_RUN_PARTITIONS
from openedx.sensors.openedx import (
    ENDED_COURSE_GRACE_PERIOD,
    ENDED_COURSE_RECHECK_INTERVAL,
    CourseCursor,
    CourseVersionCursor,
    course_version_sensor,
)

DEPLOYMENT = "mitxonline"


def http_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://lms.example.edu")
    message = f"HTTP {status_code}"
    return httpx.HTTPStatusError(
        message, request=request, response=httpx.Response(status_code, request=request)
    )


class FakeOpenEdxClient:
    """Stands in for OpenEdxApiClient, recording which outlines were requested."""

    def __init__(self, outlines: dict[str, dict[str, str | None]]):
        self.outlines = outlines
        self.requested: list[str] = []

    def get_course_outline(self, course_id: str) -> dict[str, str | None]:
        self.requested.append(course_id)
        if course_id not in self.outlines:
            raise http_error(404)
        return self.outlines[course_id]


class FakeOpenEdxApiClientFactory:
    def __init__(self, client: FakeOpenEdxClient, deployment: str = DEPLOYMENT):
        self.client = client
        self.deployment = deployment


def outline(
    published_version: str,
    course_end: str | None = None,
    published_at: str = "2026-08-05T00:00:00+00:00",
    course_start: str | None = "2026-01-01T00:00:00+00:00",
) -> dict[str, str | None]:
    return {
        "published_version": published_version,
        "published_at": published_at,
        "course_start": course_start,
        "course_end": course_end,
    }


def run_key(course_run_id: str) -> str:
    return f"course-v1:MITx+{course_run_id}+1T2026"


@pytest.fixture
def instance() -> dg.DagsterInstance:
    return dg.DagsterInstance.ephemeral()


def add_partitions(instance: dg.DagsterInstance, course_run_ids: list[str]) -> None:
    instance.add_dynamic_partitions(
        OPENEDX_COURSE_RUN_PARTITIONS[DEPLOYMENT].name, course_run_ids
    )


def evaluate(
    instance: dg.DagsterInstance,
    client: FakeOpenEdxClient,
    cursor: str | None = None,
) -> dg.SensorResult:
    context = dg.build_sensor_context(instance=instance, cursor=cursor)
    return course_version_sensor(context, FakeOpenEdxApiClientFactory(client))


def parse_cursor(result: dg.SensorResult) -> CourseVersionCursor:
    return CourseVersionCursor(**json.loads(result.cursor))


def test_batches_partitions_and_resumes_across_ticks(instance, monkeypatch):
    """Each tick checks one batch and the next picks up where it left off."""
    monkeypatch.setattr("openedx.sensors.openedx.COURSE_VERSION_BATCH_SIZE", 2)
    course_run_ids = [run_key(f"C{n}") for n in range(5)]
    add_partitions(instance, course_run_ids)
    client = FakeOpenEdxClient({key: outline("v1") for key in course_run_ids})

    cursor = None
    seen: list[str] = []
    for _ in range(3):
        result = evaluate(instance, client, cursor)
        cursor = result.cursor
        seen.extend(request.partition_key for request in result.run_requests)

    assert seen == course_run_ids
    assert client.requested == course_run_ids


def test_wraps_to_the_start_once_every_partition_is_scanned(instance, monkeypatch):
    """After the last partition the scan restarts, so versions keep being re-checked."""
    monkeypatch.setattr("openedx.sensors.openedx.COURSE_VERSION_BATCH_SIZE", 2)
    course_run_ids = [run_key(f"C{n}") for n in range(3)]
    add_partitions(instance, course_run_ids)
    client = FakeOpenEdxClient({key: outline("v1") for key in course_run_ids})

    cursor = None
    for _ in range(2):
        cursor = evaluate(instance, client, cursor).cursor
    client.requested.clear()

    result = evaluate(instance, client, cursor)

    assert client.requested == course_run_ids[:2]
    assert parse_cursor(result).last_scanned_course_run_id == course_run_ids[1]


def test_resume_position_survives_partitions_being_removed(instance, monkeypatch):
    """Resuming by course run ID, not by index, so a shrinking list can't skip runs."""
    monkeypatch.setattr("openedx.sensors.openedx.COURSE_VERSION_BATCH_SIZE", 2)
    course_run_ids = [run_key(f"C{n}") for n in range(6)]
    add_partitions(instance, course_run_ids)
    client = FakeOpenEdxClient({key: outline("v1") for key in course_run_ids})

    cursor = evaluate(instance, client).cursor
    instance.delete_dynamic_partition(
        OPENEDX_COURSE_RUN_PARTITIONS[DEPLOYMENT].name, course_run_ids[0]
    )
    client.requested.clear()

    evaluate(instance, client, cursor)

    assert client.requested == course_run_ids[2:4]


def test_only_requests_runs_for_changed_versions(instance):
    course_run_id = run_key("C0")
    add_partitions(instance, [course_run_id])
    client = FakeOpenEdxClient({course_run_id: outline("v1")})

    first = evaluate(instance, client)
    second = evaluate(instance, client, first.cursor)

    assert [request.partition_key for request in first.run_requests] == [course_run_id]
    assert first.run_requests[0].tags["published_version"] == "v1"
    assert second.run_requests == []

    client.outlines[course_run_id] = outline("v2")
    third = evaluate(instance, client, second.cursor)

    assert [request.partition_key for request in third.run_requests] == [course_run_id]
    assert third.run_requests[0].tags["published_version"] == "v2"


def test_cursor_is_refreshed_even_when_the_version_is_unchanged(instance):
    """A run whose version never changes still gets fresh metadata each pass.

    The old sensor only wrote the cursor on a version change, so a cached course_end
    could age past the grace period and strand the run permanently.
    """
    course_run_id = run_key("C0")
    add_partitions(instance, [course_run_id])
    ends_at = (datetime.now(tz=UTC) + timedelta(days=30)).isoformat()
    client = FakeOpenEdxClient({course_run_id: outline("v1", course_end=ends_at)})

    first = evaluate(instance, client)
    extended_end = (datetime.now(tz=UTC) + timedelta(days=365)).isoformat()
    client.outlines[course_run_id] = outline("v1", course_end=extended_end)
    second = evaluate(instance, client, first.cursor)

    assert second.run_requests == []
    assert parse_cursor(second).courses[
        course_run_id
    ].course_end == datetime.fromisoformat(extended_end)


def test_skips_long_ended_runs_but_rechecks_them_periodically(instance):
    recently_checked, overdue, self_paced = (
        run_key("C0"),
        run_key("C1"),
        run_key("C2"),
    )
    add_partitions(instance, [recently_checked, overdue, self_paced])
    now = datetime.now(tz=UTC)
    ended = now - ENDED_COURSE_GRACE_PERIOD - timedelta(days=1)
    cursor = CourseVersionCursor(
        courses={
            recently_checked: CourseCursor(
                published_version="v1", course_end=ended, last_checked=now
            ),
            overdue: CourseCursor(
                published_version="v1",
                course_end=ended,
                last_checked=now - ENDED_COURSE_RECHECK_INTERVAL - timedelta(days=1),
            ),
            # Self-paced runs have no end date and must never be skipped.
            self_paced: CourseCursor(
                published_version="v1", course_end=None, last_checked=now
            ),
        }
    )
    client = FakeOpenEdxClient(
        {
            recently_checked: outline("v2", course_end=ended.isoformat()),
            overdue: outline("v2", course_end=ended.isoformat()),
            self_paced: outline("v2"),
        }
    )

    result = evaluate(instance, client, cursor.model_dump_json())

    assert client.requested == [overdue, self_paced]
    assert sorted(request.partition_key for request in result.run_requests) == sorted(
        [overdue, self_paced]
    )


def test_missing_outline_is_skipped_without_failing_the_tick(instance):
    present, missing = run_key("C0"), run_key("C1")
    add_partitions(instance, [present, missing])
    client = FakeOpenEdxClient({present: outline("v1")})

    result = evaluate(instance, client)

    assert [request.partition_key for request in result.run_requests] == [present]
    assert parse_cursor(result).last_scanned_course_run_id == missing


def test_non_404_errors_are_not_swallowed(instance):
    course_run_id = run_key("C0")
    add_partitions(instance, [course_run_id])

    class ExplodingClient(FakeOpenEdxClient):
        def get_course_outline(self, course_id: str) -> dict[str, str | None]:
            self.requested.append(course_id)
            raise http_error(500)

    with pytest.raises(httpx.HTTPStatusError):
        evaluate(instance, ExplodingClient({}))


def test_legacy_flat_cursor_is_migrated_without_re_requesting_everything(instance):
    """The pre-batching cursor must be honoured, not discarded.

    Dropping it would make the first tick after deploy request a re-export of every
    partition in the deployment at once.
    """
    course_run_id = run_key("C0")
    add_partitions(instance, [course_run_id])
    legacy_cursor = json.dumps(
        {
            course_run_id: CourseCursor(
                published_version="v1",
                published_at=datetime(2026, 5, 1, tzinfo=UTC),
            ).model_dump_json()
        }
    )
    client = FakeOpenEdxClient({course_run_id: outline("v1")})

    result = evaluate(instance, client, legacy_cursor)

    assert result.run_requests == []
    assert parse_cursor(result).courses[course_run_id].published_version == "v1"


def test_empty_deployment_produces_no_runs(instance):
    result = evaluate(instance, FakeOpenEdxClient({}))

    assert result.run_requests == []
    assert parse_cursor(result).last_scanned_course_run_id is None
