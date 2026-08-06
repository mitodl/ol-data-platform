import json
from bisect import bisect_right
from datetime import UTC, datetime, timedelta

import httpx2 as httpx
from dagster import (
    AssetKey,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
)
from ol_orchestrate.lib.dagster_helpers import contains_invalid_partition_strings
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory
from pydantic import BaseModel

from openedx.lib.magic_numbers import HTTP_NOT_FOUND
from openedx.partitions.openedx import (
    OPENEDX_COURSE_RUN_PARTITIONS,
)

# `get_course_outline` is one HTTP round trip per course run and the sensor tick has a
# hard timeout, so only a slice of the partitions is checked per tick. Deployments with
# a few thousand runs cycle through everything in well under a day at hourly ticks.
COURSE_VERSION_BATCH_SIZE = 250
# How long after its end date a course run stops being polled for new versions.
ENDED_COURSE_GRACE_PERIOD = timedelta(days=90)
# Ended runs are still re-checked this often. Without it, a run whose end date is later
# pushed out would stay skipped forever on the strength of the stale cached value.
ENDED_COURSE_RECHECK_INTERVAL = timedelta(days=30)


class CourseCursor(BaseModel):
    published_version: str
    published_at: datetime | None = None
    course_start: datetime | None = None
    course_end: datetime | None = None
    last_checked: datetime | None = None


class CourseVersionCursor(BaseModel):
    """Sensor state: per-run course metadata plus the position of the batch scan."""

    courses: dict[str, CourseCursor] = {}
    last_scanned_course_run_id: str | None = None


def _load_course_version_cursor(raw_cursor: str | None) -> CourseVersionCursor:
    payload = json.loads(raw_cursor or "{}")
    if "courses" not in payload:
        # Migrate the pre-batching cursor, a flat {course_run_id: CourseCursor-as-JSON-
        # string} map. Dropping it instead would re-request every partition at once.
        payload = {"courses": {k: json.loads(v) for k, v in payload.items()}}
    return CourseVersionCursor(**payload)


def _skip_ended_course(course_cursor: CourseCursor, now: datetime) -> bool:
    """Report whether a course run has been over long enough to stop polling it.

    Self-paced runs carry no ``course_end`` and so are never skipped, and any run is
    re-checked once per ``ENDED_COURSE_RECHECK_INTERVAL`` regardless.
    """
    if (
        course_cursor.course_end is None
        or course_cursor.course_end > now - ENDED_COURSE_GRACE_PERIOD
    ):
        return False
    return (
        course_cursor.last_checked is not None
        and course_cursor.last_checked > now - ENDED_COURSE_RECHECK_INTERVAL
    )


def _parse_timestamp(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def course_run_sensor(
    context: SensorEvaluationContext,
    openedx: OpenEdxApiClientFactory,
):
    # Enumerate the course-run IDs from edX via the API
    course_id_generator = openedx.client.get_edx_course_ids()
    course_run_ids = []
    for result_set in course_id_generator:
        course_run_ids.extend(
            [
                course["id"]
                for course in result_set
                if not contains_invalid_partition_strings(course["id"])
            ]
        )
    existing_keys = set(
        OPENEDX_COURSE_RUN_PARTITIONS[openedx.deployment].get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    new_course_run_ids = set(course_run_ids) - existing_keys
    return SensorResult(
        dynamic_partitions_requests=[
            OPENEDX_COURSE_RUN_PARTITIONS[openedx.deployment].build_add_request(
                partition_keys=list(new_course_run_ids)
            )
        ],
        run_requests=[
            RunRequest(
                asset_selection=[
                    AssetKey((openedx.deployment, "openedx", "courseware")),
                    AssetKey((openedx.deployment, "openedx", "raw_data", "course_xml")),
                    AssetKey((openedx.deployment, "openedx", "course_content_webhook")),
                ],
                partition_key=course_run_id,
            )
            for course_run_id in new_course_run_ids
        ],
    )


def course_version_sensor(
    context: SensorEvaluationContext, openedx: OpenEdxApiClientFactory
):
    """Request a re-export of any course run whose published version has changed.

    ``get_course_outline`` is called for one batch of partitions per tick, resuming from
    where the previous tick stopped, so that every tick completes within the sensor
    timeout and commits its progress. Scanning the whole deployment in a single tick
    made the sensor time out before it emitted any runs or saved any cursor, which is
    what stalled re-exports for months (mitodl/hq#12739).
    """
    course_run_ids = sorted(
        OPENEDX_COURSE_RUN_PARTITIONS[openedx.deployment].get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    cursor = _load_course_version_cursor(context.cursor)

    # Resume by course run ID rather than by index: the sorted key list shifts as
    # partitions are added and removed, and an index would silently skip or repeat runs.
    resume_from = (
        bisect_right(course_run_ids, cursor.last_scanned_course_run_id)
        if cursor.last_scanned_course_run_id is not None
        else 0
    )
    if resume_from >= len(course_run_ids):
        resume_from = 0
    batch = course_run_ids[resume_from : resume_from + COURSE_VERSION_BATCH_SIZE]

    now = datetime.now(tz=UTC)
    run_requests = []
    for course_run_id in batch:
        course_cursor = cursor.courses.get(course_run_id)
        if course_cursor and _skip_ended_course(course_cursor, now):
            continue
        try:
            response = openedx.client.get_course_outline(course_run_id)
        except httpx.HTTPStatusError as e:
            if e.response.status_code != HTTP_NOT_FOUND:
                raise
            context.log.warning("Course outline not found for key %s", course_run_id)
            continue
        published_version = response["published_version"]
        # Refresh the entry on every successful fetch, not only when the version
        # changes, so that course_end can never go stale enough to strand a run that
        # ends up being extended.
        cursor.courses[course_run_id] = CourseCursor(
            published_version=published_version,
            published_at=_parse_timestamp(response["published_at"]),
            course_start=_parse_timestamp(response["course_start"]),
            course_end=_parse_timestamp(response["course_end"]),
            last_checked=now,
        )
        if course_cursor and published_version == course_cursor.published_version:
            continue
        run_requests.append(
            RunRequest(
                run_key=f"{course_run_id}:{published_version}",
                asset_selection=[
                    AssetKey((openedx.deployment, "openedx", "courseware")),
                    AssetKey((openedx.deployment, "openedx", "raw_data", "course_xml")),
                    AssetKey((openedx.deployment, "openedx", "course_content_webhook")),
                ],
                partition_key=course_run_id,
                tags={"published_version": published_version},
            )
        )

    # An empty batch means there are no partitions at all; otherwise the next tick picks
    # up after the last key handled here, wrapping to the start once the list is spent.
    cursor.last_scanned_course_run_id = batch[-1] if batch else None
    context.log.info(
        "Checked %s of %s %s course runs (offset %s), requesting %s re-exports.",
        len(batch),
        len(course_run_ids),
        openedx.deployment,
        resume_from,
        len(run_requests),
    )
    return SensorResult(
        run_requests=run_requests,
        cursor=cursor.model_dump_json(exclude_none=True),
    )
