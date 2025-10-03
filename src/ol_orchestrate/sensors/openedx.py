import json
from datetime import UTC, datetime, timedelta

import httpx
from dagster import (
    AssetKey,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)
from pydantic import BaseModel

from ol_orchestrate.lib.dagster_helpers import contains_invalid_partition_strings
from ol_orchestrate.lib.magic_numbers import HTTP_NOT_FOUND
from ol_orchestrate.lib.utils import fetch_canvas_course_ids_from_google_sheet
from ol_orchestrate.partitions.openedx import (
    OPENEDX_COURSE_RUN_PARTITIONS,
)
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory


class CourseCursor(BaseModel):
    published_version: str
    published_at: datetime | None = None
    course_start: datetime | None = None
    course_end: datetime | None = None


@sensor(
    name="openedx_courseware_sensor",
    minimum_interval_seconds=60 * 60,
    description="Query a running Open edX system for a list of course runs.",
)
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
    )


def course_version_sensor(
    context: SensorEvaluationContext, openedx: OpenEdxApiClientFactory
):
    course_run_ids = OPENEDX_COURSE_RUN_PARTITIONS[
        openedx.deployment
    ].get_partition_keys(dynamic_partitions_store=context.instance)
    # There is a dictionary consisting of course_run_ids as the keys, and the values are
    # instances of the CourseCursor pydantic class. This sensor calls the
    # openedx.client.get_course_outline method for a given course_run_id to detect the
    # current published_version and other metadata to populate an instance of the
    # CourseCursor object. For any course runs that have course_end datetime that is
    # more than 3 months in the past, don't bother fetching their versions. For any
    # course_run_ids that don't have keys in the context cursor, create an entry in the
    # cursor dictionary with the results of the call to the get_course_outline method.
    # Returning a SensorResult with a list of RunRequest objects for each course_run_id
    # instead of AssetMaterialization objects should trigger pipeline runs for the
    # updated course runs instead of recording asset events.

    cursor: dict[str, str] = json.loads(context.cursor or "{}")
    run_requests = []
    for course_run_id in course_run_ids:
        course_cursor = CourseCursor(
            **json.loads(
                cursor.get(
                    course_run_id,
                    CourseCursor(
                        published_version="",
                        course_end=datetime(9999, 12, 31, tzinfo=UTC),
                    ).model_dump_json(),
                )
            )
        )
        if (
            course_cursor
            and course_cursor.course_end
            and course_cursor.course_end <= datetime.now(tz=UTC) - timedelta(days=90)
        ):
            continue
        try:
            response = openedx.client.get_course_outline(course_run_id)
        except httpx.HTTPStatusError as e:
            if e.response.status_code != HTTP_NOT_FOUND:
                raise
            context.log.exception("Course outline not found for key %s", course_run_id)
            continue
        if response["published_version"] != course_cursor.published_version:
            course_update = CourseCursor(
                published_version=response["published_version"],
                published_at=datetime.fromisoformat(response["published_at"]),
                course_start=datetime.fromisoformat(response["course_start"])
                if response["course_start"]
                else None,
                course_end=datetime.fromisoformat(response["course_end"])
                if response["course_end"]
                else None,
            )
            run_requests.append(
                RunRequest(
                    asset_selection=[
                        AssetKey((openedx.deployment, "openedx", "courseware"))
                    ],
                    partition_key=course_run_id,
                    tags={"published_version": response["published_version"]},
                )
            )
            cursor[course_run_id] = course_update.model_dump_json()

    context.update_cursor(json.dumps(cursor))
    return SensorResult(run_requests=run_requests)


@sensor(
    description="Sensor to monitor a Google Sheet for Canvas course IDs to export.",
    minimum_interval_seconds=60 * 60,  # Check every 1 hour
    required_resource_keys={"google_sheet_config"},
    asset_selection=[
        AssetKey(["canvas", "course_content"]),
        AssetKey(["canvas", "course_metadata"]),
        AssetKey(["canvas", "course_content_metadata"]),
    ],
)
def canvas_google_sheet_course_id_sensor(context):
    google_sheet_course_ids = fetch_canvas_course_ids_from_google_sheet(context)

    # Existing dynamic partitions
    existing_partitions = set(
        context.instance.get_dynamic_partitions("canvas_course_ids")
    )

    # Register any new course IDs as partitions
    new_course_ids = google_sheet_course_ids - existing_partitions
    if new_course_ids:
        context.log.info("Adding new course IDs as partitions: %s", new_course_ids)
        # Add new IDs as partitions
        context.instance.add_dynamic_partitions(
            "canvas_course_ids", list(new_course_ids)
        )
    else:
        yield SkipReason("No new canvas course IDs found")

    for course_id in new_course_ids:
        context.log.info(
            "Triggering canvas course export for new course_id= %s", course_id
        )
        yield RunRequest(
            asset_selection=[
                AssetKey(["canvas", "course_content"]),
                AssetKey(["canvas", "course_metadata"]),
                AssetKey(["canvas", "course_content_metadata"]),
            ],
            partition_key=course_id,
        )

    existing_ids = json.loads(context.cursor) if context.cursor else []
    existing_ids_set = set(existing_ids)

    updated_ids = sorted(existing_ids_set.union(new_course_ids))
    context.update_cursor(json.dumps(updated_ids))
