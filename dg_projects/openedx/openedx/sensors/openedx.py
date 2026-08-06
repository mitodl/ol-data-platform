import json
from datetime import UTC, datetime, timedelta

import httpx2 as httpx
from dagster import (
    AssetKey,
    DagsterInstance,
    RunRequest,
    RunsFilter,
    SensorEvaluationContext,
    SensorResult,
)
from dagster._core.event_api import AssetRecordsFilter
from dagster._core.storage.dagster_run import NOT_FINISHED_STATUSES
from dagster._core.storage.tags import PARTITION_NAME_TAG, SENSOR_NAME_TAG
from ol_orchestrate.lib.dagster_helpers import contains_invalid_partition_strings
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory
from pydantic import BaseModel

from openedx.lib.magic_numbers import HTTP_NOT_FOUND
from openedx.partitions.openedx import (
    OPENEDX_COURSE_RUN_PARTITIONS,
)

COURSEWARE_PUBLISHED_VERSION_METADATA = "courseware_published_version"


def last_exported_version(
    instance: DagsterInstance, asset_key: AssetKey, partition_key: str
) -> str | None:
    """Return the course version recorded on the partition's latest export.

    Returns None when the partition has never been exported, or was exported
    before course_xml started recording the version - both of which mean "we do
    not know what is in S3", so the caller should re-export.
    """
    records = instance.fetch_materializations(
        AssetRecordsFilter(asset_key=asset_key, asset_partitions=[partition_key]),
        limit=1,
    ).records
    if not records:
        return None
    metadata = records[0].asset_materialization.metadata
    version = metadata.get(COURSEWARE_PUBLISHED_VERSION_METADATA)
    return version.value if version else None


def in_flight_partitions(instance: DagsterInstance, sensor_name: str) -> set[str]:
    """Return partition keys whose export run this sensor has already launched.

    An export can outlive the tick interval, so without this the sensor would
    re-request a partition whose run is still queued or running. NOT_FINISHED
    rather than IN_PROGRESS: the latter omits QUEUED and NOT_STARTED, which is
    exactly the case a full run queue produces.
    """
    return {
        record.dagster_run.tags[PARTITION_NAME_TAG]
        for record in instance.get_run_records(
            RunsFilter(
                tags={SENSOR_NAME_TAG: sensor_name},
                statuses=list(NOT_FINISHED_STATUSES),
            )
        )
        if PARTITION_NAME_TAG in record.dagster_run.tags
    }


class CourseCursor(BaseModel):
    published_version: str
    published_at: datetime | None = None
    course_start: datetime | None = None
    course_end: datetime | None = None


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
                        AssetKey((openedx.deployment, "openedx", "courseware")),
                        AssetKey(
                            (openedx.deployment, "openedx", "raw_data", "course_xml")
                        ),
                        AssetKey(
                            (openedx.deployment, "openedx", "course_content_webhook")
                        ),
                    ],
                    partition_key=course_run_id,
                    tags={"published_version": response["published_version"]},
                )
            )
            cursor[course_run_id] = course_update.model_dump_json()

    context.update_cursor(json.dumps(cursor))
    return SensorResult(run_requests=run_requests)
