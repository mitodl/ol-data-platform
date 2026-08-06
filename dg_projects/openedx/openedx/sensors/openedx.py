import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

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
    materialization = records[0].asset_materialization
    if materialization is None:
        return None
    version = materialization.metadata.get(COURSEWARE_PUBLISHED_VERSION_METADATA)
    return str(version.value) if version else None


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


# 16 workers measured at ~53 outline fetches/sec against mitxonline with no
# throttling. ceiling: raise only with fresh numbers from the authenticated
# endpoint, which is slower than the anonymous one used to measure.
OUTLINE_FETCH_WORKERS = 16
# The deployment sets DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=300 in
# ol-infrastructure applications/dagster/__main__.py. Change these together.
SWEEP_TIME_BUDGET = timedelta(seconds=200)
# Each request is one run holding a slot in a global pool of 30 while it polls
# Studio. ceiling: raise this only after export runs get their own concurrency
# pool, or unrelated pipelines queue behind catch-up exports.
MAX_RUN_REQUESTS_PER_TICK = 8


def course_version_sensor(  # noqa: PLR0915
    context: SensorEvaluationContext, openedx: OpenEdxApiClientFactory
) -> SensorResult:
    """Request an export for every course whose published version changed.

    Stateless by design: materialization metadata says what was exported and run
    records say what is already in flight, so there is no cursor to lose. A tick
    cut short by the budget or the cap simply emits what it collected - the work
    it did not reach still mismatches and gets picked up next tick.
    """
    deployment = openedx.deployment
    partition_keys = OPENEDX_COURSE_RUN_PARTITIONS[deployment].get_partition_keys(
        dynamic_partitions_store=context.instance
    )
    course_xml_key = AssetKey((deployment, "openedx", "raw_data", "course_xml"))
    skip_keys = in_flight_partitions(context.instance, context.sensor_name)
    pending_keys = [key for key in partition_keys if key not in skip_keys]

    sweep_start = time.monotonic()
    deadline = sweep_start + SWEEP_TIME_BUDGET.total_seconds()
    run_requests: list[RunRequest] = []
    examined = 0
    failures = 0

    executor = ThreadPoolExecutor(max_workers=OUTLINE_FETCH_WORKERS)
    try:
        futures = {
            executor.submit(openedx.client.get_course_outline, key): key
            for key in pending_keys
        }
        # timeout so a rate-limit storm that blocks every worker (see
        # fetch_with_auth's unbounded 429 retry) still lets the tick return
        # instead of hanging until the gRPC server kills it. The timeout is
        # caught only around advancing the iterator, so a TimeoutError raised
        # from within the loop body (e.g. a Postgres query timeout from
        # last_exported_version) still propagates instead of being swallowed.
        completed = as_completed(futures, timeout=SWEEP_TIME_BUDGET.total_seconds())
        while True:
            try:
                future = next(completed)
            except StopIteration:
                break
            except TimeoutError:
                context.log.info(
                    "Sweep time budget exhausted while workers were still in flight"
                )
                break
            if (
                len(run_requests) >= MAX_RUN_REQUESTS_PER_TICK
                or time.monotonic() > deadline
            ):
                break
            course_run_id = futures[future]
            examined += 1
            try:
                published_version = future.result()["published_version"]
            except httpx.HTTPStatusError as error:
                if error.response.status_code == HTTP_NOT_FOUND:
                    context.log.info(
                        "Course outline not found for key %s", course_run_id
                    )
                else:
                    failures += 1
                    context.log.exception(
                        "Failed to fetch the course outline for %s",
                        course_run_id,
                    )
                continue
            except Exception:
                failures += 1
                context.log.exception(
                    "Failed to fetch the course outline for %s", course_run_id
                )
                continue
            if published_version == last_exported_version(
                context.instance, course_xml_key, course_run_id
            ):
                continue
            run_requests.append(
                RunRequest(
                    asset_selection=[
                        AssetKey((deployment, "openedx", "courseware")),
                        course_xml_key,
                        AssetKey((deployment, "openedx", "course_content_webhook")),
                    ],
                    partition_key=course_run_id,
                    tags={"published_version": published_version},
                )
            )
    finally:
        # Not the `with` form: its __exit__ waits for in-flight workers, which
        # would block the tick for up to 60s if one is inside the 429 backoff.
        executor.shutdown(wait=False, cancel_futures=True)

    sweep_duration = time.monotonic() - sweep_start
    context.log.info(
        "Examined %s of %s partitions (%s in flight, skipped), %s failed, "
        "requesting %s exports in %.1fs",
        examined,
        len(partition_keys),
        len(skip_keys),
        failures,
        len(run_requests),
        sweep_duration,
    )
    if examined > 0 and failures == examined:
        msg = f"Course outline sweep failed for all {examined} examined partitions"
        raise RuntimeError(msg)
    return SensorResult(run_requests=run_requests)
