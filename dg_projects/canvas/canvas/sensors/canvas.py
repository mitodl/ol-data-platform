import json

from dagster import (
    AddDynamicPartitionsRequest,
    AssetKey,
    DagsterRunStatus,
    DefaultSensorStatus,
    DeleteDynamicPartitionsRequest,
    RunRequest,
    RunsFilter,
    SensorResult,
    SkipReason,
    sensor,
)

from canvas.lib.canvas import fetch_canvas_course_ids_from_google_sheet

# Dagster's own run tag. Hardcoded rather than imported: the constant lives in
# the private dagster._core.storage.tags, but the tag string itself is stable
# and documented.
PARTITION_NAME_TAG = "dagster/partition"

# Every status a run can hold while it still intends to execute. Deliberately
# wider than dagster's IN_PROGRESS_RUN_STATUSES, which omits QUEUED and
# NOT_STARTED -- a queued run whose partition is deleted before it starts fails
# exactly the same way as one already running.
PENDING_RUN_STATUSES = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
    DagsterRunStatus.CANCELING,
]


def partitions_with_a_run_in_flight(context) -> set[str]:
    """Canvas partitions that some run is currently counting on existing.

    Deleting a dynamic partition out from under a run does not cancel it. The
    run keeps going and dies when it tries to store its output, because
    ``get_output_context`` resolves the partition key range against the *current*
    partitions definition:

        DagsterInvalidInvocationError: Partition range 33842 to 33842 is not a
        valid range. Nonexistent partition keys: ['33842']

    The work is already done at that point -- the export ran, the file was
    fetched -- and it is thrown away.
    """
    runs = context.instance.get_runs(RunsFilter(statuses=PENDING_RUN_STATUSES))
    return {
        partition
        for run in runs
        if (partition := run.tags.get(PARTITION_NAME_TAG)) is not None
    }


def partition_changes(
    sheet_course_ids: set[str],
    existing_partitions: set[str],
    in_flight: set[str],
) -> tuple[set[str], set[str]]:
    """Which partitions to add and which to delete, given the sheet and the runs.

    Deletion of a partition with a run in flight is deferred rather than
    dropped: the diff is recomputed from the live partition set every tick, so
    once the run reaches a terminal status the partition is cleaned up on the
    next pass with no bookkeeping. A course lingering one extra hour costs
    nothing next to discarding a completed export.
    """
    to_add = sheet_course_ids - existing_partitions
    to_delete = (existing_partitions - sheet_course_ids) - in_flight
    return to_add, to_delete


@sensor(
    description="Sensor to monitor a Google Sheet for Canvas course IDs to export.",
    minimum_interval_seconds=60 * 60,  # Check every 1 hour
    required_resource_keys={"google_sheet_config"},
    default_status=DefaultSensorStatus.STOPPED,
    asset_selection=[
        AssetKey(["canvas", "course_content"]),
        AssetKey(["canvas", "course_metadata"]),
        AssetKey(["canvas", "course_content_metadata"]),
    ],
)
def canvas_google_sheet_course_id_sensor(context):
    google_sheet_course_ids = fetch_canvas_course_ids_from_google_sheet(context)
    if google_sheet_course_ids is None:
        # A failed read is not an empty sheet. Treating it as one would diff
        # every existing partition into removed_course_ids and delete the lot.
        return SkipReason("Could not read the Canvas course ID sheet")
    context.log.info("google_sheet_course_ids: %s", google_sheet_course_ids)

    # Existing dynamic partitions
    existing_partitions = set(
        context.instance.get_dynamic_partitions("canvas_course_ids")
    )
    context.log.info("existing_partitions: %s", existing_partitions)

    in_flight = partitions_with_a_run_in_flight(context)
    new_course_ids, removed_course_ids = partition_changes(
        google_sheet_course_ids, existing_partitions, in_flight
    )
    if deferred := (existing_partitions - google_sheet_course_ids) & in_flight:
        context.log.info(
            "Deferring deletion of %s: a run is still in flight for each.",
            sorted(deferred),
        )

    if not new_course_ids and not removed_course_ids:
        return SkipReason("No changes in canvas course IDs")

    run_requests = [
        RunRequest(
            asset_selection=[
                AssetKey(["canvas", "course_content"]),
                AssetKey(["canvas", "course_metadata"]),
                AssetKey(["canvas", "course_content_metadata"]),
            ],
            partition_key=course_id,
        )
        for course_id in new_course_ids
    ]

    updated_ids = sorted(google_sheet_course_ids)
    dynamic_requests: list[
        AddDynamicPartitionsRequest | DeleteDynamicPartitionsRequest
    ] = []
    if new_course_ids:
        dynamic_requests.append(
            AddDynamicPartitionsRequest(
                partitions_def_name="canvas_course_ids",
                partition_keys=list(new_course_ids),
            )
        )

    if removed_course_ids:
        dynamic_requests.append(
            DeleteDynamicPartitionsRequest(
                partitions_def_name="canvas_course_ids",
                partition_keys=list(removed_course_ids),
            )
        )

    return SensorResult(
        dynamic_partitions_requests=dynamic_requests,
        run_requests=run_requests,
        cursor=json.dumps(updated_ids),
    )
