import json

from dagster import (
    AddDynamicPartitionsRequest,
    AssetKey,
    DefaultSensorStatus,
    DeleteDynamicPartitionsRequest,
    RunRequest,
    SensorResult,
    SkipReason,
    sensor,
)

from canvas.lib.canvas import fetch_canvas_course_ids_from_google_sheet


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
    context.log.info("google_sheet_course_ids: %s", google_sheet_course_ids)

    # Existing dynamic partitions
    existing_partitions = set(
        context.instance.get_dynamic_partitions("canvas_course_ids")
    )
    context.log.info("existing_partitions: %s", existing_partitions)

    # Register any new course IDs as partitions
    new_course_ids = google_sheet_course_ids - existing_partitions
    removed_course_ids = existing_partitions - google_sheet_course_ids

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
    dynamic_requests = []
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
