"""Sensors for monitoring Google Sheets and triggering video processing."""

from dagster import (
    AddDynamicPartitionsRequest,
    DefaultSensorStatus,
    RunRequest,
    SensorResult,
    SkipReason,
    sensor,
)

from learning_resources.assets.video_shorts import (
    sheets_api_key,
)


@sensor(
    name="video_shorts_discovery_sensor",
    description=(
        "Manages video partitions and triggers runs for new videos from Google Sheets."
    ),
    minimum_interval_seconds=3600,  # Check every hour
    default_status=DefaultSensorStatus.STOPPED,
    job_name="video_shorts_video_job",
)
def video_shorts_discovery_sensor(context):
    """
    Discovery sensor: manages partition creation and triggers runs for new videos.

    This sensor checks for sheets_api asset materializations, extracts partition keys,
    creates dynamic partitions for new videos, and immediately triggers runs
    to process those videos (metadata, content, thumbnail, webhook).
    """
    # Check if we have a recent sheets_api materialization
    api_materialization = context.instance.get_latest_materialization_event(
        sheets_api_key
    )

    if not api_materialization:
        return SkipReason("Waiting for initial sheets_api materialization")

    # Extract metadata from the sheets_api materialization
    metadata = api_materialization.asset_materialization.metadata

    partition_keys = (
        metadata.get("partition_keys").value if metadata.get("partition_keys") else []
    )

    if not partition_keys:
        return SkipReason("No videos to process found in Google Sheets")

    # Get existing video partitions
    existing_video_partitions = set(
        context.instance.get_dynamic_partitions("video_short_ids")
    )
    new_partition_keys = set(partition_keys) - existing_video_partitions

    if not new_partition_keys:
        return SkipReason("No new videos found")

    context.log.info(
        "Adding %d new video partitions and creating run requests",
        len(new_partition_keys),
    )

    # Add new partitions and create run requests for each new video
    sorted_new_partition_keys = sorted(new_partition_keys)

    return SensorResult(
        dynamic_partitions_requests=[
            AddDynamicPartitionsRequest(
                partitions_def_name="video_short_ids",
                partition_keys=sorted_new_partition_keys,
            )
        ],
        run_requests=[
            RunRequest(
                partition_key=partition_key,
                run_key=f"video_shorts_{partition_key}_{api_materialization.run_id}",
            )
            for partition_key in sorted_new_partition_keys
        ],
    )
