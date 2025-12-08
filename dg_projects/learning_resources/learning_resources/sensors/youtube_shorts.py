"""Sensors for monitoring YouTube playlists and triggering video processing."""

from dagster import (
    AddDynamicPartitionsRequest,
    DefaultSensorStatus,
    RunRequest,
    SensorResult,
    SkipReason,
    sensor,
)

from learning_resources.assets.youtube_shorts import (
    playlist_api_key,
)


@sensor(
    name="youtube_shorts_discovery_sensor",
    description="Manages video partitions and triggers runs for new videos.",
    minimum_interval_seconds=60 * 60,  # Check every hour
    default_status=DefaultSensorStatus.STOPPED,
    job_name="youtube_shorts_video_job",
)
def youtube_shorts_discovery_sensor(context):
    """
    Discovery sensor: manages partition creation and triggers runs for new videos.

    This sensor checks for API asset materializations, extracts video IDs,
    creates dynamic partitions for new videos, and immediately triggers runs
    to process those videos (metadata, content, thumbnail, webhook).
    """
    # Check if we have a recent API materialization
    api_materialization = context.instance.get_latest_materialization_event(
        playlist_api_key
    )

    if not api_materialization:
        return SkipReason("Waiting for initial API materialization")

    # Extract metadata from the API materialization
    metadata = api_materialization.asset_materialization.metadata

    videos_to_process = (
        metadata.get("videos_to_process_ids").value
        if metadata.get("videos_to_process_ids")
        else []
    )

    if not videos_to_process:
        return SkipReason("No videos to process found in API response")

    # Get existing video partitions
    existing_video_partitions = set(
        context.instance.get_dynamic_partitions("youtube_video_ids")
    )
    new_video_ids = set(videos_to_process) - existing_video_partitions

    if not new_video_ids:
        return SkipReason("No new videos found")

    context.log.info(
        "Adding %d new video partitions and creating run requests", len(new_video_ids)
    )

    # Add new partitions and create run requests for each new video
    sorted_new_video_ids = sorted(new_video_ids)

    return SensorResult(
        dynamic_partitions_requests=[
            AddDynamicPartitionsRequest(
                partitions_def_name="youtube_video_ids",
                partition_keys=sorted_new_video_ids,
            )
        ],
        run_requests=[
            RunRequest(
                partition_key=video_id,
                run_key=f"youtube_shorts_{video_id}_{api_materialization.run_id}",
            )
            for video_id in sorted_new_video_ids
        ],
    )
