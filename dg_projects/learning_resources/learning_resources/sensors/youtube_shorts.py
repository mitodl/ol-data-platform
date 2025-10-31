"""Sensor for monitoring YouTube playlists and triggering video processing."""

from dagster import (
    AddDynamicPartitionsRequest,
    AssetKey,
    AssetMaterialization,
    DefaultSensorStatus,
    RunRequest,
    SensorResult,
    SkipReason,
    sensor,
)

from learning_resources.lib.youtube import get_videos_to_process

YOUTUBE_SHORTS_CONFIG_URL = (
    "https://raw.githubusercontent.com/mitodl/open-video-data/"
    "refs/heads/mitopen/youtube/shorts.yaml"
)

# Process top 16 most recent videos
MAX_VIDEOS_TO_PROCESS = 12


@sensor(
    description=(
        "Monitor YouTube playlists daily and process the 16 most recent videos."
    ),
    minimum_interval_seconds=86400,  # Check once per day
    required_resource_keys={"youtube_api"},
    default_status=DefaultSensorStatus.STOPPED,
    asset_selection=[
        AssetKey(["youtube_shorts", "video_content"]),
        AssetKey(["youtube_shorts", "video_thumbnail"]),
        AssetKey(["youtube_shorts", "video_metadata"]),
        AssetKey(["youtube_shorts", "video_webhook"]),
    ],
)
def youtube_shorts_sensor(context):
    """
    Daily sensor to fetch YouTube videos and process new or failed videos.

    This sensor:
    1. Fetches playlist configuration from GitHub
    2. Retrieves all videos from configured playlists
    3. Sorts videos by publish date (newest first)
    4. Selects the top 16 most recent videos
    5. Creates dynamic partitions for new videos not yet processed
    6. Checks materialization status of existing partitions (all 4 assets)
    7. Triggers asset materializations for:
       - New videos not yet in partitions
       - Existing videos where any asset failed or is unmaterialized
         (including video_content, video_thumbnail, video_metadata, video_webhook)

    This ensures that failed materializations (including webhook delivery) are
    automatically retried on subsequent sensor runs, preventing videos from
    being permanently skipped due to transient failures.
    """
    try:
        # Define asset keys we're monitoring (including webhook)
        asset_keys = [
            AssetKey(["youtube_shorts", "video_content"]),
            AssetKey(["youtube_shorts", "video_thumbnail"]),
            AssetKey(["youtube_shorts", "video_metadata"]),
            AssetKey(["youtube_shorts", "video_webhook"]),
        ]

        # Get existing partitions
        existing_partitions = set(
            context.instance.get_dynamic_partitions("youtube_video_ids")
        )
        context.log.info("Existing partitions: %s", existing_partitions)

        # Get videos needing processing using utility function
        result = get_videos_to_process(
            youtube_client=context.resources.youtube_api.client,
            config_url=YOUTUBE_SHORTS_CONFIG_URL,
            max_videos=MAX_VIDEOS_TO_PROCESS,
            existing_partitions=existing_partitions,
            instance=context.instance,
            asset_keys=asset_keys,
        )

        # Extract results
        playlist_ids = result["playlist_ids"]
        videos_to_process = result["videos_to_process"]
        new_video_ids = result["new_video_ids"]
        all_video_ids = result["all_video_ids"]

        if not playlist_ids:
            return SkipReason("No playlists found in configuration")

        if not videos_to_process:
            return SkipReason("No changes in top YouTube videos. ")

        # Create asset events for external assets to track lineage
        asset_events = []

        # Record materialization for external playlists
        asset_events.append(
            AssetMaterialization(
                asset_key=AssetKey(["youtube_shorts", "external_playlists"]),
                metadata={
                    "playlist_ids": playlist_ids,
                    "playlist_count": len(playlist_ids),
                    "config_url": YOUTUBE_SHORTS_CONFIG_URL,
                },
            )
        )

        # Record materialization for external videos
        asset_events.append(
            AssetMaterialization(
                asset_key=AssetKey(["youtube_shorts", "external_videos"]),
                metadata={
                    "total_video_count": len(all_video_ids),
                    "all_video_ids": list(all_video_ids),
                    "new_video_count": len(new_video_ids),
                    "new_video_ids": list(new_video_ids),
                    "videos_to_process": len(videos_to_process),
                    "video_ids": list(all_video_ids[:MAX_VIDEOS_TO_PROCESS]),
                },
            )
        )

        # Create run requests for all videos that need processing
        run_requests = [
            RunRequest(
                asset_selection=asset_keys,
                partition_key=video_id,
            )
            for video_id in videos_to_process
        ]

        # Prepare dynamic partition requests
        dynamic_requests = []

        if new_video_ids:
            dynamic_requests.append(
                AddDynamicPartitionsRequest(
                    partitions_def_name="youtube_video_ids",
                    partition_keys=list(new_video_ids),
                )
            )

        # Note: We intentionally don't delete old partitions to preserve history
        # Videos that drop out of the top 16 will remain as partitions but won't
        # be re-processed unless they move back into the top 16.

        return SensorResult(
            asset_events=asset_events,
            dynamic_partitions_requests=dynamic_requests,
            run_requests=run_requests,
        )

    except Exception as e:
        context.log.exception("YouTube shorts sensor failed")
        return SkipReason(f"Sensor failed with error: {e}")
