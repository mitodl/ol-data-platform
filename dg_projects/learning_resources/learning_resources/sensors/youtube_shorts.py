"""Sensor for monitoring YouTube playlists and triggering video processing."""

import json

from dagster import (
    AddDynamicPartitionsRequest,
    AssetKey,
    DagsterEventType,
    DefaultSensorStatus,
    EventRecordsFilter,
    RunRequest,
    SensorResult,
    SkipReason,
    sensor,
)

from learning_resources.lib.youtube import (
    extract_video_id_from_playlist_item,
    fetch_youtube_shorts_config,
    sort_videos_by_publish_date,
)

YOUTUBE_SHORTS_CONFIG_URL = (
    "https://raw.githubusercontent.com/mitodl/open-video-data/"
    "refs/heads/mitopen/youtube/shorts.yaml"
)

# Process top 16 most recent videos
MAX_VIDEOS_TO_PROCESS = 16


def get_successfully_materialized_partitions(context, asset_keys, partition_keys):
    """
    Check which partitions have been successfully materialized.

    Returns a set of partition keys with successful materializations for
    all specified assets.
    """
    if not partition_keys:
        return set()

    successfully_materialized = set(partition_keys)

    # Check each asset to see which partitions have been successfully materialized
    for asset_key in asset_keys:
        asset_materialized_partitions = set()

        # Get materialization events for this asset and these partitions
        for partition_key in partition_keys:
            event_records = context.instance.get_event_records(
                EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_MATERIALIZATION,
                    asset_key=asset_key,
                    asset_partitions=[partition_key],
                ),
                limit=1,  # We only need to know if at least one exists
            )

            # Check if materialization succeeded for this asset partition
            if event_records:
                asset_materialized_partitions.add(partition_key)

        # Only keep partitions that succeeded for this asset
        successfully_materialized &= asset_materialized_partitions

    return successfully_materialized


@sensor(
    description=(
        "Monitor YouTube playlists daily and process the 16 most recent videos."
    ),
    minimum_interval_seconds=60,  # Check once per minute
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
        # Fetch playlist configuration
        config = fetch_youtube_shorts_config(YOUTUBE_SHORTS_CONFIG_URL)
        context.log.info("Fetched YouTube config: %s", config)

        # Collect all playlist IDs from config
        playlist_ids = []
        for channel_config in config:
            if playlists := channel_config.get("playlists"):
                playlist_ids.extend([p["id"] for p in playlists])

        if not playlist_ids:
            return SkipReason("No playlists found in configuration")

        context.log.info("Processing %s playlists: %s", len(playlist_ids), playlist_ids)

        # Fetch all videos from all playlists
        all_video_ids = set()

        for playlist_id in playlist_ids:
            context.log.info("Fetching playlist: %s", playlist_id)
            playlist_items = context.resources.youtube_api.client.get_playlist_items(
                playlist_id
            )

            # Extract video IDs from playlist items
            video_ids = [
                extract_video_id_from_playlist_item(item) for item in playlist_items
            ]
            all_video_ids.update(video_ids)

        # Fetch detailed video metadata for all unique videos
        context.log.info("Fetching metadata for %s unique videos", len(all_video_ids))
        videos = context.resources.youtube_api.client.get_videos(list(all_video_ids))

        # Sort videos by publish date (newest first)
        sorted_videos = sort_videos_by_publish_date(videos, descending=True)

        # Select top N most recent videos
        top_videos = sorted_videos[:MAX_VIDEOS_TO_PROCESS]
        top_video_ids = {video["id"] for video in top_videos}

        context.log.info(
            "Selected top %s most recent videos: %s",
            len(top_video_ids),
            top_video_ids,
        )

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

        # Determine new video IDs to process (not yet in partitions)
        new_video_ids = top_video_ids - existing_partitions

        # Check which existing partitions have been successfully materialized
        # A partition is only successful if ALL 4 assets are materialized
        existing_top_video_ids = top_video_ids & existing_partitions
        successfully_materialized = get_successfully_materialized_partitions(
            context, asset_keys, existing_top_video_ids
        )

        # Videos needing processing: new + failed/unmaterialized
        failed_or_unmaterialized = existing_top_video_ids - successfully_materialized
        videos_to_process = new_video_ids | failed_or_unmaterialized

        # Determine video IDs to not process (no longer in top N)
        old_video_ids = existing_partitions - top_video_ids

        if not videos_to_process and not old_video_ids:
            return SkipReason(
                f"No changes in top YouTube videos. "
                f"All {len(successfully_materialized)} videos already materialized."
            )

        context.log.info("New videos to process: %s", new_video_ids)
        context.log.info(
            "Failed/unmaterialized videos to retry: %s", failed_or_unmaterialized
        )
        context.log.info(
            "Successfully materialized videos: %s", successfully_materialized
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
            dynamic_partitions_requests=dynamic_requests,
            run_requests=run_requests,
            cursor=json.dumps(sorted(top_video_ids)),
        )

    except Exception as e:
        context.log.exception("YouTube shorts sensor failed")
        return SkipReason(f"Sensor failed with error: {e}")
