"""Helper functions for YouTube data processing."""

from datetime import datetime
from typing import Any

import httpx
import yaml
from dagster import AssetKey, DagsterEventType, DagsterInstance, EventRecordsFilter


def fetch_youtube_shorts_config(config_url: str) -> list[dict[str, Any]]:
    """
    Fetch YouTube shorts playlist configuration from GitHub.

    Args:
        config_url: URL to the YAML configuration file

    Returns:
        List of configuration dictionaries containing channel and playlist info
    """
    response = httpx.get(config_url)
    response.raise_for_status()
    return yaml.safe_load(response.text)


def extract_video_id_from_playlist_item(item: dict[str, Any]) -> str:
    """
    Extract video ID from a playlist item.

    Args:
        item: Playlist item dictionary from YouTube API

    Returns:
        Video ID string
    """
    return item["contentDetails"]["videoId"]


def get_video_publish_date(video: dict[str, Any]) -> datetime:
    """
    Extract publish date from video metadata.

    Args:
        video: Video dictionary from YouTube API

    Returns:
        Publish datetime
    """
    published_at = video["snippet"]["publishedAt"]
    return datetime.fromisoformat(published_at.replace("Z", "+00:00"))


def sort_videos_by_publish_date(
    videos: list[dict[str, Any]], *, descending: bool = True
) -> list[dict[str, Any]]:
    """
    Sort videos by publish date.

    Args:
        videos: List of video dictionaries
        descending: Sort in descending order (newest first) if True

    Returns:
        Sorted list of videos
    """
    return sorted(
        videos,
        key=get_video_publish_date,
        reverse=descending,
    )


def get_highest_quality_thumbnail(video: dict[str, Any]) -> dict[str, Any]:
    """
    Get the highest quality thumbnail URL from video metadata.

    Args:
        video: Video dictionary from YouTube API

    Returns:
        Dictionary with thumbnail URL and dimensions
    """
    thumbnails = video["snippet"]["thumbnails"]
    # Priority order: maxres > standard > high > medium > default
    for quality in ["maxres", "standard", "high", "medium", "default"]:
        if quality in thumbnails:
            return thumbnails[quality]
    return thumbnails.get("default", {})


def get_successfully_materialized_partitions(instance, asset_keys, partition_keys):
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
            event_records = instance.get_event_records(
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


def get_videos_to_process(  # noqa: PLR0913
    youtube_client: Any,
    config_url: str,
    max_videos: int,
    existing_partitions: set[str],
    instance: DagsterInstance,
    asset_keys: list[AssetKey],
) -> dict[str, Any]:
    """
    Determine which YouTube videos need processing based on playlist configuration.

    This function:
    1. Fetches playlist configuration from the provided URL
    2. Retrieves all videos from configured playlists
    3. Sorts videos by publish date (newest first)
    4. Selects the top N most recent videos
    5. Determines which videos are new or need reprocessing

    Args:
        youtube_client: YouTube API client with get_playlist_items and
            get_videos methods
        config_url: URL to the YAML configuration file
        max_videos: Maximum number of most recent videos to process
        existing_partitions: Set of existing partition keys
        instance: Dagster instance for checking materialization status
        asset_keys: List of asset keys to check for materialization

    Returns:
        Dictionary containing:
            - playlist_ids: List of playlist IDs found in config
            - all_video_ids: List of all video IDs found across all playlists
            - new_video_ids: Set of new video IDs not yet in partitions
            - videos_to_process: Set of all video IDs that need processing
    """
    # Fetch playlist configuration
    config = fetch_youtube_shorts_config(config_url)

    # Collect all playlist IDs from config
    playlist_ids = []
    for channel_config in config:
        if playlists := channel_config.get("playlists"):
            playlist_ids.extend([p["id"] for p in playlists])

    # Fetch all videos from all playlists
    all_video_ids = set()
    for playlist_id in playlist_ids:
        playlist_items = youtube_client.get_playlist_items(playlist_id)
        video_ids = [
            extract_video_id_from_playlist_item(item) for item in playlist_items
        ]
        all_video_ids.update(video_ids)

    # Fetch detailed video metadata for all unique videos
    videos = youtube_client.get_videos(list(all_video_ids))

    # Sort videos by publish date (newest first)
    sorted_videos = sort_videos_by_publish_date(videos, descending=True)

    # Select top N most recent videos
    top_videos = sorted_videos[:max_videos]
    top_video_ids = {video["id"] for video in top_videos}

    # Determine new video IDs to process (not yet in partitions)
    new_video_ids = top_video_ids - existing_partitions

    # Check which existing partitions have been successfully materialized
    # A partition is only successful if ALL assets are materialized
    existing_top_video_ids = top_video_ids & existing_partitions
    successfully_materialized = get_successfully_materialized_partitions(
        instance, asset_keys, existing_top_video_ids
    )

    # Videos needing processing: new + failed/unmaterialized
    failed_or_unmaterialized = existing_top_video_ids - successfully_materialized
    videos_to_process = new_video_ids | failed_or_unmaterialized

    return {
        "playlist_ids": playlist_ids,
        "all_video_ids": list(all_video_ids),
        "new_video_ids": new_video_ids,
        "videos_to_process": videos_to_process,
    }
