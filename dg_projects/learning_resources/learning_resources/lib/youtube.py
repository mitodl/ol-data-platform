"""Helper functions for YouTube data processing."""

from datetime import datetime
from typing import Any


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
    return datetime.fromisoformat(published_at)


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


def get_videos_to_process(
    youtube_client: Any,
    config: list[dict[str, Any]],
    max_videos: int,
) -> dict[str, Any]:
    """
    Determine which YouTube videos need processing based on playlist configuration.

    This function:
    1. Takes playlist configuration as input
    2. Retrieves all videos from configured playlists
    3. Sorts videos by publish date (newest first)
    4. Returns the top N most recent videos to process, and other metadata

    Args:
        youtube_client: YouTube API client with get_playlist_items, get_playlists,
            and get_videos methods
        config: List of configuration dictionaries containing channel and playlist info
        max_videos: Maximum number of most recent videos to process

    Returns:
        Dictionary containing:
            - playlist_ids: List of unique playlist IDs found in config
            - playlist_metadata: Mapping of playlist IDs to YouTube API metadata
            - playlist_to_videos: Mapping of playlist IDs to their video IDs
            - all_video_ids: List of all video IDs found across all playlists
            - videos_to_process: Set of top N most recent video IDs
            - video_metadata: Mapping of video IDs to their YouTube API metadata
    """

    # Collect all playlist IDs from config
    playlist_ids = []
    for channel_config in config:
        if playlists := channel_config.get("playlists"):
            playlist_ids.extend([p["id"] for p in playlists])

    # Ensure playlist IDs are unique while preserving order
    playlist_ids = list(dict.fromkeys(playlist_ids))

    # Fetch all videos from all playlists, tracking playlist->video relationships
    all_video_ids = set()
    playlist_to_videos = {}
    for playlist_id in playlist_ids:
        playlist_items = youtube_client.get_playlist_items(playlist_id)
        video_ids = [
            extract_video_id_from_playlist_item(item) for item in playlist_items
        ]
        playlist_to_videos[playlist_id] = video_ids
        all_video_ids.update(video_ids)

    # Fetch playlist metadata to capture API response details
    playlist_metadata = {
        item["id"]: item for item in youtube_client.get_playlists(playlist_ids)
    }

    # Fetch detailed video metadata for all unique videos
    videos = youtube_client.get_videos(list(all_video_ids))
    videos_metadata = {video["id"]: video for video in videos}

    # Sort videos by publish date (newest first)
    sorted_videos = sort_videos_by_publish_date(videos, descending=True)

    return {
        "playlist_ids": playlist_ids,
        "playlist_metadata": playlist_metadata,
        "playlist_to_videos": playlist_to_videos,
        "all_video_ids": list(all_video_ids),
        "videos_to_process": {video["id"] for video in sorted_videos[:max_videos]},
        "video_metadata": videos_metadata,
    }
