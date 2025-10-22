"""Helper functions for YouTube data processing."""

from datetime import datetime
from typing import Any

import httpx
import yaml


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
