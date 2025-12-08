"""Assets for processing YouTube Shorts videos."""

import hashlib
import json
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any

import httpx
import yaml
import yt_dlp
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AutomationCondition,
    Backoff,
    DataVersion,
    DynamicPartitionsDefinition,
    Jitter,
    MetadataValue,
    Output,
    RetryPolicy,
    asset,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.resources.api_client_factory import ApiClientFactory
from upath import UPath

from learning_resources.lib.youtube import (
    get_highest_quality_thumbnail,
    get_videos_to_process,
)
from learning_resources.resources.youtube_api import YouTubeApiClientFactory

# Configuration
YOUTUBE_SHORTS_CONFIG_URL = (
    "https://raw.githubusercontent.com/mitodl/open-video-data/"
    "refs/heads/mitopen/youtube/shorts.yaml"
)

# Process top 12 most recent videos
MAX_VIDEOS_TO_PROCESS = 12

# Dynamic partitions for video IDs
youtube_video_ids = DynamicPartitionsDefinition(name="youtube_video_ids")

# Asset keys
playlist_config_key = AssetKey(["youtube_shorts", "playlist_config"])
playlist_api_key = AssetKey(["youtube_shorts", "playlist_api"])


@asset(
    key=playlist_config_key,
    group_name="youtube_shorts",
    description=(
        "YouTube playlist configuration file from the open-video-data "
        "GitHub repository. This YAML file defines which playlists to "
        "monitor for video shorts."
    ),
    code_version="youtube_shorts_api_v1",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def youtube_playlist_config(
    context: AssetExecutionContext,
) -> Output[list[dict[str, Any]]]:
    """Fetch and parse the YouTube Shorts playlist configuration from GitHub."""
    context.log.info("Fetching playlist config from %s", YOUTUBE_SHORTS_CONFIG_URL)
    response = httpx.get(YOUTUBE_SHORTS_CONFIG_URL)
    response.raise_for_status()
    config = yaml.safe_load(response.text)

    # Create a stable hash for versioning
    config_text = yaml.safe_dump(config, sort_keys=True)
    data_version = hashlib.sha256(config_text.encode("utf-8")).hexdigest()

    playlist_count = sum(len(channel.get("playlists", [])) for channel in config)

    context.log.info("Fetched playlist config with %d playlists", playlist_count)

    return Output(
        value=config,
        data_version=DataVersion(data_version),
        metadata={
            "config_url": YOUTUBE_SHORTS_CONFIG_URL,
            "playlist_count": playlist_count,
            "config_hash": data_version,
        },
    )


@asset(
    key=playlist_api_key,
    group_name="youtube_shorts",
    ins={"playlist_config": AssetIn(key=playlist_config_key)},
    code_version="youtube_shorts_api_v1",
    description=(
        "YouTube API response for configured playlists. "
        "Contains video and playlist metadata from YouTube Data API."
    ),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def youtube_playlist_api(
    context: AssetExecutionContext,
    youtube_api: YouTubeApiClientFactory,
    playlist_config: list[dict[str, Any]],
) -> Generator[Output[dict[str, Any]], None, None]:
    """Fetch playlist and video metadata from YouTube API based on config."""
    context.log.info("Fetching playlist and video data from YouTube API")

    result = get_videos_to_process(
        youtube_client=youtube_api.client,
        config=playlist_config,
        max_videos=MAX_VIDEOS_TO_PROCESS,
    )

    # Extract key information
    playlist_ids = result["playlist_ids"]
    playlist_metadata = result["playlist_metadata"]
    all_video_ids = result["all_video_ids"]
    videos_to_process = result["videos_to_process"]

    # Create data version from playlist ETags (indicates API data changed)
    etags = "|".join(
        playlist_metadata.get(pid, {}).get("etag", "")
        for pid in sorted(playlist_ids)  # Sort for stability
    )
    api_data_version = hashlib.sha256(etags.encode("utf-8")).hexdigest()

    # Create mapping of video_id -> etag for change detection
    video_etags = {
        video_id: result["video_metadata"].get(video_id, {}).get("etag", "")
        for video_id in videos_to_process
    }

    yield Output(
        value=result,
        data_version=DataVersion(api_data_version),
        metadata={
            "playlist_count": len(playlist_ids),
            "playlist_ids": MetadataValue.json(playlist_ids),
            "total_video_count": len(all_video_ids),
            "videos_to_process_count": len(videos_to_process),
            "videos_to_process_ids": MetadataValue.json(
                sorted(videos_to_process)[:MAX_VIDEOS_TO_PROCESS]
            ),  # Top N videos to process
            "video_etags": MetadataValue.json(video_etags),
            "api_data_version": api_data_version,
        },
    )


@asset(
    code_version="youtube_shorts_metadata_v1",
    key=AssetKey(["youtube_shorts", "video_metadata"]),
    group_name="youtube_shorts",
    description="Fetch and store YouTube video metadata with ETag-based versioning.",
    partitions_def=youtube_video_ids,
    automation_condition=(
        upstream_or_code_changes()
        | AutomationCondition.on_missing()
        | AutomationCondition.on_cron("0 * * * *")  # Check hourly for metadata changes
    ),
    io_manager_key="yt_s3file_io_manager",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def youtube_video_metadata(
    context: AssetExecutionContext, youtube_api: YouTubeApiClientFactory
) -> Generator[Output[tuple[Path, str]], None, None]:
    """
    Fetch video metadata from YouTube API and store with ETag-based data versioning.

    This asset periodically re-fetches metadata from YouTube API (hourly) to detect
    changes. When the video's ETag changes, the data_version updates automatically,
    triggering downstream assets to re-process the video content.
    """
    video_id = context.partition_key
    context.log.info("Fetching metadata for video ID: %s", video_id)

    # Fetch video metadata from YouTube API
    videos = youtube_api.client.get_videos([video_id])
    if not videos:
        msg = f"No video found for ID: {video_id}"
        raise ValueError(msg)

    video_metadata = videos[0]

    # Use YouTube API ETag as data version for change detection
    video_etag = video_metadata.get("etag", "")
    if not video_etag:
        msg = f"No etag found in video metadata for ID: {video_id}"
        raise ValueError(msg)

    # Save processed metadata as JSON
    processed_metadata = {
        "video_id": video_id,
        "video_title": video_metadata.get("snippet", {}).get("title", ""),
        "data_version": video_etag,
        "video_etag": video_etag,
        "youtube_metadata": video_metadata,
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        metadata_file = Path(temp_dir) / f"{video_id}.json"
        with metadata_file.open("w") as f:
            json.dump(processed_metadata, f, indent=2)

        # S3 path: youtube_shorts/{video_id}/{video_id}.json
        metadata_s3_path = f"{video_id}/{video_id}.json"

        context.log.info("Metadata saved: %s -> %s", metadata_file, metadata_s3_path)

        yield Output(
            value=(metadata_file, metadata_s3_path),
            data_version=DataVersion(video_etag),
        )


@asset(
    code_version="youtube_shorts_content_v1",
    key=AssetKey(["youtube_shorts", "video_content"]),
    group_name="youtube_shorts",
    description="Download YouTube video content using yt-dlp.",
    partitions_def=youtube_video_ids,
    ins={
        "video_metadata": AssetIn(key=AssetKey(["youtube_shorts", "video_metadata"])),
    },
    automation_condition=upstream_or_code_changes() | AutomationCondition.on_missing(),
    io_manager_key="yt_s3file_io_manager",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=5.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def youtube_video_content(
    context: AssetExecutionContext,
    video_metadata: UPath,
) -> Generator[Output[tuple[Path, str]], None, None]:
    """
    Download video content using yt-dlp.

    This asset depends on video_metadata and will automatically re-run
    when the video's ETag changes (indicating metadata updates).
    """
    # Load metadata from the stored JSON file
    with video_metadata.open("r") as f:
        metadata = json.load(f)

    video_id = metadata["video_id"]
    video_title = metadata["video_title"]
    data_version = metadata["data_version"]

    context.log.info("Downloading video: %s", video_title)

    # Download video using yt-dlp
    with tempfile.TemporaryDirectory() as temp_dir:
        video_output_template = str(Path(temp_dir) / f"{video_id}.%(ext)s")

        ydl_opts = {
            "format": "best[ext=mp4]",
            "outtmpl": video_output_template,
            "quiet": True,
            "no_warnings": True,
            "postprocessor_args": {
                # Recommended for fast streaming
                "ffmpeg": ["-movflags", "faststart"]
            },
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}", download=True
            )
            video_ext = info.get("ext", "mp4")
            video_file = Path(temp_dir) / f"{video_id}.{video_ext}"

        if not video_file.exists():
            msg = f"Failed to download video: {video_id}"
            raise RuntimeError(msg)

        # S3 path: youtube_shorts/{video_id}/{video_id}.mp4
        video_s3_path = f"{video_id}/{video_id}.{video_ext}"

        context.log.info("Video downloaded: %s -> %s", video_file, video_s3_path)

        yield Output(
            value=(video_file, video_s3_path),
            data_version=DataVersion(data_version),
        )


@asset(
    code_version="youtube_shorts_thumbnail_v1",
    key=AssetKey(["youtube_shorts", "video_thumbnail"]),
    group_name="youtube_shorts",
    description="Download YouTube video thumbnail.",
    partitions_def=youtube_video_ids,
    ins={
        "video_metadata": AssetIn(key=AssetKey(["youtube_shorts", "video_metadata"])),
    },
    automation_condition=upstream_or_code_changes() | AutomationCondition.on_missing(),
    io_manager_key="yt_s3file_io_manager",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=5.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def youtube_video_thumbnail(
    context: AssetExecutionContext,
    video_metadata: UPath,
) -> Generator[Output[tuple[Path, str]], None, None]:
    """
    Download video thumbnail.

    This asset depends on video_metadata and will automatically re-run
    when the video's ETag changes.
    """
    # Load metadata from the stored JSON file
    with video_metadata.open("r") as f:
        metadata = json.load(f)

    video_id = metadata["video_id"]
    data_version = metadata["data_version"]
    youtube_metadata = metadata["youtube_metadata"]

    # Download thumbnail
    thumbnail_info = get_highest_quality_thumbnail(youtube_metadata)
    thumbnail_url = thumbnail_info.get("url")

    if not thumbnail_url:
        msg = f"No thumbnail found for video: {video_id}"
        raise ValueError(msg)

    context.log.info("Downloading thumbnail from: %s", thumbnail_url)
    thumbnail_response = httpx.get(thumbnail_url)
    thumbnail_response.raise_for_status()

    # Always use .jpg extension for consistency
    thumbnail_ext = "jpg"

    with tempfile.TemporaryDirectory() as temp_dir:
        thumbnail_file = Path(temp_dir) / f"{video_id}.{thumbnail_ext}"
        thumbnail_file.write_bytes(thumbnail_response.content)

        # S3 path: youtube_shorts/{video_id}/{video_id}.jpg
        thumbnail_s3_path = f"{video_id}/{video_id}.{thumbnail_ext}"

        context.log.info("Thumbnail saved: %s -> %s", thumbnail_file, thumbnail_s3_path)

        yield Output(
            value=(thumbnail_file, thumbnail_s3_path),
            data_version=DataVersion(data_version),
        )


@asset(
    code_version="youtube_shorts_webhook_v1",
    key=AssetKey(["youtube_shorts", "video_webhook"]),
    group_name="youtube_shorts",
    description="Send webhook to Learn API after YouTube video processing.",
    partitions_def=youtube_video_ids,
    ins={
        "video_content": AssetIn(key=AssetKey(["youtube_shorts", "video_content"])),
        "video_thumbnail": AssetIn(key=AssetKey(["youtube_shorts", "video_thumbnail"])),
        "video_metadata": AssetIn(key=AssetKey(["youtube_shorts", "video_metadata"])),
    },
    automation_condition=upstream_or_code_changes(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def youtube_video_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
    video_content: UPath,  # noqa: ARG001
    video_thumbnail: UPath,  # noqa: ARG001
    video_metadata: UPath,
) -> dict[str, Any]:
    """
    Send webhook notification to Learn API after video assets are ready.

    This asset depends on all three video assets (content, thumbnail, metadata)
    and only executes after they complete successfully. It sends the YouTube
    metadata JSON to the Learn API webhook endpoint.
    """
    video_id = context.partition_key

    # Read metadata content
    metadata_content = video_metadata.read_text()
    processed_metadata = json.loads(metadata_content)

    # Construct webhook payload with YouTube metadata
    webhook_data = {
        "video_id": video_id,
        "youtube_metadata": processed_metadata["youtube_metadata"],
        "source": "youtube_shorts",
    }

    context.log.info("Webhook payload for %s: %s", video_id, webhook_data)

    try:
        # Send webhook to Learn API using HMAC-signed request
        response_data = learn_api.client.notify_video_shorts(webhook_data)

        context.log.info(
            "Webhook sent successfully for video_id=%s, response=%s",
            video_id,
            response_data,
        )

    except httpx.HTTPStatusError as error:
        error_message = (
            f"Webhook failed for video_id={video_id} "
            f"with status code {error.response.status_code}: {error}"
        )
        context.log.exception(error_message)
        raise RuntimeError(error_message) from error

    else:
        return {
            "video_id": video_id,
            "webhook_status": "success",
            "response_data": response_data,
        }
