"""
YouTube Video Shorts Assets.
"""

import hashlib
import json
import logging
import re
import tempfile
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import requests
import yt_dlp
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Config,
    DataVersion,
    DynamicPartitionsDefinition,
    Output,
    asset,
)
from dagster_aws.s3 import S3Resource
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.resources.learn_api import MITLearnApiClient

from learning_resources.resources.youtube_client import YouTubeClientFactory


def _check_video_duration(video_data: dict[str, Any], max_duration: int = 60) -> bool:
    """
    Check if video duration is within acceptable limits for shorts.

    YouTube shorts should typically be 60 seconds or less.
    """
    duration_str = video_data.get("contentDetails", {}).get("duration", "")
    if not duration_str:
        return False

    # Parse ISO 8601 duration format (PT1M30S)
    pattern = r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
    match = re.match(pattern, duration_str)

    if not match:
        return False

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds <= max_duration


log = logging.getLogger(__name__)

# Dynamic partitioning for individual video processing
youtube_video_ids = DynamicPartitionsDefinition(name="youtube_video_ids")


class YouTubeShortsConfig(Config):
    """Configuration for YouTube video shorts processing."""

    bucket_name: str = "ol-devops-sandbox"
    s3_prefix: str = "youtube_shorts"
    max_workers: int = 4
    max_video_duration: int = 60  # seconds
    cached_metadata: str | None = None  # JSON string of video metadata from sensor


def _generate_video_version(video_data: dict[str, Any]) -> str:
    """
    Generate a version hash for a video based on its metadata.

    This enables Dagster to detect when video metadata has changed
    and needs reprocessing.
    """
    # Use key fields that indicate video changes
    version_fields = {
        "id": video_data.get("id"),
        "etag": video_data.get("etag"),
        "published_at": video_data.get("snippet", {}).get("publishedAt"),
        "title": video_data.get("snippet", {}).get("title"),
        "view_count": video_data.get("statistics", {}).get("viewCount"),
    }

    version_string = json.dumps(version_fields, sort_keys=True)
    return hashlib.sha256(version_string.encode()).hexdigest()[:16]


@asset(
    key=AssetKey(["youtube_shorts", "video_metadata"]),
    group_name="youtube_shorts",
    description="Extract YouTube video metadata from API and upload to S3",
    code_version="youtube_video_metadata_v3",
    partitions_def=youtube_video_ids,
    automation_condition=upstream_or_code_changes(),
    io_manager_key="s3file_io_manager",
)
def youtube_video_metadata(
    context: AssetExecutionContext,
    config: YouTubeShortsConfig,
    youtube_client: YouTubeClientFactory,
    s3: S3Resource,
) -> Output[dict[str, Any]]:
    """
    Extract detailed metadata for a specific YouTube video and upload to S3.

    Args:
        context: Dagster execution context containing partition key (video ID)
        config: Configuration for processing (may include cached metadata from sensor)
        youtube_client: YouTube API client factory
        s3: S3 resource for uploads

    Returns:
        Video metadata dictionary with version tracking
    """
    video_id = context.partition_key
    log.info("Extracting metadata for video: %s", video_id)

    try:
        # Check if we have metadata from the sensor via run_config
        video_data = None
        from_sensor = False
        if config.cached_metadata:
            try:
                video_data = json.loads(config.cached_metadata)
                from_sensor = True
                log.info(
                    "Using metadata from sensor for video: %s",
                    video_id,
                )
            except (json.JSONDecodeError, KeyError, TypeError):
                log.warning(
                    "Failed to parse metadata from run_config, will fetch from API",
                    exc_info=True,
                )
                video_data = None

        # Fallback: Fetch video details from YouTube API if no metadata in run_config
        if video_data is None:
            log.info("Fetching metadata from YouTube API for video: %s", video_id)
            request = youtube_client.client.videos().list(
                part="snippet,contentDetails,statistics", id=video_id
            )
            response = request.execute()

            videos = response.get("items", [])
            if not videos:
                msg = f"Video {video_id} not found"
                raise ValueError(msg)  # noqa: TRY301

            video_data = videos[0]

        # Check if this is actually a short (duration filter)
        # Only check duration if we fetched from API (sensor data is pre-filtered)
        if not from_sensor and not _check_video_duration(video_data, max_duration=60):
            duration = video_data.get("contentDetails", {}).get("duration", "")
            msg = f"Video {video_id} is not a short (duration: {duration})"
            raise ValueError(msg)  # noqa: TRY301

        # Generate version for change tracking
        version = _generate_video_version(video_data)

        # Upload complete metadata to S3
        s3_key = f"{config.s3_prefix}/metadata/{video_id}.json"

        # Convert video_data to JSON and upload to S3
        metadata_json = json.dumps(video_data, indent=2, ensure_ascii=False)

        try:
            # Check if metadata already exists
            s3.get_client().head_object(Bucket=config.bucket_name, Key=s3_key)
            log.info("Metadata %s already exists in S3, updating...", video_id)
        except s3.get_client().exceptions.NoSuchKey:
            log.info("Uploading new metadata for %s to S3", video_id)

        # Upload metadata to S3
        s3.get_client().put_object(
            Bucket=config.bucket_name,
            Key=s3_key,
            Body=metadata_json.encode("utf-8"),
            ContentType="application/json",
        )

        log.info("Successfully extracted and uploaded metadata for video: %s", video_id)

        return Output(
            value=video_data,
            data_version=DataVersion(version),
            metadata={
                "video_id": video_id,
                "title": video_data.get("snippet", {}).get("title", ""),
                "s3_bucket": config.bucket_name,
                "s3_key": s3_key,
                "metadata_size": len(metadata_json),
            },
        )

    except Exception:
        log.exception("Failed to extract metadata for video: %s", video_id)
        raise


@asset(
    key=AssetKey(["youtube_shorts", "video_content"]),
    group_name="youtube_shorts",
    description="Download and upload YouTube video content to S3",
    code_version="youtube_video_content_v3",
    partitions_def=youtube_video_ids,
    automation_condition=upstream_or_code_changes(),
    ins={
        "video_thumbnail": AssetIn(key=AssetKey(["youtube_shorts", "video_thumbnail"])),
    },
    io_manager_key="s3file_io_manager",
)
def youtube_video_content(
    context: AssetExecutionContext,
    config: YouTubeShortsConfig,
    s3: S3Resource,
    learn_api: MITLearnApiClient,
    video_thumbnail: str,  # noqa: ARG001
) -> Output[str]:
    """
    Download YouTube video and upload to S3.

    Args:
        context: Dagster execution context
        config: Configuration for processing
        s3: S3 resource for uploads

    Returns:
        S3 path to the uploaded video file
    """
    video_id = context.partition_key
    video_url = f"https://www.youtube.com/watch?v={video_id}"

    log.info("Processing video content for: %s", video_id)

    # Check if video already exists in S3
    s3_key = f"{config.s3_prefix}/videos/{video_id}.mp4"

    try:
        # Check if file exists
        s3.get_client().head_object(Bucket=config.bucket_name, Key=s3_key)
        log.info("Video %s already exists in S3, skipping download", video_id)

        return Output(
            value=s3_key,
            metadata={
                "video_id": video_id,
                "s3_bucket": config.bucket_name,
                "s3_key": s3_key,
                "status": "already_exists",
            },
        )

    except s3.get_client().exceptions.NoSuchKey:
        # File doesn't exist, proceed with download
        pass

    # Download video using yt-dlp
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        output_file = temp_path / f"{video_id}.mp4"

        ydl_opts = {
            "format": "best[height<=720]/best",
            "outtmpl": str(output_file),
            "noplaylist": True,
            "extract_flat": False,
            # Anti-bot measures
            "http_headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                ),
                "Accept-Language": "en-us,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            },
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])

            if not output_file.exists():
                msg = "Downloaded video file not found"
                raise FileNotFoundError(msg)  # noqa: TRY301

            # Upload to S3
            with output_file.open("rb") as f:
                s3.get_client().upload_fileobj(
                    f,
                    config.bucket_name,
                    s3_key,
                    ExtraArgs={"ContentType": "video/mp4"},
                )

            log.info("Successfully uploaded video %s to S3", video_id)

            # Send webhook notification now that all assets are complete
            try:
                webhook_response = learn_api.notify_shorts_processed(video_id)
                log.info(
                    "Successfully sent webhook notification for video %s: %s",
                    video_id,
                    webhook_response,
                )
            except Exception as webhook_error:  # noqa: BLE001
                log.warning(
                    "Failed to send webhook notification for video %s: %s",
                    video_id,
                    webhook_error,
                )
                # Don't fail the asset if webhook fails - just log the warning

            return Output(
                value=s3_key,
                metadata={
                    "video_id": video_id,
                    "s3_bucket": config.bucket_name,
                    "s3_key": s3_key,
                    "file_size": output_file.stat().st_size,
                    "status": "uploaded",
                    "webhook_sent": True,
                },
            )

        except Exception:
            log.exception("Failed to download/upload video: %s", video_id)
            raise


@asset(
    key=AssetKey(["youtube_shorts", "video_thumbnail"]),
    group_name="youtube_shorts",
    description="Download and upload YouTube video thumbnail to S3",
    code_version="youtube_video_thumbnail_v2",
    partitions_def=youtube_video_ids,
    automation_condition=upstream_or_code_changes(),
    ins={
        "video_metadata": AssetIn(key=AssetKey(["youtube_shorts", "video_metadata"])),
    },
    io_manager_key="s3file_io_manager",
)
def youtube_video_thumbnail(
    context: AssetExecutionContext,
    config: YouTubeShortsConfig,
    s3: S3Resource,
    video_metadata: dict[str, Any],
) -> Output[str]:
    """
    Download YouTube video thumbnail and upload to S3.

    Args:
        context: Dagster execution context
        config: Configuration for processing
        s3: S3 resource for uploads
        video_metadata: Video metadata from previous asset

    Returns:
        S3 path to the uploaded thumbnail file
    """
    video_id = context.partition_key

    log.info("Processing thumbnail for video: %s", video_id)

    # Extract thumbnail URL from metadata
    thumbnails = video_metadata.get("snippet", {}).get("thumbnails", {})

    # Try to get high, medium, or default quality thumbnail
    thumbnail_url = None
    for quality in ["high", "medium", "default"]:
        if quality in thumbnails:
            thumbnail_url = thumbnails[quality]["url"]
            break

    if not thumbnail_url:
        msg = f"No thumbnail URL found for video {video_id}"
        raise ValueError(msg)

    s3_key = f"{config.s3_prefix}/thumbnails/{video_id}.jpg"

    try:
        # Check if thumbnail already exists
        s3.get_client().head_object(Bucket=config.bucket_name, Key=s3_key)
        log.info("Thumbnail %s already exists in S3, skipping download", video_id)

        return Output(
            value=s3_key,
            metadata={
                "video_id": video_id,
                "s3_bucket": config.bucket_name,
                "s3_key": s3_key,
                "status": "already_exists",
            },
        )

    except s3.get_client().exceptions.NoSuchKey:
        # File doesn't exist, proceed with download
        pass

    try:
        # Download thumbnail
        response = requests.get(thumbnail_url, timeout=30)
        response.raise_for_status()

        # Upload to S3 using a temporary file
        with NamedTemporaryFile() as temp_file:
            temp_file.write(response.content)
            temp_file.seek(0)

            s3.get_client().upload_fileobj(
                temp_file,
                config.bucket_name,
                s3_key,
                ExtraArgs={"ContentType": "image/jpeg"},
            )

        log.info("Successfully uploaded thumbnail %s to S3", video_id)

        return Output(
            value=s3_key,
            metadata={
                "video_id": video_id,
                "s3_bucket": config.bucket_name,
                "s3_key": s3_key,
                "thumbnail_url": thumbnail_url,
                "content_length": len(response.content),
                "status": "uploaded",
            },
        )

    except Exception:
        log.exception("Failed to download/upload thumbnail: %s", video_id)
        raise
