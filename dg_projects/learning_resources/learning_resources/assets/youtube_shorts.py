"""Assets for processing YouTube Shorts videos."""

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any

import httpx
import yt_dlp
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetMaterialization,
    AssetOut,
    Backoff,
    DagsterEventType,
    DataVersion,
    DynamicPartitionsDefinition,
    EventRecordsFilter,
    Jitter,
    Output,
    RetryPolicy,
    asset,
    multi_asset,
)
from upath import UPath

from learning_resources.lib.youtube import get_highest_quality_thumbnail

# Dynamic partitions for video IDs
youtube_video_ids = DynamicPartitionsDefinition(name="youtube_video_ids")


def get_latest_materialization(
    context: AssetExecutionContext,
    asset_key: AssetKey,
    partition_key: str | None = None,
) -> AssetMaterialization:
    """
    Retrieve the latest materialization for a given asset and partition.

    Args:
        context: Asset execution context
        asset_key: The asset key to query
        partition_key: Optional partition key to filter by

    Returns:
        AssetMaterialization object containing metadata from the latest materialization

    Raises:
        ValueError: If no materialization is found
    """
    materialization_records = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=asset_key,
            asset_partitions=[partition_key] if partition_key else None,
        ),
        limit=1,
    )

    if not materialization_records:
        partition_msg = f" with partition: {partition_key}" if partition_key else ""
        msg = f"No materialization found for asset {asset_key}{partition_msg}"
        raise ValueError(msg)

    return materialization_records[0].asset_materialization


@multi_asset(
    code_version="youtube_shorts_v1",
    group_name="youtube_shorts",
    required_resource_keys={"youtube_api"},
    partitions_def=youtube_video_ids,
    outs={
        "video_content": AssetOut(
            io_manager_key="yt_s3file_io_manager",
            key=AssetKey(["youtube_shorts", "video_content"]),
        ),
        "video_thumbnail": AssetOut(
            io_manager_key="yt_s3file_io_manager",
            key=AssetKey(["youtube_shorts", "video_thumbnail"]),
        ),
        "video_metadata": AssetOut(
            io_manager_key="yt_s3file_io_manager",
            key=AssetKey(["youtube_shorts", "video_metadata"]),
        ),
    },
)
def download_youtube_video_assets(context: AssetExecutionContext):
    """
    Download video content, thumbnail, and metadata for a YouTube video.

    This multi-asset downloads:
    1. Video content using yt-dlp
    2. Highest quality thumbnail
    3. Full video metadata from YouTube API

    Outputs are uploaded to S3 using S3FileObjectIOManager.
    """
    video_id = context.partition_key
    context.log.info("Processing video ID: %s", video_id)

    # Fetch video metadata from YouTube API
    videos = context.resources.youtube_api.client.get_videos([video_id])
    if not videos:
        msg = f"No video found for ID: {video_id}"
        raise ValueError(msg)

    video_metadata = videos[0]
    video_title = video_metadata["snippet"]["title"]
    video_published_at = video_metadata["snippet"]["publishedAt"]

    # Create data version from video ID, title, and publish date
    version_string = f"{video_id}|{video_title}|{video_published_at}"
    data_version = hashlib.sha256(version_string.encode()).hexdigest()

    # Download video using yt-dlp
    context.log.info("Downloading video: %s", video_title)
    with tempfile.TemporaryDirectory() as temp_dir:
        video_output_template = str(Path(temp_dir) / f"{video_id}.%(ext)s")

        ydl_opts = {
            "format": "best[ext=mp4]",
            "outtmpl": video_output_template,
            "quiet": True,
            "no_warnings": True,
            "postprocessor_args": {
                # Recommended for fast streaming, for details see:
                # https://code.pixplicity.com/ffmpeg/faststart/
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

        # Download thumbnail
        thumbnail_info = get_highest_quality_thumbnail(video_metadata)
        thumbnail_url = thumbnail_info.get("url")

        if not thumbnail_url:
            msg = f"No thumbnail found for video: {video_id}"
            raise ValueError(msg)

        context.log.info("Downloading thumbnail from: %s", thumbnail_url)
        thumbnail_response = httpx.get(thumbnail_url)
        thumbnail_response.raise_for_status()

        # Always use .jpg extension for consistency
        thumbnail_ext = "jpg"
        thumbnail_file = Path(temp_dir) / f"{video_id}.{thumbnail_ext}"
        thumbnail_file.write_bytes(thumbnail_response.content)

        # S3 path: youtube_shorts/{video_id}/{video_id}.jpg
        thumbnail_s3_path = f"{video_id}/{video_id}.{thumbnail_ext}"

        context.log.info("Thumbnail saved: %s -> %s", thumbnail_file, thumbnail_s3_path)

        # Save metadata as JSON
        metadata_file = Path(temp_dir) / f"{video_id}.json"
        with metadata_file.open("w") as f:
            json.dump(video_metadata, f, indent=2)

        # S3 path: youtube_shorts/{video_id}/{video_id}.json
        metadata_s3_path = f"{video_id}/{video_id}.json"

        context.log.info("Metadata saved: %s -> %s", metadata_file, metadata_s3_path)

        # Yield outputs with data version for change detection
        yield Output(
            value=(video_file, video_s3_path),
            output_name="video_content",
            data_version=DataVersion(data_version),
            metadata={
                "video_id": video_id,
                "video_title": video_title,
                "publish_date": video_published_at,
                "data_version": data_version,
                "file_size_bytes": video_file.stat().st_size,
                "format": video_ext,
                "s3_path": video_s3_path,
            },
        )

        yield Output(
            value=(thumbnail_file, thumbnail_s3_path),
            output_name="video_thumbnail",
            data_version=DataVersion(data_version),
            metadata={
                "video_id": video_id,
                "video_title": video_title,
                "data_version": data_version,
                "thumbnail_width": thumbnail_info.get("width"),
                "thumbnail_height": thumbnail_info.get("height"),
                "s3_path": thumbnail_s3_path,
            },
        )

        yield Output(
            value=(metadata_file, metadata_s3_path),
            output_name="video_metadata",
            data_version=DataVersion(data_version),
            metadata={
                "video_id": video_id,
                "video_title": video_title,
                "publish_date": video_published_at,
                "data_version": data_version,
                "s3_path": metadata_s3_path,
                "youtube_metadata": video_metadata,
            },
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
    required_resource_keys={"learn_api"},
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def youtube_video_webhook(
    context: AssetExecutionContext,
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

    # Construct webhook payload with YouTube metadata
    webhook_data = {
        "video_id": video_id,
        "youtube_metadata": json.loads(metadata_content),
        "source": "youtube_shorts",
    }

    context.log.info("Webhook payload for %s: %s", video_id, webhook_data)

    try:
        # Send webhook to Learn API using HMAC-signed request
        response_data = context.resources.learn_api.client.notify_video_shorts(
            webhook_data
        )

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
