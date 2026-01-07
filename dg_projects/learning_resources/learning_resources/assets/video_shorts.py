"""Assets for processing Video Shorts videos from Google Sheets."""

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any

import httpx
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

from learning_resources.lib.contants import (
    VIDEO_SHORT_THUMB_LARGE_HEIGHT,
    VIDEO_SHORT_THUMB_LARGE_WIDTH,
    VIDEO_SHORT_THUMB_SMALL_HEIGHT,
    VIDEO_SHORT_THUMB_SMALL_WIDTH,
)
from learning_resources.lib.google_sheets import (
    convert_dropbox_link_to_direct,
    fetch_video_shorts_from_google_sheet,
    generate_video_thumbnail,
)

# Process top 12 most recent videos
MAX_VIDEOS_TO_PROCESS = 12

# Dynamic partitions for video IDs
video_short_ids = DynamicPartitionsDefinition(name="video_short_ids")

# Asset keys
sheets_api_key = AssetKey(["video_shorts", "sheets_api"])


@asset(
    key=sheets_api_key,
    group_name="video_shorts",
    description=(
        "Google Sheets data containing video metadata. "
        "This sheet defines which videos to process with fields: "
        "Pub date, Video name, File name, Dropbox link."
    ),
    code_version="video_shorts_sheets_v2",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    required_resource_keys={"video_shorts_sheet_config"},
)
def google_sheets_api(
    context: AssetExecutionContext,
) -> Generator[Output[dict[str, Any]], None, None]:
    """Fetch and parse video metadata from Google Sheets."""
    # Fetch and parse video metadata from Google Sheets
    all_videos = fetch_video_shorts_from_google_sheet(context)
    context.log.info("Parsed %d valid videos from sheet", len(all_videos))

    # Take top N videos (already sorted by pub_date descending)
    videos_to_process = all_videos[:MAX_VIDEOS_TO_PROCESS]

    # Create data version from sheet content hash
    sheet_content = json.dumps(videos_to_process, sort_keys=True, default=str)
    data_version = hashlib.sha256(sheet_content.encode("utf-8")).hexdigest()

    # Extract partition keys for metadata
    partition_keys = [video["partition_key"] for video in videos_to_process]

    result = {
        "all_videos": all_videos,
        "videos_to_process": videos_to_process,
        "partition_keys": partition_keys,
    }

    yield Output(
        value=result,
        data_version=DataVersion(data_version),
        metadata={
            "total_video_count": len(all_videos),
            "videos_to_process_count": len(videos_to_process),
            "partition_keys": MetadataValue.json(partition_keys),
            "data_version": data_version,
        },
    )


@asset(
    code_version="video_shorts_metadata_v2",
    key=AssetKey(["video_shorts", "video_metadata"]),
    group_name="video_shorts",
    description="Video metadata from Google Sheets with partition-based versioning.",
    partitions_def=video_short_ids,
    ins={
        "sheets_data": AssetIn(key=sheets_api_key),
    },
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
def video_short_metadata(
    context: AssetExecutionContext,
    sheets_data: dict[str, Any],
) -> Generator[Output[tuple[Path, str]], None, None]:
    """
    Fetch video metadata from upstream sheets_api asset.

    This asset stores metadata for each video partition, providing
    data needed for downstream video download and thumbnail generation.
    """
    video_id = context.partition_key
    context.log.info("Processing metadata for partition: %s", video_id)

    # Find the video data for this partition
    videos_to_process = sheets_data["videos_to_process"]
    video_data = None
    for video in videos_to_process:
        if video["partition_key"] == video_id:
            video_data = video
            break

    if not video_data:
        msg = f"No video data found for partition: {video_id}"
        raise ValueError(msg)

    # Use the video_id as the data version
    data_version = video_id

    # Save full metadata as JSON (includes all fields from Google Sheets)
    video_ext = Path(video_data["file_name"]).suffix.lstrip(".")
    path_prefix = os.environ.get("LEARN_SHORTS_PREFIX", "shorts").rstrip("/")
    processed_metadata = {
        "video_id": video_id,
        "pub_date": video_data["pub_date_str"],
        "video_name": video_data["video_name"],
        "file_name": video_data["file_name"],
        "dropbox_link": video_data["dropbox_link"],
        "data_version": data_version,
        "video_metadata": {
            "video_id": video_id,
            "video_url": f"/{path_prefix}/{video_id}/{video_id}.{video_ext}",
            "title": video_data["video_name"],
            "published_at": video_data["pub_date_str"],
            "thumbnail_small": {
                "width": VIDEO_SHORT_THUMB_SMALL_WIDTH,
                "height": VIDEO_SHORT_THUMB_SMALL_HEIGHT,
                "url": f"/{path_prefix}/{video_id}/{video_id}_small.jpg",
            },
            "thumbnail_large": {
                "width": VIDEO_SHORT_THUMB_LARGE_WIDTH,
                "height": VIDEO_SHORT_THUMB_LARGE_HEIGHT,
                "url": f"/{path_prefix}/{video_id}/{video_id}_large.jpg",
            },
        },
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        metadata_file = Path(temp_dir) / f"{video_id}.json"
        with metadata_file.open("w") as f:
            json.dump(processed_metadata, f, indent=2)

        # S3 path: shorts/{video_id}/{video_id}.json
        metadata_s3_path = f"{video_id}/{video_id}.json"

        context.log.info("Metadata saved: %s -> %s", metadata_file, metadata_s3_path)

        yield Output(
            value=(metadata_file, metadata_s3_path),
            data_version=DataVersion(video_id),
        )


@asset(
    code_version="video_shorts_content_v2",
    key=AssetKey(["video_shorts", "video_content"]),
    group_name="video_shorts",
    description="Download video content from Dropbox.",
    partitions_def=video_short_ids,
    ins={
        "video_metadata": AssetIn(key=AssetKey(["video_shorts", "video_metadata"])),
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
def video_short_content(
    context: AssetExecutionContext,
    video_metadata: UPath,
) -> Generator[Output[tuple[Path, str]], None, None]:
    """
    Download video content from Dropbox.

    This asset depends on video_metadata and will automatically re-run
    when metadata changes.
    """
    # Load metadata from the stored JSON file
    with video_metadata.open("r") as f:
        metadata = json.load(f)

    video_id = metadata["video_id"]
    data_version = metadata["data_version"]
    dropbox_link = metadata["dropbox_link"]
    file_name = metadata["file_name"]
    video_ext = Path(file_name).suffix.lstrip(".")

    context.log.info("Downloading video from Dropbox for partition: %s", video_id)
    context.log.info("Video: %s", metadata["video_name"])

    # Convert Dropbox share link to direct download link
    direct_url = convert_dropbox_link_to_direct(dropbox_link)
    context.log.info("Direct download URL: %s", direct_url)

    # Download video
    with tempfile.TemporaryDirectory() as temp_dir:
        video_file = Path(temp_dir) / file_name

        context.log.info("Downloading from %s to %s", direct_url, video_file)
        with httpx.stream(
            "GET",
            direct_url,
            follow_redirects=True,
            timeout=300.0,
        ) as response:
            response.raise_for_status()
            with video_file.open("wb") as output_file:
                for chunk in response.iter_bytes(1024 * 1024):
                    if chunk:
                        output_file.write(chunk)

        if not video_file.exists() or video_file.stat().st_size == 0:
            msg = f"Failed to download video: {video_id}"
            raise RuntimeError(msg)

        # Full S3 path: shorts/{video_id}/{video_id}.{mp4}
        video_s3_path = f"{video_id}/{video_id}.{video_ext}"
        context.log.info("Video downloaded: %s -> %s", video_file, video_s3_path)

        yield Output(
            value=(video_file, video_s3_path),
            data_version=DataVersion(data_version),
        )


@asset(
    code_version="video_shorts_thumbnail_small_v1",
    key=AssetKey(["video_shorts", "video_thumbnail_small"]),
    group_name="video_shorts",
    description="Generate small thumbnail (270x480) from video file.",
    partitions_def=video_short_ids,
    ins={
        "video_content": AssetIn(key=AssetKey(["video_shorts", "video_content"])),
        "video_metadata": AssetIn(key=AssetKey(["video_shorts", "video_metadata"])),
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
def video_short_thumbnail_small(
    context: AssetExecutionContext,
    video_content: UPath,
    video_metadata: UPath,
) -> Generator[Output[tuple[Path, str]], None, None]:
    """
    Generate small thumbnail (270x480) from video file.

    This asset depends on video_content and will automatically re-run
    when the video changes.
    """
    # Load metadata from the stored JSON file
    with video_metadata.open("r") as f:
        metadata = json.load(f)

    video_id = metadata["video_id"]
    data_version = metadata["data_version"]

    context.log.info("Generating small thumbnail for partition: %s", video_id)

    # Download video file temporarily to generate thumbnail
    with tempfile.TemporaryDirectory() as temp_dir:
        # Copy video from S3 to temp location without loading entire file in memory
        video_temp_path = Path(temp_dir) / "video.mp4"
        with video_content.open("rb") as src, video_temp_path.open("wb") as dst:
            shutil.copyfileobj(src, dst, 1024 * 1024)

        # Generate small thumbnail
        thumbnail_file = Path(temp_dir) / f"{video_id}_small.jpg"
        generate_video_thumbnail(
            video_path=video_temp_path,
            thumbnail_path=thumbnail_file,
            width=VIDEO_SHORT_THUMB_SMALL_WIDTH,
            height=VIDEO_SHORT_THUMB_SMALL_HEIGHT,
        )

        if not thumbnail_file.exists():
            msg = f"Failed to generate small thumbnail for: {video_id}"
            raise RuntimeError(msg)

        # S3 path: video_shorts/{video_id}/{video_id}_small.jpg
        thumbnail_s3_path = f"{video_id}/{video_id}_small.jpg"

        context.log.info(
            "Small thumbnail saved: %s -> %s", thumbnail_file, thumbnail_s3_path
        )

        yield Output(
            value=(thumbnail_file, thumbnail_s3_path),
            data_version=DataVersion(data_version),
        )


@asset(
    code_version="video_shorts_thumbnail_large_v1",
    key=AssetKey(["video_shorts", "video_thumbnail_large"]),
    group_name="video_shorts",
    description="Generate large thumbnail (1080x1920) from video file.",
    partitions_def=video_short_ids,
    ins={
        "video_content": AssetIn(key=AssetKey(["video_shorts", "video_content"])),
        "video_metadata": AssetIn(key=AssetKey(["video_shorts", "video_metadata"])),
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
def video_short_thumbnail_large(
    context: AssetExecutionContext,
    video_content: UPath,
    video_metadata: UPath,
) -> Generator[Output[tuple[Path, str]], None, None]:
    """
    Generate large thumbnail (1080x1920) from video file.

    This asset depends on video_content and will automatically re-run
    when the video changes.
    """
    # Load metadata from the stored JSON file
    with video_metadata.open("r") as f:
        metadata = json.load(f)

    video_id = metadata["video_id"]
    data_version = metadata["data_version"]

    context.log.info("Generating large thumbnail for partition: %s", video_id)

    # Download video file temporarily to generate thumbnail
    with tempfile.TemporaryDirectory() as temp_dir:
        # Copy video from S3 to temp location without loading entire file in memory
        video_temp_path = Path(temp_dir) / "video.mp4"
        with video_content.open("rb") as src, video_temp_path.open("wb") as dst:
            shutil.copyfileobj(src, dst, 1024 * 1024)

        # Generate large thumbnail
        thumbnail_file = Path(temp_dir) / f"{video_id}_large.jpg"
        generate_video_thumbnail(
            video_path=video_temp_path,
            thumbnail_path=thumbnail_file,
            width=VIDEO_SHORT_THUMB_LARGE_WIDTH,
            height=VIDEO_SHORT_THUMB_LARGE_HEIGHT,
        )

        if not thumbnail_file.exists():
            msg = f"Failed to generate large thumbnail for: {video_id}"
            raise RuntimeError(msg)

        # S3 path: video_shorts/{video_id}/{video_id}_large.jpg
        thumbnail_s3_path = f"{video_id}/{video_id}_large.jpg"

        context.log.info(
            "Large thumbnail saved: %s -> %s", thumbnail_file, thumbnail_s3_path
        )

        yield Output(
            value=(thumbnail_file, thumbnail_s3_path),
            data_version=DataVersion(data_version),
        )


@asset(
    code_version="video_shorts_webhook_v3",
    key=AssetKey(["video_shorts", "video_webhook"]),
    group_name="video_shorts",
    description="Send webhook to Learn API after video processing.",
    partitions_def=video_short_ids,
    ins={
        "video_content": AssetIn(key=AssetKey(["video_shorts", "video_content"])),
        "video_thumbnail_small": AssetIn(
            key=AssetKey(["video_shorts", "video_thumbnail_small"])
        ),
        "video_thumbnail_large": AssetIn(
            key=AssetKey(["video_shorts", "video_thumbnail_large"])
        ),
        "video_metadata": AssetIn(key=AssetKey(["video_shorts", "video_metadata"])),
    },
    automation_condition=upstream_or_code_changes(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def video_short_webhook(  # noqa: PLR0913
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
    video_content: UPath,  # noqa: ARG001
    video_thumbnail_small: UPath,  # noqa: ARG001
    video_thumbnail_large: UPath,  # noqa: ARG001
    video_metadata: UPath,
) -> dict[str, Any]:
    """
    Send webhook notification to Learn API after video assets are ready.

    This asset depends on all video assets (content, both thumbnails, metadata)
    and only executes after they complete successfully.
    """
    video_id = context.partition_key

    # Read metadata content
    metadata_content = video_metadata.read_text()
    processed_metadata = json.loads(metadata_content)

    # Construct webhook payload
    webhook_data = {
        "video_id": video_id,
        "video_metadata": processed_metadata["video_metadata"],
        "source": "video_shorts",
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
