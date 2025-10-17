"""
Sensor for monitoring YouTube channels for new video shorts.

This sensor regularly checks configured YouTube channels/playlists for new video
content and triggers processing jobs for newly discovered videos.
"""

import json
import os
import time
from typing import Any

from dagster import (
    AddDynamicPartitionsRequest,
    AssetKey,
    AssetRecordsFilter,
    DagsterRunStatus,
    DefaultSensorStatus,
    DeleteDynamicPartitionsRequest,
    RunRequest,
    SensorResult,
    SkipReason,
    sensor,
)


def fetch_youtube_shorts_api_data(
    youtube_client, youtube_config_provider
) -> list[dict[str, Any]]:
    """
    Fetch YouTube video metadata from configured channels/playlists.

    Returns a list of video metadata for the 24 most recent videos across all playlists,
    sorted by upload date descending.
    """
    client = youtube_client.client

    # Get configuration - this could be from a config file or asset
    config_data = youtube_config_provider.get_config()

    all_video_data = []

    for config_item in config_data:
        # Handle the actual config structure: { "playlists": [{"id": "..."}] }
        playlists = config_item.get("playlists", [])

        for playlist_config in playlists:
            playlist_id = playlist_config.get("id")
            if not playlist_id:
                continue

            # Fetch ALL videos from playlist using pagination
            playlist_items = []
            next_page_token = None
            has_more_pages = True

            while has_more_pages:
                # Fetch one page of results
                request = client.playlistItems().list(
                    part="contentDetails,snippet",  # Need snippet for publishedAt
                    playlistId=playlist_id,
                    maxResults=50,  # Max per request
                    pageToken=next_page_token,
                )
                response = request.execute()

                # Add items from this page
                playlist_items.extend(response.get("items", []))

                # Check if there are more pages
                next_page_token = response.get("nextPageToken")
                has_more_pages = next_page_token is not None

            all_video_data.extend(playlist_items)

    # Sort by published date descending (most recent first)
    # Using publishedAt from snippet which is the upload date
    all_video_data.sort(key=lambda item: item["snippet"]["publishedAt"], reverse=True)

    # Take only the first 24 most recent videos across all playlists
    return all_video_data[:24]


def _cleanup_s3_objects_for_videos(
    context, removed_video_ids: set[str], s3_resource
) -> int:
    """
    Clean up S3 objects for specific removed video IDs.

    Args:
        context: Sensor context for logging
        removed_video_ids: Set of video IDs that were removed
        s3_resource: S3 resource for object operations

    Returns:
        Number of S3 objects successfully deleted
    """
    s3_client = s3_resource.get_client()

    # Get S3 configuration from environment (same as assets)
    bucket = os.getenv("LR_VIDEO_SHORTS_BUCKET", "ol-devops-sandbox")
    prefix = "youtube_shorts"  # Default prefix

    objects_to_delete = []

    # For each removed video ID, add its associated S3 objects
    for video_id in removed_video_ids:
        # Video file
        video_key = f"{prefix}/videos/{video_id}.mp4"
        objects_to_delete.append({"Key": video_key})

        # Thumbnail file
        thumbnail_key = f"{prefix}/thumbnails/{video_id}.jpg"
        objects_to_delete.append({"Key": thumbnail_key})

        # Metadata file (if it exists)
        metadata_key = f"{prefix}/metadata/{video_id}.json"
        objects_to_delete.append({"Key": metadata_key})

    context.log.info(
        "Attempting to delete %d S3 objects for %d removed videos",
        len(objects_to_delete),
        len(removed_video_ids),
    )

    # Delete objects in batch
    deleted_count = 0
    if objects_to_delete:
        try:
            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={
                    "Objects": objects_to_delete,
                    # Don't return successfully deleted objects (reduces response size)
                    "Quiet": True,
                },
            )

            # Count successful deletions (total minus errors)
            deleted_count = len(objects_to_delete) - len(response.get("Errors", []))

            # Log any errors
            for error in response.get("Errors", []):
                # Don't log NoSuchKey errors as warnings since files may not exist
                if error["Code"] == "NoSuchKey":
                    context.log.debug(
                        "S3 object not found (already deleted): %s", error["Key"]
                    )
                else:
                    context.log.warning(
                        "Failed to delete %s: %s - %s",
                        error["Key"],
                        error["Code"],
                        error["Message"],
                    )

            context.log.info(
                "Successfully deleted %d S3 objects for removed videos", deleted_count
            )

        except Exception:
            context.log.exception("Error during S3 cleanup for removed videos")

    return deleted_count


@sensor(
    description=(
        "Sensor to monitor YouTube channels for new video shorts and cleanup "
        "old S3 objects"
    ),
    minimum_interval_seconds=60,  # Check every 1 minute for testing
    default_status=DefaultSensorStatus.RUNNING,  # Start automatically
    required_resource_keys={
        "youtube_client",
        "youtube_config_provider",
        "s3",
        "learn_api",
    },
    asset_selection=[
        AssetKey(["youtube_shorts", "video_metadata"]),
        AssetKey(["youtube_shorts", "video_content"]),
        AssetKey(["youtube_shorts", "video_thumbnail"]),
    ],
)
def youtube_shorts_sensor(context):  # noqa: C901, PLR0912, PLR0915
    """
    Monitor YouTube channels for new videos, trigger processing, and cleanup
    old S3 objects.

    This sensor:
    1. Fetches current video IDs from configured YouTube channels/playlists
    2. Compares with existing processed videos (dynamic partitions)
    3. Triggers processing jobs for new videos
    4. Updates dynamic partitions to track processed videos
    5. Cleans up S3 objects for videos no longer in current set
    """
    try:
        # Access resources through context
        youtube_client = context.resources.youtube_client
        youtube_config_provider = context.resources.youtube_config_provider
        s3 = context.resources.s3
        learn_api = context.resources.learn_api

        # Fetch video metadata for the 24 most recent videos
        current_videos_metadata = fetch_youtube_shorts_api_data(
            youtube_client, youtube_config_provider
        )

        # Extract video IDs from metadata and create a lookup dict
        current_video_ids = {
            item["contentDetails"]["videoId"] for item in current_videos_metadata
        }

        # Create a metadata lookup dictionary for passing to assets
        metadata_by_video_id = {
            item["contentDetails"]["videoId"]: item for item in current_videos_metadata
        }

        # Debug: Check the size of metadata being passed
        if metadata_by_video_id:
            sample_video_id = next(iter(metadata_by_video_id.keys()))
            sample_metadata = metadata_by_video_id[sample_video_id]
            sample_json = json.dumps(sample_metadata)
            context.log.info(
                "Sample video metadata size: %d bytes, first 500 chars: %s",
                len(sample_json),
                sample_json[:500],
            )
            total_metadata_json = json.dumps(metadata_by_video_id)
            context.log.info(
                "Total metadata size for all %d videos: %d bytes (%.2f MB)",
                len(metadata_by_video_id),
                len(total_metadata_json),
                len(total_metadata_json) / (1024 * 1024),
            )

        context.log.info("Found %d videos in YouTube channels", len(current_video_ids))

        # Get existing dynamic partitions
        existing_partitions = set(
            context.instance.get_dynamic_partitions("youtube_video_ids")
        )
        context.log.info("Existing partitions: %d", len(existing_partitions))

        # Check which videos have been successfully materialized
        # All three assets must be successfully materialized for a video
        # to be considered complete
        required_assets = [
            AssetKey(["youtube_shorts", "video_metadata"]),
            AssetKey(["youtube_shorts", "video_content"]),
            AssetKey(["youtube_shorts", "video_thumbnail"]),
        ]
        successfully_processed = set()

        for partition_key in existing_partitions:
            # Check if ALL required assets have successful materializations
            all_assets_materialized = True

            for asset_key in required_assets:
                # Get materialization records for this asset and partition
                materialization_records = context.instance.fetch_materializations(
                    records_filter=AssetRecordsFilter(
                        asset_key=asset_key,
                        asset_partitions=[partition_key],
                    ),
                    limit=1,
                )

                if not materialization_records.records:
                    context.log.info(
                        "Video %s: Asset %s has NO materialization records",
                        partition_key,
                        asset_key,
                    )
                    all_assets_materialized = False
                    break

                # CRITICAL FIX: Check if the run that created this
                # materialization was successful. A materialization record
                # can exist even if the run failed.
                materialization = materialization_records.records[0]
                run_id = materialization.event_log_entry.run_id

                # Get the run record to check its status
                run_record = context.instance.get_run_record_by_id(run_id)

                if not run_record:
                    context.log.warning(
                        "Video %s: Asset %s run record NOT FOUND for run %s",
                        partition_key,
                        asset_key,
                        run_id,
                    )
                    all_assets_materialized = False
                    break

                run_status = run_record.dagster_run.status

                if run_status not in [DagsterRunStatus.SUCCESS]:
                    # Run failed, is still running, or was canceled
                    # Don't count as successful
                    context.log.info(
                        "Video %s: Asset %s has materialization but "
                        "run %s status is %s (not SUCCESS)",
                        partition_key,
                        asset_key,
                        run_id,
                        run_status,
                    )
                    all_assets_materialized = False
                    break

            if all_assets_materialized:
                successfully_processed.add(partition_key)

        context.log.info(
            "Successfully processed videos (all 3 assets materialized "
            "with successful runs): %d",
            len(successfully_processed),
        )

        # Log some sample video IDs for debugging
        if successfully_processed:
            sample_processed = list(successfully_processed)[:5]
            context.log.info(
                "Sample successfully processed videos: %s", sample_processed
            )

        # Determine videos that need processing (new or failed)
        videos_to_process = current_video_ids - successfully_processed
        removed_video_ids = successfully_processed - current_video_ids

        # Log which specific videos need processing
        if videos_to_process:
            sample_to_process = list(videos_to_process)[:10]
            context.log.info("Sample videos needing processing: %s", sample_to_process)

        if not videos_to_process and not removed_video_ids:
            return SkipReason(
                "No videos need processing and none were removed from YouTube"
            )

        context.log.info(
            "Videos needing processing (new or failed): %d", len(videos_to_process)
        )
        if removed_video_ids:
            context.log.info(
                "Videos no longer in YouTube channels: %d", len(removed_video_ids)
            )

        # Create run requests for videos that need processing (new or failed)
        run_requests = []
        total_config_size = 0
        for video_id in videos_to_process:
            metadata_json = (
                json.dumps(metadata_by_video_id.get(video_id))
                if video_id in metadata_by_video_id
                else None
            )
            if metadata_json:
                total_config_size += len(metadata_json)

            run_requests.append(
                RunRequest(
                    asset_selection=[
                        AssetKey(["youtube_shorts", "video_metadata"]),
                        AssetKey(["youtube_shorts", "video_content"]),
                        AssetKey(["youtube_shorts", "video_thumbnail"]),
                    ],
                    partition_key=video_id,
                    tags={"video_id": video_id},
                    run_config={
                        "ops": {
                            "youtube_shorts__video_metadata": {
                                "config": {"cached_metadata": metadata_json}
                            }
                        }
                    },
                )
            )

        context.log.info(
            "Total run_config metadata size for %d videos: %d bytes (%.2f MB)",
            len(videos_to_process),
            total_config_size,
            total_config_size / (1024 * 1024),
        )

        # Dynamic partition requests
        dynamic_requests: list[
            AddDynamicPartitionsRequest | DeleteDynamicPartitionsRequest
        ] = []

        # Add partitions for truly new videos (not already in partitions)
        videos_to_add = current_video_ids - existing_partitions
        if videos_to_add:
            dynamic_requests.append(
                AddDynamicPartitionsRequest(
                    partitions_def_name="youtube_video_ids",
                    partition_keys=list(videos_to_add),
                )
            )

        # Only delete partitions for videos no longer in YouTube channels
        videos_to_remove = existing_partitions - current_video_ids
        if videos_to_remove:
            # Send DELETE webhooks for removed videos before deleting partitions
            for video_id in videos_to_remove:
                try:
                    webhook_response = learn_api.notify_shorts_deleted(video_id)
                    context.log.info(
                        "Successfully sent deletion webhook for video %s: %s",
                        video_id,
                        webhook_response,
                    )
                except Exception as webhook_error:  # noqa: BLE001
                    context.log.warning(
                        "Failed to send deletion webhook for video %s: %s",
                        video_id,
                        webhook_error,
                    )
                    # Continue with deletion even if webhook fails

            dynamic_requests.append(
                DeleteDynamicPartitionsRequest(
                    partitions_def_name="youtube_video_ids",
                    partition_keys=list(videos_to_remove),
                )
            )

        # Clean up S3 objects for removed videos (only if there are videos to remove)
        cleaned_objects = 0
        if removed_video_ids:
            cleaned_objects = _cleanup_s3_objects_for_videos(
                context, removed_video_ids, s3
            )

        # Update cursor with current state
        cursor_data = {
            "video_count": len(current_video_ids),
            "timestamp": str(time.time()),  # Current timestamp, not previous cursor
            "successfully_processed_count": len(successfully_processed),
            "videos_to_process": len(videos_to_process),
            "removed_videos": len(removed_video_ids),
            "s3_objects_cleaned": cleaned_objects,
        }

        cursor_json = json.dumps(cursor_data)
        context.log.info(
            "Cursor size: %d bytes, run_requests: %d",
            len(cursor_json),
            len(run_requests),
        )

        return SensorResult(
            dynamic_partitions_requests=dynamic_requests,
            run_requests=run_requests,
            cursor=cursor_json,
        )

    except Exception:
        context.log.exception("Error in YouTube shorts sensor")
        return SkipReason("Sensor failed due to error - check logs")
