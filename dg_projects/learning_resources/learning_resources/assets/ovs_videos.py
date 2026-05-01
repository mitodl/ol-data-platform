"""Assets for forwarding OVS include_in_learn videos to MIT Learn."""

import hashlib
import json
import os
from collections.abc import Generator
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

OVS_PUBLIC_VIDEOS_URL_DEFAULT = (
    "https://video.odl.mit.edu/api/v0/public/videos/?include_in_learn=true"
)
OVS_REQUEST_TIMEOUT_SECONDS = 30.0

ovs_video_ids = DynamicPartitionsDefinition(name="ovs_video_ids")

video_api_key = AssetKey(["ovs_videos", "video_api"])
video_metadata_key = AssetKey(["ovs_videos", "video_metadata"])


def _partition_key_for(video: dict[str, Any]) -> str:
    # OVS exposes a stable 32-char hex `key` per video; use it directly.
    return video["key"]


def _video_content_hash(video: dict[str, Any]) -> str:
    content = json.dumps(video, sort_keys=True, default=str)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _fetch_all_pages(
    context: AssetExecutionContext, start_url: str
) -> list[dict[str, Any]]:
    videos: list[dict[str, Any]] = []
    url: str | None = start_url
    with httpx.Client(
        follow_redirects=True, timeout=OVS_REQUEST_TIMEOUT_SECONDS
    ) as client:
        while url:
            context.log.info("Fetching OVS page: %s", url)
            response = client.get(url)
            response.raise_for_status()
            payload = response.json()
            videos.extend(payload.get("results", []))
            url = payload.get("next")
    return videos


@asset(
    key=video_api_key,
    group_name="ovs_videos",
    description=(
        "Fetch include_in_learn videos from the OVS public videos API "
        "(paginated) and emit partition keys for downstream per-video work."
    ),
    code_version="ovs_videos_api_v1",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def video_api(
    context: AssetExecutionContext,
) -> Generator[Output[dict[str, Any]], None, None]:
    """Fetch include_in_learn videos from OVS and surface partition keys."""
    url = os.environ.get("OVS_PUBLIC_VIDEOS_URL") or OVS_PUBLIC_VIDEOS_URL_DEFAULT
    all_videos = _fetch_all_pages(context, url)
    context.log.info("Fetched %d videos from OVS", len(all_videos))

    if not all_videos:
        msg = (
            f"OVS API returned zero videos from {url}; aborting to protect "
            "stale-cleanup deletions."
        )
        raise RuntimeError(msg)

    videos_by_key = {_partition_key_for(v): v for v in all_videos}
    partition_keys = list(videos_by_key)
    video_content_hashes = {
        key: _video_content_hash(v) for key, v in videos_by_key.items()
    }

    api_content = json.dumps(all_videos, sort_keys=True, default=str)
    data_version = hashlib.sha256(api_content.encode("utf-8")).hexdigest()

    result = {
        "videos_by_key": videos_by_key,
        "partition_keys": partition_keys,
    }

    yield Output(
        value=result,
        data_version=DataVersion(data_version),
        metadata={
            "total_video_count": len(all_videos),
            "partition_keys": MetadataValue.json(partition_keys),
            "processing_partition_keys": MetadataValue.json(partition_keys),
            "api_partition_keys": MetadataValue.json(partition_keys),
            "video_content_hashes": MetadataValue.json(video_content_hashes),
            "data_version": data_version,
            "source_url": url,
        },
    )


@asset(
    code_version="ovs_videos_metadata_v1",
    key=video_metadata_key,
    group_name="ovs_videos",
    description="Per-video OVS JSON metadata, partitioned by OVS video key.",
    partitions_def=ovs_video_ids,
    ins={"api_data": AssetIn(key=video_api_key)},
    automation_condition=upstream_or_code_changes() | AutomationCondition.on_missing(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def video_metadata(
    context: AssetExecutionContext,
    api_data: dict[str, Any],
) -> Generator[Output[dict[str, Any]], None, None]:
    """Extract this partition's video JSON from the upstream OVS payload."""
    video_id = context.partition_key
    video = api_data["videos_by_key"].get(video_id)
    if video is None:
        msg = f"Partition {video_id} not present in upstream OVS data."
        raise ValueError(msg)
    yield Output(value=video, data_version=DataVersion(_video_content_hash(video)))


@asset(
    code_version="ovs_videos_webhook_v1",
    key=AssetKey(["ovs_videos", "video_webhook"]),
    group_name="ovs_videos",
    description="POST OVS video metadata to MIT Learn.",
    partitions_def=ovs_video_ids,
    ins={"video_metadata": AssetIn(key=video_metadata_key)},
    automation_condition=upstream_or_code_changes(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def video_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
    video_metadata: dict[str, Any],
) -> dict[str, Any]:
    """Send the OVS video JSON to MIT Learn as an HMAC-signed webhook."""
    video_id = context.partition_key
    context.log.info(
        "OVS webhook payload for %s:\n%s",
        video_id,
        json.dumps(video_metadata, indent=2),
    )
    try:
        response_data = learn_api.client.notify_ovs_video(video_metadata)
        context.log.info(
            "OVS webhook sent for video_id=%s, response=%s",
            video_id,
            response_data,
        )
    except httpx.HTTPStatusError as error:
        error_message = (
            f"OVS webhook failed for video_id={video_id} "
            f"with status code {error.response.status_code}: {error}"
        )
        context.log.exception(error_message)
        raise RuntimeError(error_message) from error
    else:
        context.add_output_metadata(
            {
                "video_id": video_id,
                "title": video_metadata.get("title", ""),
                "webhook_status": "success",
            }
        )
        return {
            "video_id": video_id,
            "webhook_status": "success",
            "response_data": response_data,
        }


@asset(
    code_version="ovs_videos_delete_webhook_v1",
    key=AssetKey(["ovs_videos", "video_delete_webhook"]),
    group_name="ovs_videos",
    description="Send delete webhook to MIT Learn for stale OVS video partitions.",
    partitions_def=ovs_video_ids,
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def video_delete_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Send a delete webhook for a stale OVS video partition."""
    video_id = context.partition_key
    payload = {"video_id": video_id, "delete": True}
    context.log.info(
        "OVS delete webhook payload for %s:\n%s",
        video_id,
        json.dumps(payload, indent=2),
    )
    try:
        response_data = learn_api.client.notify_ovs_video(payload)
        context.log.info("OVS delete webhook sent for partition: %s", video_id)
    except httpx.HTTPStatusError as error:
        error_message = (
            f"OVS delete webhook failed for video_id={video_id} "
            f"with status code {error.response.status_code}: {error}"
        )
        context.log.exception(error_message)
        raise RuntimeError(error_message) from error
    else:
        context.add_output_metadata({"video_id": video_id, "webhook_status": "success"})
        return {
            "video_id": video_id,
            "webhook_status": "success",
            "response_data": response_data,
        }
