"""YouTube webhook delivery asset.

Reads the YouTube channels, playlists, playlist membership and videos from the
``integrations__learn__youtube_*`` Iceberg tables (produced by dbt from the
youtube dlt pipeline), nests each playlist's videos inside it, and delivers the
playlists to MIT Learn as signed webhook batches.

Data flow:
    raw__youtube__api__*  (Iceberg, via dlt)
        -> integrations__learn__youtube_channels         (dbt)
        -> integrations__learn__youtube_playlists        (dbt)
        -> integrations__learn__youtube_playlist_videos  (dbt)
        -> integrations__learn__youtube_videos           (dbt)
            -> MIT Learn webhook (this asset)

Each resource is what ``transform_playlist`` in mit-learn's
``learning_resources/etl/youtube.py`` returns, with its videos as
``transform_video`` returns them, plus three keys the webhook needs:
``readable_id`` and ``resource_type`` (its serializer requires both) and
``channel`` (``transform_channel``'s dict, which ``load_playlist`` takes as a
separate argument). A receiver gets ``load_playlist``'s input by popping those
three.

RECEIVER GAP: mit-learn #3557 routes course, program, document, video and
podcast. It logs and skips ``video_playlist``, so until MIT Learn adds that
route this asset changes nothing there. Delivering the videos alone as
``video`` resources is not a substitute: for a ``create_videos = false`` (OCW)
playlist MIT Learn does not create YouTube videos at all, it attaches the
YouTube data to the matching OCW ContentFile's video.

BATCHING: MIT Learn verifies the signature over ``request.body``, which Django
refuses above ``DATA_UPLOAD_MAX_MEMORY_SIZE`` (2.5 MB, and mit-learn does not
raise it). The catalog does not fit in one body, so playlists are sent in
batches under ``MAX_BATCH_BYTES``. A batch therefore says nothing about the
playlists it leaves out: a playlist or channel that is removed upstream stops
being delivered, and unpublishing it needs a signal this payload does not carry
yet.

A short read is still guarded. ``load_playlist`` unpublishes the videos of a
playlist that are absent from it, so an empty or half-written membership or
videos table would empty every delivered playlist. ``assert_deliverable``
refuses that.

Scheduling: once a day, after its integrations models have materialized since
06:00 UTC. See delivery.lib.scheduled_automation.
"""

import json
import re
from collections import defaultdict
from collections.abc import Iterator
from typing import Any, cast

import httpx2 as httpx
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MetadataValue,
    RetryPolicy,
    asset,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.resources.api_client_factory import ApiClientFactory
from ol_orchestrate.resources.learn_api import MITLearnApiClient

from delivery.lib.sanitize import clean_html

_GLUE_DB = (
    f"ol_warehouse_{DAGSTER_ENV}_integrations"
    if DAGSTER_ENV in ("qa", "production")
    else "ol_warehouse_production_integrations"
)
_CHANNELS_TABLE = "integrations__learn__youtube_channels"
_PLAYLISTS_TABLE = "integrations__learn__youtube_playlists"
_PLAYLIST_VIDEOS_TABLE = "integrations__learn__youtube_playlist_videos"
_VIDEOS_TABLE = "integrations__learn__youtube_videos"

# Floors for the short-read guard. The production raw tables on 2026-10-02 held
# 380 playlists and about 6,700 videos for the two channels MIT Learn reads, so
# these catch an empty or truncated table, not a channel dropped from the config.
MIN_PLAYLISTS = 10
MIN_PLAYLIST_VIDEOS = 100

# Under Django's default DATA_UPLOAD_MAX_MEMORY_SIZE of 2,621,440 bytes.
MAX_BATCH_BYTES = 2_000_000
_ENVELOPE_BYTES = len(b'{"resources":[]}')

# The four below are copied from learning_resources/etl/youtube.py in mit-learn.
# A drift here is a description difference between the two pipelines.
_KEYWORD_COLON_LINE_RE = re.compile(
    r"^(Instructor|Course|View the complete course|License|YouTube Playlist|"
    r"Chapters|Watch this video in Chinese|MIT Open Learning|Instructors|"
    r"Key moments|Speakers):",
    re.MULTILINE | re.IGNORECASE,
)
_TIMESTAMP_LINE_RE = re.compile(r"^\d+:\d+(?::\d+)?\s", re.MULTILINE)
_MIT_COURSE_TITLE_RE = re.compile(
    r"^MIT\s+[\dA-Za-z]+\.[\dA-Za-z]+\S*\s.+\d{4}\s*$", re.MULTILINE
)
_OCW_BOILERPLATE = (
    "We encourage constructive comments and discussion on OCW's YouTube and "
    "other social media channels. Personal attacks, hate speech, trolling, and "
    "inappropriate comments are not allowed and may be removed."
)


def clean_youtube_description(description: str | None) -> str:
    """Clean a raw YouTube description the way mit-learn's ``transform_video`` does.

    That is ``clean_youtube_description(clean_data(description))``: strip
    disallowed HTML, then drop the OCW comment-policy boilerplate and every
    line that is a keyword label, a chapter timestamp, an MIT course title or
    carries a URL.

    :param description: The description as YouTube returned it.
    :returns: The cleaned description, ``""`` when there is none.
    :rtype: str
    """
    sanitized = clean_html(description)
    if not sanitized:
        return ""

    lines = sanitized.replace(_OCW_BOILERPLATE, "").split("\n")
    kept = [
        line
        for line in lines
        if not _KEYWORD_COLON_LINE_RE.match(line)
        and not _TIMESTAMP_LINE_RE.match(line)
        and not _MIT_COURSE_TITLE_RE.match(line)
        and "https://" not in line
        and "http://" not in line
    ]
    return re.sub(r"(\s*\n){3,}", "\n\n", "\n".join(kept).strip())


def _offered_by(code: str | None) -> dict[str, str] | None:
    """Wrap an offered_by code.

    mit-learn's ``parse_offered_by`` returns None for a code outside its
    ``OfferedBy`` enum. That check is not repeated here: ``load_offered_by``
    looks the code up and stores None when there is no such offeror, which is
    the same outcome.
    """
    return {"code": code} if code else None


def _video_to_resource(
    row: dict[str, Any], offered_by: dict[str, str] | None
) -> dict[str, Any]:
    """Map a videos row to ``transform_video``'s dict."""
    return {
        "readable_id": row["readable_id"],
        "platform": row["platform"],
        "etl_source": row["etl_source"],
        "resource_type": row["resource_type"],
        "title": row["title"],
        "description": clean_youtube_description(row["description_raw"]),
        "image": {"url": row["image_url"]},
        "last_modified": row["last_modified"],
        "url": row["url"],
        "offered_by": offered_by,
        "published": True,
        "video": {"duration": row["duration"]},
        "availability": row["availability"],
        "youtube_id": row["youtube_id"],
    }


def _playlist_to_resource(
    row: dict[str, Any],
    channel: dict[str, Any],
    video_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Map a playlists row, its channel and its videos to the webhook resource.

    offered_by is the playlist's on the playlist and on each of its videos, as
    ``transform_playlist`` passes one code to both. A video in two playlists is
    sent in each, with that playlist's offered_by.
    """
    offered_by = _offered_by(row["offered_by"])
    return {
        "readable_id": row["readable_id"],
        "resource_type": row["resource_type"],
        "channel": {
            "channel_id": channel["channel_id"],
            "title": channel["title"],
            "published": True,
        },
        "playlist_id": row["readable_id"],
        "title": row["title"],
        "published": True,
        "platform": row["platform"],
        "etl_source": row["etl_source"],
        "offered_by": offered_by,
        "videos": [_video_to_resource(video, offered_by) for video in video_rows],
        "url": row["url"],
        "image": {"url": row["image_url"], "alt": row["image_alt"]},
        "availability": row["availability"],
        "create_videos": row["create_videos"],
    }


def build_playlist_resources(
    channels: list[dict[str, Any]],
    playlists: list[dict[str, Any]],
    playlist_videos: list[dict[str, Any]],
    videos: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Nest each playlist's videos inside it, in playlist order.

    :param channels: Rows of integrations__learn__youtube_channels.
    :param playlists: Rows of integrations__learn__youtube_playlists.
    :param playlist_videos: Rows of integrations__learn__youtube_playlist_videos.
    :param videos: Rows of integrations__learn__youtube_videos.
    :returns: One webhook resource per playlist, ordered by channel and playlist id.
    :rtype: list[dict[str, Any]]
    """
    channels_by_id = {channel["channel_id"]: channel for channel in channels}
    videos_by_id = {video["readable_id"]: video for video in videos}

    videos_by_playlist: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for membership in sorted(
        playlist_videos, key=lambda m: (m["playlist_readable_id"], m["position"])
    ):
        videos_by_playlist[membership["playlist_readable_id"]].append(
            videos_by_id[membership["video_readable_id"]]
        )

    return [
        _playlist_to_resource(
            playlist,
            channels_by_id[playlist["channel_id"]],
            videos_by_playlist.get(playlist["readable_id"], []),
        )
        for playlist in sorted(
            playlists, key=lambda p: (p["channel_id"], p["readable_id"])
        )
    ]


def assert_deliverable(playlist_count: int, playlist_video_count: int) -> None:
    """Refuse to deliver a batch too small to be a real read.

    The four tables materialize separately. Healthy playlists over an empty
    membership or videos table would deliver every playlist with no videos, and
    ``load_playlist`` unpublishes the videos a playlist no longer lists.

    :param playlist_count: Playlists read.
    :param playlist_video_count: Videos nested across those playlists.
    :raises RuntimeError: When either count is under its floor.
    """
    if playlist_count < MIN_PLAYLISTS:
        msg = (
            f"Refusing to deliver {playlist_count} YouTube playlists (floor is "
            f"{MIN_PLAYLISTS}): the read is too small to be the catalog."
        )
        raise RuntimeError(msg)

    if playlist_video_count < MIN_PLAYLIST_VIDEOS:
        msg = (
            f"Refusing to deliver {playlist_video_count} videos across "
            f"{playlist_count} YouTube playlists (floor is {MIN_PLAYLIST_VIDEOS}): "
            "MIT Learn unpublishes the videos a delivered playlist no longer lists."
        )
        raise RuntimeError(msg)


def batch_resources(
    resources: list[dict[str, Any]], max_bytes: int = MAX_BATCH_BYTES
) -> Iterator[list[dict[str, Any]]]:
    """Split resources into batches whose request body stays under *max_bytes*.

    Sizes are those of the body ``MITLearnApiClient`` signs and sends: compact
    JSON, UTF-8 encoded.

    :param resources: The webhook resources, in delivery order.
    :param max_bytes: Largest request body to produce.
    :returns: The batches, in order, none of them empty.
    :rtype: Iterator[list[dict[str, Any]]]
    :raises RuntimeError: When one resource alone is over *max_bytes*.
    """
    batch: list[dict[str, Any]] = []
    batch_bytes = _ENVELOPE_BYTES
    for resource in resources:
        resource_bytes = len(json.dumps(resource, separators=(",", ":")).encode())
        if _ENVELOPE_BYTES + resource_bytes > max_bytes:
            msg = (
                f"YouTube playlist {resource['readable_id']} is {resource_bytes} "
                f"bytes with its {len(resource['videos'])} videos, over the "
                f"{max_bytes} byte batch limit, so MIT Learn would reject it."
            )
            raise RuntimeError(msg)
        # one more byte for the comma that joins it to the previous resource
        if batch and batch_bytes + resource_bytes + 1 > max_bytes:
            yield batch
            batch, batch_bytes = [], _ENVELOPE_BYTES
        batch_bytes += resource_bytes + (1 if batch else 0)
        batch.append(resource)
    if batch:
        yield batch


@asset(
    key=AssetKey(["mit_learn_delivery", "youtube_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Read YouTube channels, playlists and videos from the "
        "integrations__learn__youtube_* Iceberg tables and POST the playlists, "
        "with their videos nested, as signed webhook batches to MIT Learn."
    ),
    deps=[
        AssetKey(["integrations", _CHANNELS_TABLE]),
        AssetKey(["integrations", _PLAYLISTS_TABLE]),
        AssetKey(["integrations", _PLAYLIST_VIDEOS_TABLE]),
        AssetKey(["integrations", _VIDEOS_TABLE]),
    ],
    retry_policy=RetryPolicy(max_retries=3, delay=5.0),
)
def youtube_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Deliver YouTube playlists (with nested videos) to MIT Learn via webhook."""
    context.log.info("Reading the YouTube models from Glue database %s", _GLUE_DB)
    channels, playlists, playlist_videos, videos = (
        list(
            get_dbt_model_as_dataframe(database_name=_GLUE_DB, table_name=table)
            .collect()
            .iter_rows(named=True)
        )
        for table in (
            _CHANNELS_TABLE,
            _PLAYLISTS_TABLE,
            _PLAYLIST_VIDEOS_TABLE,
            _VIDEOS_TABLE,
        )
    )
    context.log.info(
        "Loaded %d channels, %d playlists, %d playlist videos and %d videos",
        len(channels),
        len(playlists),
        len(playlist_videos),
        len(videos),
    )

    resources = build_playlist_resources(channels, playlists, playlist_videos, videos)
    playlist_video_count = sum(len(resource["videos"]) for resource in resources)
    assert_deliverable(len(resources), playlist_video_count)

    client = cast(MITLearnApiClient, learn_api.client)
    responses = []
    for number, batch in enumerate(batch_resources(resources), start=1):
        context.log.info(
            "Delivering batch %d (%d playlists) to MIT Learn webhook",
            number,
            len(batch),
        )
        try:
            responses.append(client.notify_learning_resources(batch))
        except httpx.HTTPStatusError as exc:
            msg = (
                f"YouTube webhook batch {number} failed with status "
                f"{exc.response.status_code} after {len(responses)} delivered "
                f"batches: {exc}"
            )
            context.log.exception(msg)
            raise RuntimeError(msg) from exc

    summary = {
        "resource_count": len(resources),
        "playlist_video_count": playlist_video_count,
        "batch_count": len(responses),
        "webhook_status": "success",
    }
    context.add_output_metadata({**summary, "responses": MetadataValue.json(responses)})
    return summary
