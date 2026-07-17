"""MIT Learn YouTube ingestion via dlt.

Fetches YouTube channel configuration from the mitodl/open-video-data GitHub
repository, then loads raw channel, playlist, video, and transcript data from
the YouTube Data API v3.

The Data API is called directly over HTTP with ``requests`` (API-key auth via
the ``key`` query param), not the ``google-api-python-client`` SDK, to stay
consistent with the other ol_dlt sources. Transcripts are not part of the Data
API, so they are retrieved separately with ``youtube-transcript-api``.

Data flow:
    GitHub (mitodl/open-video-data/youtube/*.yaml)
        -> channel_id + playlist configs per channel
        -> raw__youtube__api__channels     (one row per configured channel)
        -> raw__youtube__api__playlists    (one row per playlist)
        -> raw__youtube__api__videos       (one row per video)
        -> raw__youtube__api__transcripts  (one row per video with a transcript)

Run standalone:
    DLT_PROFILE=dev YOUTUBE_DEVELOPER_KEY=... python -m ol_dlt.sources.youtube
"""

import base64
import logging
from collections.abc import Generator, Iterable
from typing import Any

import dlt
import yaml
from dlt.sources.helpers import requests

from ol_dlt import config

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
YOUTUBE_MAX_RESULTS = 50
# Sentinel playlist id meaning "ingest every playlist on the channel".
WILDCARD_PLAYLIST_ID = "all"

_CONFIG_FILE_REPO_DEFAULT = "mitodl/open-video-data"
_CONFIG_FILE_FOLDER_DEFAULT = "youtube"


def _github_headers(token: str | None) -> dict[str, str]:
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _fetch_channel_configs(
    repo: str,
    folder: str,
    branch: str,
    token: str | None,
) -> list[dict[str, Any]]:
    """Fetch and parse all YouTube channel YAML config files from a GitHub repo.

    The mitodl/open-video-data ``youtube/`` folder holds a few YAML files
    (``channels.yaml``, ``shorts.yaml``) each containing a *list* of channel
    config dicts (``- channel_id: ...``). A single top-level dict is also
    accepted for robustness. Each channel dict must contain a ``channel_id``;
    its ``playlists`` key (a list of ``{id, ignore?}`` dicts or bare id
    strings) is optional and a channel with no ``playlists`` is treated as a
    wildcard (ingest all of the channel's playlists).
    """
    headers = _github_headers(token)
    listing_url = f"{GITHUB_API_BASE}/repos/{repo}/contents/{folder}?ref={branch}"
    resp = requests.get(listing_url, headers=headers, timeout=30)
    resp.raise_for_status()

    configs = []
    for file_meta in resp.json():
        if not file_meta["name"].endswith((".yaml", ".yml")):
            continue

        file_resp = requests.get(file_meta["url"], headers=headers, timeout=30)
        file_resp.raise_for_status()
        raw_content = base64.b64decode(file_resp.json()["content"]).decode("utf-8")

        try:
            parsed = yaml.safe_load(raw_content)
        except yaml.YAMLError:
            logger.exception("Failed to parse YAML config: %s", file_meta["name"])
            continue

        # A file is either a single channel dict or a list of channel dicts.
        entries = parsed if isinstance(parsed, list) else [parsed]
        for entry in entries:
            if not isinstance(entry, dict) or "channel_id" not in entry:
                logger.warning(
                    "Skipping youtube config entry without channel_id in %s",
                    file_meta["name"],
                )
                continue
            configs.append(entry)

    logger.info("Loaded %d youtube configs from %s/%s", len(configs), repo, folder)
    return configs


def _resolve_api_key(api_key: str | None) -> str:
    """Resolve the YouTube Data API key lazily, failing loudly if it is absent."""
    return config.require_secrets(
        YOUTUBE_DEVELOPER_KEY=config.resolve_secret(api_key, "YOUTUBE_DEVELOPER_KEY")
    )["YOUTUBE_DEVELOPER_KEY"]


def _yt_paged_items(
    endpoint: str,
    params: dict[str, Any],
    api_key: str,
) -> Generator[dict[str, Any]]:
    """Yield every ``items`` entry from a paginated Data API v3 endpoint.

    Follows ``nextPageToken`` until the API stops returning one.
    """
    page_params = {**params, "key": api_key, "maxResults": YOUTUBE_MAX_RESULTS}
    page_token: str | None = None
    while True:
        if page_token:
            page_params["pageToken"] = page_token
        resp = requests.get(
            f"{YOUTUBE_API_BASE}/{endpoint}", params=page_params, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        yield from data.get("items", [])
        page_token = data.get("nextPageToken")
        if not page_token:
            break


def _playlist_ids_for_config(
    channel_config: dict[str, Any], api_key: str
) -> Generator[str]:
    """Yield the playlist ids to ingest for a single channel config.

    A config with no ``playlists`` list, or one containing the ``"all"``
    wildcard, expands to every playlist on the channel. Playlists flagged
    ``ignore: true`` are skipped.
    """
    channel_id = channel_config["channel_id"]
    playlist_configs = channel_config.get("playlists") or []
    # Each entry is either {"id": ..., "ignore"?: ...} or a bare id string;
    # accept both so a config like `playlists: [all]` still triggers the
    # wildcard instead of silently ingesting nothing.
    configs_by_id: dict[str, dict[str, Any]] = {}
    for pc in playlist_configs:
        if isinstance(pc, str):
            configs_by_id[pc] = {"id": pc}
        elif isinstance(pc, dict) and "id" in pc:
            configs_by_id[pc["id"]] = pc

    if not playlist_configs or WILDCARD_PLAYLIST_ID in configs_by_id:
        for playlist in _yt_paged_items(
            "playlists", {"part": "id", "channelId": channel_id}, api_key
        ):
            playlist_id = playlist["id"]
            if configs_by_id.get(playlist_id, {}).get("ignore", False):
                continue
            yield playlist_id
        return

    for playlist_id, playlist_config in configs_by_id.items():
        if playlist_config.get("ignore", False):
            continue
        yield playlist_id


def _video_ids_for_playlist(playlist_id: str, api_key: str) -> Generator[str]:
    """Yield the video ids contained in a playlist."""
    for item in _yt_paged_items(
        "playlistItems", {"part": "contentDetails", "playlistId": playlist_id}, api_key
    ):
        video_id = item.get("contentDetails", {}).get("videoId")
        if video_id:
            yield video_id


def _batched(items: Iterable[str], size: int) -> Generator[list[str]]:
    """Yield successive lists of at most ``size`` items."""
    batch: list[str] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _channel_video_ids(configs: list[dict[str, Any]], api_key: str) -> Generator[str]:
    """Yield the deduplicated video ids across every configured channel."""
    seen: set[str] = set()
    for channel_config in configs:
        for playlist_id in _playlist_ids_for_config(channel_config, api_key):
            for video_id in _video_ids_for_playlist(playlist_id, api_key):
                if video_id not in seen:
                    seen.add(video_id)
                    yield video_id


@dlt.source(name="youtube")
def youtube_source(  # noqa: C901
    api_key: str | None = None,
    github_access_token: str | None = None,
    github_repo: str = _CONFIG_FILE_REPO_DEFAULT,
    github_folder: str = _CONFIG_FILE_FOLDER_DEFAULT,
    github_branch: str = "main",
) -> Generator[Any]:
    """Load MIT Learn YouTube data from the Data API v3 into four raw tables.

    Channel configs are read from YAML files in the mitodl/open-video-data
    GitHub repository (one file per channel). For each channel the source
    yields records into:

      raw__youtube__api__channels    - one record per configured channel
      raw__youtube__api__playlists   - one record per ingested playlist
      raw__youtube__api__videos      - one record per video across all playlists
      raw__youtube__api__transcripts - one record per video that has a transcript

    Credentials are resolved lazily at execution time (not at import) so the
    module loads cleanly when secrets are absent in local development.

    Args:
        api_key: YouTube Data API v3 key. Resolved from YOUTUBE_DEVELOPER_KEY
            if not provided.
        github_access_token: Optional GitHub token to raise the config-fetch
            rate limit; the public repo works unauthenticated.
        github_repo: GitHub repository containing the channel YAML configs.
        github_folder: Folder within the repo containing YAML config files.
        github_branch: Git branch to read configs from.
    """
    table_format = config.active_table_format()

    def _configs() -> list[dict[str, Any]]:
        return _fetch_channel_configs(
            repo=github_repo,
            folder=github_folder,
            branch=github_branch,
            token=github_access_token,
        )

    @dlt.resource(
        name="raw__youtube__api__channels",
        write_disposition="merge",
        primary_key="channel_id",
        table_format=table_format,
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def youtube_channels() -> Generator[dict[str, Any]]:
        """Yield one record per configured YouTube channel."""
        key = _resolve_api_key(api_key)
        for channel_config in _configs():
            channel_id = channel_config["channel_id"]
            items = list(
                _yt_paged_items(
                    "channels",
                    {"part": "snippet,contentDetails,statistics", "id": channel_id},
                    key,
                )
            )
            if not items:
                logger.warning("No channel data returned for channel_id=%s", channel_id)
                continue
            yield {
                "channel_id": channel_id,
                "offered_by": channel_config.get("offered_by"),
                "etl_source": "youtube",
                **items[0],
            }

    @dlt.resource(
        name="raw__youtube__api__playlists",
        write_disposition="merge",
        primary_key="playlist_id",
        table_format=table_format,
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def youtube_playlists() -> Generator[dict[str, Any]]:
        """Yield one record per ingested playlist across all channels."""
        key = _resolve_api_key(api_key)
        for channel_config in _configs():
            channel_id = channel_config["channel_id"]
            for playlist_id in _playlist_ids_for_config(channel_config, key):
                items = list(
                    _yt_paged_items(
                        "playlists",
                        {"part": "snippet,contentDetails", "id": playlist_id},
                        key,
                    )
                )
                if not items:
                    logger.warning("No playlist data for playlist_id=%s", playlist_id)
                    continue
                yield {
                    "playlist_id": playlist_id,
                    "channel_id": channel_id,
                    "etl_source": "youtube",
                    **items[0],
                }

    @dlt.resource(
        name="raw__youtube__api__videos",
        write_disposition="merge",
        primary_key="video_id",
        table_format=table_format,
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def youtube_videos() -> Generator[dict[str, Any]]:
        """Yield one record per video across all configured playlists."""
        key = _resolve_api_key(api_key)
        for batch in _batched(_channel_video_ids(_configs(), key), YOUTUBE_MAX_RESULTS):
            for video in _yt_paged_items(
                "videos",
                {"part": "snippet,contentDetails,statistics", "id": ",".join(batch)},
                key,
            ):
                yield {
                    "video_id": video["id"],
                    "etl_source": "youtube",
                    **video,
                }

    @dlt.resource(
        name="raw__youtube__api__transcripts",
        write_disposition="merge",
        primary_key="video_id",
        table_format=table_format,
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def youtube_transcripts() -> Generator[dict[str, Any]]:
        """Yield one record per video that has a transcript.

        Transcripts are fetched with ``youtube-transcript-api`` (imported lazily
        so the module loads without the dependency present). Videos with
        transcripts disabled or unavailable are skipped, not raised.
        """
        from youtube_transcript_api import (
            NoTranscriptFound,
            TranscriptsDisabled,
            VideoUnavailable,
            YouTubeTranscriptApi,
        )
        from youtube_transcript_api.formatters import TextFormatter

        key = _resolve_api_key(api_key)
        ytt_api = YouTubeTranscriptApi()
        formatter = TextFormatter()
        for video_id in _channel_video_ids(_configs(), key):
            try:
                fetched = ytt_api.fetch(video_id)
            except (NoTranscriptFound, TranscriptsDisabled, VideoUnavailable):
                logger.debug("No transcript available for video_id=%s", video_id)
                continue
            except Exception:
                logger.exception("Failed to fetch transcript for video_id=%s", video_id)
                continue
            yield {
                "video_id": video_id,
                "etl_source": "youtube",
                "transcript": formatter.format_transcript(fetched),
                "segments": fetched.to_raw_data(),
            }

    yield youtube_channels
    yield youtube_playlists
    yield youtube_videos
    yield youtube_transcripts


youtube_pipeline = config.pipeline_for("youtube")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source (uniform entrypoint for the Dagster wrapper)."""
    return youtube_source()
