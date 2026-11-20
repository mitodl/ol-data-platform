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
        -> raw__youtube__api__playlist_items (one row per playlist + video)
        -> raw__youtube__api__videos       (one row per video)
        -> raw__youtube__api__transcripts  (one row per transcript of a video, in
           every language, or a row saying why a video has none)

The API key is resolved lazily at run time. The qa and production profiles
read it from Vault (``YOUTUBE_VAULT_PATH``), where the dagster stack in
ol-infrastructure writes MIT Learn's key. Any other profile reads
YOUTUBE_DEVELOPER_KEY from the environment.

Run standalone:
    DLT_PROFILE=dev YOUTUBE_DEVELOPER_KEY=... python -m ol_dlt.sources.youtube
"""

import base64
import functools
import json
import logging
from collections.abc import Callable, Generator, Iterable, Iterator
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

import dlt
import yaml
from dlt.sources.helpers import requests

from ol_dlt import config, vault

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
YOUTUBE_MAX_RESULTS = 50
# Sentinel playlist id meaning "ingest every playlist on the channel".
WILDCARD_PLAYLIST_ID = "all"

# fetch_status of a raw__youtube__api__transcripts row. On a video's status row,
# fetched means every transcript it lists was fetched, and failed that one or
# more were not.
TRANSCRIPT_FETCHED = "fetched"
# The video lists no transcript, or its owner disabled them.
TRANSCRIPT_NONE = "none"
TRANSCRIPT_UNAVAILABLE = "unavailable"
# YouTube refused the request (IpBlocked, RequestBlocked).
TRANSCRIPT_BLOCKED = "blocked"
TRANSCRIPT_FAILED = "failed"


class TranscriptTracks(StrEnum):
    """Which of a video's transcript tracks the source fetches.

    YouTube lists auto-generated tracks in many languages for some videos (the
    audio YouTube dubbed into other languages), each a request of its own.
    """

    # Every uploaded track, and the auto-generated track in the video's own
    # language.
    NATIVE = "native"
    # Every track YouTube lists.
    ALL = "all"


# Legacy codes YouTube still uses beside their current ones.
_LANGUAGE_ALIASES = {"iw": "he", "in": "id", "ji": "yi", "tl": "fil"}


def _language(code: str | None) -> str:
    """Return the primary subtag of a language code: en-US -> en, iw -> he."""
    language = (code or "").split("-")[0].lower()
    return _LANGUAGE_ALIASES.get(language, language)


def tracks_to_fetch(
    transcripts: list[Any], tracks: TranscriptTracks, video_languages: set[str]
) -> list[Any]:
    """Pick the transcript tracks of one video to fetch.

    :param transcripts: The tracks youtube-transcript-api lists for the video.
    :param tracks: Which tracks to keep.
    :param video_languages: Primary subtags of the languages the Data API gives
        the video (defaultAudioLanguage, defaultLanguage); may be empty.
    :returns: The tracks to fetch, uploaded first. For ``NATIVE``, a video the
        API gives no language for takes its uploaded tracks' languages as its
        own. A video with no uploaded track and no auto-generated track in its
        language keeps every auto-generated track: its metadata language can
        be the channel's default rather than the one spoken.
    """
    uploaded = [track for track in transcripts if not track.is_generated]
    generated = [track for track in transcripts if track.is_generated]
    if tracks is TranscriptTracks.ALL:
        return uploaded + generated
    native = video_languages or {_language(t.language_code) for t in uploaded}
    in_native = [t for t in generated if _language(t.language_code) in native]
    if in_native or uploaded:
        generated = in_native
    return uploaded + generated


_CONFIG_FILE_REPO_DEFAULT = "mitodl/open-video-data"
_CONFIG_FILE_FOLDER_DEFAULT = "youtube"

YOUTUBE_VAULT_MOUNT = "secret-data"
YOUTUBE_VAULT_PATH = "pipelines/youtube"


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

        # A broken file fails the run rather than being skipped. Skipping it
        # would load a run without that file's channels, and the integrations
        # models read the newest load as the complete channel set, so MIT Learn
        # would unpublish them. Learn's own ETL loads nothing on a bad config.
        parsed = yaml.safe_load(raw_content)

        # A file is either a single channel dict or a list of channel dicts.
        entries = parsed if isinstance(parsed, list) else [parsed]
        for entry in entries:
            if not isinstance(entry, dict) or "channel_id" not in entry:
                msg = f"youtube config entry without channel_id in {file_meta['name']}"
                raise ValueError(msg)
            configs.append({**entry, "config_file": file_meta["name"]})

    logger.info("Loaded %d youtube configs from %s/%s", len(configs), repo, folder)
    return configs


def _resolve_api_key(api_key: str | None) -> str:
    """Resolve the YouTube Data API key lazily, failing loudly if it is absent.

    Deployed profiles take the key from Vault. Explicit arguments and the
    environment apply only to the other profiles.
    """
    if config.active_profile() in config.DEPLOYED_PROFILES:
        return vault.read_kv_secret(YOUTUBE_VAULT_MOUNT, YOUTUBE_VAULT_PATH)[
            "developer_key"
        ]
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


def _playlist_create_videos(channel_config: dict[str, Any], playlist_id: str) -> bool:
    """Return whether MIT Learn creates videos for a playlist or matches them.

    Mirrors learning_resources/etl/youtube.py:_extract_playlists. A playlist
    listed by id takes its own ``create_videos`` and falls back to the
    channel's; a playlist reached only through the ``all`` wildcard always
    takes the channel's. The channel setting defaults to true. When false
    (the OCW channel), Learn attaches the videos to existing OCW content files
    instead of creating YouTube video resources.
    """
    channel_setting = channel_config.get("create_videos", True)
    for playlist_config in channel_config.get("playlists") or []:
        if isinstance(playlist_config, dict) and playlist_config.get("id") == (
            playlist_id
        ):
            return playlist_config.get("create_videos", channel_setting)
    return channel_setting


def _video_ids_for_playlist(playlist_id: str, api_key: str) -> Generator[str]:
    """Yield the video ids contained in a playlist."""
    for item in _yt_paged_items(
        "playlistItems", {"part": "contentDetails", "playlistId": playlist_id}, api_key
    ):
        video_id = item.get("contentDetails", {}).get("videoId")
        if video_id:
            yield video_id


def _videos(
    video_ids: Iterable[str], part: str, api_key: str
) -> Generator[dict[str, Any]]:
    """Yield the Data API's record of each video, fetched in batches."""
    for batch in _batched(video_ids, YOUTUBE_MAX_RESULTS):
        yield from _yt_paged_items(
            "videos", {"part": part, "id": ",".join(batch)}, api_key
        )


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


def _refuse_empty(
    transform: Callable[[dict[str, Any]], Iterator[dict[str, Any]]],
) -> Callable[[dict[str, Any]], Generator[dict[str, Any]]]:
    """Fail a table resource that would load an empty snapshot.

    The tables are merge-loaded and the dbt integrations models read each one
    at its newest load id. A run that writes no rows to a table writes no load
    id either, so the previous snapshot would stay current. An empty YouTube
    response is far more likely an outage than a real state, so fail the run.
    """

    @functools.wraps(transform)
    def _wrapped(listing: dict[str, Any]) -> Generator[dict[str, Any]]:
        rows = 0
        for row in transform(listing):
            rows += 1
            yield row
        if not rows:
            msg = f"{transform.__name__} produced no rows; refusing an empty snapshot"
            raise ValueError(msg)

    return _wrapped


def _unique(items: Iterable[str]) -> Generator[str]:
    """Yield each item the first time it appears, keeping order."""
    seen: set[str] = set()
    for item in items:
        if item not in seen:
            seen.add(item)
            yield item


@dlt.source(name="youtube")
def youtube_source(  # noqa: C901
    api_key: str | None = None,
    github_access_token: str | None = None,
    github_repo: str = _CONFIG_FILE_REPO_DEFAULT,
    github_folder: str = _CONFIG_FILE_FOLDER_DEFAULT,
    github_branch: str = "main",
    transcript_tracks: str = TranscriptTracks.NATIVE.value,
) -> Generator[Any]:
    """Load MIT Learn YouTube data from the Data API v3 into five raw tables.

    Channel configs are read from YAML files in the mitodl/open-video-data
    GitHub repository (one file per channel). For each channel the source
    yields records into:

      raw__youtube__api__channels    - one record per configured channel
      raw__youtube__api__playlists   - one record per ingested playlist
      raw__youtube__api__playlist_items - one record per video in a playlist
      raw__youtube__api__videos      - one record per video across all playlists
      raw__youtube__api__transcripts - one record per transcript of a video, keyed
        (video_id, language_code, is_generated), or per video with none

    Credentials are resolved lazily at execution time (not at import) so the
    module loads cleanly when secrets are absent in local development.

    Args:
        api_key: YouTube Data API v3 key, for non-deployed profiles. Resolved
            from YOUTUBE_DEVELOPER_KEY if not provided. Ignored by the qa and
            production profiles, which read it from Vault.
        github_access_token: Optional GitHub token to raise the config-fetch
            rate limit; the public repo works unauthenticated.
        github_repo: GitHub repository containing the channel YAML configs.
        github_folder: Folder within the repo containing YAML config files.
        github_branch: Git branch to read configs from.
        transcript_tracks: Which transcript tracks to fetch, a TranscriptTracks
            value: ``native`` (uploaded tracks and the auto-generated track in
            the video's own language) or ``all``.
    """
    table_format = config.active_table_format()
    tracks = TranscriptTracks(transcript_tracks)

    # One resolution per source instance: in deployed profiles each one is a Vault
    # login and read, and the listing and three of the table resources need it.
    @functools.cache
    def _api_key() -> str:
        return _resolve_api_key(api_key)

    def _table(name: str, primary_key: str | tuple[str, ...]) -> dict[str, Any]:
        return {
            "name": name,
            "write_disposition": "merge",
            "primary_key": primary_key,
            "table_format": table_format,
            "schema_contract": config.JSON_API_SCHEMA_CONTRACT,
        }

    @dlt.resource(name="youtube_listing", selected=False)
    def youtube_listing() -> Generator[dict[str, Any]]:
        """Yield the channel configs and every playlist's video ids, once.

        Every table resource is a transformer of this one, and dlt evaluates a
        shared parent once per extraction. So a run reads the config and lists
        playlists and playlist items in a single pass against the API quota
        MIT Learn's key shares, and every table of the run sees the same
        snapshot. Nothing is kept between runs.
        """
        key = _api_key()
        configs = _fetch_channel_configs(
            repo=github_repo,
            folder=github_folder,
            branch=github_branch,
            token=github_access_token,
        )
        playlists = [
            {
                "channel_config": channel_config,
                "playlist_id": playlist_id,
                "video_ids": list(_video_ids_for_playlist(playlist_id, key)),
            }
            for channel_config in configs
            for playlist_id in _playlist_ids_for_config(channel_config, key)
        ]
        # See _refuse_empty: an empty listing would leave the previous
        # snapshot current downstream.
        if not any(playlist["video_ids"] for playlist in playlists):
            msg = "youtube listing found no playlist items; refusing an empty snapshot"
            raise ValueError(msg)
        yield {
            "configs": configs,
            "playlists": playlists,
        }

    def _video_ids(listing: dict[str, Any]) -> Generator[str]:
        return _unique(
            video_id
            for playlist in listing["playlists"]
            for video_id in playlist["video_ids"]
        )

    @dlt.transformer(
        data_from=youtube_listing,
        **_table("raw__youtube__api__channels", "channel_id"),
    )
    @_refuse_empty
    def youtube_channels(listing: dict[str, Any]) -> Generator[dict[str, Any]]:
        """Yield one record per configured YouTube channel."""
        key = _api_key()
        for channel_config in listing["configs"]:
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
                # MIT Learn's YouTube ETL reads one file (YOUTUBE_CONFIG_URL),
                # so the integrations models select channels by it.
                "config_file": channel_config["config_file"],
                "etl_source": "youtube",
                **items[0],
            }

    @dlt.transformer(
        data_from=youtube_listing,
        **_table("raw__youtube__api__playlists", "playlist_id"),
    )
    @_refuse_empty
    def youtube_playlists(listing: dict[str, Any]) -> Generator[dict[str, Any]]:
        """Yield one record per ingested playlist across all channels."""
        key = _api_key()
        for playlist in listing["playlists"]:
            channel_config = playlist["channel_config"]
            playlist_id = playlist["playlist_id"]
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
                "channel_id": channel_config["channel_id"],
                "offered_by": channel_config.get("offered_by"),
                "create_videos": _playlist_create_videos(channel_config, playlist_id),
                "etl_source": "youtube",
                **items[0],
            }

    @dlt.transformer(
        data_from=youtube_listing,
        **_table("raw__youtube__api__playlist_items", ("playlist_id", "video_id")),
    )
    def youtube_playlist_items(listing: dict[str, Any]) -> Generator[dict[str, Any]]:
        """Yield one record per video in each ingested playlist, in playlist order.

        ``position`` is the video's index in the playlist listing, counting
        entries whose video the videos endpoint may not return (private or
        deleted). A video listed twice keeps its first position.
        """
        for playlist in listing["playlists"]:
            seen: set[str] = set()
            for position, video_id in enumerate(playlist["video_ids"]):
                if video_id in seen:
                    continue
                seen.add(video_id)
                yield {
                    "playlist_id": playlist["playlist_id"],
                    "video_id": video_id,
                    "position": position,
                    "etl_source": "youtube",
                }

    @dlt.transformer(
        data_from=youtube_listing,
        **_table("raw__youtube__api__videos", "video_id"),
    )
    @_refuse_empty
    def youtube_videos(listing: dict[str, Any]) -> Generator[dict[str, Any]]:
        """Yield one record per video across all configured playlists."""
        for video in _videos(
            _video_ids(listing), "snippet,contentDetails,statistics", _api_key()
        ):
            yield {
                "video_id": video["id"],
                "etl_source": "youtube",
                **video,
            }

    @dlt.transformer(
        data_from=youtube_listing,
        **_table(
            "raw__youtube__api__transcripts",
            ("video_id", "language_code", "is_generated"),
        ),
        # Nullable so dlt can add them to a table created under the old key:
        # Iceberg cannot add a required column to an existing table.
        columns={
            "language_code": {"data_type": "text", "nullable": True},
            "is_generated": {"data_type": "bool", "nullable": True},
        },
    )
    def youtube_transcripts(listing: dict[str, Any]) -> Generator[dict[str, Any]]:
        """Yield every transcript of every video, and how fetching went.

        Transcripts are fetched with ``youtube-transcript-api`` (imported lazily
        so the module loads without the dependency present). Each run writes:

        - one row per transcript fetched, keyed by its ``language_code`` and
          ``is_generated``: the tracks ``transcript_tracks`` picks (by default
          every uploaded track and the auto-generated track in the video's own
          language). Which one a consumer reads, e.g. MIT Learn's English one,
          is its choice. A row stays until the same transcript is fetched
          again, so a failed or blocked run keeps the last good text.
        - one status row per video, with an empty ``language_code``: the
          outcome of this run's attempt (``fetch_status``), in ``error`` the
          exception of each failure, prefixed by its language when one
          transcript failed, and in ``listed_tracks`` every track YouTube
          lists for the video, fetched or not.

        Once YouTube blocks the run's IP, every later request fails the same
        way, so the rest of the videos are marked blocked without a request.
        """
        from youtube_transcript_api import (
            RequestBlocked,
            TranscriptsDisabled,
            VideoUnavailable,
            YouTubeTranscriptApi,
        )
        from youtube_transcript_api.formatters import TextFormatter

        ytt_api = YouTubeTranscriptApi()
        formatter = TextFormatter()
        fetched_at = datetime.now(tz=UTC)
        blocked = False

        def row(video_id: str, **fields: Any) -> dict[str, Any]:
            return {
                "video_id": video_id,
                "etl_source": "youtube",
                "language_code": "",
                "language": None,
                "is_generated": False,
                "fetch_status": None,
                "error": None,
                "transcript": None,
                "segments": None,
                "listed_tracks": None,
                "fetched_at": fetched_at,
                **fields,
            }

        def describe(exc: Exception) -> str:
            return f"{type(exc).__name__}: {exc}".strip().removesuffix(":")[:500]

        video_ids = list(_video_ids(listing))
        # The language each video is spoken in, for picking its native track:
        # one snippet request per 50 videos.
        video_languages: dict[str, set[str]] = {}
        if tracks is TranscriptTracks.NATIVE:
            for video in _videos(video_ids, "snippet", _api_key()):
                snippet = video.get("snippet") or {}
                language = _language(
                    snippet.get("defaultAudioLanguage")
                    or snippet.get("defaultLanguage")
                )
                video_languages[video["id"]] = {language} if language else set()

        for video_id in video_ids:
            if blocked:
                yield row(video_id, fetch_status=TRANSCRIPT_BLOCKED)
                continue
            try:
                transcripts = list(ytt_api.list(video_id))
            except TranscriptsDisabled:
                yield row(video_id, fetch_status=TRANSCRIPT_NONE)
                continue
            except VideoUnavailable as exc:
                yield row(
                    video_id, fetch_status=TRANSCRIPT_UNAVAILABLE, error=describe(exc)
                )
                continue
            except RequestBlocked as exc:
                logger.warning("YouTube blocks transcript requests: %s", type(exc))
                blocked = True
                yield row(
                    video_id, fetch_status=TRANSCRIPT_BLOCKED, error=describe(exc)
                )
                continue
            except Exception as exc:
                logger.exception("Failed to list transcripts for video_id=%s", video_id)
                yield row(video_id, fetch_status=TRANSCRIPT_FAILED, error=describe(exc))
                continue

            errors = []
            fetched_any = False
            for transcript in tracks_to_fetch(
                transcripts, tracks, video_languages.get(video_id, set())
            ):
                try:
                    fetched = transcript.fetch()
                except RequestBlocked as exc:
                    logger.warning("YouTube blocks transcript requests: %s", type(exc))
                    blocked = True
                    errors.append(f"{transcript.language_code}: {describe(exc)}")
                    break
                except Exception as exc:
                    logger.exception(
                        "Failed to fetch the %s transcript of video_id=%s",
                        transcript.language_code,
                        video_id,
                    )
                    errors.append(f"{transcript.language_code}: {describe(exc)}")
                    continue
                yield row(
                    video_id,
                    language_code=transcript.language_code,
                    language=transcript.language,
                    is_generated=transcript.is_generated,
                    fetch_status=TRANSCRIPT_FETCHED,
                    transcript=formatter.format_transcript(fetched),
                    segments=fetched.to_raw_data(),
                )
                fetched_any = True
            if blocked:
                status = TRANSCRIPT_BLOCKED
            elif errors:
                status = TRANSCRIPT_FAILED
            else:
                status = TRANSCRIPT_FETCHED if fetched_any else TRANSCRIPT_NONE
            yield row(
                video_id,
                fetch_status=status,
                error="; ".join(errors) or None,
                # Every track YouTube lists, fetched or not, so a later choice
                # to fetch more knows what there is.
                listed_tracks=json.dumps(
                    [
                        {
                            "language_code": t.language_code,
                            "is_generated": t.is_generated,
                        }
                        for t in transcripts
                    ]
                ),
            )

    yield youtube_listing
    yield youtube_channels
    yield youtube_playlists
    yield youtube_playlist_items
    yield youtube_videos
    yield youtube_transcripts


youtube_pipeline = config.pipeline_for("youtube")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source (uniform entrypoint for the Dagster wrapper)."""
    return config.with_nullable_load_id(youtube_source())
