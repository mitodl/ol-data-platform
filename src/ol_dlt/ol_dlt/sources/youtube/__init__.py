"""MIT Learn YouTube ingestion via dlt.

Fetches YouTube channel configuration from the mitodl/open-video-data GitHub
repository, then loads raw channel, playlist, video, and transcript data from
the YouTube Data API v3.

The Data API is called directly over HTTP with ``requests``, not the
``google-api-python-client`` SDK, to stay consistent with the other ol_dlt
sources. Channels, playlists and videos are public data, read with an API key
(the ``key`` query param). Transcripts are caption tracks, which the Data API
lists and downloads only for an OAuth user who can edit the video
(captions.list, captions.download). A refresh token authorizes as the one
channel picked at consent, so captions are read with one OAuth client and a
refresh token per channel, and each video's captions with its own channel's
token.

Data flow:
    GitHub (mitodl/open-video-data/youtube/*.yaml)
        -> channel_id + playlist configs per channel
        -> raw__youtube__api__channels     (one row per configured channel)
        -> raw__youtube__api__playlists    (one row per playlist)
        -> raw__youtube__api__playlist_items (one row per playlist + video)
        -> raw__youtube__api__videos       (one row per video)
        -> raw__youtube__api__transcripts  (one row per caption track downloaded,
           and a status row per video listed, for videos OCW has no transcript
           for)

Credentials are resolved lazily at run time. The qa and production profiles
read them from Vault (``YOUTUBE_VAULT_PATH``): MIT Learn's API key, which the
dagster stack in ol-infrastructure writes there, and, where captions are
fetched, the OAuth client and a refresh token per channel. Any other profile
reads YOUTUBE_DEVELOPER_KEY and the YOUTUBE_CAPTIONS_* variables from the
environment. Without caption credentials the transcripts table is not loaded.

Run standalone:
    DLT_PROFILE=dev YOUTUBE_DEVELOPER_KEY=... python -m ol_dlt.sources.youtube
"""

import base64
import functools
import html
import json
import logging
import re
from collections.abc import Callable, Generator, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from http import HTTPStatus
from typing import Any

import dlt
import yaml
from dlt.common.libs import pyiceberg as dlt_pyiceberg
from dlt.sources.helpers import requests
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

from ol_dlt import config, vault

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
YOUTUBE_MAX_RESULTS = 50
# Sentinel playlist id meaning "ingest every playlist on the channel".
WILDCARD_PLAYLIST_ID = "all"

OAUTH_GRANT_URL = "https://oauth2.googleapis.com/token"
# Seconds before an access token's expiry at which it is refreshed.
TOKEN_REFRESH_MARGIN_SECONDS = 60

# Quota units each call costs, from the "Quota impact" of each method's
# reference page (developers.google.com/youtube/v3/docs/captions/list and
# /download).
CAPTIONS_LIST_UNITS = 50
CAPTIONS_DOWNLOAD_UNITS = 200
# channels.list, which a run calls once per refresh token to learn its channel.
CHANNELS_LIST_UNITS = 1
# What one run may spend on captions. A project gets 10,000 units a day by
# default, and the source runs daily.
CAPTION_QUOTA_UNITS_DEFAULT = 9_000
# A video's tracks are listed again once their last listing is this old.
CAPTIONS_RELIST_DAYS = 90
# Data API error reasons that mean the day's quota is spent.
QUOTA_REASONS = frozenset({"quotaExceeded", "dailyLimitExceeded", "rateLimitExceeded"})
# Data API error reasons that mean the credentials cannot read any captions.
AUTH_REASONS = frozenset(
    {"authError", "insufficientPermissions", "youtubeSignupRequired"}
)

# Videos OCW has transcript text for, built by dbt from the OCW bucket. Their
# captions are not fetched.
OCW_CONTENT_FILES_TABLE = "int__ocw__content_files"

# fetch_status of a raw__youtube__api__transcripts row. On a video's status row,
# fetched means every track it picks was fetched (now or, unchanged, before),
# and failed that one or more were not.
TRANSCRIPT_FETCHED = "fetched"
# The video has no caption track to fetch.
TRANSCRIPT_NONE = "none"
# captions.list 404s: the video went private or was deleted between videos.list
# and captions.list. A video videos.list does not return is not listed, so it
# gets no row.
TRANSCRIPT_UNAVAILABLE = "unavailable"
# captions.list 403s: its channel's token cannot edit the video.
TRANSCRIPT_FORBIDDEN = "forbidden"
# This many forbidden listings in a row from one channel mean its token cannot
# edit the channel's videos at all, so the run stops using it rather than
# spending the shared quota marking each video.
FORBIDDEN_STREAK_LIMIT = 3
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


@dataclass(frozen=True)
class CaptionTrack:
    """A caption track of a video, as captions.list gives it.

    :param caption_id: The track's id, which captions.download takes.
    :param language_code: Its BCP-47 language, e.g. en or fr-FR.
    :param name: The name its uploader gave it; empty for most tracks.
    :param is_generated: Whether YouTube's speech recognition made it
        (trackKind ASR) rather than an uploader.
    :param track_kind: trackKind, lowercased: standard, asr or forced (only
        the parts of the video not in its spoken language).
    :param last_updated: When it last changed, which decides whether a
        download from an earlier run is still current.
    :param fetchable: Whether it is downloadable: serving, and not a draft.
    """

    caption_id: str
    language_code: str
    name: str | None
    is_generated: bool
    track_kind: str
    last_updated: str | None
    fetchable: bool

    @classmethod
    def from_api(cls, item: dict[str, Any]) -> "CaptionTrack":
        """Build a track from one captions.list item."""
        snippet = item["snippet"]
        return cls(
            caption_id=item["id"],
            language_code=snippet["language"],
            name=snippet.get("name") or None,
            is_generated=snippet.get("trackKind", "").lower() == "asr",
            track_kind=snippet.get("trackKind", "").lower(),
            last_updated=snippet.get("lastUpdated"),
            fetchable=snippet.get("status") == "serving"
            and not snippet.get("isDraft", False),
        )


def one_per_key(transcripts: list[CaptionTrack]) -> list[CaptionTrack]:
    """Keep one track per (language_code, is_generated), the table's key.

    A video can hold two uploaded tracks in a language (a standard and a forced
    one, or two named ones), and two rows with one key fail the merge load.

    :param transcripts: A video's tracks.
    :returns: For each key, a standard track over a forced one, then the most
        recently updated, in the order the keys first appear.
    :rtype: list[CaptionTrack]
    """
    best: dict[tuple[str, bool], CaptionTrack] = {}
    for track in transcripts:
        key = (track.language_code, track.is_generated)
        kept = best.get(key)
        if kept is None or (track.track_kind != "forced", track.last_updated or "") > (
            kept.track_kind != "forced",
            kept.last_updated or "",
        ):
            best[key] = track
    return list(best.values())


def tracks_to_fetch(
    transcripts: list[CaptionTrack],
    tracks: TranscriptTracks,
    video_languages: set[str],
) -> list[CaptionTrack]:
    """Pick the transcript tracks of one video to fetch.

    :param transcripts: The video's downloadable caption tracks.
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
# The OAuth client and the refresh tokens captions are read with, as keys of
# the Vault secret and, outside the deployed profiles, environment variables.
CAPTION_CREDENTIAL_KEYS = {
    "client_id": "captions_client_id",
    "client_secret": "captions_client_secret",  # pragma: allowlist secret
    "refresh_tokens": "captions_refresh_tokens",  # noqa: S105
}


_MARKUP = re.compile(r"<[^>]*>")
_SRT_TIME = re.compile(r"(\d+):(\d{2}):(\d{2})[,.](\d{3})")


def _srt_seconds(timestamp: str) -> float:
    match = _SRT_TIME.search(timestamp)
    if not match:
        msg = f"Not an SRT timestamp: {timestamp!r}"
        raise ValueError(msg)
    hours, minutes, seconds, millis = (int(part) for part in match.groups())
    return hours * 3600 + minutes * 60 + seconds + millis / 1000


def srt_segments(body: str) -> list[dict[str, Any]]:
    """Parse an SRT caption file into timed segments.

    :param body: The file, as captions.download returns it with ``tfmt=srt``.
    :returns: One ``{text, start, duration}`` per cue with text, in seconds,
        the shape youtube-transcript-api gave (and MIT Learn reads).
    :rtype: list[dict[str, Any]]
    :raises ValueError: A cue's timing line does not parse.
    """
    segments = []
    for cue in re.split(r"\n\s*\n", body.lstrip("﻿").replace("\r\n", "\n")):
        lines = cue.strip().split("\n")
        timing = next((i for i, line in enumerate(lines) if "-->" in line), None)
        if timing is None:
            continue
        start_text, end_text = lines[timing].split("-->", 1)
        start = _srt_seconds(start_text)
        # As youtube-transcript-api gave it: no markup, entities decoded.
        text = html.unescape(_MARKUP.sub("", "\n".join(lines[timing + 1 :]))).strip()
        if text:
            segments.append(
                {
                    "text": text,
                    "start": start,
                    "duration": round(_srt_seconds(end_text) - start, 3),
                }
            )
    return segments


@dataclass(frozen=True)
class CaptionCredentials:
    """An OAuth client and the refresh tokens channels granted it.

    :param refresh_tokens: Each token granted youtube.force-ssl as one channel,
        by a label that only names it in logs (e.g. ``ocw``). Which channel a
        token is for is asked of the API, not configured.
    """

    client_id: str
    client_secret: str
    refresh_tokens: dict[str, str]


def _refresh_tokens(value: object) -> dict[str, str]:
    """Read the refresh tokens: a mapping of label to token, or its JSON."""
    tokens = json.loads(value) if isinstance(value, str) else value
    if (
        not isinstance(tokens, dict)
        or not tokens
        or not all(isinstance(t, str) and t for t in tokens.values())
    ):
        msg = "YouTube caption refresh_tokens must map a label to each refresh token"
        raise ValueError(msg)
    return {str(label): token for label, token in tokens.items()}


def _resolve_caption_credentials() -> CaptionCredentials | None:
    """Read the credentials captions are fetched with.

    Deployed profiles read them from the Vault secret the API key is in; the
    others from YOUTUBE_CAPTIONS_CLIENT_ID, YOUTUBE_CAPTIONS_CLIENT_SECRET and
    YOUTUBE_CAPTIONS_REFRESH_TOKENS (a JSON object of label to token).

    :returns: The credentials, or None when none are configured, which turns
        caption fetching off for the environment.
    :raises ValueError: Some but not all of them are configured, or the
        refresh tokens are not a mapping of label to token.
    """
    if config.active_profile() in config.DEPLOYED_PROFILES:
        secret = vault.read_kv_secret(YOUTUBE_VAULT_MOUNT, YOUTUBE_VAULT_PATH)
        values = {
            field: secret.get(key) for field, key in CAPTION_CREDENTIAL_KEYS.items()
        }
    else:
        values = {
            field: config.resolve_secret(None, f"YOUTUBE_{key.upper()}")
            for field, key in CAPTION_CREDENTIAL_KEYS.items()
        }
    if not any(values.values()):
        return None
    if missing := sorted(field for field, value in values.items() if not value):
        msg = f"YouTube caption credentials are missing {', '.join(missing)}"
        raise ValueError(msg)
    return CaptionCredentials(
        client_id=str(values["client_id"]),
        client_secret=str(values["client_secret"]),
        refresh_tokens=_refresh_tokens(values["refresh_tokens"]),
    )


class CaptionsUnavailableError(RuntimeError):
    """A refresh token cannot read its channel's captions at all."""


class CaptionsClient:
    """Calls the captions endpoints as one channel, from its refresh token."""

    def __init__(self, credentials: CaptionCredentials, label: str) -> None:
        self.label = label
        self._client_id = credentials.client_id
        self._client_secret = credentials.client_secret
        self._refresh_token = credentials.refresh_tokens[label]
        self._token: str | None = None
        self._expires_at = datetime.min.replace(tzinfo=UTC)

    def _authorization(self) -> dict[str, str]:
        now = datetime.now(tz=UTC)
        if self._token is None or now >= self._expires_at:
            try:
                resp = requests.post(
                    OAUTH_GRANT_URL,
                    data={
                        "client_id": self._client_id,
                        "client_secret": self._client_secret,
                        "refresh_token": self._refresh_token,
                        "grant_type": "refresh_token",
                    },
                    timeout=30,
                )
                grant = resp.json()
                token = grant["access_token"]
                expires_in = grant["expires_in"]
            # Not a per-video failure: a revoked grant, or a token endpoint
            # that is down past dlt's retries, stops this channel for the run,
            # rather than marking each of its videos failed for
            # CAPTIONS_RELIST_DAYS.
            except (requests.RequestException, KeyError, ValueError) as exc:
                msg = f"Could not get an access token from refresh token {self.label}"
                raise CaptionsUnavailableError(msg) from exc
            self._token = token
            self._expires_at = now + timedelta(
                seconds=expires_in - TOKEN_REFRESH_MARGIN_SECONDS
            )
        return {"Authorization": f"Bearer {self._token}"}

    def channel_id(self) -> str:
        """Return the id of the channel the refresh token authorizes as.

        :raises CaptionsUnavailableError: Google refuses the token, or its
            account has no channel.
        """
        try:
            resp = requests.get(
                f"{YOUTUBE_API_BASE}/channels",
                params={"part": "id", "mine": "true"},
                headers=self._authorization(),
                timeout=30,
            )
        except requests.HTTPError as exc:
            msg = (
                f"Refresh token {self.label} cannot read its channel: {_describe(exc)}"
            )
            raise CaptionsUnavailableError(msg) from exc
        items = resp.json().get("items") or []
        if not items:
            msg = f"Refresh token {self.label} authorizes no YouTube channel"
            raise CaptionsUnavailableError(msg)
        return items[0]["id"]

    def list_tracks(self, video_id: str) -> list[dict[str, Any]]:
        """Return captions.list's items for a video (one page holds them all)."""
        resp = requests.get(
            f"{YOUTUBE_API_BASE}/captions",
            params={"part": "snippet", "videoId": video_id},
            headers=self._authorization(),
            timeout=30,
        )
        return resp.json().get("items", [])

    def download(self, caption_id: str) -> str:
        """Return a caption track as SRT."""
        resp = requests.get(
            f"{YOUTUBE_API_BASE}/captions/{caption_id}",
            params={"tfmt": "srt"},
            headers=self._authorization(),
            timeout=60,
        )
        return resp.content.decode("utf-8")


def caption_clients(
    credentials: CaptionCredentials,
    make_client: Callable[[CaptionCredentials, str], CaptionsClient] = CaptionsClient,
) -> dict[str, CaptionsClient]:
    """Return a captions client for each channel a refresh token authorizes as.

    A token Google refuses, or whose account has no channel, is logged and
    left out, so one bad token does not stop the other channels.

    :returns: Each client by the id of its channel.
    :rtype: dict[str, CaptionsClient]
    """
    clients: dict[str, CaptionsClient] = {}
    for label in credentials.refresh_tokens:
        client = make_client(credentials, label)
        try:
            channel_id = client.channel_id()
        # KeyError and ValueError: a response not shaped as documented.
        except (
            CaptionsUnavailableError,
            requests.RequestException,
            KeyError,
            ValueError,
        ):
            logger.exception("Not fetching captions with refresh token %s", label)
            continue
        if channel_id in clients:
            logger.warning(
                "Refresh tokens %s and %s are both for channel %s: using %s",
                clients[channel_id].label,
                label,
                channel_id,
                clients[channel_id].label,
            )
            continue
        logger.info("Refresh token %s reads captions of channel %s", label, channel_id)
        clients[channel_id] = client
    return clients


def _api_error_reason(exc: requests.HTTPError) -> str | None:
    """Return the Data API's reason for an error response, e.g. quotaExceeded."""
    try:
        return exc.response.json()["error"]["errors"][0]["reason"]
    except (AttributeError, ValueError, KeyError, IndexError, TypeError):
        return None


def _status_code(exc: requests.HTTPError) -> int | None:
    return exc.response.status_code if exc.response is not None else None


def _ocw_transcript_video_ids() -> frozenset[str]:
    """Return the YouTube ids of the videos OCW has transcript text for.

    Read from the dbt build of int__ocw__content_files in the deployed
    profiles; empty in the others, and when the table is not built yet. A
    stale build only moves which videos a run spends its quota on.
    """
    if config.active_profile() not in config.DEPLOYED_PROFILES:
        return frozenset()
    destination = config.destination_for("youtube")
    catalog = dlt_pyiceberg.get_catalog(
        credentials=destination.config_params.get("credentials")
    )
    namespace = config.dataset_name().removesuffix("_raw") + "_intermediate"
    try:
        table = catalog.load_table((namespace, OCW_CONTENT_FILES_TABLE))
    except (NoSuchTableError, NoSuchNamespaceError):
        logger.warning(
            "%s.%s is missing: fetching captions without skipping OCW's videos",
            namespace,
            OCW_CONTENT_FILES_TABLE,
        )
        return frozenset()
    # Batches, so the content column the filter reads (page and file text) is
    # held one file at a time.
    batches = table.scan(
        row_filter="youtube_id IS NOT NULL AND content != ''",
        selected_fields=("youtube_id",),
    ).to_arrow_batch_reader()
    return frozenset(
        video_id
        for batch in batches
        for video_id in batch.column("youtube_id").to_pylist()
    )


def captions_queue(
    video_ids: Iterable[str],
    captioned: set[str],
    fetched: dict[str, dict[str, Any]],
    now: datetime,
) -> list[str]:
    """Order the videos whose captions a run may fetch.

    :param video_ids: The candidates, in listing order.
    :param captioned: Those the Data API says have uploaded captions.
    :param fetched: What earlier runs recorded per video: ``listed_at`` (ISO
        8601, None when a run stopped partway) and ``tracks``.
    :param now: The run's time.
    :returns: Videos a run left partway, then those never listed (captioned
        ones first), then those listed more than CAPTIONS_RELIST_DAYS ago,
        oldest first.
    :rtype: list[str]
    """
    candidates = list(video_ids)
    new = [v for v in candidates if not fetched.get(v, {}).get("listed_at")]
    new.sort(key=lambda v: (v not in fetched, v not in captioned))
    relist_before = (now - timedelta(days=CAPTIONS_RELIST_DAYS)).isoformat()
    stale = sorted(
        (
            v
            for v in candidates
            if fetched.get(v, {}).get("listed_at")
            and fetched[v]["listed_at"] < relist_before
        ),
        key=lambda v: fetched[v]["listed_at"],
    )
    return new + stale


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


def _transcript_row(
    video_id: str, fetched_at: datetime, **fields: Any
) -> dict[str, Any]:
    return {
        "video_id": video_id,
        "etl_source": "youtube",
        "language_code": "",
        "is_generated": False,
        "caption_id": None,
        "track_name": None,
        "track_last_updated": None,
        "fetch_status": None,
        "error": None,
        "transcript": None,
        "segments": None,
        "listed_tracks": None,
        "fetched_at": fetched_at,
        **fields,
    }


def _describe(exc: Exception) -> str:
    reason = _api_error_reason(exc) if isinstance(exc, requests.HTTPError) else None
    text = f"{type(exc).__name__}: {reason or exc}"
    return text.strip().removesuffix(":")[:500]


def _listing_failure(
    state: dict[str, dict[str, Any]],
    video_id: str,
    now: datetime,
    status: str,
    exc: Exception,
) -> dict[str, Any]:
    """Record a video whose tracks could not be listed, and return its row.

    It is listed again after CAPTIONS_RELIST_DAYS, like any other video, so a
    video that always fails cannot hold the head of the queue.
    """
    state[video_id] = {
        "listed_at": now.isoformat(),
        "tracks": state.get(video_id, {}).get("tracks", {}),
    }
    return _transcript_row(video_id, now, fetch_status=status, error=_describe(exc))


@dataclass
class _Budget:
    """The quota units a run may still spend on captions."""

    units: int
    spent: int = 0

    def take(self, units: int) -> bool:
        """Spend ``units`` if they fit, and say whether they did."""
        if self.spent + units > self.units:
            return False
        self.spent += units
        return True


def _is_auth_error(exc: requests.HTTPError) -> bool:
    """Whether an error means the token cannot read its channel's captions."""
    return (
        _status_code(exc) == HTTPStatus.UNAUTHORIZED
        or _api_error_reason(exc) in AUTH_REASONS
    )


def _video_caption_rows(  # noqa: PLR0913
    client: CaptionsClient,
    video_id: str,
    video_languages: set[str],
    state: dict[str, dict[str, Any]],
    *,
    tracks: TranscriptTracks,
    budget: _Budget,
    now: datetime,
) -> Generator[dict[str, Any], None, str | None]:
    """List and download one video's captions; see caption_rows.

    :returns: The video's fetch_status, or None when the run is out of quota
        and stops here.
    :raises CaptionsUnavailableError: The client's token cannot read captions.
        Tracks downloaded before it are kept in the state, and when the video
        was last listed is left as it was.
    """
    if not budget.take(CAPTIONS_LIST_UNITS):
        return None
    try:
        items = client.list_tracks(video_id)
    except requests.HTTPError as exc:
        reason = _api_error_reason(exc)
        if reason in QUOTA_REASONS:
            logger.warning("YouTube caption quota spent: %s", _describe(exc))
            return None
        if _is_auth_error(exc):
            msg = f"Refresh token {client.label} cannot list captions: {_describe(exc)}"
            raise CaptionsUnavailableError(msg) from exc
        if _status_code(exc) == HTTPStatus.NOT_FOUND:
            status = TRANSCRIPT_UNAVAILABLE
        elif reason == "forbidden":
            status = TRANSCRIPT_FORBIDDEN
        else:
            logger.exception("Failed to list captions of video_id=%s", video_id)
            status = TRANSCRIPT_FAILED
        yield _listing_failure(state, video_id, now, status, exc)
        return status
    except (requests.RequestException, ValueError) as exc:
        # A timeout or dropped connection that dlt's retries did not get
        # past, or a body that is not JSON.
        logger.exception("Failed to list captions of video_id=%s", video_id)
        yield _listing_failure(state, video_id, now, TRANSCRIPT_FAILED, exc)
        return TRANSCRIPT_FAILED

    listed = [CaptionTrack.from_api(item) for item in items]
    chosen = tracks_to_fetch(
        one_per_key([t for t in listed if t.fetchable]), tracks, video_languages
    )
    known = state.get(video_id, {}).get("tracks", {})
    current = {
        t.caption_id: t.last_updated
        for t in chosen
        if t.last_updated and known.get(t.caption_id) == t.last_updated
    }
    errors = []

    def _keep_downloaded() -> None:
        if video_id not in state and not current:
            return
        state[video_id] = {
            "listed_at": state.get(video_id, {}).get("listed_at"),
            "tracks": {**known, **current},
        }

    for track in (t for t in chosen if t.caption_id not in current):
        if not budget.take(CAPTIONS_DOWNLOAD_UNITS):
            break
        try:
            segments = srt_segments(client.download(track.caption_id))
        except CaptionsUnavailableError:
            _keep_downloaded()
            raise
        except requests.HTTPError as exc:
            if _api_error_reason(exc) in QUOTA_REASONS:
                logger.warning("YouTube caption quota spent: %s", _describe(exc))
                break
            if _is_auth_error(exc):
                _keep_downloaded()
                msg = (
                    f"Refresh token {client.label} cannot download captions: "
                    f"{_describe(exc)}"
                )
                raise CaptionsUnavailableError(msg) from exc
            logger.exception(
                "Failed to download caption %s of video_id=%s",
                track.caption_id,
                video_id,
            )
            errors.append(f"{track.language_code}: {_describe(exc)}")
            continue
        except (requests.RequestException, UnicodeDecodeError, ValueError) as exc:
            logger.exception(
                "Failed to read caption %s of video_id=%s", track.caption_id, video_id
            )
            errors.append(f"{track.language_code}: {_describe(exc)}")
            continue
        yield _transcript_row(
            video_id,
            now,
            language_code=track.language_code,
            is_generated=track.is_generated,
            caption_id=track.caption_id,
            track_name=track.name,
            track_last_updated=track.last_updated,
            fetch_status=TRANSCRIPT_FETCHED,
            transcript="\n".join(segment["text"] for segment in segments),
            segments=segments,
        )
        if track.last_updated:
            current[track.caption_id] = track.last_updated
    else:
        if errors:
            status = TRANSCRIPT_FAILED
        else:
            status = TRANSCRIPT_FETCHED if chosen else TRANSCRIPT_NONE
        yield _transcript_row(
            video_id,
            now,
            fetch_status=status,
            error="; ".join(errors) or None,
            # Every track captions.list gives, fetched or not, so a later
            # choice to fetch more knows what there is.
            listed_tracks=json.dumps(
                [
                    {
                        "caption_id": t.caption_id,
                        "language_code": t.language_code,
                        "is_generated": t.is_generated,
                        "track_kind": t.track_kind,
                        "fetchable": t.fetchable,
                        "last_updated": t.last_updated,
                    }
                    for t in listed
                ]
            ),
        )
        # A track that failed is not in current, so the video's next listing
        # downloads it again.
        state[video_id] = {"listed_at": now.isoformat(), "tracks": current}
        return status
    # Out of quota partway: what was downloaded is kept, and the video is
    # listed first next run for the rest.
    state[video_id] = {"listed_at": None, "tracks": current}
    return None


def caption_rows(  # noqa: PLR0913
    clients: dict[str, CaptionsClient],
    video_channels: dict[str, str],
    queue: Iterable[str],
    video_languages: dict[str, set[str]],
    state: dict[str, dict[str, Any]],
    *,
    tracks: TranscriptTracks,
    quota_units: int,
    now: datetime,
) -> Generator[dict[str, Any]]:
    """List and download the captions of videos in order, within a quota.

    Each video is read with the client of the channel it belongs to. A video
    on a channel no client is for is passed over: no request, no row, and its
    state is left as it was.

    :param clients: A captions client per channel id (see caption_clients).
    :param video_channels: The channel id of each video.
    :param queue: The videos to try, in order (see captions_queue).
    :param video_languages: Each video's languages, for tracks_to_fetch.
    :param state: What earlier runs recorded per video, updated in place:
        ``listed_at`` and the ``lastUpdated`` of each track downloaded.
    :param tracks: Which tracks to fetch.
    :param quota_units: The most the run may spend, across every channel: the
        quota is the OAuth client's project's, which all the tokens share.
    :param now: The run's time, stamped on every row.
    :returns: Transcript and status rows of raw__youtube__api__transcripts.
        When a channel's token turns out unable to read captions (a 401, a
        grant without the youtube.force-ssl scope, or FORBIDDEN_STREAK_LIMIT
        forbidden listings in a row), the run stops using it and goes on with
        the others. The video it was on keeps when it was last listed.
    """
    clients = dict(clients)
    budget = _Budget(quota_units)
    forbidden_streak: dict[str, int] = {}
    for video_id in queue:
        channel_id = video_channels.get(video_id, "")
        client = clients.get(channel_id)
        if client is None:
            continue
        try:
            status = yield from _video_caption_rows(
                client,
                video_id,
                video_languages.get(video_id, set()),
                state,
                tracks=tracks,
                budget=budget,
                now=now,
            )
        except CaptionsUnavailableError:
            logger.exception("Stopped fetching captions of channel %s", channel_id)
            del clients[channel_id]
            if not clients:
                break
            continue
        if status is None:
            break
        if status == TRANSCRIPT_FORBIDDEN:
            forbidden_streak[channel_id] = forbidden_streak.get(channel_id, 0) + 1
        else:
            forbidden_streak[channel_id] = 0
        if forbidden_streak[channel_id] >= FORBIDDEN_STREAK_LIMIT:
            logger.error(
                "Refresh token %s cannot edit channel %s's videos: stopped "
                "fetching its captions after %d forbidden listings in a row",
                client.label,
                channel_id,
                FORBIDDEN_STREAK_LIMIT,
            )
            del clients[channel_id]
            if not clients:
                break
    logger.info("Spent %d of %d caption quota units", budget.spent, quota_units)


@dlt.source(name="youtube")
def youtube_source(  # noqa: C901
    api_key: str | None = None,
    github_access_token: str | None = None,
    github_repo: str = _CONFIG_FILE_REPO_DEFAULT,
    github_folder: str = _CONFIG_FILE_FOLDER_DEFAULT,
    github_branch: str = "main",
    transcript_tracks: str = TranscriptTracks.NATIVE.value,
    caption_quota_units: int = CAPTION_QUOTA_UNITS_DEFAULT,
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
        (video_id, language_code, is_generated), and a status row per video

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
        caption_quota_units: The Data API quota units a run may spend on
            captions.list and captions.download.
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
    def youtube_transcripts(  # noqa: C901, PLR0912, PLR0915
        listing: dict[str, Any],
    ) -> Generator[dict[str, Any]]:
        """Yield the caption tracks of the videos OCW has no transcript for.

        Captions come from the Data API (captions.list, then captions.download
        as SRT for each track ``transcript_tracks`` picks), each video's with
        the refresh token of the channel that owns it (snippet.channelId). A
        video on a channel no token is for is not listed. A run spends at most
        ``caption_quota_units``, so it covers part of the videos: those never
        listed (with uploaded captions first), then those listed more than
        CAPTIONS_RELIST_DAYS ago. The resource state remembers when each video
        was listed and the ``lastUpdated`` of each track downloaded, and a track
        that has not changed is not downloaded again. A video OCW has
        transcript text for (int__ocw__content_files) is skipped. Each video a
        run lists gets:

        - one row per track downloaded, keyed by its ``language_code`` and
          ``is_generated``. A row stays until the track is downloaded again, so
          a failed download keeps the last good text.
        - one status row, with an empty ``language_code``: how the listing went
          (``fetch_status``), in ``error`` each failure, prefixed by its
          language when one track failed, and in ``listed_tracks`` every track
          captions.list gives, fetched or not.

        A video the run does not reach keeps its rows from earlier runs. When
        the day's quota runs out, the run stops there. Without caption
        credentials the resource yields nothing. An error that stops captions
        altogether (no token Google accepts, OCW's table unreadable) is logged
        and ends this resource only, so the other tables still load.
        """
        fetched_at = datetime.now(tz=UTC)
        # Captions fail on their own: an error here (a malformed caption
        # secret included) would otherwise fail the extraction, and with it the
        # channels, playlists and videos MIT Learn reads.
        try:
            credentials = _resolve_caption_credentials()
            if credentials is None:
                logger.warning("No YouTube caption credentials: not fetching captions")
                return
            clients = caption_clients(credentials)
            if not clients:
                logger.warning("No caption refresh token works: not fetching captions")
                return
            skip = _ocw_transcript_video_ids()
            candidates = [v for v in _video_ids(listing) if v not in skip]
            # Each video's channel, whether it has uploaded captions
            # (contentDetails.caption) and the language it is spoken in: one
            # videos.list request per 50 videos.
            video_channels: dict[str, str] = {}
            captioned: set[str] = set()
            video_languages: dict[str, set[str]] = {}
            for video in _videos(candidates, "snippet,contentDetails", _api_key()):
                snippet = video.get("snippet") or {}
                video_channels[video["id"]] = snippet.get("channelId", "")
                language = _language(
                    snippet.get("defaultAudioLanguage")
                    or snippet.get("defaultLanguage")
                )
                video_languages[video["id"]] = {language} if language else set()
                if (video.get("contentDetails") or {}).get("caption") == "true":
                    captioned.add(video["id"])
        except Exception:
            logger.exception("Not fetching captions")
            return
        uncovered: dict[str, int] = {}
        for video_id in candidates:
            channel_id = video_channels.get(video_id, "")
            if channel_id not in clients:
                uncovered[channel_id] = uncovered.get(channel_id, 0) + 1
        for channel_id, count in sorted(uncovered.items()):
            logger.info(
                "No caption refresh token for channel %s: passing over %d videos",
                channel_id or "(unknown)",
                count,
            )

        state = dlt.current.resource_state().setdefault("videos", {})
        try:
            yield from caption_rows(
                clients,
                video_channels,
                captions_queue(
                    [v for v in candidates if video_channels.get(v) in clients],
                    captioned,
                    state,
                    fetched_at,
                ),
                video_languages,
                state,
                tracks=tracks,
                # The channel lookups came out of the same quota.
                quota_units=caption_quota_units
                - CHANNELS_LIST_UNITS * len(credentials.refresh_tokens),
                now=fetched_at,
            )
        # Anything caption_rows did not expect.
        except Exception:
            logger.exception("Stopped fetching captions")

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
