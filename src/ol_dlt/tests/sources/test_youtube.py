"""Unit + materialization tests for the YouTube source."""

import base64
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import dlt
import pyarrow as pa
import pytest
from dlt.extract.exceptions import ResourceExtractionError
from dlt.sources.helpers import requests

from ol_dlt import config
from ol_dlt.sources import youtube
from tests.conftest import FakeResponse


def _queue_get(monkeypatch, payloads):
    """Patch youtube.requests.get to return queued JSON payloads in order."""
    queue = [FakeResponse(json_data=p) for p in payloads]
    calls = []

    def _fake_get(url, params=None, **_kwargs):
        calls.append({"url": url, "params": params or {}})
        return queue.pop(0)

    monkeypatch.setattr(youtube.requests, "get", _fake_get)
    return calls


def test_resolve_api_key_prefers_argument(monkeypatch):
    monkeypatch.delenv("YOUTUBE_DEVELOPER_KEY", raising=False)
    assert youtube._resolve_api_key("explicit-key") == "explicit-key"


def test_resolve_api_key_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("YOUTUBE_DEVELOPER_KEY", "env-key")
    assert youtube._resolve_api_key(None) == "env-key"


def test_resolve_api_key_raises_when_missing(monkeypatch):
    monkeypatch.delenv("YOUTUBE_DEVELOPER_KEY", raising=False)
    with pytest.raises(ValueError, match="YOUTUBE_DEVELOPER_KEY"):
        youtube._resolve_api_key(None)


@pytest.mark.parametrize("profile", ["qa", "production"])
def test_resolve_api_key_reads_vault_in_deployed_profiles(monkeypatch, profile):
    monkeypatch.setenv("DLT_PROFILE", profile)
    monkeypatch.setenv("YOUTUBE_DEVELOPER_KEY", "env-key")
    reads: list[tuple[str, str]] = []

    def _read_kv_secret(mount: str, path: str) -> dict[str, str]:
        reads.append((mount, path))
        return {"developer_key": "vault-key"}

    monkeypatch.setattr(youtube.vault, "read_kv_secret", _read_kv_secret)
    assert youtube._resolve_api_key("explicit-key") == "vault-key"
    assert reads == [("secret-data", "pipelines/youtube")]


def test_github_headers_includes_token_when_present():
    assert youtube._github_headers(None) == {"Accept": "application/vnd.github.v3+json"}
    assert youtube._github_headers("tok")["Authorization"] == "Bearer tok"


def test_batched_splits_into_chunks():
    assert list(youtube._batched(range(5), 2)) == [[0, 1], [2, 3], [4]]
    assert list(youtube._batched([], 2)) == []


def test_yt_paged_items_follows_next_page_token(monkeypatch):
    calls = _queue_get(
        monkeypatch,
        [
            {"items": [{"id": "a"}], "nextPageToken": "PAGE2"},
            {"items": [{"id": "b"}]},
        ],
    )
    items = list(youtube._yt_paged_items("videos", {"part": "id"}, "k"))
    assert [i["id"] for i in items] == ["a", "b"]
    assert calls[0]["params"]["key"] == "k"
    assert calls[0]["params"]["maxResults"] == youtube.YOUTUBE_MAX_RESULTS
    assert calls[1]["params"]["pageToken"] == "PAGE2"


def test_playlist_ids_explicit_configs_skip_ignored(monkeypatch):
    def _boom(*_args, **_kwargs):
        pytest.fail("should not call the API for explicit playlists")

    monkeypatch.setattr(youtube.requests, "get", _boom)
    channel_config = {
        "channel_id": "chan",
        "playlists": [{"id": "keep"}, {"id": "drop", "ignore": True}],
    }
    assert list(youtube._playlist_ids_for_config(channel_config, "k")) == ["keep"]


def test_playlist_ids_wildcard_lists_channel_playlists(monkeypatch):
    _queue_get(monkeypatch, [{"items": [{"id": "p1"}, {"id": "p2"}]}])
    channel_config = {"channel_id": "chan", "playlists": [{"id": "all"}]}
    assert list(youtube._playlist_ids_for_config(channel_config, "k")) == ["p1", "p2"]


def test_playlist_ids_empty_config_is_wildcard(monkeypatch):
    _queue_get(monkeypatch, [{"items": [{"id": "p1"}]}])
    assert list(youtube._playlist_ids_for_config({"channel_id": "c"}, "k")) == ["p1"]


def test_video_ids_for_playlist_reads_content_details(monkeypatch):
    _queue_get(
        monkeypatch,
        [
            {
                "items": [
                    {"contentDetails": {"videoId": "v1"}},
                    {"contentDetails": {}},
                    {"contentDetails": {"videoId": "v2"}},
                ]
            }
        ],
    )
    assert list(youtube._video_ids_for_playlist("pl", "k")) == ["v1", "v2"]


def test_unique_keeps_first_occurrence_order():
    assert list(youtube._unique(["v1", "v2", "v1", "v3", "v2"])) == ["v1", "v2", "v3"]


def test_resources_share_one_playlist_listing(monkeypatch, tmp_path):
    """Playlists, items and videos read one listing per run, not one each."""
    monkeypatch.setattr(youtube, "_resolve_api_key", lambda _key: "k")
    config_fetches: list[int] = []

    def _configs(**_kw):
        config_fetches.append(1)
        return [{"channel_id": "c", "playlists": [{"id": "all"}]}]

    monkeypatch.setattr(youtube, "_fetch_channel_configs", _configs)
    listings: list[str] = []

    def _fake_get(url, params=None, **_kwargs):
        params = params or {}
        if url.endswith("/playlists") and "channelId" in params:
            listings.append("channel_playlists")
            return FakeResponse(json_data={"items": [{"id": "pl1"}, {"id": "pl2"}]})
        if url.endswith("/playlistItems"):
            listings.append(params["playlistId"])
            videos = {"pl1": ["v1", "v2"], "pl2": ["v2", "v3"]}[params["playlistId"]]
            return FakeResponse(
                json_data={
                    "items": [{"contentDetails": {"videoId": v}} for v in videos]
                }
            )
        if url.endswith("/videos"):
            return FakeResponse(
                json_data={"items": [{"id": v} for v in params["id"].split(",")]}
            )
        return FakeResponse(json_data={"items": [{"id": params.get("id")}]})

    monkeypatch.setattr(youtube.requests, "get", _fake_get)
    source = youtube.youtube_source(api_key="k").with_resources(
        "raw__youtube__api__playlists",
        "raw__youtube__api__playlist_items",
        "raw__youtube__api__videos",
    )
    pipeline = dlt.pipeline(
        pipeline_name="youtube_listing_once", pipelines_dir=str(tmp_path)
    )
    pipeline.extract(source)

    assert sorted(listings) == ["channel_playlists", "pl1", "pl2"]
    assert len(config_fetches) == 1


def _http_error(status: int, reason: str | None = None) -> requests.HTTPError:
    """An HTTPError as dlt's requests helper raises it for an error response."""
    response = requests.Response()
    response.status_code = status
    body = {"error": {"errors": [{"reason": reason}]}} if reason else {}
    response._content = json.dumps(body).encode()  # noqa: SLF001
    return requests.HTTPError(f"{status}", response=response)


def _srt(*texts: str) -> str:
    return "\n\n".join(
        f"{i}\n00:00:0{i},000 --> 00:00:0{i + 1},500\n{text}"
        for i, text in enumerate(texts, start=1)
    )


def _item(
    caption_id: str,
    language: str,
    kind: str = "standard",
    *,
    last_updated: str = "2026-01-01T00:00:00Z",
    status: str = "serving",
    draft: bool = False,
) -> dict[str, Any]:
    """One captions.list item."""
    return {
        "id": caption_id,
        "snippet": {
            "language": language,
            "trackKind": kind,
            "name": "",
            "lastUpdated": last_updated,
            "status": status,
            "isDraft": draft,
        },
    }


class _FakeCaptions:
    """Stands in for CaptionsClient.

    ``listings`` maps a video id to its captions.list items, or an exception
    list_tracks raises; ``texts`` maps a caption id to its text, or an exception
    download raises. A video it does not name has one English track.
    """

    def __init__(self, listings=None, texts=None):
        self.listings = listings or {}
        self.texts = texts or {}
        self.listed: list[str] = []
        self.downloaded: list[str] = []

    def list_tracks(self, video_id):
        self.listed.append(video_id)
        listing = self.listings.get(video_id, [_item(f"{video_id}-en", "en")])
        if isinstance(listing, Exception):
            raise listing
        return listing

    def download(self, caption_id):
        self.downloaded.append(caption_id)
        text = self.texts.get(caption_id, f"text of {caption_id}")
        if isinstance(text, Exception):
            raise text
        return _srt(text)


NOW = datetime(2026, 11, 21, tzinfo=UTC)


def _caption_rows(
    client,
    video_ids,
    *,
    state=None,
    languages=None,
    tracks=youtube.TranscriptTracks.NATIVE,
    quota_units=youtube.CAPTION_QUOTA_UNITS_DEFAULT,
    now=NOW,
):
    """Run caption_rows over a fake client; ``languages`` maps a video to its
    languages, English when it is not named."""
    state = {} if state is None else state
    languages = languages or {}
    rows = list(
        youtube.caption_rows(
            client,
            video_ids,
            {v: languages.get(v, {"en"}) for v in video_ids},
            state,
            tracks=tracks,
            quota_units=quota_units,
            now=now,
        )
    )
    return rows, state


def _summary(rows):
    return [
        (
            row["video_id"],
            row["language_code"],
            row["is_generated"],
            row["fetch_status"],
            row["transcript"],
            row["error"],
        )
        for row in rows
    ]


def test_srt_segments_parse_cues_into_timed_text():
    body = (
        "﻿1\r\n00:00:01,000 --> 00:00:04,250\r\nHello\r\nthere\r\n\r\n"
        "2\r\n00:01:02.500 --> 00:01:03,000 align:start\r\nagain\r\n\r\n"
        "3\r\n00:01:04,000 --> 00:01:05,000\r\n\r\n"
    )
    assert youtube.srt_segments(body) == [
        {"text": "Hello\nthere", "start": 1.0, "duration": 3.25},
        {"text": "again", "start": 62.5, "duration": 0.5},
    ]


def test_srt_segments_drop_markup_and_decode_entities():
    body = (
        "1\n00:00:01,000 --> 00:00:02,000\n"
        "<i>Tom &amp; Jerry</i> &gt; <font color=red>x</font>"
    )
    assert youtube.srt_segments(body)[0]["text"] == "Tom & Jerry > x"


def test_srt_segments_refuse_a_bad_timing_line():
    with pytest.raises(ValueError, match="SRT timestamp"):
        youtube.srt_segments("1\nsoon --> later\nHello")


def test_caption_track_reads_a_captions_list_item():
    asr = youtube.CaptionTrack.from_api(_item("c1", "en", "ASR"))
    assert asr.is_generated
    assert asr.fetchable
    assert asr.name is None
    assert not youtube.CaptionTrack.from_api(_item("c2", "en", draft=True)).fetchable
    assert not youtube.CaptionTrack.from_api(
        _item("c3", "en", status="syncing")
    ).fetchable


def test_captions_land_every_track_the_video_picks(monkeypatch):
    client = _FakeCaptions(
        {
            "vid1": [
                _item("c-en", "en"),
                _item("c-en-asr", "en", "asr"),
                _item("c-es", "es"),
                _item("c-fr-asr", "fr", "asr"),
                _item("c-de", "de", draft=True),
            ]
        },
        {"c-en": "hello", "c-en-asr": "hello auto", "c-es": "hola"},
    )
    rows, state = _caption_rows(client, ["vid1"])

    assert _summary(rows) == [
        ("vid1", "en", False, "fetched", "hello", None),
        ("vid1", "es", False, "fetched", "hola", None),
        ("vid1", "en", True, "fetched", "hello auto", None),
        ("vid1", "", False, "fetched", None, None),
    ]
    assert rows[0]["caption_id"] == "c-en"
    assert rows[0]["segments"] == [{"text": "hello", "start": 1.0, "duration": 1.5}]
    assert {row["fetched_at"] for row in rows} == {NOW}
    assert [t["caption_id"] for t in json.loads(rows[-1]["listed_tracks"])] == [
        "c-en",
        "c-en-asr",
        "c-es",
        "c-fr-asr",
        "c-de",
    ]
    assert state == {
        "vid1": {
            "listed_at": NOW.isoformat(),
            "tracks": {
                "c-en": "2026-01-01T00:00:00Z",
                "c-en-asr": "2026-01-01T00:00:00Z",
                "c-es": "2026-01-01T00:00:00Z",
            },
        }
    }


def test_a_video_without_captions_has_a_status_row_saying_why():
    client = _FakeCaptions(
        {
            "none": [],
            "gone": _http_error(404, "videoNotFound"),
            "not-ours": _http_error(403, "forbidden"),
            "broken": _http_error(400, "invalidValue"),
        }
    )
    rows, state = _caption_rows(client, ["none", "gone", "not-ours", "broken"])

    assert _summary(rows) == [
        ("none", "", False, "none", None, None),
        ("gone", "", False, "unavailable", None, "HTTPError: videoNotFound"),
        ("not-ours", "", False, "forbidden", None, "HTTPError: forbidden"),
        ("broken", "", False, "failed", None, "HTTPError: invalidValue"),
    ]
    # Each waits for its next listing, so none holds the head of the queue.
    assert {v["listed_at"] for v in state.values()} == {NOW.isoformat()}


def test_a_track_that_fails_writes_no_row_and_is_downloaded_next_listing():
    """So a failed download never overwrites the text an earlier run landed."""
    client = _FakeCaptions(
        {"v": [_item("c-en", "en"), _item("c-es", "es")]},
        {"c-es": _http_error(403, "forbidden")},
    )
    rows, state = _caption_rows(client, ["v"])

    assert _summary(rows) == [
        ("v", "en", False, "fetched", "text of c-en", None),
        ("v", "", False, "failed", None, "es: HTTPError: forbidden"),
    ]
    assert state["v"]["tracks"] == {"c-en": "2026-01-01T00:00:00Z"}


def test_an_unchanged_track_is_not_downloaded_again():
    state = {
        "v": {
            "listed_at": "2026-01-01T00:00:00+00:00",
            "tracks": {"c-en": "2026-01-01T00:00:00Z", "c-es": "2025-01-01T00:00:00Z"},
        }
    }
    client = _FakeCaptions({"v": [_item("c-en", "en"), _item("c-es", "es")]})
    rows, state = _caption_rows(client, ["v"], state=state)

    assert client.downloaded == ["c-es"]
    assert _summary(rows) == [
        ("v", "es", False, "fetched", "text of c-es", None),
        ("v", "", False, "fetched", None, None),
    ]
    assert state["v"]["tracks"] == {
        "c-en": "2026-01-01T00:00:00Z",
        "c-es": "2026-01-01T00:00:00Z",
    }


def test_a_run_stops_at_its_quota():
    """Two videos of one track each cost 2 * (50 + 200) units."""
    client = _FakeCaptions()
    rows, state = _caption_rows(client, ["a", "b", "c"], quota_units=520)

    assert client.listed == ["a", "b"]
    assert [row["video_id"] for row in rows if not row["language_code"]] == ["a", "b"]
    assert sorted(state) == ["a", "b"]


def test_a_video_whose_downloads_do_not_fit_is_finished_over_several_runs():
    """Each run keeps what fit, and the next lists the video first for the rest."""
    client = _FakeCaptions(
        {"big": [_item("c1", "en"), _item("c2", "es"), _item("c3", "fr")]}
    )
    rows, state = _caption_rows(client, ["big", "small"], quota_units=500)

    assert client.listed == ["big"]
    assert client.downloaded == ["c1", "c2"]
    assert [row["caption_id"] for row in rows] == ["c1", "c2"]
    assert state["big"]["listed_at"] is None

    queue = youtube.captions_queue(["small", "big"], {"small", "big"}, state, NOW)
    assert queue == ["big", "small"]
    rows, state = _caption_rows(client, queue, state=state, quota_units=500)
    assert client.listed == ["big", "big", "small"]
    assert client.downloaded == ["c1", "c2", "c3", "small-en"]
    assert state["big"]["listed_at"] == NOW.isoformat()
    assert sorted(state["big"]["tracks"]) == ["c1", "c2", "c3"]


def test_one_track_is_kept_per_language_and_kind():
    """Two rows with one (video_id, language_code, is_generated) fail the merge."""
    client = _FakeCaptions(
        {
            "v": [
                _item("forced", "en", "forced", last_updated="2026-06-01T00:00:00Z"),
                _item("cc", "en", last_updated="2026-01-01T00:00:00Z"),
                _item("cc-new", "en", last_updated="2026-03-01T00:00:00Z"),
                _item("asr", "en", "asr"),
            ]
        }
    )
    rows, _state = _caption_rows(client, ["v"])

    assert client.downloaded == ["cc-new", "asr"]
    keys = [(r["language_code"], r["is_generated"]) for r in rows]
    assert len(keys) == len(set(keys))
    assert len(json.loads(rows[-1]["listed_tracks"])) == 4


@pytest.mark.parametrize(
    "error", [requests.ConnectionError("reset"), requests.Timeout("slow")]
)
def test_a_request_that_never_succeeds_is_a_failed_row_not_a_failed_run(error):
    client = _FakeCaptions({"down": error, "slow": [_item("s1", "en")]}, {"s1": error})
    rows, state = _caption_rows(client, ["down", "slow", "ok"])

    assert [(r["video_id"], r["language_code"], r["fetch_status"]) for r in rows] == [
        ("down", "", "failed"),
        ("slow", "", "failed"),
        ("ok", "en", "fetched"),
        ("ok", "", "fetched"),
    ]
    assert {v["listed_at"] for v in state.values()} == {NOW.isoformat()}


@pytest.mark.parametrize(
    "error",
    [_http_error(401, "authError"), _http_error(403, "insufficientPermissions")],
)
def test_credentials_that_cannot_read_captions_stop_the_run_untouched(error):
    client = _FakeCaptions({"b": error})
    state: dict[str, Any] = {}
    rows = []
    with pytest.raises(youtube.CaptionsUnavailableError):
        for row in youtube.caption_rows(
            client,
            ["a", "b", "c"],
            {},
            state,
            tracks=youtube.TranscriptTracks.NATIVE,
            quota_units=youtube.CAPTION_QUOTA_UNITS_DEFAULT,
            now=NOW,
        ):
            rows.append(row)

    assert sorted(state) == ["a"]
    assert {row["video_id"] for row in rows} == {"a"}


@pytest.mark.parametrize("during", ["list", "download"])
def test_once_the_quota_is_spent_no_more_requests_are_made(during):
    quota = _http_error(403, "quotaExceeded")
    if during == "list":
        client = _FakeCaptions({"b": quota})
    else:
        client = _FakeCaptions(
            {"b": [_item("b-en", "en"), _item("b-es", "es")]}, {"b-es": quota}
        )
    rows, state = _caption_rows(client, ["a", "b", "c"])

    assert client.listed == ["a", "b"]
    if during == "list":
        assert _summary(rows)[-1] == ("a", "", False, "fetched", None, None)
        assert "b" not in state
    else:
        # What was downloaded stays; the video is first in line next run.
        assert _summary(rows)[-1] == ("b", "en", False, "fetched", "text of b-en", None)
        assert state["b"] == {
            "listed_at": None,
            "tracks": {"b-en": "2026-01-01T00:00:00Z"},
        }


def test_captions_queue_puts_new_captioned_videos_first_then_the_stalest():
    fetched = {
        "recent": {"listed_at": (NOW - timedelta(days=1)).isoformat()},
        "stale": {"listed_at": (NOW - timedelta(days=100)).isoformat()},
        "staler": {"listed_at": (NOW - timedelta(days=200)).isoformat()},
        "partway": {"listed_at": None},
    }
    queue = youtube.captions_queue(
        ["uncaptioned", "recent", "stale", "captioned", "staler", "partway"],
        {"captioned"},
        fetched,
        NOW,
    )
    assert queue == ["partway", "captioned", "uncaptioned", "staler", "stale"]


DUBBED = [
    _item("en", "en"),
    _item("ar-asr", "ar", "asr"),
    _item("en-asr", "en", "asr"),
    _item("fr-asr", "fr-FR", "asr"),
]


def test_native_tracks_are_uploaded_ones_and_the_auto_track_in_the_video_language():
    client = _FakeCaptions(
        {
            "dubbed": DUBBED,
            "spanish": [_item("es", "es", "asr"), _item("en", "en", "asr")],
            "mislabeled": [_item("es", "es", "asr")],
            "hebrew": [_item("iw", "iw", "asr"), _item("en", "en", "asr")],
            "no-language": [
                _item("de", "de"),
                _item("de-asr", "de", "asr"),
                _item("en-asr", "en", "asr"),
            ],
            "nothing-known": [_item("ja", "ja", "asr"), _item("ko", "ko", "asr")],
        }
    )
    rows, _state = _caption_rows(
        client,
        ["dubbed", "spanish", "mislabeled", "hebrew", "no-language", "nothing-known"],
        languages={
            "spanish": {"es"},
            "hebrew": {"he"},
            "no-language": set(),
            "nothing-known": set(),
        },
    )

    by_video: dict[str, list[tuple[str, bool]]] = {}
    for row in rows:
        if row["language_code"]:
            by_video.setdefault(row["video_id"], []).append(
                (row["language_code"], row["is_generated"])
            )
    assert by_video == {
        "dubbed": [("en", False), ("en", True)],
        "spanish": [("es", True)],
        # Labelled English but only Spanish is listed: the label is wrong.
        "mislabeled": [("es", True)],
        "hebrew": [("iw", True)],
        # No language from the API: the uploaded track's language is the video's.
        "no-language": [("de", False), ("de", True)],
        # Nothing to go on: every auto-generated track is kept.
        "nothing-known": [("ja", True), ("ko", True)],
    }


def test_all_tracks_fetches_every_listed_track():
    client = _FakeCaptions({"dubbed": DUBBED})
    rows, _state = _caption_rows(
        client, ["dubbed"], tracks=youtube.TranscriptTracks.ALL
    )

    assert [
        (row["language_code"], row["is_generated"])
        for row in rows
        if row["language_code"]
    ] == [("en", False), ("ar", True), ("en", True), ("fr-FR", True)]


def test_an_unknown_track_choice_is_refused():
    with pytest.raises(ValueError, match="everything"):
        youtube.youtube_source(api_key="k", transcript_tracks="everything")


def _transcripts_resource(
    monkeypatch, video_ids, client, *, skip=frozenset(), videos=None, extract_with=None
):
    """Run the transcripts resource over a fake listing and captions client.

    ``videos`` maps a video id to its videos.list record; a video it does not
    name is in English with uploaded captions.
    """
    videos = videos or {}
    parts: list[str] = []

    def _fake_videos(ids, part, _key):
        parts.append(part)
        for video_id in ids:
            yield videos.get(
                video_id,
                {
                    "id": video_id,
                    "snippet": {"defaultAudioLanguage": "en"},
                    "contentDetails": {"caption": "true"},
                },
            )

    monkeypatch.setattr(youtube, "_videos", _fake_videos)
    monkeypatch.setattr(youtube, "_resolve_api_key", lambda _key: "k")
    monkeypatch.setattr(
        youtube,
        "_resolve_caption_credentials",
        lambda: youtube.CaptionCredentials("id", "secret", "refresh"),
    )
    monkeypatch.setattr(youtube, "CaptionsClient", lambda _credentials: client)
    monkeypatch.setattr(youtube, "_ocw_transcript_video_ids", lambda: skip)
    monkeypatch.setattr(
        youtube, "_fetch_channel_configs", lambda **_kw: [{"channel_id": "c"}]
    )
    monkeypatch.setattr(
        youtube, "_playlist_ids_for_config", lambda _config, _key: iter(["pl"])
    )
    monkeypatch.setattr(
        youtube, "_video_ids_for_playlist", lambda _playlist_id, _key: iter(video_ids)
    )
    source = youtube.youtube_source(api_key="k")
    if extract_with is not None:
        extract_with.extract(source.with_resources("raw__youtube__api__transcripts"))
        return [], parts
    return list(source.resources["raw__youtube__api__transcripts"]), parts


def test_transcripts_skip_ocw_videos_and_list_captioned_ones_first(monkeypatch):
    client = _FakeCaptions()
    rows, parts = _transcripts_resource(
        monkeypatch,
        ["ocw", "silent", "spoken"],
        client,
        skip=frozenset({"ocw"}),
        videos={
            "silent": {
                "id": "silent",
                "snippet": {},
                "contentDetails": {"caption": "false"},
            }
        },
    )

    assert parts == ["snippet,contentDetails"]
    assert client.listed == ["spoken", "silent"]
    assert {row["video_id"] for row in rows} == {"spoken", "silent"}


def test_a_second_run_does_not_download_unchanged_tracks(monkeypatch, tmp_path):
    """The resource state carries what was downloaded from one run to the next."""
    client = _FakeCaptions()
    pipeline = dlt.pipeline(
        pipeline_name="youtube_captions", pipelines_dir=str(tmp_path)
    )
    _transcripts_resource(monkeypatch, ["v1", "v2"], client, extract_with=pipeline)
    # A video listed within CAPTIONS_RELIST_DAYS is not listed again.
    _transcripts_resource(monkeypatch, ["v1", "v2"], client, extract_with=pipeline)
    assert client.listed == ["v1", "v2"]

    # Listed again, its unchanged track is not downloaded again.
    monkeypatch.setattr(youtube, "CAPTIONS_RELIST_DAYS", 0)
    _transcripts_resource(monkeypatch, ["v1", "v2"], client, extract_with=pipeline)
    assert client.listed == ["v1", "v2", "v1", "v2"]
    assert client.downloaded == ["v1-en", "v2-en"]
    # Later tests iterate resources outside a pipeline; leave none active.
    pipeline.deactivate()


def test_a_caption_outage_ends_only_the_transcripts_resource(monkeypatch):
    client = _FakeCaptions({"v2": _http_error(401, "authError")})
    rows, _parts = _transcripts_resource(monkeypatch, ["v1", "v2", "v3"], client)
    assert {row["video_id"] for row in rows} == {"v1"}

    def _unreadable():
        raise PermissionError("glue")

    client = _FakeCaptions()
    monkeypatch.setattr(youtube, "_ocw_transcript_video_ids", _unreadable)
    source = youtube.youtube_source(api_key="k")
    assert list(source.resources["raw__youtube__api__transcripts"]) == []
    assert client.listed == []


def test_ocw_transcript_video_ids_read_the_dbt_build(monkeypatch, tmp_path):
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=tmp_path.as_uri(),
    )
    monkeypatch.setenv("DLT_PROFILE", "production")
    monkeypatch.setattr(youtube.dlt_pyiceberg, "get_catalog", lambda **_kwargs: catalog)
    assert youtube._ocw_transcript_video_ids() == frozenset()

    catalog.create_namespace("ol_warehouse_production_intermediate")
    table = catalog.create_table(
        ("ol_warehouse_production_intermediate", "int__ocw__content_files"),
        schema=pa.schema([("youtube_id", pa.string()), ("content", pa.string())]),
    )
    table.append(
        pa.table(
            {
                "youtube_id": ["has-text", "empty", "no-text", None],
                "content": ["transcript", "", None, "a page"],
            }
        )
    )
    assert youtube._ocw_transcript_video_ids() == frozenset({"has-text"})


def test_transcripts_load_nothing_without_caption_credentials(monkeypatch):
    client = _FakeCaptions()
    monkeypatch.setattr(youtube, "_resolve_caption_credentials", lambda: None)
    monkeypatch.setattr(youtube, "_resolve_api_key", lambda _key: "k")
    monkeypatch.setattr(
        youtube, "_fetch_channel_configs", lambda **_kw: [{"channel_id": "c"}]
    )
    monkeypatch.setattr(
        youtube, "_playlist_ids_for_config", lambda _config, _key: iter(["pl"])
    )
    monkeypatch.setattr(
        youtube, "_video_ids_for_playlist", lambda _playlist_id, _key: iter(["v"])
    )
    source = youtube.youtube_source(api_key="k")
    assert list(source.resources["raw__youtube__api__transcripts"]) == []
    assert client.listed == []


def test_resolve_caption_credentials_from_the_environment(monkeypatch):
    for name in ("CLIENT_ID", "CLIENT_SECRET", "REFRESH_TOKEN"):
        monkeypatch.delenv(f"YOUTUBE_CAPTIONS_{name}", raising=False)
    assert youtube._resolve_caption_credentials() is None

    monkeypatch.setenv("YOUTUBE_CAPTIONS_CLIENT_ID", "id")
    with pytest.raises(ValueError, match="client_secret, refresh_token"):
        youtube._resolve_caption_credentials()

    monkeypatch.setenv("YOUTUBE_CAPTIONS_CLIENT_SECRET", "secret")
    monkeypatch.setenv("YOUTUBE_CAPTIONS_REFRESH_TOKEN", "refresh")
    assert youtube._resolve_caption_credentials() == youtube.CaptionCredentials(
        "id", "secret", "refresh"
    )


@pytest.mark.parametrize("profile", ["qa", "production"])
def test_resolve_caption_credentials_reads_vault_in_deployed_profiles(
    monkeypatch, profile
):
    monkeypatch.setenv("DLT_PROFILE", profile)
    monkeypatch.setenv("YOUTUBE_CAPTIONS_CLIENT_ID", "env-id")
    secrets = {"developer_key": "k"}
    monkeypatch.setattr(youtube.vault, "read_kv_secret", lambda _mount, _path: secrets)
    # The API key alone: no captions in this environment.
    assert youtube._resolve_caption_credentials() is None

    secrets.update(
        captions_client_id="id",
        captions_client_secret="secret",  # pragma: allowlist secret
        captions_refresh_token="refresh",  # noqa: S106
    )
    assert youtube._resolve_caption_credentials() == youtube.CaptionCredentials(
        "id", "secret", "refresh"
    )


def test_captions_client_refreshes_its_access_token_when_it_expires(monkeypatch):
    grants: list[dict[str, str]] = []
    gets: list[tuple[str, dict[str, Any], str]] = []

    def _fake_post(url, data=None, **_kwargs):
        grants.append(data)
        return FakeResponse(
            json_data={"access_token": f"token-{len(grants)}", "expires_in": 3599}
        )

    def _fake_get(url, params=None, headers=None, **_kwargs):
        gets.append((url, params, headers["Authorization"]))
        if url.endswith("/captions"):
            return FakeResponse(json_data={"items": [_item("c1", "en")]})
        return FakeResponse(content=_srt("hello").encode())

    monkeypatch.setattr(youtube.requests, "post", _fake_post)
    monkeypatch.setattr(youtube.requests, "get", _fake_get)
    client = youtube.CaptionsClient(
        youtube.CaptionCredentials("id", "secret", "refresh")
    )

    assert client.list_tracks("v1") == [_item("c1", "en")]
    assert "hello" in client.download("c1")
    assert len(grants) == 1
    assert grants[0]["grant_type"] == "refresh_token"
    assert grants[0]["refresh_token"] == "refresh"  # noqa: S105
    assert gets[0][1] == {"part": "snippet", "videoId": "v1"}
    assert gets[1][0].endswith("/captions/c1")
    assert gets[1][1] == {"tfmt": "srt"}
    assert {auth for *_rest, auth in gets} == {"Bearer token-1"}

    client._expires_at = datetime.now(tz=UTC) - timedelta(seconds=1)  # noqa: SLF001
    client.list_tracks("v2")
    assert gets[-1][2] == "Bearer token-2"


def test_captions_client_fails_the_run_on_a_refused_refresh_token(monkeypatch):
    def _fake_post(*_args, **_kwargs):
        raise _http_error(400, "invalid_grant")

    monkeypatch.setattr(youtube.requests, "post", _fake_post)
    client = youtube.CaptionsClient(
        youtube.CaptionCredentials("id", "secret", "refresh")
    )
    with pytest.raises(RuntimeError, match="refresh token"):
        client.list_tracks("v")


def test_ocw_transcript_video_ids_are_empty_outside_deployed_profiles(monkeypatch):
    monkeypatch.setenv("DLT_PROFILE", "test")
    assert youtube._ocw_transcript_video_ids() == frozenset()


def test_source_resolves_the_api_key_once_for_all_resources(monkeypatch):
    resolutions: list[str | None] = []

    def _resolve(api_key: str | None) -> str:
        resolutions.append(api_key)
        return "k"

    monkeypatch.setattr(youtube, "_resolve_api_key", _resolve)
    monkeypatch.setattr(
        youtube,
        "_resolve_caption_credentials",
        lambda: youtube.CaptionCredentials("id", "secret", "refresh"),
    )
    monkeypatch.setattr(youtube, "CaptionsClient", lambda _credentials: _FakeCaptions())
    monkeypatch.setattr(youtube, "_ocw_transcript_video_ids", frozenset)
    monkeypatch.setattr(
        youtube,
        "_fetch_channel_configs",
        lambda **_kw: [
            {"channel_id": "c", "playlists": [{"id": "PL"}], "config_file": "c.yaml"}
        ],
    )
    monkeypatch.setattr(
        youtube, "_video_ids_for_playlist", lambda _playlist_id, _key: iter(["v1"])
    )
    monkeypatch.setattr(youtube, "_yt_paged_items", lambda *_args: iter([{"id": "x"}]))

    source = youtube.youtube_source(api_key="k")
    for resource in source.resources.values():
        list(resource)
    assert resolutions == ["k"]


# Matches the real mitodl/open-video-data format: a YAML *list* of channel dicts.
_CHANNELS_YAML = (
    b"---\n- channel_id: CHAN\n  offered_by: ocw\n  playlists:\n    - id: all\n"
)


def _fake_channel_get(url, params=None, **_kwargs):
    """Serve GitHub config listing + a single channel from the Data API."""
    if "contents" in url:
        return FakeResponse(
            json_data=[
                {"name": "channels.yaml", "url": "https://api/file/channels.yaml"}
            ]
        )
    if "api/file" in url:
        return FakeResponse(
            json_data={"content": base64.b64encode(_CHANNELS_YAML).decode("ascii")}
        )
    if "youtube/v3/channels" in url:
        return FakeResponse(
            json_data={"items": [{"id": "CHAN", "snippet": {"title": "A channel"}}]}
        )
    # The listing needs at least one playlist item, or it refuses the snapshot.
    if "youtube/v3/playlistItems" in url:
        return FakeResponse(json_data={"items": [{"contentDetails": {"videoId": "v"}}]})
    if "youtube/v3/playlists" in url:
        return FakeResponse(json_data={"items": [{"id": "PL"}]})
    return FakeResponse(json_data={"items": []})


def _serve_config_file(monkeypatch, yaml_bytes):
    def _fake_get(url, params=None, **_kwargs):
        if "contents" in url:
            return FakeResponse(
                json_data=[{"name": "channels.yaml", "url": "https://api/f.yaml"}]
            )
        return FakeResponse(
            json_data={"content": base64.b64encode(yaml_bytes).decode("ascii")}
        )

    monkeypatch.setattr(youtube.requests, "get", _fake_get)


@pytest.mark.parametrize(
    "yaml_bytes",
    [
        b"---\n- channel_id: CHAN_A\n- offered_by: no_channel_id\n",
        b"---\n- channel_id: [unclosed\n",
    ],
)
def test_fetch_channel_configs_fails_on_a_bad_file(monkeypatch, yaml_bytes):
    """A bad file fails the run instead of loading a short channel set."""
    _serve_config_file(monkeypatch, yaml_bytes)
    with pytest.raises((ValueError, youtube.yaml.YAMLError)):
        youtube._fetch_channel_configs(
            repo="mitodl/open-video-data", folder="youtube", branch="main", token=None
        )


def test_fetch_channel_configs_parses_yaml_list(monkeypatch):
    """A YAML file that is a list of channel dicts loads every entry."""
    yaml_bytes = b"---\n- channel_id: CHAN_A\n  offered_by: ocw\n- channel_id: CHAN_B\n"
    _serve_config_file(monkeypatch, yaml_bytes)
    configs = youtube._fetch_channel_configs(
        repo="mitodl/open-video-data", folder="youtube", branch="main", token=None
    )
    assert [c["channel_id"] for c in configs] == ["CHAN_A", "CHAN_B"]
    assert {c["config_file"] for c in configs} == {"channels.yaml"}


def test_playlist_ids_bare_string_wildcard(monkeypatch):
    """A bare-string ``all`` entry (not ``{id: all}``) still triggers the wildcard."""
    _queue_get(monkeypatch, [{"items": [{"id": "p1"}, {"id": "p2"}]}])
    channel_config = {"channel_id": "chan", "playlists": ["all"]}
    assert list(youtube._playlist_ids_for_config(channel_config, "k")) == ["p1", "p2"]


def test_channels_resource_builds_record(monkeypatch):
    monkeypatch.setattr(youtube.requests, "get", _fake_channel_get)
    source = youtube.youtube_source(api_key="k")
    rows = list(source.resources["raw__youtube__api__channels"])
    assert len(rows) == 1
    assert rows[0]["channel_id"] == "CHAN"
    assert rows[0]["offered_by"] == "ocw"
    assert rows[0]["config_file"] == "channels.yaml"
    assert rows[0]["etl_source"] == "youtube"
    assert rows[0]["snippet"]["title"] == "A channel"


@pytest.mark.integration
def test_youtube_channels_materialization(test_profile: Path, monkeypatch):
    monkeypatch.setattr(youtube.requests, "get", _fake_channel_get)
    pipeline = config.pipeline_for("youtube")
    source = youtube.youtube_source(api_key="k").with_resources(
        "raw__youtube__api__channels"
    )
    info = pipeline.run(source)
    assert not info.has_failed_jobs

    dataset = pipeline.dataset()
    assert dataset["raw__youtube__api__channels"].arrow().num_rows == 1


@pytest.mark.parametrize(
    ("channel_config", "playlist_id", "expected"),
    [
        # No setting anywhere: Learn's default is to create videos.
        ({"channel_id": "c", "playlists": [{"id": "all"}]}, "p", True),
        # Wildcard-only playlists take the channel setting.
        (
            {"channel_id": "c", "create_videos": False, "playlists": [{"id": "all"}]},
            "p",
            False,
        ),
        # A listed playlist overrides the channel...
        (
            {
                "channel_id": "c",
                "create_videos": False,
                "playlists": [{"id": "p", "create_videos": True}],
            },
            "p",
            True,
        ),
        # ...and falls back to it when it says nothing.
        (
            {"channel_id": "c", "create_videos": False, "playlists": [{"id": "p"}]},
            "p",
            False,
        ),
    ],
)
def test_playlist_create_videos_follows_learn_precedence(
    channel_config, playlist_id, expected
):
    assert youtube._playlist_create_videos(channel_config, playlist_id) is expected


def test_playlists_resource_carries_offered_by_and_create_videos(monkeypatch):
    monkeypatch.setattr(youtube, "_resolve_api_key", lambda _key: "k")
    monkeypatch.setattr(
        youtube,
        "_fetch_channel_configs",
        lambda **_kw: [
            {
                "channel_id": "CHAN",
                "offered_by": "ocw",
                "create_videos": False,
                "playlists": [{"id": "PL"}],
            }
        ],
    )

    def _fake_get(url, params=None, **_kwargs):
        if url.endswith("/playlistItems"):
            return FakeResponse(
                json_data={"items": [{"contentDetails": {"videoId": "v"}}]}
            )
        return FakeResponse(
            json_data={"items": [{"id": "PL", "snippet": {"title": "t"}}]}
        )

    monkeypatch.setattr(youtube.requests, "get", _fake_get)

    source = youtube.youtube_source(api_key="k")
    rows = list(source.resources["raw__youtube__api__playlists"])
    assert len(rows) == 1
    assert rows[0]["playlist_id"] == "PL"
    assert rows[0]["channel_id"] == "CHAN"
    assert rows[0]["offered_by"] == "ocw"
    assert rows[0]["create_videos"] is False


def test_playlist_items_resource_keeps_order_and_first_duplicate(monkeypatch):
    monkeypatch.setattr(youtube, "_resolve_api_key", lambda _key: "k")
    monkeypatch.setattr(
        youtube,
        "_fetch_channel_configs",
        lambda **_kw: [{"channel_id": "CHAN", "playlists": [{"id": "PL"}]}],
    )
    _queue_get(
        monkeypatch,
        [
            {
                "items": [
                    {"contentDetails": {"videoId": "v1"}},
                    {"contentDetails": {"videoId": "v2"}},
                    {"contentDetails": {"videoId": "v1"}},
                    {"contentDetails": {"videoId": "v3"}},
                ]
            }
        ],
    )

    source = youtube.youtube_source(api_key="k")
    rows = list(source.resources["raw__youtube__api__playlist_items"])
    assert [(r["playlist_id"], r["video_id"], r["position"]) for r in rows] == [
        ("PL", "v1", 0),
        ("PL", "v2", 1),
        ("PL", "v3", 3),
    ]


def test_listing_refuses_a_snapshot_with_no_playlist_items(monkeypatch):
    """No playlist items is treated as an outage, not as every playlist emptied."""
    monkeypatch.setattr(youtube, "_resolve_api_key", lambda _key: "k")
    monkeypatch.setattr(
        youtube,
        "_fetch_channel_configs",
        lambda **_kw: [{"channel_id": "c", "playlists": [{"id": "PL"}]}],
    )
    monkeypatch.setattr(
        youtube, "_video_ids_for_playlist", lambda _playlist_id, _key: iter([])
    )
    source = youtube.youtube_source(api_key="k")
    with pytest.raises(ResourceExtractionError, match="no playlist items"):
        list(source.resources["raw__youtube__api__playlist_items"])


def test_table_resource_refuses_an_empty_snapshot(monkeypatch):
    """Every configured channel returning nothing fails instead of loading 0 rows."""
    monkeypatch.setattr(youtube, "_resolve_api_key", lambda _key: "k")
    monkeypatch.setattr(
        youtube,
        "_fetch_channel_configs",
        lambda **_kw: [
            {"channel_id": "c", "playlists": [{"id": "PL"}], "config_file": "c.yaml"}
        ],
    )
    monkeypatch.setattr(
        youtube, "_video_ids_for_playlist", lambda _playlist_id, _key: iter(["v1"])
    )
    monkeypatch.setattr(youtube, "_yt_paged_items", lambda *_args: iter([]))
    source = youtube.youtube_source(api_key="k")
    with pytest.raises(
        ResourceExtractionError, match="youtube_channels produced no rows"
    ):
        list(source.resources["raw__youtube__api__channels"])
