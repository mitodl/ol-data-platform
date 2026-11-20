"""Unit + materialization tests for the YouTube source."""

import base64
import json
import sys
import types
from pathlib import Path
from typing import Any

import dlt
import pytest
from dlt.extract.exceptions import ResourceExtractionError

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


class _FakeTranscriptErrors:
    TranscriptsDisabled = type("TranscriptsDisabled", (Exception,), {})
    VideoUnavailable = type("VideoUnavailable", (Exception,), {})
    RequestBlocked = type("RequestBlocked", (Exception,), {})
    IpBlocked = type("IpBlocked", (RequestBlocked,), {})


def _install_fake_transcript_api(monkeypatch, listings=None):
    """Inject a fake youtube-transcript-api matching the 1.x list() interface.

    ``listings`` maps a video id to the transcripts list() returns, as
    (language_code, is_generated, text or exception) tuples, or to an exception
    list() raises. A video it does not name has one English transcript.
    """
    listings = listings or {}
    calls: list[str] = []

    class _Fetched:
        def __init__(self, text):
            self.text = text

        def to_raw_data(self):
            return [{"text": self.text, "start": 0.0, "duration": 1.0}]

    class _Transcript:
        def __init__(self, language_code, is_generated, text):
            self.language_code = language_code
            self.language = f"Language {language_code}"
            self.is_generated = is_generated
            self._text = text

        def fetch(self):
            if isinstance(self._text, Exception):
                raise self._text
            return _Fetched(self._text)

    class _Api:
        def list(self, video_id):
            calls.append(video_id)
            listing = listings.get(video_id, [("en", False, "hello")])
            if isinstance(listing, Exception):
                raise listing
            return [_Transcript(*transcript) for transcript in listing]

    fake: Any = types.ModuleType("youtube_transcript_api")
    fake.YouTubeTranscriptApi = _Api
    for name in (
        "TranscriptsDisabled",
        "VideoUnavailable",
        "RequestBlocked",
        "IpBlocked",
    ):
        setattr(fake, name, getattr(_FakeTranscriptErrors, name))

    fmt_mod: Any = types.ModuleType("youtube_transcript_api.formatters")

    class _TextFormatter:
        def format_transcript(self, fetched):
            return fetched.text

    fmt_mod.TextFormatter = _TextFormatter
    monkeypatch.setitem(sys.modules, "youtube_transcript_api", fake)
    monkeypatch.setitem(sys.modules, "youtube_transcript_api.formatters", fmt_mod)
    return calls


def _transcript_rows(
    monkeypatch, video_ids, listings=None, languages=None, **source_kwargs
):
    """Run the transcripts resource over fakes.

    ``languages`` maps a video id to the snippet language fields the Data API
    gives it; a video it does not name is spoken in English.
    """
    calls = _install_fake_transcript_api(monkeypatch, listings)
    languages = languages or {}
    snippet_requests: list[str] = []

    def _fake_videos(ids, part, _key):
        snippet_requests.append(part)
        for video_id in ids:
            yield {
                "id": video_id,
                "snippet": languages.get(video_id, {"defaultAudioLanguage": "en"}),
            }

    monkeypatch.setattr(youtube, "_videos", _fake_videos)
    monkeypatch.setattr(youtube, "_resolve_api_key", lambda _key: "k")
    monkeypatch.setattr(
        youtube, "_fetch_channel_configs", lambda **_kw: [{"channel_id": "c"}]
    )
    monkeypatch.setattr(
        youtube, "_playlist_ids_for_config", lambda _config, _key: iter(["pl"])
    )
    monkeypatch.setattr(
        youtube, "_video_ids_for_playlist", lambda _playlist_id, _key: iter(video_ids)
    )
    source = youtube.youtube_source(api_key="k", **source_kwargs)
    rows = list(source.resources["raw__youtube__api__transcripts"])
    return rows, calls, snippet_requests


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


def test_transcripts_land_every_language_uploaded_and_generated(monkeypatch):
    rows, _calls, _snippets = _transcript_rows(
        monkeypatch,
        ["vid1"],
        {
            "vid1": [
                ("en", False, "hello"),
                ("en", True, "hello auto"),
                ("es", False, "hola"),
            ]
        },
    )

    assert _summary(rows) == [
        ("vid1", "en", False, "fetched", "hello", None),
        ("vid1", "es", False, "fetched", "hola", None),
        ("vid1", "en", True, "fetched", "hello auto", None),
        ("vid1", "", False, "fetched", None, None),
    ]
    assert rows[0]["language"] == "Language en"
    assert rows[1]["segments"] == [{"text": "hola", "start": 0.0, "duration": 1.0}]
    assert len({row["fetched_at"] for row in rows}) == 1


def test_a_video_without_transcripts_has_a_status_row_saying_why(monkeypatch):
    rows, _calls, _snippets = _transcript_rows(
        monkeypatch,
        ["disabled", "gone", "broken", "empty"],
        {
            "disabled": _FakeTranscriptErrors.TranscriptsDisabled(),
            "gone": _FakeTranscriptErrors.VideoUnavailable("private"),
            "broken": ValueError("unparsable"),
            "empty": [],
        },
    )

    assert _summary(rows) == [
        ("disabled", "", False, "none", None, None),
        ("gone", "", False, "unavailable", None, "VideoUnavailable: private"),
        ("broken", "", False, "failed", None, "ValueError: unparsable"),
        ("empty", "", False, "none", None, None),
    ]


def test_a_transcript_that_fails_writes_no_row_of_its_own(monkeypatch):
    """So a failed fetch never overwrites the text an earlier run landed."""
    rows, _calls, _snippets = _transcript_rows(
        monkeypatch,
        ["bad-es"],
        {"bad-es": [("en", False, "hello"), ("es", False, ValueError("bad"))]},
    )

    assert _summary(rows) == [
        ("bad-es", "en", False, "fetched", "hello", None),
        ("bad-es", "", False, "failed", None, "es: ValueError: bad"),
    ]


@pytest.mark.parametrize("blocked_by", ["list", "fetch"])
def test_once_youtube_blocks_the_run_no_more_requests_are_made(monkeypatch, blocked_by):
    block = _FakeTranscriptErrors.IpBlocked("429")
    listings = {
        "blocked": block
        if blocked_by == "list"
        else [("en", False, block), ("es", False, "hola")]
    }
    rows, calls, _snippets = _transcript_rows(
        monkeypatch, ["ok", "blocked", "later1", "later2"], listings
    )

    error = "IpBlocked: 429" if blocked_by == "list" else "en: IpBlocked: 429"
    assert calls == ["ok", "blocked"]
    assert _summary(rows) == [
        ("ok", "en", False, "fetched", "hello", None),
        ("ok", "", False, "fetched", None, None),
        ("blocked", "", False, "blocked", None, error),
        ("later1", "", False, "blocked", None, None),
        ("later2", "", False, "blocked", None, None),
    ]


DUBBED = [
    ("en", False, "hello"),
    ("ar", True, "auto ar"),
    ("en", True, "hello auto"),
    ("fr-FR", True, "auto fr"),
]


def _fetched(rows):
    return [
        (row["language_code"], row["is_generated"])
        for row in rows
        if row["language_code"]
    ]


def test_native_tracks_are_the_uploaded_ones_and_the_auto_track_in_the_videos_language(
    monkeypatch,
):
    rows, _calls, snippets = _transcript_rows(
        monkeypatch,
        [
            "dubbed",
            "spanish",
            "no-language",
            "nothing-known",
            "mislabeled",
            "hebrew",
            "title-only",
        ],
        {
            "dubbed": DUBBED,
            "spanish": [("es", True, "hola"), ("en", True, "hello auto")],
            "mislabeled": [("es", True, "hola")],
            "hebrew": [("iw", True, "shalom"), ("en", True, "x")],
            "title-only": [("fr", True, "bonjour"), ("en", True, "x")],
            "no-language": [
                ("de", False, "hallo"),
                ("de", True, "x"),
                ("en", True, "y"),
            ],
            "nothing-known": [("ja", True, "a"), ("ko", True, "b")],
        },
        {
            "spanish": {"defaultAudioLanguage": "es-419"},
            "no-language": {},
            "nothing-known": {},
            "hebrew": {"defaultAudioLanguage": "he"},
            "title-only": {"defaultLanguage": "fr"},
        },
    )

    # One videos.list request per 50 videos, for their languages.
    assert snippets == ["snippet"]
    by_video: dict[str, list[tuple[str, bool]]] = {}
    for row in rows:
        if row["language_code"]:
            by_video.setdefault(row["video_id"], []).append(
                (row["language_code"], row["is_generated"])
            )
    assert by_video == {
        "dubbed": [("en", False), ("en", True)],
        "spanish": [("es", True)],
        # No language from the API: the uploaded track's language is the video's.
        "no-language": [("de", False), ("de", True)],
        # Nothing to go on: every auto-generated track is kept.
        "nothing-known": [("ja", True), ("ko", True)],
        # Labelled English but only Spanish is listed: the label is wrong.
        "mislabeled": [("es", True)],
        "hebrew": [("iw", True)],
        "title-only": [("fr", True)],
    }
    status = next(
        row for row in rows if row["video_id"] == "dubbed" and not row["language_code"]
    )
    assert json.loads(status["listed_tracks"]) == [
        {"language_code": code, "is_generated": generated}
        for code, generated, _text in DUBBED
    ]


def test_all_tracks_fetches_every_listed_track(monkeypatch):
    rows, _calls, snippets = _transcript_rows(
        monkeypatch, ["dubbed"], {"dubbed": DUBBED}, transcript_tracks="all"
    )

    assert snippets == []
    assert _fetched(rows) == [
        ("en", False),
        ("ar", True),
        ("en", True),
        ("fr-FR", True),
    ]


def test_an_unknown_track_choice_is_refused():
    with pytest.raises(ValueError, match="everything"):
        youtube.youtube_source(api_key="k", transcript_tracks="everything")


def test_source_resolves_the_api_key_once_for_all_resources(monkeypatch):
    _install_fake_transcript_api(monkeypatch)
    resolutions: list[str | None] = []

    def _resolve(api_key: str | None) -> str:
        resolutions.append(api_key)
        return "k"

    monkeypatch.setattr(youtube, "_resolve_api_key", _resolve)
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
