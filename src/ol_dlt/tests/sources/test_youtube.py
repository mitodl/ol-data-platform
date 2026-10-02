"""Unit + materialization tests for the YouTube source."""

import base64
import sys
import types
from pathlib import Path
from typing import Any

import dlt
import pytest

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


def _install_fake_transcript_api(monkeypatch):
    """Inject a fake youtube-transcript-api matching the 1.x fetch() interface."""

    class _Fetched:
        def to_raw_data(self):
            return [{"text": "hello", "start": 0.0, "duration": 1.0}]

    class _Api:
        def fetch(self, _video_id):
            return _Fetched()

    fake: Any = types.ModuleType("youtube_transcript_api")
    fake.YouTubeTranscriptApi = _Api
    fake.NoTranscriptFound = type("NoTranscriptFound", (Exception,), {})
    fake.TranscriptsDisabled = type("TranscriptsDisabled", (Exception,), {})
    fake.VideoUnavailable = type("VideoUnavailable", (Exception,), {})

    fmt_mod: Any = types.ModuleType("youtube_transcript_api.formatters")

    class _TextFormatter:
        def format_transcript(self, _fetched):
            return "hello"

    fmt_mod.TextFormatter = _TextFormatter
    monkeypatch.setitem(sys.modules, "youtube_transcript_api", fake)
    monkeypatch.setitem(sys.modules, "youtube_transcript_api.formatters", fmt_mod)


def test_transcripts_resource_yields_formatted_text(monkeypatch):
    _install_fake_transcript_api(monkeypatch)
    monkeypatch.setattr(youtube, "_resolve_api_key", lambda _key: "k")
    monkeypatch.setattr(
        youtube, "_fetch_channel_configs", lambda **_kw: [{"channel_id": "c"}]
    )
    monkeypatch.setattr(
        youtube, "_playlist_ids_for_config", lambda _config, _key: iter(["pl"])
    )
    monkeypatch.setattr(
        youtube, "_video_ids_for_playlist", lambda _playlist_id, _key: iter(["vid1"])
    )

    source = youtube.youtube_source(api_key="k")
    rows = list(source.resources["raw__youtube__api__transcripts"])
    assert rows == [
        {
            "video_id": "vid1",
            "etl_source": "youtube",
            "transcript": "hello",
            "segments": [{"text": "hello", "start": 0.0, "duration": 1.0}],
        }
    ]


def test_source_resolves_the_api_key_once_for_all_resources(monkeypatch):
    _install_fake_transcript_api(monkeypatch)
    resolutions: list[str | None] = []

    def _resolve(api_key: str | None) -> str:
        resolutions.append(api_key)
        return "k"

    monkeypatch.setattr(youtube, "_resolve_api_key", _resolve)
    monkeypatch.setattr(youtube, "_fetch_channel_configs", lambda **_kw: [])

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
            return FakeResponse(json_data={"items": []})
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
