"""Unit + materialization tests for the podcast RSS source."""

import base64
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

from ol_dlt import config
from ol_dlt.sources import podcast_rss
from tests.conftest import FakeResponse

_RSS_XML = b"""<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>Test Podcast</title>
    <description>A test feed</description>
    <language>en</language>
    <item>
      <title>Episode 1</title>
      <link>https://example.com/ep1</link>
      <guid>ep-1</guid>
      <enclosure url="https://example.com/ep1.mp3" type="audio/mpeg"/>
    </item>
    <item>
      <title>No audio episode</title>
      <link>https://example.com/ep2</link>
    </item>
  </channel>
</rss>
"""

_YAML = b"rss_url: https://example.com/feed.xml\nwebsite: https://example.com\n"


def _channel_elem() -> ET.Element:
    root = ET.fromstring(_RSS_XML)  # noqa: S314
    channel = root.find("channel")
    assert channel is not None
    return channel


def test_parse_channel_record() -> None:
    record = podcast_rss._parse_channel_record(
        _channel_elem(), {"rss_url": "https://example.com/feed.xml", "website": "x"}
    )
    assert record["readable_id"] == "example.com/feed.xml"
    assert record["title"] == "Test Podcast"
    assert record["etl_source"] == "podcast"


def test_parse_episode_record_skips_items_without_audio() -> None:
    channel = _channel_elem()
    items = channel.findall("item")
    ep1 = podcast_rss._parse_episode_record(items[0], "chan", "rss", None)
    ep2 = podcast_rss._parse_episode_record(items[1], "chan", "rss", None)
    assert ep1 is not None
    assert ep1["readable_id"] == "ep-1"
    assert ep2 is None


def _fake_get(url: str, **_kwargs: object) -> FakeResponse:
    if "contents" in url:
        return FakeResponse(
            json_data=[{"name": "test.yaml", "url": "https://api/file/test.yaml"}]
        )
    if "api/file" in url:
        return FakeResponse(
            json_data={"content": base64.b64encode(_YAML).decode("ascii")}
        )
    return FakeResponse(content=_RSS_XML)


def test_episodes_resource_filters_audioless(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(podcast_rss.requests, "get", _fake_get)
    source = podcast_rss.podcast_rss_source()
    episodes = list(source.resources["raw__podcast__rss__episodes"])
    assert [e["readable_id"] for e in episodes] == ["ep-1"]


def test_configs_and_feed_fetched_once_across_both_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """channels + episodes share one config listing and one feed fetch/parse.

    Both tables are derived from a single upstream ``podcast_feeds`` resource
    (see ``podcast_rss_source``) rather than each independently re-fetching
    GitHub configs and re-fetching/re-parsing every RSS feed.
    """
    feed_fetches = 0
    config_fetches = 0

    def _counting_get(url: str, **kwargs: object) -> object:
        nonlocal feed_fetches, config_fetches
        if "contents" in url or "api/file" in url:
            config_fetches += 1
        else:
            feed_fetches += 1
        return _fake_get(url, **kwargs)

    monkeypatch.setattr(podcast_rss.requests, "get", _counting_get)
    source = podcast_rss.podcast_rss_source()
    items = list(source)

    assert len(items) == 2  # one channel record, one episode record
    assert feed_fetches == 1
    # 1 listing call + 1 per-file fetch for the single configured podcast.
    assert config_fetches == 2


_MALICIOUS_YAML = (
    b"rss_url: https://malicious.example.com/feed.xml\n"
    b"website: https://malicious.example.com\n"
)
_MALICIOUS_RSS_XML = (
    b'<?xml version="1.0"?>'
    b'<!DOCTYPE rss [<!ENTITY xxe "pwned">]>'
    b"<rss><channel><title>&xxe;</title></channel></rss>"
)


def test_malicious_feed_is_skipped_not_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    """A feed defusedxml rejects must be skipped, not crash the whole run.

    defusedxml raises DefusedXmlException (e.g. EntitiesForbidden) for a
    hostile DOCTYPE/entity declaration -- a different exception class than
    ET.ParseError. Both must be caught so one bad feed doesn't abort every
    other configured podcast's channel/episode records.
    """

    def _get(url: str, **_kwargs: object) -> FakeResponse:
        if "contents" in url:
            return FakeResponse(
                json_data=[
                    {"name": "good.yaml", "url": "https://api/file/good.yaml"},
                    {"name": "evil.yaml", "url": "https://api/file/evil.yaml"},
                ]
            )
        if "good.yaml" in url:
            return FakeResponse(
                json_data={"content": base64.b64encode(_YAML).decode("ascii")}
            )
        if "evil.yaml" in url:
            return FakeResponse(
                json_data={"content": base64.b64encode(_MALICIOUS_YAML).decode("ascii")}
            )
        if "malicious.example.com" in url:
            return FakeResponse(content=_MALICIOUS_RSS_XML)
        return FakeResponse(content=_RSS_XML)

    monkeypatch.setattr(podcast_rss.requests, "get", _get)
    source = podcast_rss.podcast_rss_source()
    items = list(source)

    # Only the good feed's channel + episode records survive; the malicious
    # feed is logged and skipped rather than raising out of the whole run.
    assert len(items) == 2
    assert {i.get("rss_url") or i.get("channel_rss_url") for i in items} == {
        "https://example.com/feed.xml"
    }


@pytest.mark.integration
def test_podcast_rss_materialization(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(podcast_rss.requests, "get", _fake_get)
    pipeline = config.pipeline_for("podcast", pipeline_name="podcast_rss")
    info = pipeline.run(podcast_rss.podcast_rss_source())
    assert not info.has_failed_jobs

    dataset = pipeline.dataset()
    assert dataset["raw__podcast__rss__channels"].arrow().num_rows == 1
    assert dataset["raw__podcast__rss__episodes"].arrow().num_rows == 1
