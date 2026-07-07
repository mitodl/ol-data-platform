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
