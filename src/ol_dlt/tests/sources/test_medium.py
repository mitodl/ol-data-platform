"""Unit + materialization tests for the Medium RSS source."""

import json
from pathlib import Path

import pytest

from ol_dlt import config
from ol_dlt.sources import medium
from tests.conftest import FakeResponse

_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:dc="http://purl.org/dc/elements/1.1/"
     xmlns:content="http://purl.org/rss/1.0/modules/content/"
     xmlns:atom="http://www.w3.org/2005/Atom" version="2.0">
  <channel>
    <title><![CDATA[MIT Open Learning - Medium]]></title>
    <description><![CDATA[News, ideas, and thought leadership - Medium]]></description>
    <image>
      <url>https://cdn-images-1.medium.com/proxy/logo.png</url>
      <title>MIT Open Learning - Medium</title>
    </image>
    <item>
      <title><![CDATA[A post]]></title>
      <link>https://medium.com/open-learning/a-post-d9ccc1667ae6?source=rss</link>
      <guid isPermaLink="false">https://medium.com/p/d9ccc1667ae6</guid>
      <category><![CDATA[open-education]]></category>
      <category><![CDATA[mit]]></category>
      <dc:creator><![CDATA[MIT Open Learning]]></dc:creator>
      <pubDate>Wed, 18 Feb 2026 14:28:25 GMT</pubDate>
      <atom:updated>2026-02-18T14:28:27.228Z</atom:updated>
      <content:encoded><![CDATA[<figure><img alt="A" src="https://cdn/a.jpeg" />
        </figure><p>Text</p>]]></content:encoded>
    </item>
  </channel>
</rss>
"""


def _fake_get(_url: str, **_kwargs: object) -> FakeResponse:
    return FakeResponse(content=_RSS)


def test_post_records(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(medium.requests, "get", _fake_get)
    (post,) = list(medium.medium_source().resources["raw__medium__rss__posts"])
    assert post["guid"] == "https://medium.com/p/d9ccc1667ae6"
    assert post["content_encoded"].startswith("<figure><img")
    assert post["description"] is None
    assert json.loads(post["categories"]) == ["open-education", "mit"]
    assert json.loads(post["creators"]) == ["MIT Open Learning"]
    assert post["pub_date"] == "Wed, 18 Feb 2026 14:28:25 GMT"
    assert post["feed_title"] == "MIT Open Learning - Medium"
    assert post["feed_image_url"] == "https://cdn-images-1.medium.com/proxy/logo.png"


def test_feed_without_channel_fails() -> None:
    with pytest.raises(ValueError, match="No <channel>"):
        medium.post_records(b"<rss/>", "https://example", "now")


@pytest.mark.integration
def test_medium_materialization(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(medium.requests, "get", _fake_get)
    pipeline = config.pipeline_for("medium")
    info = pipeline.run(medium.medium_source())
    assert not info.has_failed_jobs

    table = pipeline.dataset()["raw__medium__rss__posts"].arrow()
    assert table.num_rows == 1
    assert table.schema.field("description").type == "string"
