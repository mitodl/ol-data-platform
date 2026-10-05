"""MIT Open Learning's Medium publication ingestion via dlt.

Loads the posts in the publication's RSS feed, which MIT Learn's news_events app
reads as its "MIT Open Learning - Medium" news source
(news_events/etl/medium_mit_news.py). Medium's feed carries only the latest ten
posts, and Learn keeps exactly what the feed holds, so the table is fully
replaced on every run, behind ``config.guard_against_replace_truncation``.

Each post row repeats the channel's title, description and image, which Learn
loads as the feed source.

Data flow:
    https://medium.com/feed/open-learning -> raw__medium__rss__posts

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.medium
"""

import json
import logging
from collections.abc import Generator, Iterator
from datetime import UTC, datetime
from typing import Any
from xml.etree.ElementTree import Element

import dlt
from defusedxml import ElementTree as ET  # noqa: N817
from dlt.sources.helpers import requests

from ol_dlt import config

logger = logging.getLogger(__name__)

MEDIUM_FEED_URL = "https://medium.com/feed/open-learning"
_CONTENT_NS = "http://purl.org/rss/1.0/modules/content/"
_DC_NS = "http://purl.org/dc/elements/1.1/"
_ATOM_NS = "http://www.w3.org/2005/Atom"
# Medium answers a bare requests User-Agent with a bot challenge.
_RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

POST_COLUMNS = (
    "guid",
    "title",
    "link",
    "description",
    "content_encoded",
    "creators",
    "categories",
    "pub_date",
    "updated",
    "feed_url",
    "feed_title",
    "feed_description",
    "feed_image_url",
    "feed_image_title",
    "retrieved_at",
)


def _text(parent: Element | None, tag: str) -> str | None:
    elem = parent.find(tag) if parent is not None else None
    return elem.text if elem is not None else None


def post_records(
    feed_xml: bytes, feed_url: str, retrieved_at: str
) -> list[dict[str, Any]]:
    """Flatten an RSS document into one row per <item>."""
    channel = ET.fromstring(feed_xml).find("channel")
    if channel is None:
        msg = f"No <channel> element in the RSS at {feed_url}"
        raise ValueError(msg)
    image = channel.find("image")
    feed_fields = {
        "feed_url": feed_url,
        "feed_title": _text(channel, "title"),
        "feed_description": _text(channel, "description"),
        "feed_image_url": _text(image, "url"),
        "feed_image_title": _text(image, "title"),
    }
    return [
        {
            "guid": _text(item, "guid"),
            "title": _text(item, "title"),
            "link": _text(item, "link"),
            "description": _text(item, "description"),
            "content_encoded": _text(item, f"{{{_CONTENT_NS}}}encoded"),
            "creators": json.dumps(
                [elem.text for elem in item.findall(f"{{{_DC_NS}}}creator")]
            ),
            "categories": json.dumps(
                [elem.text for elem in item.findall("category") if elem.text]
            ),
            "pub_date": _text(item, "pubDate"),
            "updated": _text(item, f"{{{_ATOM_NS}}}updated"),
            **feed_fields,
            "retrieved_at": retrieved_at,
        }
        for item in channel.findall("item")
    ]


@dlt.source(name="medium_ingest")
def medium_source(feed_url: str = MEDIUM_FEED_URL) -> Generator[Any]:
    """Load the posts in a Medium publication's RSS feed.

    Args:
        feed_url: The publication's RSS feed URL.
    """

    @dlt.resource(
        name="raw__medium__rss__posts",
        primary_key="guid",
        write_disposition="replace",
        columns={
            name: {"data_type": "text", "nullable": name != "retrieved_at"}
            for name in POST_COLUMNS
        },
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def posts() -> Iterator[dict[str, Any]]:
        logger.info("Fetching Medium RSS from %s", feed_url)
        resp = requests.get(feed_url, headers=_RSS_HEADERS, timeout=30)
        resp.raise_for_status()
        records = post_records(resp.content, feed_url, datetime.now(tz=UTC).isoformat())
        config.guard_against_replace_truncation("raw__medium__rss__posts", len(records))
        yield from records

    yield posts


medium_pipeline = config.pipeline_for("medium")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source (uniform entrypoint for Dagster)."""
    return config.with_nullable_load_id(medium_source())
