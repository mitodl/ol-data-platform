"""MIT Professional Education (MIT PE) feeds ingestion via dlt.

Fetches courses, news and events from the MIT PE feeds API. Each feed is
page-based: incrementing ``page`` from 0 until an empty array is returned. Every
table is fully replaced on each run; ``config.guard_against_replace_truncation``
refuses to commit a fetch whose row count dropped sharply from the last
successful load, so a run that ends early on a transient empty page fails loudly
instead of silently truncating the table. MIT PE has no separate programs
endpoint — programs are mixed into the courses feed.

Data flow:
    MITPE_BASE_URL/feeds/courses/  -> raw__mitpe__api__courses
    MITPE_BASE_URL/feeds/news/     -> raw__mitpe__api__news
    MITPE_BASE_URL/feeds/events/   -> raw__mitpe__api__events

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.mitpe
"""

import logging
import os
from collections.abc import Generator, Iterator
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin

import dlt
from dlt.sources.helpers import requests

from ol_dlt import config

logger = logging.getLogger(__name__)

_MITPE_BASE_URL_DEFAULT = "https://professional.mit.edu"

# The news and events fields staging reads, declared as text so dlt's timestamp
# detection leaves the feed's date strings as the feed sent them, and so a field
# that is empty in every record of a load still gets a column.
NEWS_COLUMNS = ("id", "title", "date", "author", "summary", "image", "url")
EVENT_COLUMNS = (
    "id",
    "title",
    "start_date",
    "end_date",
    "time_range",
    "summary",
    "image",
    "url",
)


def _columns(names: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    return {
        **{name: {"data_type": "text", "nullable": True} for name in names},
        "retrieved_at": {"data_type": "text", "nullable": False},
    }


def _fetch_pages(base_url: str, path: str) -> list[dict[str, Any]]:
    """Return every record of one feed, fetching pages until one is empty."""
    feed_url = urljoin(base_url, path)
    records: list[dict[str, Any]] = []
    page = 0
    while True:
        logger.info("Fetching MIT PE page %d from %s", page, feed_url)
        resp = requests.get(feed_url, params={"page": page}, timeout=30)
        resp.raise_for_status()
        page_records = resp.json()
        if not page_records:
            logger.info("MIT PE %s: reached empty page at page=%d", path, page)
            return records
        records.extend(page_records)
        page += 1


@dlt.source(name="mitpe_ingest")
def mitpe_source(
    base_url: str = _MITPE_BASE_URL_DEFAULT,
) -> Generator[Any]:
    """Load MIT Professional Education courses, news and events.

    Args:
        base_url: Base URL of the MIT PE site (e.g. ``https://professional.mit.edu``).
    """

    def _feed(table_name: str, path: str) -> Iterator[dict[str, Any]]:
        records = _fetch_pages(base_url, path)
        config.guard_against_replace_truncation(table_name, len(records))
        retrieved_at = datetime.now(tz=UTC).isoformat()
        for record in records:
            yield {**record, "retrieved_at": retrieved_at}

    @dlt.resource(
        name="raw__mitpe__api__courses",
        # MIT PE courses have no stable UUID; use title+url as a composite key
        # to deduplicate records across pages within a single run.
        primary_key=["title", "url"],
        write_disposition="replace",
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def courses() -> Generator[dict[str, Any]]:
        """Yield all MIT PE courses, fetching pages until the API returns empty."""
        records = _fetch_pages(base_url, "/feeds/courses/")
        config.guard_against_replace_truncation(
            "raw__mitpe__api__courses", len(records)
        )
        yield from records

    # MIT Learn's news_events app reads these two feeds (news_events/etl/
    # mitpe_news.py and mitpe_events.py). The events feed lists past events
    # too; Learn drops those in its transform, and so does the integrations
    # model, so the raw table keeps them.
    @dlt.resource(
        name="raw__mitpe__api__news",
        primary_key="id",
        write_disposition="replace",
        columns=_columns(NEWS_COLUMNS),
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def news() -> Iterator[dict[str, Any]]:
        yield from _feed("raw__mitpe__api__news", "/feeds/news/")

    @dlt.resource(
        name="raw__mitpe__api__events",
        primary_key="id",
        write_disposition="replace",
        columns=_columns(EVENT_COLUMNS),
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def events() -> Iterator[dict[str, Any]]:
        yield from _feed("raw__mitpe__api__events", "/feeds/events/")

    yield courses
    yield news
    yield events


mitpe_pipeline = config.pipeline_for("mitpe")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source, honouring the base-URL override from the env."""
    return config.with_nullable_load_id(
        mitpe_source(base_url=os.getenv("MITPE_BASE_URL", _MITPE_BASE_URL_DEFAULT))
    )
