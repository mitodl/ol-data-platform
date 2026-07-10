"""MIT Professional Education (MIT PE) course ingestion via dlt.

Fetches courses from the MIT PE feeds API. The feed is page-based: incrementing
``page`` from 0 until an empty array is returned. The table is fully replaced
on each run; ``config.guard_against_replace_truncation`` refuses to commit a
fetch whose row count dropped sharply from the last successful load, so a run
that ends early on a transient empty page fails loudly instead of silently
truncating the table. MIT PE has no separate programs endpoint — programs are
mixed into the courses feed.

Data flow:
    MITPE_BASE_URL/feeds/courses/  -> raw__mitpe__api__courses

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.mitpe
"""

import logging
import os
from collections.abc import Generator
from typing import Any
from urllib.parse import urljoin

import dlt
from dlt.sources.helpers import requests

from ol_dlt import config

logger = logging.getLogger(__name__)

_MITPE_BASE_URL_DEFAULT = "https://professional.mit.edu"


@dlt.source(name="mitpe_ingest")
def mitpe_source(
    base_url: str = _MITPE_BASE_URL_DEFAULT,
) -> Generator[Any]:
    """Load MIT Professional Education courses from the feeds API.

    Args:
        base_url: Base URL of the MIT PE site (e.g. ``https://professional.mit.edu``).
    """

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
        feed_url = urljoin(base_url, "/feeds/courses/")
        records: list[dict[str, Any]] = []
        page = 0
        while True:
            logger.info("Fetching MIT PE courses page %d from %s", page, feed_url)
            resp = requests.get(feed_url, params={"page": page}, timeout=30)
            resp.raise_for_status()
            page_records = resp.json()
            if not page_records:
                logger.info("MIT PE courses: reached empty page at page=%d", page)
                break
            records.extend(page_records)
            page += 1

        config.guard_against_replace_truncation(
            "raw__mitpe__api__courses", len(records)
        )
        yield from records

    yield courses


mitpe_pipeline = config.pipeline_for("mitpe")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source, honouring the base-URL override from the env."""
    return mitpe_source(base_url=os.getenv("MITPE_BASE_URL", _MITPE_BASE_URL_DEFAULT))
