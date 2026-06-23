"""
MIT Professional Education (MIT PE) course ingestion via dlt.

Fetches courses from the MIT PE feeds API. The feed is page-based:
incrementing ``page`` from 0 until an empty array is returned.
The table is fully replaced on each run (write_disposition="replace").

Note: MIT PE does not have a separate /feeds/programs/ endpoint. Programs
are mixed in with courses in the /feeds/courses/ feed.

Data flow:
    MITPE_BASE_URL/feeds/courses/  ─► raw__mitpe__api__courses  (Iceberg)

Usage (standalone):
    python -m data_loading.defs.mitpe_ingest.loads
"""

import logging
import os
from collections.abc import Generator
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import dlt
import requests

logger = logging.getLogger(__name__)

_DLT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent
if _DLT_PROJECT_DIR.exists():
    os.environ.setdefault("DLT_PROJECT_DIR", str(_DLT_PROJECT_DIR))

_MITPE_BASE_URL_DEFAULT = "https://professional.mit.edu"


@dlt.source(name="mitpe_ingest")
def mitpe_source(
    base_url: str = _MITPE_BASE_URL_DEFAULT,
) -> Generator[Any, None, None]:
    """
    Load MIT Professional Education courses from the feeds API.

    The feed paginates via a ``page`` query parameter (0-indexed). Fetching
    stops when the response is an empty array.

    Args:
        base_url: Base URL of the MIT PE site (e.g. ``https://professional.mit.edu``).
    """

    @dlt.resource(
        name="raw__mitpe__api__courses",
        # MIT PE courses have no stable UUID; use title+url as a composite key
        # to deduplicate records across pages within a single run.
        primary_key=["title", "url"],
        write_disposition="replace",
        table_format=_table_format,
    )
    def courses() -> Generator[dict[str, Any], None, None]:
        """Yield all MIT PE courses, fetching pages until the API returns empty."""
        feed_url = urljoin(base_url, "/feeds/courses/")
        page = 0
        while True:
            logger.info("Fetching MIT PE courses page %d from %s", page, feed_url)
            resp = requests.get(feed_url, params={"page": page}, timeout=30)
            resp.raise_for_status()
            records = resp.json()
            if not records:
                logger.info("MIT PE courses: reached empty page at page=%d", page)
                break
            yield from records
            page += 1

    yield courses


# ---------------------------------------------------------------------------
# Module-level instances referenced by defs.yaml
# ---------------------------------------------------------------------------

_dagster_env = os.getenv("DAGSTER_ENVIRONMENT", "dev")

if _dagster_env in ("qa", "production"):
    _destination_name = f"mitpe_{_dagster_env}"
    _dataset_name = f"ol_warehouse_{_dagster_env}_raw"
    _table_format = "iceberg"
else:
    _destination_name = "mitpe_local"
    _dataset_name = "mitpe_local"
    _table_format = "parquet"

mitpe_load_source = mitpe_source(
    base_url=os.getenv("MITPE_BASE_URL", _MITPE_BASE_URL_DEFAULT),
)

mitpe_pipeline = dlt.pipeline(
    pipeline_name="mitpe",
    destination=_destination_name,
    dataset_name=_dataset_name,
    progress="log",
)


def _run_pipeline() -> None:
    """Execute the MIT PE pipeline (for standalone testing)."""
    logging.basicConfig(level=logging.INFO)
    logger.info(
        "Running MIT PE pipeline: destination=%s dataset=%s",
        _destination_name,
        _dataset_name,
    )
    load_info = mitpe_pipeline.run(mitpe_load_source)
    logger.info("Pipeline completed: %s", load_info)


if __name__ == "__main__":
    _run_pipeline()
