"""
MIT Climate Portal article ingestion via dlt.

Fetches articles from two MIT Climate JSON feeds:
  - Explainers: general explainer articles
  - Ask MIT Climate: Q&A style articles

Both feeds return a flat JSON array with no pagination. Records are
upserted by UUID so re-runs are safe and idempotent. Both feeds land in
a single ``raw__mit_climate__api__articles`` table; the ``feed_type`` column
distinguishes them.

Data flow:
    MIT_CLIMATE_EXPLAINERS_API_URL  ─┐
                                     ├─► raw__mit_climate__api__articles (Iceberg)
    ASK_MIT_CLIMATE_API_URL         ─┘

Usage (standalone):
    python -m data_loading.defs.mit_climate_ingest.loads
"""

import logging
import os
from collections.abc import Generator
from pathlib import Path
from typing import Any

import dlt
import requests

logger = logging.getLogger(__name__)

# Resolve dlt project dir so config.toml is found when run from repo root
_DLT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent
if _DLT_PROJECT_DIR.exists():
    os.environ.setdefault("DLT_PROJECT_DIR", str(_DLT_PROJECT_DIR))

_EXPLAINERS_URL_DEFAULT = "https://climate.mit.edu/api/explainers"
_ASK_URL_DEFAULT = "https://climate.mit.edu/api/ask-mit-climate"


@dlt.source(name="mit_climate_ingest")
def mit_climate_source(
    explainers_url: str = _EXPLAINERS_URL_DEFAULT,
    ask_url: str = _ASK_URL_DEFAULT,
) -> Generator[Any, None, None]:
    """
    Load MIT Climate Portal articles from two JSON feed endpoints.

    Both feeds are fetched in a single resource pass so there is only one
    HTTP round-trip per feed URL per pipeline run. The ``feed_type`` column
    (``"explainer"`` or ``"ask_mit_climate"``) is added to every row.

    Args:
        explainers_url: Full URL of the Explainers JSON feed.
        ask_url: Full URL of the Ask MIT Climate JSON feed.
    """

    @dlt.resource(
        name="raw__mit_climate__api__articles",
        primary_key="uuid",
        write_disposition="merge",
    )
    def articles() -> Generator[dict[str, Any], None, None]:
        """Yield articles from both MIT Climate feeds, tagged by feed_type."""
        feeds = [
            (explainers_url, "explainer"),
            (ask_url, "ask_mit_climate"),
        ]
        for url, feed_type in feeds:
            logger.info("Fetching MIT Climate feed: %s", url)
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            for record in resp.json():
                yield {**record, "feed_type": feed_type}

    yield articles


# ---------------------------------------------------------------------------
# Module-level instances referenced by defs.yaml
# ---------------------------------------------------------------------------

_dagster_env = os.getenv("DAGSTER_ENVIRONMENT", "dev")

if _dagster_env in ("qa", "production"):
    _destination_name = f"mit_climate_{_dagster_env}"
    _dataset_name = f"ol_warehouse_{_dagster_env}_raw"
else:
    _destination_name = "mit_climate_local"
    _dataset_name = "mit_climate_local"

mit_climate_load_source = mit_climate_source(
    explainers_url=os.getenv("MIT_CLIMATE_EXPLAINERS_API_URL", _EXPLAINERS_URL_DEFAULT),
    ask_url=os.getenv("ASK_MIT_CLIMATE_API_URL", _ASK_URL_DEFAULT),
)

mit_climate_pipeline = dlt.pipeline(
    pipeline_name="mit_climate",
    destination=_destination_name,
    dataset_name=_dataset_name,
    progress="log",
)


def _run_pipeline() -> None:
    """Execute the MIT Climate pipeline (for standalone testing)."""
    logging.basicConfig(level=logging.INFO)
    logger.info(
        "Running MIT Climate pipeline: destination=%s dataset=%s",
        _destination_name,
        _dataset_name,
    )
    load_info = mit_climate_pipeline.run(mit_climate_load_source)
    logger.info("Pipeline completed: %s", load_info)


if __name__ == "__main__":
    _run_pipeline()
