"""MIT Climate Portal article ingestion via dlt.

Fetches articles from two MIT Climate JSON feeds (Explainers and Ask MIT
Climate). Both feeds return a flat JSON array with no pagination. Records are
upserted by UUID so re-runs are idempotent. Both feeds land in a single
``raw__mit_climate__api__articles`` table; the ``feed_type`` column
distinguishes them.

Data flow:
    explainers feed  -+
                      +-> raw__mit_climate__api__articles
    ask-mit-climate  -+

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.mit_climate
"""

import logging
import os
from collections.abc import Generator
from typing import Any

import dlt
from dlt.sources.helpers import requests

from ol_dlt import config

logger = logging.getLogger(__name__)

_EXPLAINERS_URL_DEFAULT = "https://climate.mit.edu/feeds/explainers"
_ASK_URL_DEFAULT = "https://climate.mit.edu/feeds/ask-mit-climate"


@dlt.source(name="mit_climate_ingest")
def mit_climate_source(
    explainers_url: str = _EXPLAINERS_URL_DEFAULT,
    ask_url: str = _ASK_URL_DEFAULT,
) -> Generator[Any]:
    """Load MIT Climate Portal articles from two JSON feed endpoints.

    Args:
        explainers_url: Full URL of the Explainers JSON feed.
        ask_url: Full URL of the Ask MIT Climate JSON feed.
    """

    @dlt.resource(
        name="raw__mit_climate__api__articles",
        primary_key="uuid",
        write_disposition="merge",
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def articles() -> Generator[dict[str, Any]]:
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


mit_climate_pipeline = config.pipeline_for("mit_climate")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source, honouring URL overrides from the environment."""
    explainers = os.getenv("MIT_CLIMATE_EXPLAINERS_API_URL", _EXPLAINERS_URL_DEFAULT)
    ask = os.getenv("ASK_MIT_CLIMATE_API_URL", _ASK_URL_DEFAULT)
    return mit_climate_source(explainers_url=explainers, ask_url=ask)
