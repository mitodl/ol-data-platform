"""Open Learning Library (OLL) course ingestion via dlt.

OLL course metadata is maintained as a static CSV in the mit-learn repository
at ``learning_resources/data/oll_metadata.csv``. This pipeline fetches that CSV
and loads it into the raw warehouse layer.

Data flow:
    mit-learn oll_metadata.csv  -> raw__oll__google_sheets__courses

Run standalone (writes to the active DLT_PROFILE's destination):
    DLT_PROFILE=dev python -m ol_dlt.sources.oll
"""

import csv
import logging
from collections.abc import Generator
from io import StringIO
from typing import Any

import dlt
from dlt.sources.helpers import requests

from ol_dlt import config

logger = logging.getLogger(__name__)

_OLL_METADATA_CSV_URL = (
    "https://raw.githubusercontent.com/mitodl/mit-learn/main"
    "/learning_resources/data/oll_metadata.csv"
)


@dlt.source(name="oll_ingest")
def oll_source(
    csv_url: str = _OLL_METADATA_CSV_URL,
) -> Generator[Any]:
    """Load Open Learning Library course metadata from the mit-learn static CSV.

    Args:
        csv_url: URL of the OLL metadata CSV file.
    """

    @dlt.resource(
        name="raw__oll__google_sheets__courses",
        primary_key=["readable_id"],
        write_disposition="replace",
        table_format=config.active_table_format(),
    )
    def courses() -> Generator[dict[str, Any]]:
        """Fetch and yield all OLL course rows."""
        logger.info("Fetching OLL course CSV from %s", csv_url)
        resp = requests.get(csv_url, timeout=30)
        resp.raise_for_status()
        records = list(csv.DictReader(StringIO(resp.content.decode("utf-8"))))
        config.guard_against_replace_truncation(
            "raw__oll__google_sheets__courses", len(records)
        )
        yield from records

    yield courses


oll_pipeline = config.pipeline_for("oll")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the OLL source (uniform entrypoint for the Dagster wrapper)."""
    return oll_source()
