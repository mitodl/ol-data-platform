"""
Open Learning Library (OLL) course ingestion via dlt.

OLL course metadata is maintained as a static CSV in the mit-learn repository
at ``learning_resources/data/oll_metadata.csv``. This pipeline fetches that
CSV and loads it into the raw warehouse layer.

Data flow:
    mit-learn oll_metadata.csv  ─► raw__oll__google_sheets__courses (Iceberg)

Usage (standalone):
    python -m data_loading.defs.oll_ingest.loads
"""

import csv
import logging
import os
from collections.abc import Generator
from io import StringIO
from pathlib import Path
from typing import Any

import dlt
import requests

logger = logging.getLogger(__name__)

_DLT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent
if _DLT_PROJECT_DIR.exists():
    os.environ.setdefault("DLT_PROJECT_DIR", str(_DLT_PROJECT_DIR))

_OLL_METADATA_CSV_URL = (
    "https://raw.githubusercontent.com/mitodl/mit-learn/main"
    "/learning_resources/data/oll_metadata.csv"
)


@dlt.source(name="oll_ingest")
def oll_source(
    csv_url: str = _OLL_METADATA_CSV_URL,
) -> Generator[Any, None, None]:
    """
    Load Open Learning Library course metadata from the mit-learn static CSV.

    Args:
        csv_url: URL of the OLL metadata CSV file.
    """

    @dlt.resource(
        name="raw__oll__google_sheets__courses",
        primary_key=["OLL Course", "Semester", "Year"],
        write_disposition="replace",
        table_format=_table_format,
    )
    def courses() -> Generator[dict[str, Any], None, None]:
        """Fetch and yield all OLL course rows."""
        logger.info("Fetching OLL course CSV from %s", csv_url)
        resp = requests.get(csv_url, timeout=30)
        resp.raise_for_status()
        yield from csv.DictReader(StringIO(resp.content.decode("utf-8")))

    yield courses


# ---------------------------------------------------------------------------
# Module-level instances referenced by defs.yaml
# ---------------------------------------------------------------------------

_dagster_env = os.getenv("DAGSTER_ENVIRONMENT", "dev")
_table_format = "iceberg"

if _dagster_env in ("qa", "production"):
    _destination_name = f"oll_{_dagster_env}"
    _dataset_name = f"ol_warehouse_{_dagster_env}_raw"
else:
    _destination_name = "oll_local"
    _dataset_name = "oll_local"

oll_load_source = oll_source()

oll_pipeline = dlt.pipeline(
    pipeline_name="oll",
    destination=_destination_name,
    dataset_name=_dataset_name,
    progress="log",
)


def _run_pipeline() -> None:
    """Execute the OLL pipeline (for standalone testing)."""
    logging.basicConfig(level=logging.INFO)
    logger.info(
        "Running OLL pipeline: destination=%s dataset=%s",
        _destination_name,
        _dataset_name,
    )
    load_info = oll_pipeline.run(oll_load_source)
    logger.info("Pipeline completed: %s", load_info)


if __name__ == "__main__":
    _run_pipeline()
