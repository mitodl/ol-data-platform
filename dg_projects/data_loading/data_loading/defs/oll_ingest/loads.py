"""
Open Learning Library (OLL) course ingestion via dlt.

OLL does not expose a REST API. Course metadata is maintained in a Google
Sheets spreadsheet that is exported as CSV. This pipeline fetches that CSV
and loads it into the raw warehouse layer.

Data flow:
    Google Sheets CSV export  ─► raw__oll__google_sheets__courses (Iceberg)

The Google Sheets document ID is read from the ``OLL_GOOGLE_SHEETS_ID``
environment variable (same setting used by MIT Learn). When unset, the
pipeline falls back to the static snapshot CSV committed to the mit-learn
repository at ``learning_resources/data/oll_metadata.csv``. The static CSV
has the same column layout as the live sheet and is sufficient for local
development and CI.

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

_SHEETS_EXPORT_URL = (
    "https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
)
# Static snapshot of OLL metadata committed to the mit-learn repo.
# Used as a fallback when OLL_GOOGLE_SHEETS_ID is not configured.
_OLL_STATIC_CSV_URL = (
    "https://raw.githubusercontent.com/mitodl/mit-learn/main"
    "/learning_resources/data/oll_metadata.csv"
)


def _fetch_oll_csv(sheet_id: str | None) -> str:
    """Return OLL course CSV content from the live sheet or the static fallback."""
    if sheet_id:
        url = _SHEETS_EXPORT_URL.format(sheet_id=sheet_id)
        logger.info("Fetching OLL course CSV from Google Sheets: %s", url)
    else:
        url = _OLL_STATIC_CSV_URL
        logger.info(
            "OLL_GOOGLE_SHEETS_ID not set — falling back to static CSV: %s", url
        )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.content.decode("utf-8")


@dlt.source(name="oll_ingest")
def oll_source(
    sheet_id: str | None = None,
) -> Generator[Any, None, None]:
    """
    Load Open Learning Library course metadata from a Google Sheets CSV export.

    Args:
        sheet_id: Google Sheets document ID. When provided, the live sheet is
            fetched (no OAuth required — the sheet must be publicly accessible).
            When ``None`` (default), the static snapshot CSV from the mit-learn
            repository is used instead, allowing local development without
            credentials.
    """

    @dlt.resource(
        name="raw__oll__google_sheets__courses",
        # OLL courses use a composite key from course code + semester + year.
        # Deduplicates rows within a single run; replace disposition drops the
        # table each run so there is no cross-run merging.
        primary_key=["OLL Course", "Semester", "Year"],
        write_disposition="replace",
        table_format=_table_format,
    )
    def courses() -> Generator[dict[str, Any], None, None]:
        """Fetch and yield all OLL course rows."""
        csv_content = _fetch_oll_csv(sheet_id)
        reader = csv.DictReader(StringIO(csv_content))
        yield from reader

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

oll_load_source = oll_source(
    sheet_id=os.getenv("OLL_GOOGLE_SHEETS_ID") or None,
)

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
