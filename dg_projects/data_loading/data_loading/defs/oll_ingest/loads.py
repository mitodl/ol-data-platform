"""
Open Learning Library (OLL) course ingestion via dlt.

OLL does not expose a REST API. Course metadata is maintained in a Google
Sheets spreadsheet that is exported as CSV. This pipeline fetches that CSV
and loads it into the raw warehouse layer.

Data flow:
    Google Sheets CSV export  ─► raw__oll__google_sheets__courses (Iceberg)

The sheet ID is read from the ``OLL_GOOGLE_SHEETS_ID`` environment variable
(same setting used by MIT Learn). If unset, the pipeline falls back to a
known public sheet ID. In either case the export URL requires no auth.

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

# Public sheet ID used by MIT Learn; overridden via OLL_GOOGLE_SHEETS_ID env var.
_OLL_SHEET_ID_DEFAULT = "1RJrZHCGNFT17prJnVKKpBqS5ZBZP3tIbRQR-rfn5A9E"
_SHEETS_EXPORT_URL = (
    "https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
)


@dlt.source(name="oll_ingest")
def oll_source(
    sheet_id: str = _OLL_SHEET_ID_DEFAULT,
) -> Generator[Any, None, None]:
    """
    Load Open Learning Library course metadata from a Google Sheets CSV export.

    Args:
        sheet_id: Google Sheets document ID. The sheet must be publicly
            accessible (no OAuth required for CSV export).
    """

    @dlt.resource(
        name="raw__oll__google_sheets__courses",
        # OLL courses use a composite readable_id derived from the OLL course
        # code and semester/year. Use the row as-is; dlt will hash if no pk.
        primary_key=["OLL Course", "Semester", "Year"],
        write_disposition="replace",
    )
    def courses() -> Generator[dict[str, Any], None, None]:
        """Fetch and yield all OLL course rows from the Google Sheets CSV."""
        export_url = _SHEETS_EXPORT_URL.format(sheet_id=sheet_id)
        logger.info("Fetching OLL course CSV from %s", export_url)
        resp = requests.get(export_url, timeout=30)
        resp.raise_for_status()
        reader = csv.DictReader(StringIO(resp.content.decode("utf-8")))
        yield from reader

    yield courses


# ---------------------------------------------------------------------------
# Module-level instances referenced by defs.yaml
# ---------------------------------------------------------------------------

_dagster_env = os.getenv("DAGSTER_ENVIRONMENT", "dev")

if _dagster_env in ("qa", "production"):
    _destination_name = f"oll_{_dagster_env}"
    _dataset_name = f"ol_warehouse_{_dagster_env}_raw"
else:
    _destination_name = "oll_local"
    _dataset_name = "oll_local"

oll_load_source = oll_source(
    sheet_id=os.getenv("OLL_GOOGLE_SHEETS_ID", _OLL_SHEET_ID_DEFAULT),
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
