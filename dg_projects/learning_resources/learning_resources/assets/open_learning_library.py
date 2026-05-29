"""
Open Learning Library (OLL) webhook delivery asset.

OLL hosts courses that are no longer active on edX.org or MITx Online. The
course metadata is maintained in a Google Sheets spreadsheet exported as CSV.
This asset fetches that CSV, transforms each row into MIT Learn's
LearningResource payload shape, and delivers the full catalog as a single
signed webhook POST.

The raw CSV is also ingested to the warehouse separately by the
``oll_ingest`` dlt pipeline in the ``data_loading`` code location.

Scheduling: daily at 06:30 UTC. Configured in definitions.py.

Notes on the OLL source:
- OCW-origin courses in OLL (``Offered by = OCW``) are skipped to avoid
  duplicating records already ingested by the OCW Trino-pull path.
- Readable IDs follow the same derivation as ``learning_resources/etl/oll.py``:
  ``MITx+{course_code}`` for MITx courses, OCW-style for OCW courses.
"""

import logging
import math
import os
import re
from csv import DictReader
from io import StringIO
from typing import Any

import httpx2 as httpx
import requests
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MetadataValue,
    RetryPolicy,
    asset,
)
from ol_orchestrate.resources.api_client_factory import ApiClientFactory


def _slugify(text: str) -> str:
    """Simple slugify: lowercase, replace non-alphanumeric runs with hyphens."""
    text = text.lower().strip()
    return re.sub(r"[^\w]+", "-", text).strip("-")

log = logging.getLogger(__name__)

_OLL_SHEET_ID_DEFAULT = "1RJrZHCGNFT17prJnVKKpBqS5ZBZP3tIbRQR-rfn5A9E"
_SHEETS_EXPORT_URL = (
    "https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
)

# OLL course IDs that duplicate OCW courses — skip to avoid collisions.
_SKIP_OCW_COURSES = {"OCW+18.031+2019_Spring"}


def _fetch_oll_csv(sheet_id: str) -> list[dict[str, Any]]:
    """Fetch OLL course CSV from Google Sheets and return rows as dicts."""
    url = _SHEETS_EXPORT_URL.format(sheet_id=sheet_id)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return list(DictReader(StringIO(resp.content.decode("utf-8"))))


def _parse_readable_id(row: dict[str, Any], semester: str, year: str) -> str:
    """Derive a stable readable_id from OLL row data."""
    course_code = row.get("OLL Course", "")
    if row.get("Offered by") == "OCW":
        return f"{course_code}+{_slugify(semester)}_{year}"
    return f"MITx+{course_code}"


def _parse_duration(row: dict[str, Any]) -> str | None:
    """Return a human-readable duration string, or None if missing."""
    raw = row.get("Duration")
    if not raw:
        return None
    try:
        weeks = math.ceil(float(raw))
        return f"{weeks} weeks"
    except (ValueError, TypeError):
        return raw


def _parse_topics(row: dict[str, Any]) -> list[dict[str, str]]:
    """Parse pipe-separated subject strings into dicts."""
    raw = row.get("Subjects") or row.get("Topics") or ""
    return [{"name": t.strip()} for t in raw.split("|") if t.strip()]


def _transform_course(row: dict[str, Any]) -> dict[str, Any] | None:
    """
    Transform a raw OLL CSV row into MIT Learn's resource shape.

    Returns None for rows that should be skipped (OCW duplicates, empty IDs).
    """
    semester = row.get("Semester", "")
    year = row.get("Year", "")
    readable_id = _parse_readable_id(row, semester, year)

    if readable_id in _SKIP_OCW_COURSES:
        return None

    image_url = row.get("Course Image URL Flat")
    run_id = row.get("Run ID") or readable_id

    return {
        "readable_id": readable_id,
        "title": row.get("Title") or row.get("title", ""),
        "url": row.get("URL"),
        "description": row.get("Description"),
        "image": {"url": image_url, "alt": row.get("Title", "")} if image_url else None,
        "topics": _parse_topics(row),
        "published": True,
        "etl_source": "oll",
        "offered_by": {"code": "oll"},
        "platform": "oll",
        "resource_type": "course",
        "runs": [
            {
                "run_id": run_id,
                "title": row.get("Title", ""),
            }
        ],
        "length": _parse_duration(row),
    }


@asset(
    key=AssetKey(["mit_learn_delivery", "oll_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Fetch the Open Learning Library course catalog from Google Sheets CSV, "
        "transform to MIT Learn resource shape, and POST as a signed webhook batch."
    ),
    retry_policy=RetryPolicy(max_retries=3, delay=5.0),
)
def oll_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Deliver OLL courses to MIT Learn via signed webhook."""
    sheet_id = os.environ.get("OLL_GOOGLE_SHEETS_ID", _OLL_SHEET_ID_DEFAULT)

    context.log.info("Fetching OLL CSV from Google Sheets (sheet_id=%s)", sheet_id)
    rows = _fetch_oll_csv(sheet_id)
    context.log.info("Fetched %d OLL rows", len(rows))

    resources = []
    skipped = 0
    for row in rows:
        transformed = _transform_course(row)
        if transformed is None:
            skipped += 1
            continue
        resources.append(transformed)

    context.log.info(
        "Delivering %d OLL courses to MIT Learn webhook (%d skipped)",
        len(resources),
        skipped,
    )
    try:
        response = learn_api.client.notify_oll(resources)
    except httpx.HTTPStatusError as exc:
        msg = f"OLL webhook failed with status {exc.response.status_code}: {exc}"
        context.log.exception(msg)
        raise RuntimeError(msg) from exc

    context.add_output_metadata(
        {
            "total_rows": len(rows),
            "delivered_count": len(resources),
            "skipped_count": skipped,
            "webhook_status": "success",
            "response": MetadataValue.json(response),
        }
    )
    return {
        "delivered_count": len(resources),
        "skipped_count": skipped,
        "webhook_status": "success",
    }
