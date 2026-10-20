"""
Open Learning Library (OLL) webhook delivery asset.

Reads pre-transformed course records from the
``integrations__learn__oll_courses`` Iceberg table (produced by dbt from the
oll dlt pipeline) and delivers them to MIT Learn via a single signed webhook
POST.

The dbt model owns the OLL semantics (ids, offeror, levels, terms, durations);
this asset only shapes rows into the payload MIT Learn's legacy OLL ETL
(learning_resources/etl/oll.py) hands its course loader.

Data flow:
    raw__oll__google_sheets__courses (Iceberg, via dlt)
        → integrations__learn__oll_courses (dbt)
            → MIT Learn webhook (this asset)

Scheduling: once a day, after its integrations models have materialized since
06:00 UTC. See delivery.lib.scheduled_automation.
"""

import logging
from collections.abc import Iterable, Mapping
from typing import Any, cast

import httpx2 as httpx
import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MetadataValue,
    RetryPolicy,
    asset,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.resources.api_client_factory import ApiClientFactory
from ol_orchestrate.resources.learn_api import MITLearnApiClient, webhook_status

log = logging.getLogger(__name__)

_GLUE_DB = (
    f"ol_warehouse_{DAGSTER_ENV}_integrations"
    if DAGSTER_ENV in ("qa", "production")
    else "ol_warehouse_production_integrations"
)
_TABLE = "integrations__learn__oll_courses"


_ETL_SOURCE = "oll"
_PLATFORM = "oll"
_AVAILABILITY = "anytime"
_PACE = ["self_paced"]
_FORMAT = ["asynchronous"]


def _course_numbers(course_number: str) -> list[dict[str, Any]]:
    """generate_course_numbers_json(course_number, is_ocw=False) in MIT Learn.

    Its non-OCW department pattern needs a "+" in the number, which OLL course
    numbers never have, so the department is always None and the sort key is the
    number itself.
    """
    return [
        {
            "value": course_number,
            "listing_type": "primary",
            "department": None,
            "sort_coursenum": course_number,
            "primary": True,
        }
    ]


def _row_to_resource(row: Mapping[str, Any]) -> dict[str, Any]:
    """Map an integrations__learn__oll_courses row to MIT Learn's course shape."""
    image = {"url": row["image_url"], "alt": row["title"]}
    # [] rather than None: MIT Learn clears topics and instructors on [] but leaves
    # existing ones alone on None.
    instructors = [{"full_name": name} for name in row["instructors"] or []]
    run = {
        "title": row["title"],
        "run_id": row["run_id"],
        "url": row["url"],
        "published": row["published"],
        "description": row["description"],
        "image": image,
        "level": [row["level"]] if row["level"] else [],
        "year": row["year"],
        "semester": row["semester"],
        "instructors": instructors,
        "status": "Archived",
        "availability": _AVAILABILITY,
        "pace": _PACE,
        "format": _FORMAT,
        "duration": row["duration"] or "",
        "time_commitment": row["time_commitment"] or "",
        "min_weeks": row["min_weeks"],
        "max_weeks": row["max_weeks"],
        "min_weekly_hours": row["min_weekly_hours"],
        "max_weekly_hours": row["max_weekly_hours"],
    }
    return {
        "title": row["title"],
        "readable_id": row["readable_id"],
        "url": row["url"],
        "description": row["description"],
        "full_description": row["description"],
        "offered_by": {"code": row["offered_by"]},
        "platform": _PLATFORM,
        "published": row["published"],
        "topics": [{"name": topic} for topic in row["topics"] or []],
        "course": {"course_numbers": _course_numbers(row["course_number"])},
        "runs": [run],
        "image": image,
        "etl_source": _ETL_SOURCE,
        "resource_type": "course",
        "availability": _AVAILABILITY,
        "pace": _PACE,
        "format": _FORMAT,
    }


def build_resources(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Build the webhook batch, one course per integrations row."""
    return [_row_to_resource(row) for row in rows]


@asset(
    key=AssetKey(["mit_learn_delivery", "oll_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Read Open Learning Library courses from the integrations__learn__oll_courses "
        "Iceberg table and POST as a signed webhook batch to MIT Learn."
    ),
    deps=[AssetKey(["integrations", "integrations__learn__oll_courses"])],
    retry_policy=RetryPolicy(max_retries=3, delay=5.0),
)
def oll_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Deliver OLL courses to MIT Learn via signed webhook."""
    context.log.info("Reading %s from Glue database %s", _TABLE, _GLUE_DB)
    df: pl.DataFrame = get_dbt_model_as_dataframe(
        database_name=_GLUE_DB,
        table_name=_TABLE,
    ).collect()
    context.log.info("Loaded %d OLL courses from Iceberg", len(df))

    resources = build_resources(df.iter_rows(named=True))

    context.log.info("Delivering %d OLL courses to MIT Learn webhook", len(resources))
    try:
        response = cast(MITLearnApiClient, learn_api.client).notify_learning_resources(
            resources
        )
    except httpx.HTTPStatusError as exc:
        msg = f"OLL webhook failed with status {exc.response.status_code}: {exc}"
        context.log.exception(msg)
        raise RuntimeError(msg) from exc

    status = webhook_status(response)
    context.add_output_metadata(
        {
            "delivered_count": len(resources),
            "webhook_status": status,
            "response": MetadataValue.json(response),
        }
    )
    return {"delivered_count": len(resources), "webhook_status": status}
