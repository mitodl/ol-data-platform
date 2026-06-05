"""
MIT Professional Education (MIT PE) webhook delivery asset.

Reads pre-transformed course records from the
``integrations__learn__mitpe_courses`` Iceberg table (produced by dbt from the
mitpe dlt pipeline) and delivers them to MIT Learn via a signed webhook POST.

Note: MIT PE does not expose a separate programs API endpoint. Programs are
mixed in with courses in the /feeds/courses/ feed and are delivered here via
the courses table (resource_type distinguishes them if present).

Data flow:
    raw__mitpe__api__courses (Iceberg, via dlt)
        → integrations__learn__mitpe_courses (dbt)
            → MIT Learn webhook (this asset)

Scheduling: daily at 06:15 UTC. Configured in definitions.py.
"""

import logging
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
from ol_orchestrate.resources.learn_api import MITLearnApiClient

log = logging.getLogger(__name__)

_GLUE_DB = (
    f"ol_warehouse_{DAGSTER_ENV}_integrations"
    if DAGSTER_ENV in ("qa", "production")
    else "ol_warehouse_production_integrations"
)
_COURSES_TABLE = "integrations__learn__mitpe_courses"


def _row_to_resource(row: dict[str, Any]) -> dict[str, Any]:
    """Map an integrations table row to the MIT Learn LearningResource shape."""
    image_url = row.get("image_url")
    image_alt = row.get("image_alt") or ""
    topics_raw = row.get("topics") or ""
    topics = [{"name": t.strip()} for t in topics_raw.split(",") if t.strip()]

    return {
        "readable_id": row["readable_id"],
        "title": row["title"],
        "url": row.get("url"),
        "description": row.get("description"),
        "image": {"url": image_url, "alt": image_alt} if image_url else None,
        "topics": topics,
        "published": True,
        "professional": True,
        "etl_source": "mitpe",
        "offered_by": {"code": "mitpe"},
        "platform": "mitpe",
        "resource_type": row.get("resource_type", "course"),
    }


@asset(
    key=AssetKey(["mit_learn_delivery", "mitpe_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Read MIT Professional Education courses from the "
        "integrations__learn__mitpe_courses Iceberg table and POST as a signed "
        "webhook batch to MIT Learn."
    ),
    deps=[
        AssetKey(["integrations", "learn", "integrations__learn__mitpe_courses"]),
    ],
    retry_policy=RetryPolicy(max_retries=3, delay=5.0),
)
def mitpe_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Deliver MIT PE courses to MIT Learn via signed webhook."""
    context.log.info("Reading %s from Glue database %s", _COURSES_TABLE, _GLUE_DB)

    courses_df: pl.DataFrame = get_dbt_model_as_dataframe(
        database_name=_GLUE_DB, table_name=_COURSES_TABLE
    ).collect()
    context.log.info("Loaded %d MIT PE courses from Iceberg", len(courses_df))

    resources = [_row_to_resource(row) for row in courses_df.iter_rows(named=True)]

    context.log.info(
        "Delivering %d MIT PE courses to MIT Learn webhook", len(resources)
    )
    try:
        response = cast(MITLearnApiClient, learn_api.client).notify_learning_resources(
            resources
        )
    except httpx.HTTPStatusError as exc:
        msg = f"MIT PE webhook failed with status {exc.response.status_code}: {exc}"
        context.log.exception(msg)
        raise RuntimeError(msg) from exc

    context.add_output_metadata(
        {
            "delivered_count": len(resources),
            "webhook_status": "success",
            "response": MetadataValue.json(response),
        }
    )
    return {"delivered_count": len(resources), "webhook_status": "success"}
