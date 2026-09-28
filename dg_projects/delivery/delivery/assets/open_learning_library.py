"""
Open Learning Library (OLL) webhook delivery asset.

Reads pre-transformed course records from the
``integrations__learn__oll_courses`` Iceberg table (produced by dbt from the
oll dlt pipeline) and delivers them to MIT Learn via a single signed webhook
POST.

OCW-origin courses are excluded at the staging layer and are never present in
the integration table, so no additional filtering is needed here.

Data flow:
    raw__oll__google_sheets__courses (Iceberg, via dlt)
        → integrations__learn__oll_courses (dbt)
            → MIT Learn webhook (this asset)

Scheduling: daily at 06:30 UTC. Configured in definitions.py.
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
_TABLE = "integrations__learn__oll_courses"


def _row_to_resource(row: dict[str, Any]) -> dict[str, Any]:
    """Map an integrations table row to the MIT Learn LearningResource shape."""
    image_url = row.get("image_url")
    topics_raw = row.get("topics") or ""
    topics = [{"name": t.strip()} for t in topics_raw.split(",") if t.strip()]

    # Reconstruct the single run from the pipe-delimited runs string
    runs = []
    if runs_str := row.get("runs"):
        parts = runs_str.split("|")
        runs = [{"run_id": parts[0]}]

    return {
        "readable_id": row["readable_id"],
        "title": row["title"],
        "url": row.get("url"),
        "description": row.get("description"),
        "image": {"url": image_url, "alt": row["readable_id"]} if image_url else None,
        "topics": topics,
        "published": True,
        "etl_source": "oll",
        "offered_by": {"code": "oll"},
        "platform": "oll",
        "resource_type": "course",
        "runs": runs,
    }


@asset(
    key=AssetKey(["mit_learn_delivery", "oll_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Read Open Learning Library courses from the integrations__learn__oll_courses "
        "Iceberg table and POST as a signed webhook batch to MIT Learn."
    ),
    deps=[AssetKey(["integrations", "learn", "integrations__learn__oll_courses"])],
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

    resources = [_row_to_resource(row) for row in df.iter_rows(named=True)]

    context.log.info("Delivering %d OLL courses to MIT Learn webhook", len(resources))
    try:
        response = cast(MITLearnApiClient, learn_api.client).notify_learning_resources(
            resources
        )
    except httpx.HTTPStatusError as exc:
        msg = f"OLL webhook failed with status {exc.response.status_code}: {exc}"
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
