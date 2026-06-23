"""
MIT Climate Portal webhook delivery asset.

Reads pre-transformed article records from the
``integrations__learn__mit_climate_articles`` Iceberg table (produced by dbt
from the mit_climate dlt pipeline) and delivers them to MIT Learn via a
single signed webhook POST.

Data flow:
    raw__mit_climate__api__articles (Iceberg, via dlt)
        → integrations__learn__mit_climate_articles (dbt)
            → MIT Learn webhook (this asset)

Scheduling: daily at 06:00 UTC. Configured in definitions.py.
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
_TABLE = "integrations__learn__mit_climate_articles"


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
        "etl_source": "mit_climate",
        "offered_by": {"code": "climate"},
        "platform": "climate",
        "resource_type": "Article",
        "created_on": row.get("last_modified"),
    }


@asset(
    key=AssetKey(["mit_learn_delivery", "mit_climate_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Read MIT Climate Portal articles from the "
        "integrations__learn__mit_climate_articles "
        "Iceberg table and POST as a signed webhook batch to MIT Learn."
    ),
    deps=[
        AssetKey(["integrations", "learn", "integrations__learn__mit_climate_articles"])
    ],
    retry_policy=RetryPolicy(max_retries=3, delay=5.0),
)
def mit_climate_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Deliver MIT Climate articles to MIT Learn via signed webhook."""
    context.log.info("Reading %s from Glue database %s", _TABLE, _GLUE_DB)
    df: pl.DataFrame = get_dbt_model_as_dataframe(
        database_name=_GLUE_DB,
        table_name=_TABLE,
    ).collect()
    context.log.info("Loaded %d MIT Climate articles from Iceberg", len(df))

    resources = [_row_to_resource(row) for row in df.iter_rows(named=True)]

    context.log.info(
        "Delivering %d MIT Climate articles to MIT Learn webhook", len(resources)
    )
    try:
        response = cast(MITLearnApiClient, learn_api.client).notify_learning_resources(
            resources
        )
    except httpx.HTTPStatusError as exc:
        msg = (
            f"MIT Climate webhook failed with status {exc.response.status_code}: {exc}"
        )
        context.log.exception(msg)
        raise RuntimeError(msg) from exc

    context.add_output_metadata(
        {
            "resource_count": len(resources),
            "webhook_status": "success",
            "response": MetadataValue.json(response),
        }
    )
    return {"resource_count": len(resources), "webhook_status": "success"}
