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
_TABLE = "integrations__learn__mit_climate_articles"


def _row_to_resource(row: Mapping[str, Any]) -> dict[str, Any]:
    """Map an integrations table row to the MIT Learn LearningResource shape."""
    image_url = row["image_url"]
    description = row["description"]

    return {
        "readable_id": row["readable_id"],
        "title": row["title"],
        "url": row["url"],
        "description": description,
        # MIT Learn's update only writes keys it is sent, so leaving this out would
        # keep the stored text forever and give a new article none.
        "full_description": "\n".join(
            [description or "", row["footnotes"] or "", row["byline"] or ""]
        ),
        "image": {"url": image_url, "alt": row["image_alt"] or ""}
        if image_url
        else None,
        # [] sends the article to MIT Learn's similar-topic lookup, as legacy ETL did
        "topics": [{"name": topic} for topic in row["topics"] or []],
        "published": True,
        "etl_source": "mit_climate",
        "offered_by": {"code": "climate"},
        "platform": "climate",
        # MIT Learn has no "article" resource_type; the legacy Celery ETL loads
        # Climate articles as documents (load_documents) with an "Article"
        # resource_category, so match that contract for the webhook path too.
        "resource_type": "document",
        "resource_category": "Article",
        "created_on": row["last_modified"],
    }


def build_resources(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Build the webhook batch, one document per integrations row."""
    return [_row_to_resource(row) for row in rows]


@asset(
    key=AssetKey(["mit_learn_delivery", "mit_climate_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Read MIT Climate Portal articles from the "
        "integrations__learn__mit_climate_articles "
        "Iceberg table and POST as a signed webhook batch to MIT Learn."
    ),
    deps=[AssetKey(["integrations", "integrations__learn__mit_climate_articles"])],
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

    resources = build_resources(df.iter_rows(named=True))

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

    status = webhook_status(response)
    context.add_output_metadata(
        {
            "resource_count": len(resources),
            "webhook_status": status,
            "response": MetadataValue.json(response),
        }
    )
    return {"resource_count": len(resources), "webhook_status": status}
