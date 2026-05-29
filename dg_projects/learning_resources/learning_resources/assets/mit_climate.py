"""
MIT Climate Portal webhook delivery asset.

Fetches articles from both MIT Climate JSON feeds, transforms them into
MIT Learn's LearningResource payload shape, and delivers the full catalog
as a single signed webhook POST. MIT Learn's handler calls
``load_courses()`` directly; no transformation logic lives in the handler.

This asset is independent of the data_loading dlt pipeline: it fetches
from the source APIs directly so that delivery latency is decoupled from
the raw warehouse ingestion schedule. The dlt pipeline in data_loading
serves the warehouse/dbt use case; this asset serves the MIT Learn
real-time catalog.

Scheduling: daily at 06:00 UTC (after the nightly MIT Climate content
refresh cycle). Configured in definitions.py.
"""

import html
import logging
import os
from datetime import UTC
from typing import Any
from zoneinfo import ZoneInfo

import dateutil.parser
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

log = logging.getLogger(__name__)

_EXPLAINERS_URL_DEFAULT = "https://climate.mit.edu/api/explainers"
_ASK_URL_DEFAULT = "https://climate.mit.edu/api/ask-mit-climate"
_MIT_CLIMATE_BASE_URL_DEFAULT = "https://climate.mit.edu"


def _fetch_articles(url: str) -> list[dict[str, Any]]:
    """Fetch a flat JSON array from a MIT Climate feed URL."""
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _transform_article(article: dict[str, Any], base_url: str) -> dict[str, Any]:
    """
    Transform a raw MIT Climate article dict into MIT Learn's resource shape.

    Mirrors the transform logic in ``learning_resources/etl/mit_climate.py``
    so the webhook payload is byte-for-byte compatible with the existing
    ``load_courses()`` / ``load_articles()`` loaders.
    """
    article_url = f"{base_url}{article.get('url', '')}"
    image_src = article.get("image_src", "")
    image_url = f"{base_url}{image_src}" if image_src else None

    summary = article.get("summary", "")
    full_description = "\n".join(
        filter(
            None,
            [
                summary,
                article.get("footnotes", ""),
                article.get("byline", ""),
            ],
        )
    )

    raw_topics = article.get("topics", "") or ""
    topics = [
        {"name": html.unescape(t.strip())} for t in raw_topics.split("|") if t.strip()
    ]

    created_on = None
    if created_str := article.get("created"):
        try:
            created_on = (
                dateutil.parser.parse(created_str)
                .replace(tzinfo=ZoneInfo("US/Eastern"))
                .astimezone(UTC)
                .isoformat()
            )
        except (ValueError, TypeError):
            log.warning("Could not parse created date: %s", created_str)

    return {
        "readable_id": article.get("uuid"),
        "title": html.unescape(article.get("title") or ""),
        "url": article_url,
        "image": {"url": image_url, "alt": article.get("image_alt", "")}
        if image_url
        else None,
        "description": summary,
        "full_description": full_description,
        "topics": topics,
        "published": True,
        "created_on": created_on,
        "etl_source": "mit_climate",
        "offered_by": {"code": "climate"},
        "resource_category": "Article",
        "platform": "climate",
    }


@asset(
    key=AssetKey(["mit_learn_delivery", "mit_climate_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Fetch all MIT Climate Portal articles (Explainers + Ask MIT Climate), "
        "transform to MIT Learn resource shape, and POST as a signed webhook batch."
    ),
    retry_policy=RetryPolicy(max_retries=3, delay=5.0),
)
def mit_climate_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Deliver MIT Climate articles to MIT Learn via signed webhook."""
    explainers_url = os.environ.get(
        "MIT_CLIMATE_EXPLAINERS_API_URL", _EXPLAINERS_URL_DEFAULT
    )
    ask_url = os.environ.get("ASK_MIT_CLIMATE_API_URL", _ASK_URL_DEFAULT)
    base_url = os.environ.get("MIT_CLIMATE_BASE_URL", _MIT_CLIMATE_BASE_URL_DEFAULT)

    raw_articles: list[dict[str, Any]] = []
    for url, feed_label in [
        (explainers_url, "explainers"),
        (ask_url, "ask_mit_climate"),
    ]:
        context.log.info("Fetching MIT Climate feed: %s (%s)", url, feed_label)
        articles = _fetch_articles(url)
        context.log.info("  → %d articles from %s", len(articles), feed_label)
        raw_articles.extend(articles)

    resources = [_transform_article(a, base_url) for a in raw_articles]
    # Drop any records missing a readable_id — they cannot be upserted
    resources = [r for r in resources if r.get("readable_id")]

    context.log.info(
        "Delivering %d MIT Climate articles to MIT Learn webhook", len(resources)
    )
    try:
        response = learn_api.client.notify_mit_climate(resources)
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
