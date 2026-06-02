"""
MIT edX (MITx on edX.org) programs webhook delivery asset.

Reads pre-transformed program records from the
``integrations__learn__mit_edx_programs`` Iceberg table (produced by dbt from
the mit_edx_programs dlt pipeline) and delivers them to MIT Learn via a single
signed webhook POST.

MicroMasters programs are excluded — the dlt pipeline filters them at ingest
time and they are handled separately by the Cohort 1 Trino-pull path via
integrations__learn__micromasters_programs.

Data flow:
    raw__edxorg__discovery__api__programs (Iceberg, via dlt)
        → integrations__learn__mit_edx_programs (dbt)
            → MIT Learn webhook (this asset)

Scheduling: daily at 06:45 UTC. Configured in definitions.py.
"""

import json
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
_TABLE = "integrations__learn__mit_edx_programs"


def _parse_topics(topics_json: str | None) -> list[dict[str, str]]:
    """Parse the subjects JSON array into MIT Learn topic dicts."""
    if not topics_json:
        return []
    try:
        subjects = json.loads(topics_json)
        return [{"name": s["name"]} for s in subjects if s.get("name")]
    except (json.JSONDecodeError, TypeError):
        log.warning("Could not parse topics_json: %r", topics_json)
        return []


def _parse_courses(courses_json: str | None) -> list[dict[str, str]]:
    """Parse the courses JSON array into MIT Learn course readable_id dicts."""
    if not courses_json:
        return []
    try:
        courses = json.loads(courses_json)
        return [{"readable_id": c["key"]} for c in courses if c.get("key")]
    except (json.JSONDecodeError, TypeError):
        log.warning("Could not parse courses_json: %r", courses_json)
        return []


def _row_to_resource(row: dict[str, Any]) -> dict[str, Any]:
    """Map an integrations table row to the MIT Learn LearningResource shape."""
    image_url = row.get("image_url")

    return {
        "readable_id": row["readable_id"],
        "title": row["title"],
        "url": row.get("url"),
        "description": row.get("description"),
        "image": {"url": image_url, "alt": row.get("title", "")} if image_url else None,
        "topics": _parse_topics(row.get("topics_json")),
        "published": True,
        "etl_source": "mit_edx",
        "offered_by": {"code": "mitx"},
        "platform": "edxorg",
        "resource_type": "program",
        "courses": _parse_courses(row.get("courses_json")),
    }


@asset(
    key=AssetKey(["mit_learn_delivery", "mit_edx_programs_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Read active MIT-authored edX.org programs from the "
        "integrations__learn__mit_edx_programs Iceberg table and POST as a "
        "signed webhook batch to MIT Learn. Excludes MicroMasters (handled via "
        "the Cohort 1 Trino-pull path)."
    ),
    deps=[AssetKey(["integrations", "learn", "integrations__learn__mit_edx_programs"])],
    retry_policy=RetryPolicy(max_retries=3, delay=10.0),
)
def mit_edx_programs_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Deliver MIT edX programs to MIT Learn via signed webhook."""
    context.log.info("Reading %s from Glue database %s", _TABLE, _GLUE_DB)
    df: pl.DataFrame = get_dbt_model_as_dataframe(
        database_name=_GLUE_DB,
        table_name=_TABLE,
    ).collect()
    context.log.info("Loaded %d MIT edX programs from Iceberg", len(df))

    resources = [_row_to_resource(row) for row in df.iter_rows(named=True)]

    context.log.info(
        "Delivering %d MIT edX programs to MIT Learn webhook", len(resources)
    )
    try:
        response = cast(MITLearnApiClient, learn_api.client).notify_learning_resources(
            resources
        )
    except httpx.HTTPStatusError as exc:
        msg = (
            f"MIT edX programs webhook failed with status "
            f"{exc.response.status_code}: {exc}"
        )
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
