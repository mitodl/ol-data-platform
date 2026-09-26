"""
MIT Professional Education (MIT PE) webhook delivery asset.

Reads MIT PE courses, programs and their published runs from the
``integrations__learn__mitpe_*`` Iceberg tables and delivers them to MIT Learn via
a signed webhook POST. The dbt models own the MIT PE semantics (which items are
courses or programs, run publication, durations, program membership); this asset
only shapes rows into the payload MIT Learn's loaders read.

Data flow:
    raw__mitpe__api__courses (Iceberg, via dlt)
        → int__mitpe__* (dbt)
            → integrations__learn__mitpe_{courses,programs,runs} (dbt)
                → MIT Learn webhook (this asset)

Scheduling: daily at 06:15 UTC. Configured in definitions.py.
"""

import logging
from collections import defaultdict
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
from ol_orchestrate.resources.learn_api import MITLearnApiClient

from delivery.lib.sanitize import clean_html

log = logging.getLogger(__name__)

_GLUE_DB = (
    f"ol_warehouse_{DAGSTER_ENV}_integrations"
    if DAGSTER_ENV in ("qa", "production")
    else "ol_warehouse_production_integrations"
)
_COURSES_TABLE = "integrations__learn__mitpe_courses"
_PROGRAMS_TABLE = "integrations__learn__mitpe_programs"
_RUNS_TABLE = "integrations__learn__mitpe_runs"

_PLATFORM = "mitpe"
_CURRENCY_USD = "USD"


def _prices(price: Any) -> list[dict[str, str]]:
    # json.dumps can't encode the Decimal polars returns for decimal columns.
    if price is None:
        return []
    return [{"amount": str(price), "currency": _CURRENCY_USD}]


def _run_to_payload(
    run: Mapping[str, Any], row: Mapping[str, Any], description: str | None
) -> dict[str, Any]:
    """Map a published run to MIT Learn's run shape.

    Price, instructors, location and duration are item-level in the MIT PE feed,
    so every run of an item carries the same values.
    """
    return {
        "run_id": run["run_id"],
        "title": row["title"],
        "description": description,
        "start_date": run["start_date"],
        "end_date": run["end_date"],
        "enrollment_end": run["enrollment_end"],
        "published": True,
        "prices": _prices(row["price"]),
        "url": row["url"],
        "instructors": [{"full_name": name} for name in row["instructors"] or []],
        "format": ["asynchronous"],
        "pace": ["instructor_paced"],
        "availability": "dated",
        "delivery": [row["delivery"]],
        "location": row["location"],
        "duration": row["duration"],
        "min_weeks": row["min_weeks"],
        "max_weeks": row["max_weeks"],
    }


def _row_to_resource(
    row: Mapping[str, Any], runs: Iterable[Mapping[str, Any]]
) -> dict[str, Any]:
    """Map an integrations course or program row to MIT Learn's resource shape."""
    # The legacy Celery ETL ran descriptions through clean_data() before they
    # reached the database; nothing on the webhook path does, so sanitize here to
    # keep the migration behaviour-preserving. No allowlist override, because
    # mitpe.parse_description passes none either.
    description = clean_html(row["description"])
    resource: dict[str, Any] = {
        "readable_id": row["readable_id"],
        "etl_source": row["etl_source"],
        "platform": row["platform"],
        "resource_type": row["resource_type"],
        "offered_by": {"code": "mitpe"},
        "professional": True,
        "certification": True,
        "certification_type": "professional",
        "title": row["title"],
        "url": row["url"],
        "image": (
            {"url": row["image_url"], "alt": row["image_alt"]}
            if row["image_url"]
            else None
        ),
        "description": description,
        "delivery": [row["delivery"]],
        "published": True,
        # [] rather than None: MIT Learn clears topics on [] but leaves existing
        # ones alone on None.
        "topics": [{"name": topic} for topic in row["topics"] or []],
        "runs": [
            _run_to_payload(run, row, description)
            for run in sorted(runs, key=lambda run: run["run_position"])
        ],
        "format": ["asynchronous"],
        "pace": ["instructor_paced"],
        "availability": "dated",
    }
    if row["resource_type"] == "course":
        resource["course"] = {"course_numbers": []}
    else:
        # MIT Learn looks program courses up rather than upserting them, so a
        # reference to a course delivered in the same batch is enough.
        resource["courses"] = [
            {"readable_id": course_id, "platform": _PLATFORM}
            for course_id in row["course_readable_ids"] or []
        ]
    return resource


def build_resources(
    courses: Iterable[Mapping[str, Any]],
    programs: Iterable[Mapping[str, Any]],
    runs: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Build the webhook batch: every course, then every program.

    Courses go first because MIT Learn loads groups in the order it first sees
    them, and program courses are looked up from what is already loaded.
    """
    runs_by_resource: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for run in runs:
        runs_by_resource[run["readable_id"]].append(run)
    return [
        _row_to_resource(row, runs_by_resource[row["readable_id"]])
        for row in [*courses, *programs]
    ]


def _read_table(context: AssetExecutionContext, table: str) -> pl.DataFrame:
    context.log.info("Reading %s from Glue database %s", table, _GLUE_DB)
    return get_dbt_model_as_dataframe(
        database_name=_GLUE_DB, table_name=table
    ).collect()


@asset(
    key=AssetKey(["mit_learn_delivery", "mitpe_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Read MIT Professional Education courses, programs and their published runs "
        "from the integrations__learn__mitpe_* Iceberg tables and POST them as a "
        "signed webhook batch to MIT Learn."
    ),
    deps=[
        AssetKey(["integrations", "learn", _COURSES_TABLE]),
        AssetKey(["integrations", "learn", _PROGRAMS_TABLE]),
        AssetKey(["integrations", "learn", _RUNS_TABLE]),
    ],
    retry_policy=RetryPolicy(max_retries=3, delay=5.0),
)
def mitpe_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Deliver MIT PE courses and programs to MIT Learn via signed webhook."""
    courses_df = _read_table(context, _COURSES_TABLE)
    programs_df = _read_table(context, _PROGRAMS_TABLE)
    runs_df = _read_table(context, _RUNS_TABLE)
    context.log.info(
        "Loaded %d MIT PE courses, %d programs and %d runs from Iceberg",
        len(courses_df),
        len(programs_df),
        len(runs_df),
    )

    resources = build_resources(
        courses_df.iter_rows(named=True),
        programs_df.iter_rows(named=True),
        runs_df.iter_rows(named=True),
    )

    context.log.info(
        "Delivering %d MIT PE resources to MIT Learn webhook", len(resources)
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
            "course_count": len(courses_df),
            "program_count": len(programs_df),
            "webhook_status": "success",
            "response": MetadataValue.json(response),
        }
    )
    return {"delivered_count": len(resources), "webhook_status": "success"}
