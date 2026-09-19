"""
MIT edX (MITx on edX.org) programs webhook delivery asset.

Reads the edX.org programs MIT Learn lists, and their instructors, from the
``integrations__learn__mit_edx_program*`` Iceberg tables and delivers them to MIT
Learn via a signed webhook POST. The dbt models own the semantics (which programs are
listed, the program run's dates, price, pace and availability, and what a program
takes from its courses); this asset only shapes rows into the payload MIT Learn's
loaders read.

Data flow:
    raw__edxorg__s3__program{,_course}, raw__edxorg__s3__mitx_course{,_run}
    (edxorg code location, edX discovery API)
        → int__edxorg__mitx_learn_* (dbt)
            → integrations__learn__mit_edx_program{s,_instructors} (dbt)
                → MIT Learn webhook (this asset)

When the latest programs extraction lists no program MIT Learn ingests, there is
nothing to deliver, and MIT Learn would reject an empty batch. Its legacy ETL left
existing programs published in that case, and so does this asset, but it opens a
GitHub issue per program MIT Learn still publishes so a person decides whether to
unpublish it. If extractions stop arriving altogether, the models keep the last one
and this asset re-sends it; the freshness check on raw__edxorg__s3__program flags
that.

Scheduling: daily at 06:45 UTC. Configured in definitions.py.
"""

import logging
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import httpx2 as httpx
import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MetadataValue,
    RetryPolicy,
    asset,
)
from github import Github
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.resources.api_client_factory import ApiClientFactory
from ol_orchestrate.resources.github import GithubApiClientFactory
from ol_orchestrate.resources.learn_api import MITLearnApiClient
from pydantic import Field

from delivery.lib.sanitize import clean_html

log = logging.getLogger(__name__)

_GLUE_DB = (
    f"ol_warehouse_{DAGSTER_ENV}_integrations"
    if DAGSTER_ENV in ("qa", "production")
    else "ol_warehouse_production_integrations"
)
_PROGRAMS_TABLE = "integrations__learn__mit_edx_programs"
_INSTRUCTORS_TABLE = "integrations__learn__mit_edx_program_instructors"

_PLATFORM = "edx"
_REVIEW_TITLE_PREFIX = "Review whether MIT Learn should unpublish edX program"


class MitEdxProgramsWebhookConfig(Config):
    """Run config for mit_edx_programs_webhook."""

    review_repository: str = Field(
        default="mitodl/mit-learn",
        description=(
            "GitHub repository (owner/name) to open unpublish-review issues in when "
            "no edX program is listed."
        ),
    )


def _program_to_resource(
    row: Mapping[str, Any], instructors: Iterable[Mapping[str, Any]]
) -> dict[str, Any]:
    """Map an integrations program row to MIT Learn's program shape.

    MIT Learn models an edX program as a single run carrying its dates, price and
    effort. The description is sanitized as MIT Learn's clean_data did; MIT Learn
    stored the run's copy of it unsanitized, which is the one deliberate change.
    """
    description = clean_html(row["description"])
    image = (
        {"url": row["image_url"], "description": row["title"]}
        if row["image_url"]
        else None
    )
    run = {
        "run_id": row["readable_id"],
        "title": row["title"],
        "description": description,
        "full_description": description,
        "level": [row["level"]] if row["level"] else [],
        "start_date": row["start_date"],
        "end_date": row["end_date"],
        "last_modified": row["last_modified"],
        "published": True,
        "enrollment_start": row["enrollment_start"],
        "enrollment_end": row["enrollment_end"],
        "image": image,
        "status": "Current",
        "url": row["url"],
        # json.dumps can't encode the Decimal polars returns for decimal columns.
        "prices": [{"amount": str(row["price"]), "currency": row["currency"]}],
        "instructors": [
            {
                "first_name": instructor["first_name"],
                "last_name": instructor["last_name"],
                "full_name": instructor["full_name"],
            }
            for instructor in sorted(
                instructors, key=lambda instructor: instructor["instructor_position"]
            )
        ],
        "availability": row["availability"],
        "format": ["asynchronous"],
        "pace": row["pace"] or [],
        "duration": row["duration"],
        "min_weeks": row["min_weeks"],
        "max_weeks": row["max_weeks"],
        "time_commitment": row["time_commitment"],
        "min_weekly_hours": row["min_weekly_hours"],
        "max_weekly_hours": row["max_weekly_hours"],
    }
    return {
        "readable_id": row["readable_id"],
        "etl_source": row["etl_source"],
        "platform": row["platform"],
        "resource_type": row["resource_type"],
        "offered_by": {"code": "mitx"},
        "title": row["title"],
        "description": description,
        "full_description": description,
        "last_modified": row["last_modified"],
        "image": image,
        "url": row["url"],
        # [] rather than None: MIT Learn clears topics on [] but leaves existing ones
        # alone on None.
        "topics": [{"name": topic} for topic in row["topics"] or []],
        "runs": [run],
        "published": True,
        "certification": True,
        "certification_type": "completion",
        "availability": row["availability"],
        "format": ["asynchronous"],
        "pace": row["pace"] or [],
        # MIT Learn looks program courses up rather than upserting them, so a
        # reference is enough; ones it doesn't publish are skipped.
        "courses": [
            {"readable_id": course_id, "platform": _PLATFORM}
            for course_id in row["course_readable_ids"] or []
        ],
    }


def build_resources(
    programs: Iterable[Mapping[str, Any]], instructors: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Build the webhook batch, one resource per program."""
    instructors_by_program: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for instructor in instructors:
        instructors_by_program[instructor["readable_id"]].append(instructor)
    return [
        _program_to_resource(row, instructors_by_program[row["readable_id"]])
        for row in programs
    ]


def unpublish_review_issue(
    program: Mapping[str, Any], *, checked_on: str
) -> tuple[str, str]:
    """Title and body of the issue asking whether to unpublish ``program``.

    ``program`` is a program as MIT Learn's programs API returns it.
    """
    readable_id = program["readable_id"]
    title = f"{_REVIEW_TITLE_PREFIX} {readable_id}"
    body = (
        f"On {checked_on}, edX's programs API listed no program MIT Learn ingests "
        "(active, authored by MITx or MITx_PRO, not MicroMasters), so the "
        "`mit_edx_programs_webhook` delivery sent nothing. MIT Learn still publishes "
        f"this edX program:\n\n"
        f"- {program['title']} (`{readable_id}`, MIT Learn id {program['id']})\n"
        f"- {program.get('url') or 'no URL'}\n\n"
        "MIT Learn's legacy ETL never unpublished programs when edX listed none, and "
        "the webhook delivery keeps that behavior, so this program stays published "
        "until someone decides otherwise. Check its status on edX and unpublish it in "
        "MIT Learn if it is no longer offered, then close this issue.\n\n"
        "Opened by the ol-data-platform `delivery` code location."
    )
    return title, body


def open_unpublish_reviews(
    github: Github,
    repository: str,
    programs: Iterable[Mapping[str, Any]],
    *,
    checked_on: str,
) -> list[str]:
    """Make sure each program has a review issue; return their URLs.

    Any issue whose title names the program counts, open or closed: an open one is
    still under review, and closing one records the decision, so a program that
    stays unlisted is raised once rather than every run. Issues updated in the last
    two days are checked too, because search indexes new issues with a delay and a
    retry soon after a partial failure would otherwise file duplicates.
    """
    repo = github.get_repo(repository)
    recent_issues = list(
        repo.get_issues(state="all", since=datetime.now(tz=UTC) - timedelta(days=2))
    )
    urls = []
    for program in programs:
        readable_id = program["readable_id"]
        recent = next(
            (issue for issue in recent_issues if readable_id in issue.title), None
        )
        if recent is not None:
            urls.append(recent.html_url)
            continue
        found = github.search_issues(
            f'repo:{repository} is:issue in:title "{readable_id}"'
        )
        if found.totalCount:
            urls.append(found[0].html_url)
            continue
        title, body = unpublish_review_issue(program, checked_on=checked_on)
        urls.append(repo.create_issue(title=title, body=body).html_url)
    return urls


def _read_table(context: AssetExecutionContext, table: str) -> pl.DataFrame:
    context.log.info("Reading %s from Glue database %s", table, _GLUE_DB)
    return get_dbt_model_as_dataframe(
        database_name=_GLUE_DB, table_name=table
    ).collect()


@asset(
    key=AssetKey(["mit_learn_delivery", "mit_edx_programs_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Read the edX.org programs MIT Learn lists from the "
        "integrations__learn__mit_edx_program* Iceberg tables and POST them as a "
        "signed webhook batch to MIT Learn. With none listed, open GitHub issues to "
        "review the programs MIT Learn still publishes instead."
    ),
    deps=[
        AssetKey(["integrations", "learn", _PROGRAMS_TABLE]),
        AssetKey(["integrations", "learn", _INSTRUCTORS_TABLE]),
    ],
    retry_policy=RetryPolicy(max_retries=3, delay=10.0),
)
def mit_edx_programs_webhook(
    context: AssetExecutionContext,
    config: MitEdxProgramsWebhookConfig,
    learn_api: ApiClientFactory,
    github_api: GithubApiClientFactory,
) -> dict[str, Any]:
    """Deliver MIT edX programs to MIT Learn via signed webhook."""
    programs_df = _read_table(context, _PROGRAMS_TABLE)
    instructors_df = _read_table(context, _INSTRUCTORS_TABLE)
    resources = build_resources(
        programs_df.iter_rows(named=True), instructors_df.iter_rows(named=True)
    )
    learn_client = cast(MITLearnApiClient, learn_api.client)

    if not resources:
        still_published = learn_client.get_published_programs(_PLATFORM)
        context.log.warning(
            "No edX programs listed; skipping delivery. MIT Learn still publishes %d.",
            len(still_published),
        )
        issue_urls = open_unpublish_reviews(
            github_api.get_client(),
            config.review_repository,
            still_published,
            checked_on=datetime.now(tz=UTC).date().isoformat(),
        )
        context.add_output_metadata(
            {
                "delivered_count": 0,
                "webhook_status": "skipped",
                "review_issues": MetadataValue.json(issue_urls),
            }
        )
        return {"delivered_count": 0, "webhook_status": "skipped"}

    context.log.info(
        "Delivering %d MIT edX programs to MIT Learn webhook", len(resources)
    )
    try:
        response = learn_client.notify_learning_resources(resources)
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
