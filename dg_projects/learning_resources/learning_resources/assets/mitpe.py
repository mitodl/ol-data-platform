"""
MIT Professional Education (MIT PE) webhook delivery asset.

Fetches courses and programs from the MIT PE feeds API, transforms them
into MIT Learn's LearningResource payload shape, and delivers both
catalogs as signed webhook POST batches.

MIT PE's API paginates via a ``page`` query parameter (0-indexed);
fetching stops when an empty array is returned.

Scheduling: daily at 06:15 UTC. Configured in definitions.py.
"""

import html
import logging
import os
from typing import Any
from urllib.parse import urljoin

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

_MITPE_BASE_URL_DEFAULT = "https://professional.mit.edu"


def _fetch_all_pages(url: str) -> list[dict[str, Any]]:
    """Fetch all pages from a MIT PE paginated endpoint."""
    results: list[dict[str, Any]] = []
    page = 0
    while True:
        resp = requests.get(url, params={"page": page}, timeout=30)
        resp.raise_for_status()
        page_data = resp.json()
        if not page_data:
            break
        results.extend(page_data)
        page += 1
    return results


def _parse_topics(resource: dict[str, Any]) -> list[dict[str, str]]:
    """Parse pipe-separated topic strings into dicts."""
    raw = resource.get("topics") or ""
    return [{"name": html.unescape(t)} for t in raw.split("|") if t.strip()]


def _parse_image(resource: dict[str, Any], base_url: str) -> dict[str, str] | None:
    """Build image dict from MIT PE image fields."""
    img_src = resource.get("image__src")
    if not img_src:
        return None
    return {
        "url": urljoin(base_url, img_src),
        "alt": resource.get("image__alt", ""),
    }


def _transform_course(course: dict[str, Any], base_url: str) -> dict[str, Any]:
    """Transform a raw MIT PE course dict into MIT Learn's resource shape."""
    return {
        "readable_id": course.get("readable_id") or course.get("url"),
        "title": html.unescape(course.get("title") or ""),
        "url": urljoin(base_url, course.get("url", "")),
        "description": course.get("description"),
        "image": _parse_image(course, base_url),
        "topics": _parse_topics(course),
        "published": True,
        "professional": True,
        "etl_source": "mitpe",
        "offered_by": {"code": "mitpe"},
        "platform": "mitpe",
        "resource_type": "course",
    }


def _transform_program(program: dict[str, Any], base_url: str) -> dict[str, Any]:
    """Transform a raw MIT PE program dict into MIT Learn's resource shape."""
    return {
        "readable_id": program.get("readable_id") or program.get("url"),
        "title": html.unescape(program.get("title") or ""),
        "url": urljoin(base_url, program.get("url", "")),
        "description": program.get("description"),
        "image": _parse_image(program, base_url),
        "topics": _parse_topics(program),
        "published": True,
        "professional": True,
        "etl_source": "mitpe",
        "offered_by": {"code": "mitpe"},
        "platform": "mitpe",
        "resource_type": "program",
        "courses": [],
    }


@asset(
    key=AssetKey(["mit_learn_delivery", "mitpe_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Fetch all MIT Professional Education courses and programs, "
        "transform to MIT Learn resource shape, and POST as signed webhook batches."
    ),
    retry_policy=RetryPolicy(max_retries=3, delay=5.0),
)
def mitpe_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Deliver MIT PE courses and programs to MIT Learn via signed webhooks."""
    base_url = os.environ.get("MITPE_BASE_URL", _MITPE_BASE_URL_DEFAULT)
    courses_url = urljoin(base_url, "/feeds/courses/")
    programs_url = urljoin(base_url, "/feeds/programs/")

    context.log.info("Fetching MIT PE courses from %s", courses_url)
    raw_courses = _fetch_all_pages(courses_url)
    context.log.info("Fetched %d MIT PE courses", len(raw_courses))

    context.log.info("Fetching MIT PE programs from %s", programs_url)
    raw_programs = _fetch_all_pages(programs_url)
    context.log.info("Fetched %d MIT PE programs", len(raw_programs))

    course_resources = [
        r
        for c in raw_courses
        if (r := _transform_course(c, base_url)).get("readable_id")
    ]
    program_resources = [
        r
        for p in raw_programs
        if (r := _transform_program(p, base_url)).get("readable_id")
    ]
    all_resources = course_resources + program_resources

    context.log.info(
        "Delivering %d MIT PE resources (%d courses, %d programs) to MIT Learn",
        len(all_resources),
        len(course_resources),
        len(program_resources),
    )
    try:
        response = learn_api.client.notify_learning_resources(all_resources)
    except httpx.HTTPStatusError as exc:
        msg = f"MIT PE webhook failed with status {exc.response.status_code}: {exc}"
        context.log.exception(msg)
        raise RuntimeError(msg) from exc

    context.add_output_metadata(
        {
            "course_count": len(course_resources),
            "program_count": len(program_resources),
            "total_resource_count": len(all_resources),
            "webhook_status": "success",
            "response": MetadataValue.json(response),
        }
    )
    return {
        "course_count": len(course_resources),
        "program_count": len(program_resources),
        "webhook_status": "success",
    }
