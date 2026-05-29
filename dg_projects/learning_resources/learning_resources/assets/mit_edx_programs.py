"""
MIT edX (MITx on edX.org) programs webhook delivery asset.

Fetches active MIT-authored programs from the edX.org Programs API using
OAuth2 client credentials, transforms them into MIT Learn's
LearningResource payload shape, and delivers them as a single signed
webhook POST batch.

MicroMasters programs are excluded — they are handled by Cohort 1's
Trino-pull path via the integrations__learn__micromasters_programs dbt model.

Scheduling: daily at 06:30 UTC. Configured in definitions.py.
"""

import logging
import os
from typing import Any

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

# MIT owner keys — kept in sync with learning_resources/etl/openedx.py
_MIT_OWNER_KEYS = frozenset(
    [
        "MITx",
        "MITx_PRO",
        "mitx",
        "mitxpro",
        "MITProfessionalX",
        "MITgcfx",
        "MITOCWx",
        "MITLinkedInDataScienceProf",
        "MITx_CMS",
    ]
)


def _get_oauth_token(token_url: str, client_id: str, client_secret: str) -> str:
    """Obtain an OAuth2 client credentials access token from edX."""
    resp = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "token_type": "jwt",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _fetch_all_programs(api_url: str, token: str) -> list[dict[str, Any]]:
    """Fetch all programs from the edX Programs API, following next-page links."""
    headers = {"Authorization": f"JWT {token}"}
    programs: list[dict[str, Any]] = []
    url: str | None = api_url
    while url:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        programs.extend(data.get("results", []))
        url = data.get("next")
    return programs


def _is_mit_program(program: dict[str, Any]) -> bool:
    """Return True if the program is a live, non-MicroMasters MIT program."""
    orgs = program.get("authoring_organizations") or []
    return (
        any(org.get("key") in _MIT_OWNER_KEYS for org in orgs)
        and "micromasters" not in (program.get("type") or "").lower()
        and program.get("status") == "active"
    )


def _transform_program(program: dict[str, Any]) -> dict[str, Any]:
    """Transform a raw edX program dict into MIT Learn's resource shape."""
    courses = [
        {"readable_id": c["key"]}
        for c in (program.get("courses") or [])
        if c.get("key")
    ]
    banner_image = program.get("banner_image")
    image_url = program.get("card_image_url") or (
        banner_image.get("large") if isinstance(banner_image, dict) else None
    )
    return {
        "readable_id": program.get("uuid"),
        "title": program.get("title", ""),
        "url": program.get("marketing_url"),
        "description": program.get("subtitle") or program.get("overview"),
        "image": {"url": image_url, "alt": program.get("title", "")}
        if image_url
        else None,
        "topics": [
            {"name": s["name"]}
            for s in (program.get("subjects") or [])
            if s.get("name")
        ],
        "published": program.get("status") == "active",
        "etl_source": "mit_edx",
        "offered_by": {"code": "mitx"},
        "platform": "edxorg",
        "resource_type": "program",
        "courses": courses,
    }


@asset(
    key=AssetKey(["mit_learn_delivery", "mit_edx_programs_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Fetch active MIT-authored programs from the edX.org Programs API, "
        "transform to MIT Learn resource shape, and POST as a signed webhook batch. "
        "Excludes MicroMasters (handled via Trino-pull in Cohort 1)."
    ),
    retry_policy=RetryPolicy(max_retries=3, delay=10.0),
)
def mit_edx_programs_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Deliver MIT edX programs to MIT Learn via signed webhook."""
    client_id = os.environ["EDX_API_CLIENT_ID"]
    client_secret = os.environ["EDX_API_CLIENT_SECRET"]
    token_url = os.environ["EDX_API_ACCESS_TOKEN_URL"]
    programs_api_url = os.environ["EDX_PROGRAMS_API_URL"]

    context.log.info("Obtaining edX OAuth2 token from %s", token_url)
    token = _get_oauth_token(token_url, client_id, client_secret)

    context.log.info("Fetching programs from %s", programs_api_url)
    all_programs = _fetch_all_programs(programs_api_url, token)
    context.log.info("Fetched %d total programs from edX", len(all_programs))

    mit_programs = [p for p in all_programs if _is_mit_program(p)]
    context.log.info(
        "Filtered to %d MIT-authored active non-MicroMasters programs",
        len(mit_programs),
    )

    resources = [
        r for p in mit_programs if (r := _transform_program(p)).get("readable_id")
    ]

    context.log.info(
        "Delivering %d MIT edX programs to MIT Learn webhook", len(resources)
    )
    try:
        response = learn_api.client.notify_mit_edx_programs(resources)
    except httpx.HTTPStatusError as exc:
        msg = (
            f"MIT edX programs webhook failed with status "
            f"{exc.response.status_code}: {exc}"
        )
        context.log.exception(msg)
        raise RuntimeError(msg) from exc

    context.add_output_metadata(
        {
            "total_fetched": len(all_programs),
            "mit_programs_count": len(mit_programs),
            "delivered_count": len(resources),
            "webhook_status": "success",
            "response": MetadataValue.json(response),
        }
    )
    return {
        "delivered_count": len(resources),
        "webhook_status": "success",
    }
