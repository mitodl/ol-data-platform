"""MIT podcast webhook delivery asset.

Reads pre-transformed podcast channels and episodes from the
``integrations__learn__podcasts`` / ``integrations__learn__podcast_episodes``
Iceberg tables (produced by dbt from the podcast_rss dlt pipeline), nests each
podcast's episodes inside it, and delivers the batch to MIT Learn via a single
signed webhook POST.

Data flow:
    raw__podcast__rss__channels  (Iceberg, via dlt)
    raw__podcast__rss__episodes  (Iceberg, via dlt)
        -> integrations__learn__podcasts          (dbt)
        -> integrations__learn__podcast_episodes  (dbt)
            -> MIT Learn webhook (this asset)

The payload shape mirrors ``learning_resources/etl/podcast.py:transform()`` in
mit-learn exactly, because MIT Learn's ``load_podcasts()`` pops a fixed set of
keys and passes whatever remains straight through as ``LearningResource``
model fields -- an unexpected key is a hard error, not an ignored extra.

FULL-SYNC HAZARD: ``load_podcasts()`` unpublishes every podcast and every
episode that is absent from the delivered batch. A partial read -- an empty
Iceberg table, a half-written dbt run -- would therefore unpublish the live
catalog. ``MIN_PODCASTS`` below is the floor that guards against that; the
asset raises rather than delivering a suspiciously small batch.

Scheduling: daily at 07:00 UTC. Configured in definitions.py.
"""

import logging
import re
from collections import defaultdict
from email.utils import parsedate_to_datetime
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
_PODCASTS_TABLE = "integrations__learn__podcasts"
_EPISODES_TABLE = "integrations__learn__podcast_episodes"

# Floor for the full-sync guard described in the module docstring. There are 38
# podcasts configured in mitodl/open-podcast-data as of 2026-08-19, so this is a
# deliberately loose floor rather than a close one: the dlt source skips any
# individual feed that fails to fetch or parse, and a handful of dead feeds is
# normal. Single digits, though, means the read is wrong -- not the catalog.
MIN_PODCASTS = 5

_HHMMSS = re.compile(r"^(?:(\d+):)?(\d{1,2}):(\d{2})$")


def _iso8601_duration(duration: str | None) -> str | None:
    """Normalize a raw itunes:duration to an ISO-8601 duration string.

    Mirrors ``learning_resources/etl/utils.py:iso8601_duration`` in mit-learn.
    Accepts "HH:MM:SS", "MM:SS", or a bare seconds count; returns None when the
    value is absent or unparseable, matching the Celery ETL's behaviour.

    Parity with that function is deliberate down to its rough edge: zero
    components are omitted, so most values stay short, but an episode of 10+
    hours with non-zero minutes and seconds yields 11 characters
    ("PT10H30M15S") and overflows ``PodcastEpisode.duration``'s 10-character
    column. The Celery ETL has the same latent failure, so this is not a new
    divergence -- keeping the two byte-identical matters more during parallel
    validation than papering over it here would.
    """
    if not duration:
        return None
    raw = duration.strip()
    if raw.startswith("PT"):
        return raw

    match = _HHMMSS.match(raw)
    if match:
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2))
        seconds = int(match.group(3))
    elif raw.isdigit():
        total = int(raw)
        hours, remainder = divmod(total, 3600)
        minutes, seconds = divmod(remainder, 60)
    else:
        log.warning("Could not parse duration string %s", raw)
        return None

    if not (hours or minutes or seconds):
        return "PT0S"
    return (
        "PT"
        f"{f'{hours}H' if hours else ''}"
        f"{f'{minutes}M' if minutes else ''}"
        f"{f'{seconds}S' if seconds else ''}"
    )


def _parse_pub_date(pub_date: str | None) -> str | None:
    """Parse an RFC 2822 <pubDate> into an ISO-8601 timestamp string."""
    if not pub_date:
        return None
    try:
        return parsedate_to_datetime(pub_date).isoformat()
    except (TypeError, ValueError):
        log.warning("Could not parse pubDate %s", pub_date)
        return None


def _topics(raw: str | None) -> list[dict[str, str]]:
    """Split the comma-separated topics column into MIT Learn's topic dicts."""
    if not raw:
        return []
    return [{"name": topic.strip()} for topic in raw.split(",") if topic.strip()]


def _offered_by(raw: str | None) -> dict[str, str] | None:
    """Wrap the offered_by name, or None when the config omits it."""
    return {"name": raw} if raw else None


def _image(url: str | None) -> dict[str, str] | None:
    """Wrap an image URL, or None when the feed exposes none."""
    return {"url": url} if url else None


def _episode_to_resource(
    row: dict[str, Any],
    topics: list[dict[str, str]],
    offered_by: dict[str, str] | None,
    parent_image: dict[str, str] | None,
) -> dict[str, Any]:
    """Map an episodes row to MIT Learn's podcast_episode payload shape.

    ``podcast_episode.rss`` (the raw <item> XML the Celery ETL stores) is
    deliberately omitted: the dlt source does not capture per-item XML, and
    the field is nullable. Omitting it leaves any existing value intact,
    because ``load_podcast_episode`` passes this dict as ``defaults=`` to
    ``update_or_create`` -- absent keys are not written.
    """
    return {
        "readable_id": row["readable_id"],
        "etl_source": "podcast",
        "resource_type": "podcast_episode",
        "title": row.get("title"),
        "offered_by": offered_by,
        "description": row.get("description"),
        "url": row.get("url"),
        "image": _image(row.get("image_url")) or parent_image,
        "last_modified": _parse_pub_date(row.get("published_on_raw")),
        "published": True,
        "topics": topics,
        "podcast_episode": {
            "audio_url": row["audio_url"],
            "episode_link": row.get("episode_link"),
            "duration": _iso8601_duration(row.get("duration_raw")),
        },
        "availability": "anytime",
    }


def _podcast_to_resource(
    row: dict[str, Any], episode_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    """Map a podcasts row plus its episodes to MIT Learn's podcast payload."""
    topics = _topics(row.get("topics"))
    offered_by = _offered_by(row.get("offered_by"))
    image = _image(row.get("image_url"))

    return {
        "readable_id": row["readable_id"],
        "title": row["title"],
        "etl_source": "podcast",
        "resource_type": "podcast",
        "offered_by": offered_by,
        "description": row.get("description"),
        "image": image,
        "published": True,
        "url": row.get("url"),
        "topics": topics,
        "episodes": [
            _episode_to_resource(episode, topics, offered_by, image)
            for episode in episode_rows
        ],
        "podcast": {
            "apple_podcasts_url": row.get("apple_podcasts_url"),
            "google_podcasts_url": row.get("google_podcasts_url"),
            "rss_url": row.get("rss_url"),
        },
        "availability": "anytime",
    }


def build_podcast_resources(
    podcasts: list[dict[str, Any]], episodes: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Join episodes onto their podcasts and build the webhook payload."""
    episodes_by_podcast: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for episode in episodes:
        episodes_by_podcast[episode["podcast_readable_id"]].append(episode)

    return [
        _podcast_to_resource(
            podcast, episodes_by_podcast.get(podcast["readable_id"], [])
        )
        for podcast in podcasts
    ]


@asset(
    key=AssetKey(["mit_learn_delivery", "podcast_webhook"]),
    group_name="mit_learn_delivery",
    description=(
        "Read MIT podcast channels and episodes from the "
        "integrations__learn__podcasts / integrations__learn__podcast_episodes "
        "Iceberg tables and POST as a signed webhook batch to MIT Learn."
    ),
    deps=[
        AssetKey(["integrations", "learn", "integrations__learn__podcasts"]),
        AssetKey(["integrations", "learn", "integrations__learn__podcast_episodes"]),
    ],
    retry_policy=RetryPolicy(max_retries=3, delay=5.0),
)
def podcast_webhook(
    context: AssetExecutionContext,
    learn_api: ApiClientFactory,
) -> dict[str, Any]:
    """Deliver MIT podcasts (with nested episodes) to MIT Learn via webhook."""
    context.log.info(
        "Reading %s and %s from Glue database %s",
        _PODCASTS_TABLE,
        _EPISODES_TABLE,
        _GLUE_DB,
    )
    podcasts_df: pl.DataFrame = get_dbt_model_as_dataframe(
        database_name=_GLUE_DB,
        table_name=_PODCASTS_TABLE,
    ).collect()
    episodes_df: pl.DataFrame = get_dbt_model_as_dataframe(
        database_name=_GLUE_DB,
        table_name=_EPISODES_TABLE,
    ).collect()
    context.log.info(
        "Loaded %d podcasts and %d episodes from Iceberg",
        len(podcasts_df),
        len(episodes_df),
    )

    # Full-sync guard -- see the module docstring. MIT Learn unpublishes every
    # podcast and episode missing from this batch, so a short read is a
    # catalog-wide outage, not a small delivery.
    if len(podcasts_df) < MIN_PODCASTS:
        msg = (
            f"Refusing to deliver {len(podcasts_df)} podcasts (floor is "
            f"{MIN_PODCASTS}): MIT Learn full-syncs this batch and would "
            "unpublish every podcast and episode not included."
        )
        raise RuntimeError(msg)

    resources = build_podcast_resources(
        list(podcasts_df.iter_rows(named=True)),
        list(episodes_df.iter_rows(named=True)),
    )
    episode_count = sum(len(resource["episodes"]) for resource in resources)

    context.log.info(
        "Delivering %d podcasts (%d episodes) to MIT Learn webhook",
        len(resources),
        episode_count,
    )
    try:
        response = cast(MITLearnApiClient, learn_api.client).notify_learning_resources(
            resources
        )
    except httpx.HTTPStatusError as exc:
        msg = f"Podcast webhook failed with status {exc.response.status_code}: {exc}"
        context.log.exception(msg)
        raise RuntimeError(msg) from exc

    context.add_output_metadata(
        {
            "resource_count": len(resources),
            "episode_count": episode_count,
            "webhook_status": "success",
            "response": MetadataValue.json(response),
        }
    )
    return {
        "resource_count": len(resources),
        "episode_count": episode_count,
        "webhook_status": "success",
    }
