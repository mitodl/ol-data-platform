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
catalog. ``assert_deliverable`` guards both tables against that, raising
rather than delivering a suspiciously small batch. The two floors are
separate because the two models materialize independently: healthy channels
plus an empty episode table empties every delivered podcast while leaving the
podcasts themselves published, which is the failure that looks most like a
success.

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

from learning_resources.lib.sanitize import (
    ALLOWED_HTML_ATTRIBUTES_WITH_LINKS,
    ALLOWED_HTML_TAGS_WITH_LINKS,
    clean_html,
)

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
# Companion floor for the episode table, which materializes independently of
# the channel table. Deliberately far below the real episode count (thousands
# across 38 feeds) for the same reason MIN_PODCASTS is loose -- this catches an
# empty or truncated read, not natural variation. A per-podcast coverage
# assertion would be stricter, but a podcast whose feed legitimately yields no
# usable <item> (all entries missing <enclosure>) is a real, non-broken state,
# so a coverage rule would block valid deliveries.
MIN_EPISODES = 50

# "HH:MM:SS" or "MM:SS". The minute and hour groups are deliberately unbounded:
# mit-learn accepts "72:59" and normalizes it to PT1H12M59S rather than
# rejecting it or emitting PT72M59S, so every clock form is summed to seconds
# below and re-split, instead of being mapped component-for-component.
_CLOCK = re.compile(r"^(?:(\d+):)?(\d+):(\d{1,2})$")
# Strict ISO-8601 duration. Django's parse_duration -- which mit-learn's
# iso8601_duration delegates to -- REJECTS a malformed value like "PTBarnum"
# and returns None. A `startswith("PT")` passthrough would forward that string
# to PodcastEpisode.duration unvalidated, so the shape is matched properly.
_ISO = re.compile(r"^P(?!$)(?:(\d+)D)?(?:T(?!$)(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$")


def _duration_seconds(raw: str) -> int | None:
    """Reduce any accepted duration spelling to a total number of seconds."""
    clock = _CLOCK.match(raw)
    if clock:
        hours, minutes, seconds = (int(g or 0) for g in clock.groups())
        return hours * 3600 + minutes * 60 + seconds

    iso = _ISO.match(raw)
    if iso and any(group is not None for group in iso.groups()):
        days, hours, minutes, seconds = (int(g or 0) for g in iso.groups())
        return days * 86400 + hours * 3600 + minutes * 60 + seconds

    if raw.isdigit():
        return int(raw)

    return None


def _iso8601_duration(duration: str | None) -> str | None:
    """Normalize a raw itunes:duration to an ISO-8601 duration string.

    Mirrors ``learning_resources/etl/utils.py:iso8601_duration`` in mit-learn,
    whose behaviour is pinned by the parametrized table in
    ``learning_resources/etl/utils_test.py`` -- that table is ported verbatim
    into this project's tests, so parity is proven rather than asserted.

    Accepts "HH:MM:SS", "MM:SS", a bare seconds count, or an ISO-8601 duration;
    returns None (and warns) when a non-empty value cannot be parsed.

    Everything is reduced to total seconds and re-split, because mit-learn
    normalizes overflow: "72:59" is 1h12m59s, not 72 minutes.

    Parity is deliberate down to one rough edge: zero components are omitted,
    so most values stay short, but an episode of 10+ hours with non-zero
    minutes and seconds yields 11 characters ("PT10H30M15S") and overflows
    ``PodcastEpisode.duration``'s 10-character column. The Celery ETL has the
    same latent failure, so this is not a new divergence -- keeping the two
    byte-identical matters more during parallel validation than papering over
    it here would.
    """
    if not duration:
        return None

    total = _duration_seconds(duration.strip())
    if total is None:
        log.warning("Could not parse duration string %s", duration)
        return None

    hours, remainder = divmod(total, 3600)
    minutes, seconds = divmod(remainder, 60)

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


# mit-learn's podcast ETL is the one caller that passes the _WITH_LINKS
# allowlists (learning_resources/etl/podcast.py:205,275): RSS show notes carry
# resource links worth keeping, unlike the MIT PE descriptions clean_html
# sanitizes by default. Bound once here so both payload builders below cannot
# drift apart.
def _clean(value: str | None) -> str | None:
    """Sanitize an RSS field the way mit-learn's podcast ETL does.

    A falsy value is delivered unchanged rather than coerced to "" the way
    ``clean_data`` does. Not protection against clobbering: the "description"
    key is always present and ``LearningResource.description`` is
    ``TextField(null=True, blank=True)``, so ``update_or_create(defaults=...)``
    writes whatever we send -- None replaces a populated value just as "" would.
    The reason to preserve it is fidelity. Coercing None to "" would flip a NULL
    description to an empty string on every delivery, a diff the Celery ETL
    would not produce and therefore noise during parallel validation.
    """
    return clean_html(
        value,
        tags=ALLOWED_HTML_TAGS_WITH_LINKS,
        attributes=ALLOWED_HTML_ATTRIBUTES_WITH_LINKS,
    )


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
        "description": _clean(row.get("description")),
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
        "description": _clean(row.get("description")),
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


def assert_deliverable(podcast_count: int, episode_count: int) -> None:
    """Refuse to deliver a batch too small to be a real full sync.

    MIT Learn unpublishes every podcast and every episode absent from the
    delivered batch, so a short read is a catalog-wide outage rather than a
    small delivery. Both tables are checked independently: they materialize
    separately, so the episode table can be empty or half-written while the
    channel table looks entirely healthy. That combination empties every
    delivered podcast while the podcasts themselves survive -- the failure
    mode that looks most like a success.
    """
    if podcast_count < MIN_PODCASTS:
        msg = (
            f"Refusing to deliver {podcast_count} podcasts (floor is "
            f"{MIN_PODCASTS}): MIT Learn full-syncs this batch and would "
            "unpublish every podcast and episode not included."
        )
        raise RuntimeError(msg)

    if episode_count < MIN_EPISODES:
        msg = (
            f"Refusing to deliver {episode_count} episodes across "
            f"{podcast_count} podcasts (floor is {MIN_EPISODES}): MIT Learn "
            "unpublishes every episode absent from this batch, so a short "
            "episode read silently empties the podcasts it does deliver."
        )
        raise RuntimeError(msg)


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

    assert_deliverable(len(podcasts_df), len(episodes_df))

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
