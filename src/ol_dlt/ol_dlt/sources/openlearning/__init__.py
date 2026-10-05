"""MIT Open Learning website (openlearning.mit.edu) events ingestion via dlt.

Loads every event node from the site's Drupal JSON:API. MIT Learn's news_events
app reads the same endpoint (news_events/etl/ol_events.py) but then makes one
request per event for each of its audience, category, location and image
relationships. Here the JSON:API ``include`` parameter returns those related
entities alongside each page, and the loader resolves them onto the event row:
the taxonomy names as JSON arrays in relationship order, and the image's URL,
alt and title.

The table is fully replaced on every run, behind
``config.guard_against_replace_truncation``. Learn reads only the first page
(the 50 latest events) and keeps the upcoming ones; this loads every page so the
warehouse holds past events too.

Data flow:
    https://openlearning.mit.edu/jsonapi/node/event -> raw__openlearning__api__events

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.openlearning
"""

import json
import logging
from collections.abc import Generator, Iterator
from datetime import UTC, datetime
from typing import Any

import dlt
from dlt.sources.helpers import requests

from ol_dlt import config

logger = logging.getLogger(__name__)

OL_EVENTS_API_URL = "https://openlearning.mit.edu/jsonapi/node/event"
_TAXONOMY_RELATIONSHIPS = {
    "event_audience": "field_event_audience",
    "event_category": "field_event_category",
    "location_tag": "field_location_tag",
}
_INCLUDE = ",".join(
    [
        *_TAXONOMY_RELATIONSHIPS.values(),
        "field_event_image",
        "field_event_image.field_media_image",
    ]
)

EVENT_COLUMNS = (
    "id",
    "title",
    "path_alias",
    "event_date",
    "event_end_date",
    "body_value",
    "body_summary",
    "status",
    "created",
    "changed",
    "event_audience",
    "event_category",
    "location_tag",
    "image_url",
    "image_alt",
    "image_title",
    "retrieved_at",
)


def _related(
    included: dict[tuple[str, str], dict[str, Any]], ref: dict[str, Any] | None
) -> dict[str, Any] | None:
    return included.get((ref["type"], ref["id"])) if ref else None


def event_record(
    event: dict[str, Any],
    included: dict[tuple[str, str], dict[str, Any]],
    retrieved_at: str,
) -> dict[str, Any]:
    """Flatten one JSON:API event node, resolving its included relationships."""
    attributes = event["attributes"]
    relationships = event["relationships"]
    body = attributes.get("body") or {}
    event_date = attributes.get("field_event_date") or {}

    record: dict[str, Any] = {
        "id": event["id"],
        "title": attributes.get("title"),
        "path_alias": (attributes.get("path") or {}).get("alias"),
        "event_date": event_date.get("value"),
        "event_end_date": event_date.get("end_value"),
        "body_value": body.get("value"),
        "body_summary": body.get("summary"),
        "status": str(attributes.get("status")).lower(),
        "created": attributes.get("created"),
        "changed": attributes.get("changed"),
        "retrieved_at": retrieved_at,
    }
    for column, relationship in _TAXONOMY_RELATIONSHIPS.items():
        terms = [
            _related(included, ref)
            for ref in relationships.get(relationship, {}).get("data") or []
        ]
        record[column] = json.dumps(
            [term["attributes"]["name"] for term in terms if term]
        )

    # The media entity carries the alt/title on its file reference; the file
    # entity carries the URL. Learn reads them the same way.
    media = _related(included, relationships.get("field_event_image", {}).get("data"))
    file_ref = media["relationships"]["field_media_image"]["data"] if media else None
    image_file = _related(included, file_ref)
    record["image_url"] = image_file["attributes"]["uri"]["url"] if image_file else None
    record["image_alt"] = file_ref["meta"].get("alt") if file_ref else None
    record["image_title"] = file_ref["meta"].get("title") if file_ref else None
    return record


@dlt.source(name="openlearning_ingest")
def openlearning_source(api_url: str = OL_EVENTS_API_URL) -> Generator[Any]:
    """Load every event on openlearning.mit.edu.

    Args:
        api_url: The Drupal JSON:API collection URL for event nodes.
    """

    @dlt.resource(
        name="raw__openlearning__api__events",
        primary_key="id",
        write_disposition="replace",
        columns={
            name: {"data_type": "text", "nullable": name != "retrieved_at"}
            for name in EVENT_COLUMNS
        },
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def events() -> Iterator[dict[str, Any]]:
        retrieved_at = datetime.now(tz=UTC).isoformat()
        records: list[dict[str, Any]] = []
        url: str | None = api_url
        params: dict[str, str] | None = {
            "sort": "-field_event_date.value",
            "include": _INCLUDE,
        }
        while url:
            logger.info("Fetching Open Learning events from %s", url)
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            page = resp.json()
            included = {
                (entity["type"], entity["id"]): entity
                for entity in page.get("included", [])
            }
            records.extend(
                event_record(event, included, retrieved_at) for event in page["data"]
            )
            # The next link carries the sort, include and offset itself.
            url = page.get("links", {}).get("next", {}).get("href")
            params = None

        config.guard_against_replace_truncation(
            "raw__openlearning__api__events", len(records)
        )
        yield from records

    yield events


openlearning_pipeline = config.pipeline_for("openlearning")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source (uniform entrypoint for Dagster)."""
    return config.with_nullable_load_id(openlearning_source())
