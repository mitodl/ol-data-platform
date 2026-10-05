"""Unit + materialization tests for the openlearning.mit.edu events source."""

import json
from pathlib import Path
from typing import Any

import pytest

from ol_dlt import config
from ol_dlt.sources import openlearning
from tests.conftest import FakeResponse

_NEXT = f"{openlearning.OL_EVENTS_API_URL}?page%5Boffset%5D=50"


def _event(event_id: str, *, image: bool) -> dict[str, Any]:
    return {
        "type": "node--event",
        "id": event_id,
        "attributes": {
            "title": f"Event {event_id}",
            "path": {"alias": f"/events/{event_id}"},
            "field_event_date": {
                "value": "2026-10-06T17:30:00-04:00",
                "end_value": "2026-10-06T18:30:00-04:00",
            },
            "body": {"value": "<p>Body</p>", "summary": "", "format": "full_html"},
            "status": True,
            "created": "2026-09-01T12:00:00+00:00",
            "changed": "2026-09-02T12:00:00+00:00",
        },
        "relationships": {
            "field_event_audience": {
                "data": [
                    {"type": "taxonomy_term--event_audience", "id": "aud-2"},
                    {"type": "taxonomy_term--event_audience", "id": "aud-1"},
                ]
            },
            "field_event_category": {
                "data": [{"type": "taxonomy_term--event_type", "id": "cat-1"}]
            },
            "field_location_tag": {"data": []},
            "field_event_image": {
                "data": {"type": "media--event_image", "id": "media-1"}
                if image
                else None
            },
        },
    }


def _term(term_type: str, term_id: str, name: str) -> dict[str, Any]:
    return {"type": term_type, "id": term_id, "attributes": {"name": name}}


_INCLUDED = [
    _term("taxonomy_term--event_audience", "aud-1", "Faculty"),
    _term("taxonomy_term--event_audience", "aud-2", "Public"),
    _term("taxonomy_term--event_type", "cat-1", "Webinar"),
    {
        "type": "media--event_image",
        "id": "media-1",
        "relationships": {
            "field_media_image": {
                "data": {
                    "type": "file--file",
                    "id": "file-1",
                    "meta": {"alt": "Speakers", "title": ""},
                }
            }
        },
    },
    {
        "type": "file--file",
        "id": "file-1",
        "attributes": {"uri": {"url": "/sites/default/files/event.png"}},
    },
]
_PAGES = {
    openlearning.OL_EVENTS_API_URL: {
        "data": [_event("e1", image=True)],
        "included": _INCLUDED,
        "links": {"next": {"href": _NEXT}},
    },
    _NEXT: {"data": [_event("e2", image=False)], "included": _INCLUDED[:3]},
}


def _fake_get(url: str, **_kwargs: object) -> FakeResponse:
    return FakeResponse(json_data=_PAGES[url])


def test_relationships_resolve_from_included(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(openlearning.requests, "get", _fake_get)
    first, second = list(
        openlearning.openlearning_source().resources["raw__openlearning__api__events"]
    )
    # Relationship order, not the order the included entities arrive in.
    assert json.loads(first["event_audience"]) == ["Public", "Faculty"]
    assert json.loads(first["event_category"]) == ["Webinar"]
    assert json.loads(first["location_tag"]) == []
    assert first["image_url"] == "/sites/default/files/event.png"
    assert first["image_alt"] == "Speakers"
    assert first["event_date"] == "2026-10-06T17:30:00-04:00"
    assert first["status"] == "true"
    assert second["id"] == "e2"
    assert second["image_url"] is None


@pytest.mark.integration
def test_openlearning_materialization(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(openlearning.requests, "get", _fake_get)
    pipeline = config.pipeline_for("openlearning")
    info = pipeline.run(openlearning.openlearning_source())
    assert not info.has_failed_jobs

    table = pipeline.dataset()["raw__openlearning__api__events"].arrow()
    assert table.num_rows == 2  # noqa: PLR2004
    # Declared text, so the location column exists even though no event has one
    # and dlt's timestamp detection leaves the event dates as sent.
    assert table.schema.field("location_tag").type == "string"
    assert table.schema.field("event_date").type == "string"
