"""Tests for the Canvas assets."""

from types import SimpleNamespace
from typing import Any

import dagster as dg
import pytest
from canvas.assets.canvas import course_content_metadata

COURSE_ID = "155"


class _RecordingLearnClient:
    """Capture the webhook payload instead of sending it."""

    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    def notify_course_export(self, data: dict[str, Any]) -> dict[str, Any]:
        self.payloads.append(data)
        return {"status": "success", "message": "Webhook received"}


def _send_webhook(sis_course_id: str | None) -> dict[str, Any]:
    """Invoke the asset for one partition and return the payload it sent."""
    learn_client = _RecordingLearnClient()
    canvas_client = SimpleNamespace(
        get_course=lambda course_id: {  # noqa: ARG005
            "name": "MITx Test",
            "course_code": "MITxTest01",
            "sis_course_id": sis_course_id,
        }
    )
    context = dg.build_asset_context(
        partition_key=COURSE_ID,
        resources={
            "canvas_api": SimpleNamespace(client=canvas_client),
            "learn_api": SimpleNamespace(client=learn_client),
        },
    )

    course_content_metadata(
        context,
        f"canvas/course_content/{COURSE_ID}/abc.imscc",
        f"canvas/course_content/{COURSE_ID}/abc.metadata.json",
    )

    assert len(learn_client.payloads) == 1
    return learn_client.payloads[0]


def test_null_sis_course_id_is_omitted_from_the_payload():
    """Learn rejects a null course_readable_id, so the key must be absent."""
    payload = _send_webhook(sis_course_id=None)
    assert "course_readable_id" not in payload


def test_a_real_sis_course_id_is_sent():
    payload = _send_webhook(sis_course_id="2026SP:7.572")
    assert payload["course_readable_id"] == "2026SP:7.572"


@pytest.mark.parametrize("sis_course_id", [None, "2026SP:7.572"])
def test_the_rest_of_the_payload_is_unchanged(sis_course_id):
    """Omitting the key must not disturb the fields Learn actually uses."""
    payload = _send_webhook(sis_course_id=sis_course_id)
    assert payload["source"] == "canvas"
    assert payload["course_id"] == int(COURSE_ID)
    assert payload["content_path"] == f"canvas/course_content/{COURSE_ID}/abc.imscc"
    assert (
        payload["metadata_path"]
        == f"canvas/course_content/{COURSE_ID}/abc.metadata.json"
    )
