"""A dropped export-status poll must not rerun course_xml and re-export the course."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx2
import pytest
from ol_orchestrate.lib.openedx import (
    CourseExportOutcome,
    classify_course_export_state,
)
from openedx.assets.openedx import _poll_export_status


def context_whose_poll(side_effect: object) -> SimpleNamespace:
    client = MagicMock()
    client.check_course_export_status.side_effect = side_effect
    return SimpleNamespace(
        resources=SimpleNamespace(openedx=SimpleNamespace(client=client)),
        log=MagicMock(),
    )


def test_a_dropped_poll_reads_as_still_pending() -> None:
    context = context_whose_poll(httpx2.ReadError("Connection reset by peer"))

    status = _poll_export_status(context, "course-v1:MITx+1+2T2026", "task-1")

    assert classify_course_export_state(status.get("state")) is (
        CourseExportOutcome.PENDING
    )
    assert status["details"] == "status request failed: ReadError"


def test_an_http_error_status_still_raises() -> None:
    error = httpx2.HTTPStatusError(
        "500",
        request=httpx2.Request("GET", "https://studio.example.com"),
        response=httpx2.Response(500),
    )
    context = context_whose_poll(error)

    with pytest.raises(httpx2.HTTPStatusError):
        _poll_export_status(context, "course-v1:MITx+1+2T2026", "task-1")


def test_a_successful_poll_returns_studio_status() -> None:
    context = context_whose_poll(None)
    context.resources.openedx.client.check_course_export_status.return_value = {
        "state": "Succeeded"
    }

    status = _poll_export_status(context, "course-v1:MITx+1+2T2026", "task-1")

    assert status == {"state": "Succeeded"}
