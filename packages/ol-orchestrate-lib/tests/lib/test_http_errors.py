"""Tests for ol_orchestrate.lib.http_errors."""

import httpx2 as httpx
import pytest
from ol_orchestrate.lib.http_errors import http_failure, is_retryable

WEBHOOK_URL = "https://api.learn.mit.edu/api/v1/webhooks/content_files/"


def _error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", WEBHOOK_URL)
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(
        f"HTTP {status_code}", request=request, response=response
    )


@pytest.mark.parametrize("status_code", [500, 502, 503, 504, 408, 429])
def test_transient_statuses_are_retryable(status_code: int) -> None:
    """5xx plus the two 4xx codes that invite another attempt."""
    assert is_retryable(status_code) is True
    assert http_failure(_error(status_code), "boom").allow_retries is True


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 405, 422])
def test_permanent_client_errors_are_not_retryable(status_code: int) -> None:
    """A rejected request stays rejected however many times it is sent.

    DAGSTER-4 is a 405 repeated 53 times against the Learn webhook, and
    DAGSTER-C a 404 for a Canvas course that no longer exists. Neither can be
    cleared by run_retries, which was re-running each one four times.
    """
    assert is_retryable(status_code) is False
    assert http_failure(_error(status_code), "boom").allow_retries is False


def test_failure_description_names_the_request_and_the_verdict() -> None:
    """The Sentry issue should say what was attempted and whether to bother."""
    failure = http_failure(
        _error(405), "Learn API webhook notification failed for course_id=155"
    )

    description = str(failure.description)
    assert "course_id=155" in description
    assert "HTTP 405" in description
    assert f"POST {WEBHOOK_URL}" in description
    assert "Retrying cannot clear this" in description


def test_failure_carries_structured_metadata() -> None:
    """Status and URL land on the Dagster event, not only in the message."""
    failure = http_failure(_error(502), "boom", metadata={"course_id": 155})

    metadata = failure.metadata
    assert metadata["status_code"].value == 502
    assert metadata["retryable"].value is True
    assert metadata["course_id"].value == 155
