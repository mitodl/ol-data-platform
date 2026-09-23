"""Tests for ol_orchestrate.lib.http_errors."""

import httpx2 as httpx
import pytest
from dagster import Failure
from ol_orchestrate.lib.http_errors import (
    RESPONSE_BODY_LIMIT,
    PermanentHTTPFailure,
    TransientHTTPFailure,
    http_failure,
    is_retryable,
)

WEBHOOK_URL = "https://api.learn.mit.edu/api/v1/webhooks/content_files/"


def _error(status_code: int, content: bytes = b"") -> httpx.HTTPStatusError:
    request = httpx.Request("POST", WEBHOOK_URL)
    response = httpx.Response(status_code, request=request, content=content)
    return httpx.HTTPStatusError(
        f"HTTP {status_code}", request=request, response=response
    )


@pytest.mark.parametrize("status_code", [500, 502, 503, 504, 599, 408, 429])
def test_transient_statuses_are_retryable(status_code: int) -> None:
    """5xx plus the two 4xx codes that invite another attempt."""
    assert is_retryable(status_code) is True
    failure = http_failure(_error(status_code), "boom")
    assert failure.allow_retries is True
    assert isinstance(failure, TransientHTTPFailure)


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 405, 422])
def test_permanent_client_errors_are_not_retryable(status_code: int) -> None:
    """A rejected request stays rejected however many times it is sent.

    DAGSTER-4 is a 405 repeated 53 times against the Learn webhook, and
    DAGSTER-C a 404 for a Canvas course that no longer exists.
    """
    assert is_retryable(status_code) is False
    failure = http_failure(_error(status_code), "boom")
    assert failure.allow_retries is False
    assert isinstance(failure, PermanentHTTPFailure)


@pytest.mark.parametrize("status_code", [301, 302, 307, 308])
def test_redirects_are_not_retryable(status_code: int) -> None:
    """The policy is 5xx plus 408/429, not "anything that is not 4xx".

    The first cut inverted the 4xx test, so a redirect surfacing as an
    HTTPStatusError was labelled retryable and re-run.
    """
    assert is_retryable(status_code) is False
    assert isinstance(http_failure(_error(status_code), "boom"), PermanentHTTPFailure)


def test_transient_and_permanent_fingerprint_differently() -> None:
    """A 405 and a 502 from one step must not collapse into one Sentry issue.

    Both reporting paths fingerprint on the exception's class name -- the hook
    via ``type(exception).__name__`` and the run sensor via the serialized
    ``cls_name`` -- so returning a plain ``Failure`` for every status made
    every HTTP failure from a given step the same issue.
    """
    permanent = http_failure(_error(405), "boom")
    transient = http_failure(_error(502), "boom")

    assert type(permanent).__name__ != type(transient).__name__
    # Still Failures, so Dagster re-raises them without wrapping and the two
    # fingerprint paths stay aligned with each other.
    assert isinstance(permanent, Failure)
    assert isinstance(transient, Failure)


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


def test_failure_carries_the_rejection_reason_from_the_body() -> None:
    """MIT Learn names the rejected field in the body; that is the diagnosis.

    DAGSTER-53/54 reported an OVS webhook 400 for a week with nothing to say
    which field was wrong, because the body was discarded here.
    """
    body = b'{"video": {"thumbnail_url": ["URL host is not allowed."]}}'
    failure = http_failure(_error(400, body), "OVS video webhook notification failed")

    assert failure.metadata["response_body"].value == body.decode()
    assert "thumbnail_url" not in str(failure.description), (
        "the body belongs in metadata; in the description it changes the title "
        "for every distinct payload"
    )


def test_a_long_body_is_truncated() -> None:
    failure = http_failure(_error(502, b"x" * (RESPONSE_BODY_LIMIT * 3)), "boom")

    assert len(failure.metadata["response_body"].value) == RESPONSE_BODY_LIMIT


def test_an_empty_body_adds_no_metadata() -> None:
    assert "response_body" not in http_failure(_error(404), "boom").metadata


def test_an_unread_streamed_body_adds_no_metadata() -> None:
    """A ``raise_for_status()`` inside ``stream()`` fires before the body is read.

    Touching ``.text`` there raises ``ResponseNotRead``, which would replace the
    HTTP failure with an error about the error.
    """
    request = httpx.Request("GET", WEBHOOK_URL)
    response = httpx.Response(
        404, request=request, stream=httpx.ByteStream(b"not found")
    )
    error = httpx.HTTPStatusError("HTTP 404", request=request, response=response)

    failure = http_failure(error, "boom")

    assert "response_body" not in failure.metadata
    assert failure.metadata["status_code"].value == 404
