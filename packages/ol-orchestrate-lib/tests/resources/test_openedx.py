"""Tests for ol_orchestrate.resources.openedx.

Focused on pagination, which is where the subtle bug was: the ``next`` URL was
parsed as though it were a bare query string, so the whole URL became a query
parameter *name* and each page nested it one level deeper.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx2 as httpx
import pytest
from ol_orchestrate.resources.openedx import OpenEdxApiClient, next_page_params

COURSES = "https://courses.learn.mit.edu/api/courses/v1/courses/"


def test_only_the_query_component_becomes_parameters() -> None:
    assert next_page_params(f"{COURSES}?page=2") == {"page": ["2"]}


def test_the_url_itself_never_becomes_a_parameter_name() -> None:
    """The DAGSTER-E mechanism.

    ``parse_qs`` on a full URL takes everything before the first ``=`` as the
    key, so this used to return
    ``{"https://courses.learn.mit.edu/api/courses/v1/courses/?page": ["2"]}``
    and that key was sent as a query parameter name. The server echoed the
    mangled parameters into the next ``next``, so each page nested the base URL
    one level deeper until the request 429'd.
    """
    params = next_page_params(f"{COURSES}?page=2")

    assert not any(key.startswith("http") for key in params), (
        f"a URL leaked into the parameter names: {list(params)}"
    )


def test_several_parameters_all_survive() -> None:
    params = next_page_params(f"{COURSES}?page=3&page_size=100&username=svc")

    assert params == {"page": ["3"], "page_size": ["100"], "username": ["svc"]}


def test_a_next_url_with_no_query_yields_nothing() -> None:
    """The last page can hand back a bare URL; that must not invent a filter."""
    assert next_page_params(COURSES) == {}


def test_a_relative_next_url_is_handled() -> None:
    """Some DRF configurations return a path rather than an absolute URL."""
    assert next_page_params("/api/courses/v1/courses/?page=4") == {"page": ["4"]}


class _PostingClient:
    """httpx.Client stand-in that records POSTs and answers with a fixed status."""

    def __init__(self, status_code: int, body: dict[str, Any]) -> None:
        self.status_code = status_code
        self.body = body
        self.posts: list[tuple[str, dict[str, Any]]] = []

    def post(self, url: str, **kwargs) -> httpx.Response:
        self.posts.append((url, kwargs))
        return httpx.Response(
            self.status_code, json=self.body, request=httpx.Request("POST", url)
        )


def _studio_client(http_client: _PostingClient) -> OpenEdxApiClient:
    client = OpenEdxApiClient(
        client_id="id",
        client_secret="secret",  # pragma: allowlist secret
        token_type="JWT",
        token_url="https://lms.example.com/oauth2/access_token",
        base_url="https://lms.example.com",
        studio_url="https://studio.example.com",
    )
    client._http_client = http_client
    client._access_token = "token"  # noqa: S105
    client._access_token_expires = datetime.now(tz=UTC) + timedelta(hours=1)
    return client


def test_content_versions_are_asked_of_studio_in_one_post() -> None:
    """The course ids go in the body, so a 200-course batch is one request."""
    body = {"versions": {}, "missing": ["course-v1:a+b+c"]}
    http_client = _PostingClient(200, body)

    result = _studio_client(http_client).get_course_content_versions(
        ["course-v1:a+b+c"]
    )

    assert result == body
    [(url, kwargs)] = http_client.posts
    assert url == "https://studio.example.com/api/courses/v0/export/versions/"
    assert kwargs["json"] == {"courses": ["course-v1:a+b+c"]}
    assert kwargs["headers"] == {"Authorization": "JWT token"}


def test_a_studio_without_the_versions_endpoint_raises() -> None:
    """A 404 must fail the batch, not read as a batch of courses with no facts."""
    http_client = _PostingClient(404, {"detail": "Not found."})

    with pytest.raises(httpx.HTTPStatusError):
        _studio_client(http_client).get_course_content_versions(["course-v1:a+b+c"])
