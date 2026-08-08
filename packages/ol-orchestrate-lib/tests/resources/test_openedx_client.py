"""Tests for ol_orchestrate.resources.openedx.OpenEdxApiClient."""

from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

import pytest
from ol_orchestrate.resources.openedx import OpenEdxApiClient

BASE_URL = "https://lms.example.com"
COURSES_URL = f"{BASE_URL}/api/courses/v1/courses/"


class _PaginatingClient:
    """Serves three pages of courses and records the params it was asked for.

    Mirrors Django REST Framework, which is what the Open edX courses API runs
    on: ``pagination.next`` is an absolute URL built from the *current request*
    with ``page`` swapped in, so whatever the client sent is echoed straight
    back. That echo is what let the old parse_qs bug compound -- each round
    trip folded the previous URL into the next one's query string.
    """

    def __init__(self) -> None:
        self.received_params: list[dict[str, str]] = []

    def get(self, url, headers=None, params=None):  # noqa: ARG002
        recorded = dict(params or {})
        self.received_params.append(recorded)
        page = int(recorded.get("page", 1))
        return _PageResponse(page, recorded)


class _PageResponse:
    total_pages = 3

    def __init__(self, page: int, request_params: dict[str, str]) -> None:
        self._page = page
        self._request_params = request_params

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        next_page = None
        if self._page < self.total_pages:
            echoed = {**self._request_params, "page": str(self._page + 1)}
            next_page = f"{COURSES_URL}?{urlencode(echoed)}"
        return {
            "results": [{"id": f"course-v1:Org+Num+Run{self._page}"}],
            "pagination": {"next": next_page},
        }


@pytest.fixture
def client() -> OpenEdxApiClient:
    api_client = OpenEdxApiClient(
        client_id="id",
        client_secret="secret",  # pragma: allowlist secret
        token_type="JWT",
        token_url=f"{BASE_URL}/oauth2/access_token",
        base_url=BASE_URL,
    )
    api_client._http_client = _PaginatingClient()
    api_client._access_token = "token"  # noqa: S105
    api_client._access_token_expires = datetime.now(tz=UTC) + timedelta(hours=1)
    api_client._cached_username = "svc-account"
    return api_client


def test_pagination_sends_only_the_next_page_query(client: OpenEdxApiClient) -> None:
    """Only the `next` URL's query string is forwarded, not the whole URL.

    ``parse_qs`` on an absolute URL returns a single entry whose key is
    everything up to the first '=' -- the entire URL. That meant the real
    ``page`` parameter was never sent and each request re-fetched page 1,
    while the junk key accumulated another copy of the URL every round trip
    until it was long enough to trip the rate limiter.
    """
    pages = list(client.get_edx_course_ids())

    http_client = client._http_client
    assert http_client is not None
    requested_pages = [params.get("page") for params in http_client.received_params]
    assert requested_pages == [None, "2", "3"]

    # No parameter name may be a URL -- that is the signature of the bug.
    for params in http_client.received_params:
        assert not any(key.startswith("http") for key in params)

    assert [entry[0]["id"] for entry in pages] == [
        "course-v1:Org+Num+Run1",
        "course-v1:Org+Num+Run2",
        "course-v1:Org+Num+Run3",
    ]


def test_pagination_terminates(client: OpenEdxApiClient) -> None:
    """The walk stops when the API stops offering a next page."""
    assert len(list(client.get_edx_course_ids())) == _PageResponse.total_pages


# ── discovery.edx.org walkers ─────────────────────────────────────────────────
#
# These use the plain DRF envelope -- `next`/`count` at the top level rather
# than nested under `pagination` -- but had the identical parse_qs defect.

DISCOVERY_PROGRAMS_URL = "https://discovery.edx.org/api/v1/programs/"
DISCOVERY_COURSES_URL = "https://discovery.edx.org/api/v1/catalogs/10/courses/"


class _DiscoveryResponse:
    total_pages = 3

    def __init__(self, page: int, base_url: str) -> None:
        self._page = page
        self._base_url = base_url

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        next_page = (
            f"{self._base_url}?page={self._page + 1}"
            if self._page < self.total_pages
            else None
        )
        return {
            "results": [{"uuid": f"program-{self._page}"}],
            "next": next_page,
            "count": self.total_pages,
        }


class _DiscoveryClient:
    def __init__(self, base_url: str) -> None:
        self._base_url = base_url
        self.received_params: list[dict[str, str]] = []

    def get(self, url, headers=None, params=None):  # noqa: ARG002
        recorded = dict(params or {})
        self.received_params.append(recorded)
        return _DiscoveryResponse(int(recorded.get("page", 1)), self._base_url)


@pytest.mark.parametrize(
    ("method_name", "base_url"),
    [
        ("get_edxorg_programs", DISCOVERY_PROGRAMS_URL),
        ("get_edxorg_mitx_courses", DISCOVERY_COURSES_URL),
    ],
)
def test_discovery_walkers_send_only_the_next_page_query(
    client: OpenEdxApiClient, method_name: str, base_url: str
) -> None:
    """Both discovery walkers had the same whole-URL-as-parameter-name bug.

    Left out of the first pass on this file; flagged on review as HIGH because
    both are reachable from edxorg/assets/edxorg_api.py, so the loop really
    does run against a live API.
    """
    http_client = _DiscoveryClient(base_url)
    client._http_client = http_client

    pages = list(getattr(client, method_name)())

    requested = [params.get("page") for params in http_client.received_params]
    assert requested == [None, "2", "3"]
    for params in http_client.received_params:
        assert not any(key.startswith("http") for key in params)
    # First yield is (count, results); the rest are bare result lists.
    assert pages[0][0] == _DiscoveryResponse.total_pages
    assert len(pages) == _DiscoveryResponse.total_pages
