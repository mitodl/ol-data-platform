"""Tests for ol_orchestrate.resources.oauth."""

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import httpx2 as httpx
import pytest
from ol_orchestrate.resources.oauth import OAuthApiClient


class _Response:
    """Minimal stand-in for an httpx response."""

    status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, str]:
        return {"username": "svc-account"}


class _ThrottlingClient:
    """httpx.Client stand-in that answers every GET with HTTP 429.

    Retry-After is 0 so the bounded-retry test exercises the retry path
    without actually sleeping.
    """

    def __init__(self) -> None:
        self.gets = 0

    def get(self, *args, **kwargs) -> httpx.Response:  # noqa: ARG002
        self.gets += 1
        request = httpx.Request("GET", "https://lms.example.com/api/thing/")
        return httpx.Response(429, request=request, headers={"Retry-After": "0"})


class _CountingClient:
    """httpx.Client stand-in that counts GETs and records their headers."""

    def __init__(self) -> None:
        self.gets = 0
        self.headers: list[dict[str, str]] = []

    def get(self, *args, **kwargs) -> _Response:  # noqa: ARG002
        self.gets += 1
        self.headers.append(kwargs.get("headers") or {})
        return _Response()


def _build_client(token_type: str = "JWT") -> OAuthApiClient:  # noqa: S107
    api_client = OAuthApiClient(
        client_id="id",
        client_secret="secret",  # pragma: allowlist secret
        token_type=token_type,
        token_url="https://lms.example.com/oauth2/access_token",
        base_url="https://lms.example.com",
    )
    api_client._http_client = _CountingClient()
    api_client._access_token = "token"  # noqa: S105
    api_client._access_token_expires = datetime.now(tz=UTC) + timedelta(hours=1)
    return api_client


@pytest.fixture
def client() -> OAuthApiClient:
    """Return a client with a pre-seeded token so no token fetch happens."""
    return _build_client()


def test_username_is_fetched_once(client: OAuthApiClient) -> None:
    """The /me lookup is cached, so repeated access costs one HTTP call."""
    assert client._username == "svc-account"
    assert client._username == "svc-account"
    assert client._username == "svc-account"

    http_client = client._http_client
    assert http_client is not None
    assert http_client.gets == 1


def test_username_is_fetched_once_under_concurrent_first_access() -> None:
    """A cold client hit from many threads must still make one /me request.

    The openedx/courseware observation fans outline fetches over a worker pool,
    and each of those calls fetch_with_auth, which reads this property. An
    unsynchronized check-then-set lets every worker observe the cold cache at
    once and issue its own lookup -- an authentication burst on every fresh
    resource instance, which is exactly what the cache exists to prevent.
    """
    workers = 8
    client = _build_client()
    barrier = threading.Barrier(workers)

    def read_username(_: int) -> str:
        barrier.wait(timeout=5)  # maximise the overlap on the cold cache
        return client._username

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(read_username, range(workers)))

    assert set(results) == {"svc-account"}
    http_client = client._http_client
    assert http_client is not None
    assert http_client.gets == 1


def test_rate_limit_retries_are_bounded() -> None:
    """429 retries stop, so a caller is guaranteed a return or an exception.

    The openedx/courseware observation waits on its whole worker pool, so a
    thread parked in an unbounded 429 backoff would hang the observation run
    for as long as the rate limit lasted.
    """
    client = _build_client()
    throttling = _ThrottlingClient()
    client._http_client = throttling
    client._cached_username = "svc-account"  # skip the /me lookup

    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_with_auth(
            "https://lms.example.com/api/thing/", rate_limit_retries=2
        )

    # The initial attempt plus exactly two retries.
    assert throttling.gets == 3


def test_username_lookup_honours_the_configured_token_type() -> None:
    """The /me call must use the resource's token type, not a hard-coded JWT.

    Every other authenticated request already interpolates ``token_type``, so
    a Bearer-configured client would have failed authentication on this one
    endpoint alone.
    """
    client = _build_client(token_type="Bearer")

    assert client._username == "svc-account"

    http_client = client._http_client
    assert http_client is not None
    assert http_client.headers[0]["Authorization"] == "Bearer token"


class _ThrottlingClientWithoutRetryAfter(_ThrottlingClient):
    """Answers 429 with no Retry-After header at all.

    Which is the ordinary case: the header is optional, and the edX courses API
    frequently omits it.
    """

    def get(self, *args, **kwargs) -> httpx.Response:  # noqa: ARG002
        self.gets += 1
        request = httpx.Request("GET", "https://lms.example.com/api/thing/")
        return httpx.Response(429, request=request)


def test_a_429_without_retry_after_still_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DAGSTER-E: the retry path for 429s crashed on a 429.

    The header default was the int 60, so ``retry_after.isdigit()`` raised
    AttributeError -- and the traceback then named an int rather than the rate
    limit that actually caused it.
    """
    slept: list[float] = []
    monkeypatch.setattr(
        "ol_orchestrate.resources.oauth.time.sleep",
        slept.append,
    )
    client = _build_client()
    throttling = _ThrottlingClientWithoutRetryAfter()
    client._http_client = throttling
    client._cached_username = "svc-account"

    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_with_auth(
            "https://lms.example.com/api/thing/", rate_limit_retries=2
        )

    assert throttling.gets == 3, "the retry path ran rather than raising AttributeError"
    assert slept == [60, 60], "and fell back to the documented 60 second wait"


def test_a_non_numeric_retry_after_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry-After may be an HTTP-date rather than a delta-seconds count."""
    slept: list[float] = []
    monkeypatch.setattr(
        "ol_orchestrate.resources.oauth.time.sleep",
        slept.append,
    )

    class _HttpDateRetryAfter(_ThrottlingClient):
        def get(self, *args, **kwargs) -> httpx.Response:  # noqa: ARG002
            self.gets += 1
            request = httpx.Request("GET", "https://lms.example.com/api/thing/")
            return httpx.Response(
                429,
                request=request,
                headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"},
            )

    client = _build_client()
    client._http_client = _HttpDateRetryAfter()
    client._cached_username = "svc-account"

    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_with_auth(
            "https://lms.example.com/api/thing/", rate_limit_retries=1
        )

    assert slept == [60]
