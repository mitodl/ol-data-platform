"""Tests for ol_orchestrate.resources.oauth."""

from datetime import UTC, datetime, timedelta

import pytest
from ol_orchestrate.resources.oauth import OAuthApiClient


class _Response:
    """Minimal stand-in for an httpx response."""

    status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, str]:
        return {"username": "svc-account"}


class _CountingClient:
    """httpx.Client stand-in that counts GETs."""

    def __init__(self) -> None:
        self.gets = 0

    def get(self, *args, **kwargs) -> _Response:  # noqa: ARG002
        self.gets += 1
        return _Response()


@pytest.fixture
def client() -> OAuthApiClient:
    """Return a client with a pre-seeded token so no token fetch happens."""
    api_client = OAuthApiClient(
        client_id="id",
        client_secret="secret",  # pragma: allowlist secret
        # pragma: allowlist secret
        token_url="https://lms.example.com/oauth2/access_token",
        base_url="https://lms.example.com",
    )
    api_client._http_client = _CountingClient()
    api_client._access_token = "token"  # noqa: S105
    api_client._access_token_expires = datetime.now(tz=UTC) + timedelta(hours=1)
    return api_client


def test_username_is_fetched_once(client: OAuthApiClient) -> None:
    """The /me lookup is cached, so repeated access costs one HTTP call."""
    assert client._username == "svc-account"
    assert client._username == "svc-account"
    assert client._username == "svc-account"

    http_client = client._http_client
    assert http_client is not None
    assert http_client.gets == 1
