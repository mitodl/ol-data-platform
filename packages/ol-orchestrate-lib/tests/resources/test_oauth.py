"""Tests for ol_orchestrate.resources.oauth."""

from unittest.mock import MagicMock

import httpx2 as httpx
import pytest
from ol_orchestrate.resources.oauth import OAuthApiClient

BASE_URL = "https://lms.example.edu"


def _json_response(payload: dict[str, object]) -> MagicMock:
    response = MagicMock(spec=httpx.Response)
    response.status_code = 200
    response.json.return_value = payload
    response.raise_for_status = MagicMock()
    return response


def build_client(token_url: str | None = None) -> OAuthApiClient:
    api_client = OAuthApiClient(
        client_id="test-id",
        client_secret="test-secret",  # pragma: allowlist secret
        base_url=BASE_URL,
        token_url=token_url or f"{BASE_URL}/oauth2/access_token",
    )
    http_client = MagicMock(spec=httpx.Client)
    http_client.post.return_value = _json_response(
        {"access_token": "test-token", "expires_in": 3600}
    )
    api_client._http_client = http_client
    return api_client


@pytest.fixture
def client() -> OAuthApiClient:
    return build_client()


def test_username_is_fetched_once_and_reused(client):
    """The /me lookup is cached, so it does not double the request count.

    fetch_with_auth passes the username as a query param on every call; re-resolving
    it per request is what made the course version sensor issue two HTTP round trips
    per course run (mitodl/hq#12739).
    """
    client.http_client.get.side_effect = [
        _json_response({"username": "service-account"}),
        _json_response({"result": "first"}),
        _json_response({"result": "second"}),
    ]

    assert client.fetch_with_auth(f"{BASE_URL}/api/thing/") == {"result": "first"}
    assert client.fetch_with_auth(f"{BASE_URL}/api/thing/") == {"result": "second"}

    me_calls = [
        call
        for call in client.http_client.get.call_args_list
        if call.args and call.args[0].endswith("/api/user/v1/me")
    ]
    assert len(me_calls) == 1
    for call in client.http_client.get.call_args_list[1:]:
        assert call.kwargs["params"]["username"] == "service-account"


def test_username_is_not_requested_for_non_openedx_token_urls():
    """Clients whose token URL is external don't get the username param at all."""
    client = build_client(token_url="https://auth.example.com/oauth2/token")
    client.http_client.get.side_effect = [_json_response({"result": "first"})]

    client.fetch_with_auth(f"{BASE_URL}/api/thing/")

    assert "username" not in client.http_client.get.call_args.kwargs["params"]
