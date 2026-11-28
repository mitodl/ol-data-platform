from typing import Any

import pytest
from fastapi.testclient import TestClient
from jupyterhub.services.auth import check_scopes
from notebook_publisher.main import create_app
from notebook_publisher.service import Publisher
from notebook_publisher.settings import Settings

from notebook_publisher_tests.conftest import NOTEBOOK

SCOPE = "access:services!service=notebook-publisher"
BASE = "/services/notebook-publisher/api/notebooks"
TOKENS: dict[str, dict[str, Any]] = {
    "alice-token": {"kind": "user", "name": "alice@mit.edu", "scopes": [SCOPE]},
    "bob-token": {"kind": "user", "name": "bob@mit.edu", "scopes": [SCOPE]},
    # A server token from a hub that didn't grant the publisher's scope.
    "unscoped-token": {
        "kind": "user",
        "name": "alice@mit.edu",
        "scopes": ["access:servers!server=alice@mit.edu/"],
    },
    "service-token": {"kind": "service", "name": "other", "scopes": [SCOPE]},
}


class StubHubAuth:
    """Answers like HubAuth for a fixed set of tokens."""

    access_scopes = frozenset({SCOPE})

    async def user_for_token(self, token: str, *, sync: bool) -> dict[str, Any] | None:
        assert sync is False
        return TOKENS.get(token)

    def check_scopes(self, required: frozenset[str], model: dict[str, Any]) -> set[str]:
        return check_scopes(required, set(model["scopes"]))


@pytest.fixture
def api(settings: Settings, publisher: Publisher) -> TestClient:
    app = create_app(settings, publisher, StubHubAuth())
    return TestClient(app)


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"token {token}"}


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({}, 401),
        ({"Authorization": "Basic abc"}, 401),
        (_auth("unknown"), 401),
        (_auth("service-token"), 401),
        (_auth("unscoped-token"), 403),
    ],
)
def test_requires_a_scoped_user_token(
    api: TestClient, headers: dict[str, str], expected: int
) -> None:
    assert api.get(BASE, headers=headers).status_code == expected


def test_publish_update_and_unpublish(api: TestClient) -> None:
    created = api.post(
        BASE, json={"name": "q3", "content": NOTEBOOK}, headers=_auth("alice-token")
    )
    assert created.status_code == 201, created.text
    assert created.json()["url"] == "https://apps.nb-ci.data.ol.mit.edu/q3/"

    listed = api.get(BASE, headers=_auth("alice-token")).json()
    assert [n["name"] for n in listed] == ["q3"]
    assert api.get(BASE, headers=_auth("bob-token")).json() == []

    bob_update = api.put(
        f"{BASE}/q3", json={"content": NOTEBOOK}, headers=_auth("bob-token")
    )
    assert bob_update.status_code == 403
    assert api.delete(f"{BASE}/q3", headers=_auth("bob-token")).status_code == 403
    taken = api.post(
        BASE, json={"name": "q3", "content": NOTEBOOK}, headers=_auth("bob-token")
    )
    assert taken.status_code == 409

    updated = api.put(
        f"{BASE}/q3", json={"content": NOTEBOOK}, headers=_auth("alice-token")
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["revision"] != created.json()["revision"]

    assert api.delete(f"{BASE}/q3", headers=_auth("alice-token")).status_code == 204
    assert api.get(f"{BASE}/q3", headers=_auth("alice-token")).status_code == 404


def test_rejected_notebooks_say_why(api: TestClient) -> None:
    response = api.post(
        BASE,
        json={"name": "q3", "content": "print('hi')\n"},
        headers=_auth("alice-token"),
    )
    assert response.status_code == 422
    assert "script" in response.json()["detail"]


def test_health_needs_no_token(api: TestClient) -> None:
    assert api.get("/healthz").status_code == 200
