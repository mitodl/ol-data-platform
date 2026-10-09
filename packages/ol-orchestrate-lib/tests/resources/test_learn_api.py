"""Tests for the MIT Learn API client."""

import json
from typing import Any, cast

import httpx2 as httpx
import pytest
from ol_orchestrate.resources import api_client_factory
from ol_orchestrate.resources.api_client_factory import ApiClientFactory
from ol_orchestrate.resources.learn_api import MITLearnApiClient, webhook_status
from ol_orchestrate.resources.secrets.vault import Vault

FIRST_PAGE = "https://learn.example.com/api/v1/programs/?platform=edx&limit=100"
SECOND_PAGE = f"{FIRST_PAGE}&offset=100"


def test_get_published_programs_follows_pagination() -> None:
    """Every page is read, and the platform filter goes on the first request."""
    requests: list[httpx.Request] = []
    pages = {
        FIRST_PAGE: {"results": [{"readable_id": "a"}], "next": SECOND_PAGE},
        SECOND_PAGE: {"results": [{"readable_id": "b"}], "next": None},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=pages[str(request.url)])

    client = MITLearnApiClient(base_url="https://learn.example.com", token="t")
    client._http_client = httpx.Client(transport=httpx.MockTransport(handler))

    programs = client.get_published_programs("edx")

    assert [p["readable_id"] for p in programs] == ["a", "b"]
    assert len(requests) == 2


def test_webhook_status_names_a_shadow_run() -> None:
    """A response carrying shadow counts was loaded and rolled back, not delivered."""
    delivered = {"status": "success", "message": "Webhook received"}
    shadowed = {
        **delivered,
        "shadow": [
            {"etl_source": "mitpe", "resource_type": "course", "counts": {"created": 1}}
        ],
    }

    assert webhook_status(delivered) == "success"
    assert webhook_status(shadowed) == "shadow"


def _sent_batch(**kwargs: Any) -> dict[str, Any]:
    sent: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, json={"status": "success"})

    client = MITLearnApiClient(base_url="https://learn.example.com", token="t")
    client._http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client.notify_learning_resources(**kwargs)
    return json.loads(sent[0].content)


def test_notify_learning_resources_declares_sync_pairs() -> None:
    """Declared pairs go in the batch, so a pair with no resources is pruned."""
    assert _sent_batch(resources=[], sync=[("mitpe", "program")]) == {
        "resources": [],
        "sync": [{"etl_source": "mitpe", "resource_type": "program"}],
    }


def test_notify_learning_resources_omits_sync_by_default() -> None:
    """A batch that declares nothing has the shape it always had."""
    resource = {"readable_id": "a", "etl_source": "oll", "resource_type": "course"}
    assert _sent_batch(resources=[resource]) == {"resources": [resource]}


LOCAL_LEARN = "https://api.learn.mit.dev"
VAULT_SECRET = {"learn": {"url": "https://learn.example.com", "token": "from-vault"}}


def _learn_api_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[ApiClientFactory, list[str]]:
    """Build the delivery location's learn_api, recording each Vault read."""
    vault_reads: list[str] = []

    def read_vault_secret(_self: ApiClientFactory, **kwargs: str) -> dict[str, Any]:
        vault_reads.append(kwargs["path"])
        return VAULT_SECRET

    monkeypatch.setattr(ApiClientFactory, "_read_vault_secret", read_vault_secret)
    factory = ApiClientFactory(
        deployment="mit-learn",
        client_class="MITLearnApiClient",
        mount_point="secret-global",
        config_path="shared_hmac",
        kv_version="2",
        vault=Vault(vault_addr="https://vault.example.com", vault_auth_type="oidc"),
    )
    return factory, vault_reads


def test_a_local_mit_learn_is_configured_without_vault(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A base URL and a secret in the environment are a complete configuration."""
    monkeypatch.setenv("MIT_LEARN_BASE_URL", LOCAL_LEARN)
    monkeypatch.setenv("MIT_LEARN_WEBHOOK_SECRET", "local-secret")
    factory, vault_reads = _learn_api_factory(monkeypatch)

    client = cast(MITLearnApiClient, factory.client)

    assert (client.base_url, client.token) == (LOCAL_LEARN, "local-secret")
    assert vault_reads == []


def test_one_variable_alone_still_reads_vault(monkeypatch: pytest.MonkeyPatch) -> None:
    """A base URL on its own overrides the Vault secret, it does not replace it."""
    monkeypatch.setenv("MIT_LEARN_BASE_URL", LOCAL_LEARN)
    monkeypatch.delenv("MIT_LEARN_WEBHOOK_SECRET", raising=False)
    factory, vault_reads = _learn_api_factory(monkeypatch)

    client = cast(MITLearnApiClient, factory.client)

    assert (client.base_url, client.token) == (LOCAL_LEARN, "from-vault")
    assert vault_reads == ["shared_hmac"]


def test_a_deployed_environment_reads_vault_whatever_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Outside dev the environment cannot stand in for the Vault secret."""
    monkeypatch.setattr(api_client_factory, "DAGSTER_ENV", "production")
    monkeypatch.setenv("MIT_LEARN_BASE_URL", LOCAL_LEARN)
    monkeypatch.setenv("MIT_LEARN_WEBHOOK_SECRET", "local-secret")
    factory, vault_reads = _learn_api_factory(monkeypatch)

    _ = factory.client

    assert vault_reads == ["shared_hmac"]
