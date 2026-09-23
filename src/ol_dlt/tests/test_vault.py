"""Tests for Vault credential resolution."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import hvac
import pytest

from ol_dlt import vault


@pytest.fixture(autouse=True)
def _clear_credential_cache() -> None:
    vault._CREDENTIAL_CACHE.clear()  # noqa: SLF001


class FakeVaultClient:
    """Records reads and returns a canned database-credentials response.

    ``read_delay`` widens the window between a cache miss and the cache write,
    which is what a real network round-trip to Vault does. Without it a fake
    read returns so fast that a racing thread is never actually scheduled
    inside the window, and a concurrency test passes whether or not the code
    under test is correct.
    """

    def __init__(
        self,
        response: Any = None,  # noqa: ANN401
        error: Exception | None = None,
        read_delay: float = 0.0,
    ) -> None:
        self.response = response
        self.error = error
        self.read_delay = read_delay
        self.reads: list[str] = []

    def read(self, path: str) -> Any:  # noqa: ANN401
        self.reads.append(path)
        if self.read_delay:
            time.sleep(self.read_delay)
        if self.error:
            raise self.error
        return self.response


def _credentials_response(lease_duration: int = 3600) -> dict[str, Any]:
    return {
        "lease_duration": lease_duration,
        "data": {
            "username": "v-user-1",
            "password": "v-pass-1",  # pragma: allowlist secret
        },
    }


def test_vault_address_prefers_the_explicit_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VAULT_ADDR", "https://vault.example.test")
    assert vault.vault_address() == "https://vault.example.test"


def test_vault_address_for_dev_points_at_qa(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local development must never reach production Vault by default."""
    monkeypatch.delenv("VAULT_ADDR", raising=False)
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", "dev")
    assert vault.vault_address() == "https://vault-qa.odl.mit.edu"


def test_vault_address_follows_the_dagster_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VAULT_ADDR", raising=False)
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", "production")
    assert vault.vault_address() == "https://vault-production.odl.mit.edu"


def test_reads_the_readonly_credentials_path(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeVaultClient(_credentials_response())
    monkeypatch.setattr(vault, "_authenticated_client", lambda: client)
    assert vault.read_database_credentials("postgres-keycloak") == (
        "v-user-1",
        "v-pass-1",
    )
    assert client.reads == ["postgres-keycloak/creds/readonly"]


def test_credentials_are_cached_within_the_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeVaultClient(_credentials_response(lease_duration=3600))
    monkeypatch.setattr(vault, "_authenticated_client", lambda: client)
    vault.read_database_credentials("postgres-keycloak")
    vault.read_database_credentials("postgres-keycloak")
    # One lease serves every table in a run rather than one lease per table.
    assert len(client.reads) == 1


def test_credentials_are_refetched_once_the_lease_is_half_spent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A code-location pod outlives any single lease, so the cache must expire."""
    client = FakeVaultClient(_credentials_response(lease_duration=100))
    monkeypatch.setattr(vault, "_authenticated_client", lambda: client)
    clock = [1000.0]
    monkeypatch.setattr(vault.time, "monotonic", lambda: clock[0])

    vault.read_database_credentials("postgres-keycloak")
    clock[0] += 60  # past 50% of the 100s lease
    vault.read_database_credentials("postgres-keycloak")
    assert len(client.reads) == 2  # noqa: PLR2004


def test_concurrent_callers_share_one_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parallel extraction must not open a lease per worker.

    Extraction is single-threaded today, but the cache's whole purpose is
    "one lease per run", so it should not quietly depend on that.
    """
    client = FakeVaultClient(
        _credentials_response(lease_duration=3600), read_delay=0.05
    )
    monkeypatch.setattr(vault, "_authenticated_client", lambda: client)
    start = threading.Barrier(8)

    def _fetch() -> tuple[str, str]:
        start.wait()  # maximize the odds of a check-then-act overlap
        return vault.read_database_credentials("postgres-keycloak")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = [
            future.result() for future in [pool.submit(_fetch) for _ in range(8)]
        ]

    assert len(client.reads) == 1
    assert set(results) == {("v-user-1", "v-pass-1")}


def test_a_response_without_credentials_names_the_likely_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-database mount must not surface as a bare KeyError."""
    kv_v2_shaped = {"lease_duration": 0, "data": {"data": {"foo": "bar"}}}
    monkeypatch.setattr(
        vault, "_authenticated_client", lambda: FakeVaultClient(kv_v2_shaped)
    )
    with pytest.raises(RuntimeError, match="database secrets-engine mount"):
        vault.read_database_credentials("secret-data")


def test_a_denied_path_names_the_policy_to_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeVaultClient(error=hvac.exceptions.Forbidden("denied"))
    monkeypatch.setattr(vault, "_authenticated_client", lambda: client)
    with pytest.raises(RuntimeError, match="dagster_server_policy.hcl"):
        vault.read_database_credentials("postgres-keycloak")


def test_a_missing_path_is_reported_clearly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vault, "_authenticated_client", lambda: FakeVaultClient(None))
    with pytest.raises(RuntimeError, match="Vault path not found"):
        vault.read_database_credentials("postgres-nonexistent")


def test_no_kubernetes_token_and_no_vault_token_fails_loudly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,  # noqa: ANN401
) -> None:
    monkeypatch.setattr(vault, "K8S_SERVICE_ACCOUNT_TOKEN", tmp_path / "absent")
    monkeypatch.delenv("VAULT_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="No Vault Kubernetes service-account token"):
        vault._authenticated_client()  # noqa: SLF001
