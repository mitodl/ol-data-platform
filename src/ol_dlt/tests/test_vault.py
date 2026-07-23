"""Tests for Vault credential resolution."""

from typing import Any

import hvac
import pytest

from ol_dlt import vault


@pytest.fixture(autouse=True)
def _clear_credential_cache() -> None:
    vault._CREDENTIAL_CACHE.clear()  # noqa: SLF001


class FakeVaultClient:
    """Records reads and returns a canned database-credentials response."""

    def __init__(self, response: Any = None, error: Exception | None = None) -> None:  # noqa: ANN401
        self.response = response
        self.error = error
        self.reads: list[str] = []

    def read(self, path: str) -> Any:  # noqa: ANN401
        self.reads.append(path)
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
