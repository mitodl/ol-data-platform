"""Tests for ml.resources.llm.LLMClientFactory."""

from ml.resources.llm import LLMClientFactory
from ol_orchestrate.resources.secrets.vault import Vault


class _FakeKvV1:
    def __init__(self, secrets: dict[str, dict[str, str]]) -> None:
        self._secrets = secrets
        self.reads = 0

    def read_secret(self, mount_point: str, path: str) -> dict[str, dict[str, str]]:
        self.reads += 1
        return {"data": self._secrets[f"{mount_point}/{path}"]}


class _FakeHvacClient:
    """Stands in for the authenticated hvac.Client Vault.client returns."""

    def __init__(self, kv_v1: _FakeKvV1) -> None:
        kv = type("_Kv", (), {"v1": kv_v1})()
        self.secrets = type("_Secrets", (), {"kv": kv})()

    def is_authenticated(self) -> bool:
        return True


def _build_vault(kv_v1: _FakeKvV1) -> Vault:
    vault = Vault(
        vault_addr="https://vault.example.com", vault_auth_type="token", vault_token="x"
    )
    vault._client = _FakeHvacClient(kv_v1)
    return vault


def test_get_client_reads_the_documented_vault_path_and_caches() -> None:
    """pipelines/feedback-llm/api_key is the ops path; one Vault read per client."""
    fake_api_key = "sk-ant-test"  # pragma: allowlist secret
    kv_v1 = _FakeKvV1({"secret-data/pipelines/feedback-llm": {"api_key": fake_api_key}})
    factory = LLMClientFactory(vault=_build_vault(kv_v1))

    first = factory.get_client()
    second = factory.get_client()

    assert first.api_key == fake_api_key
    assert first is second
    assert kv_v1.reads == 1
