import pytest
from ml.lib.courseware import local


class _StubFactory:
    def __init__(self, **_):
        pass

    def get_client(self):
        return _StubClient()


class _StubClient:
    pass


@pytest.fixture
def stub_llm_client(monkeypatch):
    """Give the courseware local runner a client the fake model never calls."""
    monkeypatch.setattr(local, "LLMClientFactory", _StubFactory)
    monkeypatch.setattr(local, "AnthropicBedrock", _StubClient)
