import os

import pytest

# The courseware assets resolve their dev schema from this at import time, and
# fail without it.
os.environ.setdefault("DBT_SCHEMA_SUFFIX", "test")


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
    from ml.lib.courseware import local  # noqa: PLC0415

    monkeypatch.setattr(local, "LLMClientFactory", _StubFactory)
    monkeypatch.setattr(local, "AnthropicBedrock", _StubClient)
