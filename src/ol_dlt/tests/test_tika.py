"""Tests for the Tika client."""

from typing import Any

import pytest
import requests

from ol_dlt import tika
from tests.conftest import FakeResponse


def _client(monkeypatch: pytest.MonkeyPatch, parts: list[dict[str, Any]]) -> Any:
    client = tika.TikaClient("https://tika.example/", "token")
    calls: list[tuple[str, dict[str, Any]]] = []

    def put(url: str, **kwargs: Any) -> FakeResponse:
        calls.append((url, kwargs))
        return FakeResponse(json_data=parts)

    monkeypatch.setattr(client._session, "put", put)  # noqa: SLF001
    return client, calls


def test_extract_text_joins_the_document_and_its_embedded_parts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """tika-python, which MIT Learn uses, concatenates every part unstripped."""
    client, calls = _client(
        monkeypatch,
        [
            {"X-TIKA:content": "\n\nBody \n", "dc:title": "Notes"},
            {"resourceName": "image.png"},
            {"X-TIKA:content": "Attachment"},
        ],
    )

    assert client.extract_text(b"%PDF") == "\n\nBody \nAttachment"
    assert calls[0][0] == "https://tika.example/rmeta/text"
    assert calls[0][1]["data"] == b"%PDF"


def test_extract_text_is_none_when_tika_finds_no_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _calls = _client(monkeypatch, [{"resourceName": "scan.pdf"}])

    assert client.extract_text(b"%PDF") is None


def test_client_for_profile_needs_a_token_outside_deployed_profiles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DLT_PROFILE", "test")
    monkeypatch.delenv("TIKA_ACCESS_TOKEN", raising=False)

    with pytest.raises(Exception, match="TIKA_ACCESS_TOKEN"):
        tika.client_for_profile()


def test_extract_text_retries_a_dropped_connection_but_not_a_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = tika.TikaClient("https://tika.example", "token")
    outcomes: list[Any] = [
        requests.ConnectionError("Connection aborted."),
        FakeResponse(json_data=[{"X-TIKA:content": "Body"}]),
    ]

    def put(_url: str, **_kwargs: Any) -> FakeResponse:
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(client._session, "put", put)  # noqa: SLF001
    assert client.extract_text(b"%PDF") == "Body"

    outcomes[:] = [requests.ReadTimeout(), FakeResponse(json_data=[])]
    with pytest.raises(requests.ReadTimeout):
        client.extract_text(b"%PDF")
    assert len(outcomes) == 1
