"""Tests for the Tika client."""

import socket
import threading
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


class _Server:
    """A local server that takes connections and then hangs or hangs up."""

    def __init__(self, *, hang: bool) -> None:
        self.connections = 0
        self._held: list[socket.socket] = []
        self._socket = socket.create_server(("127.0.0.1", 0))
        self.url = f"http://127.0.0.1:{self._socket.getsockname()[1]}"
        threading.Thread(target=self._serve, args=(hang,), daemon=True).start()

    def _serve(self, hang: bool) -> None:  # noqa: FBT001
        while True:
            try:
                connection, _address = self._socket.accept()
            except OSError:
                return
            self.connections += 1
            if hang:
                self._held.append(connection)
            else:
                connection.close()

    def close(self) -> None:
        self._socket.close()
        for connection in self._held:
            connection.close()


def test_hung_tika_is_tried_once() -> None:
    """Through the real session: a retry here doubles every file's timeout."""
    server = _Server(hang=True)
    try:
        with pytest.raises(requests.ReadTimeout):
            tika.TikaClient(server.url, "token").extract_text(b"%PDF", timeout=0.5)
        assert server.connections == 1
    finally:
        server.close()


def test_dropped_connection_is_tried_again() -> None:
    server = _Server(hang=False)
    try:
        with pytest.raises(requests.ConnectionError):
            tika.TikaClient(server.url, "token").extract_text(b"%PDF", timeout=5)
        assert server.connections == tika.DROPPED_CONNECTION_ATTEMPTS
    finally:
        server.close()
