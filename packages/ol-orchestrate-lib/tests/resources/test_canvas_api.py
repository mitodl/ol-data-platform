"""Tests for CanvasApiClient.download_course_export resuming a cut-off body."""

from collections.abc import Callable, Iterator
from pathlib import Path

import httpx2 as httpx
import pytest
from ol_orchestrate.resources.canvas_api import (
    DOWNLOAD_RESUME_ATTEMPTS,
    CanvasApiClient,
)

EXPORT_URL = "https://cdn.example.com/export.imscc"
EXPORT = b"0123456789abcdefghij"


class CutOffStream(httpx.SyncByteStream):
    """A body that sends ``chunk`` and then drops the connection."""

    def __init__(self, chunk: bytes) -> None:
        self.chunk = chunk

    def __iter__(self) -> Iterator[bytes]:
        yield self.chunk
        msg = "peer closed connection without sending complete message body"
        raise httpx.RemoteProtocolError(msg)


def client_answering(
    handler: Callable[[httpx.Request], httpx.Response],
) -> CanvasApiClient:
    client = CanvasApiClient(base_url="https://canvas.example.com", access_token="t")
    client._http_client = httpx.Client(transport=httpx.MockTransport(handler))
    return client


def test_a_cut_off_download_resumes_from_where_the_file_ends(tmp_path: Path) -> None:
    ranges: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        ranges.append(request.headers.get("Range"))
        if len(ranges) == 1:
            return httpx.Response(200, stream=CutOffStream(EXPORT[:8]))
        return httpx.Response(
            206,
            content=EXPORT[8:],
            headers={"Content-Range": f"bytes 8-{len(EXPORT) - 1}/{len(EXPORT)}"},
        )

    path = client_answering(handler).download_course_export(
        EXPORT_URL, tmp_path / "export.imscc"
    )

    assert path.read_bytes() == EXPORT
    assert ranges == [None, "bytes=8-"]


def test_a_server_that_ignores_the_range_gets_the_file_rewritten(
    tmp_path: Path,
) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(200, stream=CutOffStream(EXPORT[:8]))
        return httpx.Response(200, content=EXPORT)

    path = client_answering(handler).download_course_export(
        EXPORT_URL, tmp_path / "export.imscc"
    )

    assert path.read_bytes() == EXPORT


def test_a_misaligned_content_range_starts_over(tmp_path: Path) -> None:
    ranges: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        ranges.append(request.headers.get("Range"))
        if len(ranges) == 1:
            return httpx.Response(200, stream=CutOffStream(EXPORT[:8]))
        if len(ranges) == 2:
            return httpx.Response(
                206, content=EXPORT[4:], headers={"Content-Range": "bytes 4-19/20"}
            )
        return httpx.Response(200, content=EXPORT)

    path = client_answering(handler).download_course_export(
        EXPORT_URL, tmp_path / "export.imscc"
    )

    assert path.read_bytes() == EXPORT
    assert ranges == [None, "bytes=8-", None]


def test_a_connection_that_keeps_dropping_still_fails(tmp_path: Path) -> None:
    """Each attempt gets one more byte through and then drops."""
    ranges: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        ranges.append(request.headers.get("Range"))
        start = len(ranges) - 1
        if start == 0:
            return httpx.Response(200, stream=CutOffStream(EXPORT[:1]))
        return httpx.Response(
            206,
            stream=CutOffStream(EXPORT[start : start + 1]),
            headers={"Content-Range": f"bytes {start}-19/20"},
        )

    path = tmp_path / "export.imscc"
    with pytest.raises(httpx.RemoteProtocolError):
        client_answering(handler).download_course_export(EXPORT_URL, path)

    assert ranges == [None, "bytes=1-", "bytes=2-", "bytes=3-"]
    assert len(ranges) == DOWNLOAD_RESUME_ATTEMPTS + 1
    assert path.read_bytes() == EXPORT[:4]


def test_a_misaligned_range_every_time_gives_up(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers.get("Range") is None and not (tmp_path / "x").exists():
            (tmp_path / "x").touch()
            return httpx.Response(200, stream=CutOffStream(EXPORT[:8]))
        return httpx.Response(
            206, content=EXPORT[4:], headers={"Content-Range": "bytes 4-19/20"}
        )

    with pytest.raises(RuntimeError, match="without receiving the whole file"):
        client_answering(handler).download_course_export(
            EXPORT_URL, tmp_path / "export.imscc"
        )


def test_an_http_error_is_not_resumed(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        return httpx.Response(503)

    with pytest.raises(httpx.HTTPStatusError):
        client_answering(handler).download_course_export(
            EXPORT_URL, tmp_path / "export.imscc"
        )


def test_a_stale_file_from_an_earlier_run_is_not_resumed(tmp_path: Path) -> None:
    path = tmp_path / "export.imscc"
    path.write_bytes(b"left over from a previous run")
    ranges: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        ranges.append(request.headers.get("Range"))
        return httpx.Response(200, content=EXPORT)

    client_answering(handler).download_course_export(EXPORT_URL, path)

    assert path.read_bytes() == EXPORT
    assert ranges == [None]


def test_a_drop_after_the_last_byte_counts_as_finished(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers.get("Range") is None:
            return httpx.Response(200, stream=CutOffStream(EXPORT))
        return httpx.Response(416, headers={"Content-Range": f"bytes */{len(EXPORT)}"})

    path = client_answering(handler).download_course_export(
        EXPORT_URL, tmp_path / "export.imscc"
    )

    assert path.read_bytes() == EXPORT


def test_the_download_asks_for_no_content_encoding(tmp_path: Path) -> None:
    encodings: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        encodings.append(request.headers.get("Accept-Encoding"))
        return httpx.Response(200, content=EXPORT)

    client_answering(handler).download_course_export(
        EXPORT_URL, tmp_path / "export.imscc"
    )

    assert encodings == ["identity"]


def test_a_clean_but_short_206_is_resumed_until_the_file_is_whole(
    tmp_path: Path,
) -> None:
    """A server may answer a range with less than the rest and end the body cleanly."""
    ranges: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        ranges.append(request.headers.get("Range"))
        if len(ranges) == 1:
            return httpx.Response(200, stream=CutOffStream(EXPORT[:8]))
        if len(ranges) == 2:
            return httpx.Response(
                206, content=EXPORT[8:12], headers={"Content-Range": "bytes 8-11/20"}
            )
        return httpx.Response(
            206, content=EXPORT[12:], headers={"Content-Range": "bytes 12-19/20"}
        )

    path = client_answering(handler).download_course_export(
        EXPORT_URL, tmp_path / "export.imscc"
    )

    assert path.read_bytes() == EXPORT
    assert ranges == [None, "bytes=8-", "bytes=12-"]


def test_a_206_that_never_completes_the_file_fails(tmp_path: Path) -> None:
    ranges: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        ranges.append(request.headers.get("Range"))
        if len(ranges) == 1:
            return httpx.Response(200, stream=CutOffStream(EXPORT[:8]))
        start = 8 + len(ranges) - 2
        return httpx.Response(
            206,
            content=EXPORT[start : start + 1],
            headers={"Content-Range": f"bytes {start}-{start}/20"},
        )

    with pytest.raises(RuntimeError, match="without receiving the whole file"):
        client_answering(handler).download_course_export(
            EXPORT_URL, tmp_path / "export.imscc"
        )
