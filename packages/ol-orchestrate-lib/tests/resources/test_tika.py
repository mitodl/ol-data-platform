"""Tests for ol_orchestrate.resources.tika."""

from unittest.mock import MagicMock, patch

import httpx
import pytest
from ol_orchestrate.resources.tika import SUPPORTED_CONTENT_TYPES, TikaResource


@pytest.fixture
def tika() -> TikaResource:
    """Return a TikaResource pointed at a fake endpoint."""
    return TikaResource(
        base_url="https://tika.example.com",
        access_token="test-token",
        timeout=30,
    )


def _mock_response(text: str = "extracted text", status_code: int = 200) -> MagicMock:
    """Build a minimal mock httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    resp.json.return_value = {"Content-Type": "application/pdf", "Author": "Test"}
    resp.raise_for_status = MagicMock()
    return resp


def _make_mock_http_client(response: MagicMock) -> MagicMock:
    """Return an httpx.Client mock whose put() returns *response*."""
    mock = MagicMock(spec=httpx.Client)
    mock.put.return_value = response
    return mock


# ---------------------------------------------------------------------------
# extract_text
# ---------------------------------------------------------------------------


def test_extract_text_returns_content_on_success(tika: TikaResource) -> None:
    """extract_text returns stripped response text from /tika."""
    mock_http = _make_mock_http_client(_mock_response("  Hello world  "))
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_text(b"%PDF-1.4 ...", "application/pdf")

    assert result == "Hello world"
    mock_http.put.assert_called_once()
    call_kwargs = mock_http.put.call_args
    assert call_kwargs.args[0] == "https://tika.example.com/tika"
    assert call_kwargs.kwargs["headers"]["X-Access-Token"] == "test-token"
    assert call_kwargs.kwargs["headers"]["Accept"] == "text/plain"
    assert call_kwargs.kwargs["headers"]["Content-Type"] == "application/pdf"


def test_extract_text_returns_none_for_empty_response(tika: TikaResource) -> None:
    """extract_text returns None when Tika returns whitespace-only content."""
    mock_http = _make_mock_http_client(_mock_response("   \n   "))
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_text(b"<html></html>", "text/html")

    assert result is None


def test_extract_text_skips_unsupported_content_type(tika: TikaResource) -> None:
    """extract_text returns None and makes no HTTP call for unsupported MIME types."""
    mock_http = MagicMock(spec=httpx.Client)
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_text(b"...", "application/zip")

    assert result is None
    mock_http.put.assert_not_called()


def test_extract_text_passes_ocr_strategy_header(tika: TikaResource) -> None:
    """extract_text includes X-Tika-PDFOcrStrategy when ocr_strategy is provided."""
    mock_http = _make_mock_http_client(_mock_response("text from ocr"))
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        tika.extract_text(b"%PDF...", "application/pdf", ocr_strategy="no_ocr")

    headers = mock_http.put.call_args.kwargs["headers"]
    assert headers["X-Tika-PDFOcrStrategy"] == "no_ocr"


def test_extract_text_omits_ocr_header_when_not_specified(tika: TikaResource) -> None:
    """extract_text does not include X-Tika-PDFOcrStrategy by default."""
    mock_http = _make_mock_http_client(_mock_response("text"))
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        tika.extract_text(b"%PDF...", "application/pdf")

    headers = mock_http.put.call_args.kwargs["headers"]
    assert "X-Tika-PDFOcrStrategy" not in headers


def test_extract_text_propagates_http_error(tika: TikaResource) -> None:
    """extract_text propagates httpx.HTTPStatusError from raise_for_status."""
    mock_resp = _mock_response(status_code=401)
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Unauthorized", request=MagicMock(), response=mock_resp
    )
    mock_http = _make_mock_http_client(mock_resp)
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        with pytest.raises(httpx.HTTPStatusError):
            tika.extract_text(b"%PDF...", "application/pdf")


# ---------------------------------------------------------------------------
# extract_metadata
# ---------------------------------------------------------------------------


def test_extract_metadata_returns_parsed_json(tika: TikaResource) -> None:
    """extract_metadata returns the parsed JSON dict from /meta."""
    expected = {"Content-Type": "application/pdf", "Author": "Test"}
    mock_http = _make_mock_http_client(_mock_response())
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_metadata(b"%PDF...", "application/pdf")

    assert result == expected
    call_url = mock_http.put.call_args.args[0]
    assert call_url == "https://tika.example.com/meta"
    headers = mock_http.put.call_args.kwargs["headers"]
    assert headers["Accept"] == "application/json"
    assert headers["X-Access-Token"] == "test-token"


def test_extract_metadata_returns_empty_dict_on_parse_failure(
    tika: TikaResource,
) -> None:
    """extract_metadata returns {} when Tika returns non-JSON."""
    bad_resp = _mock_response("not json")
    bad_resp.json.side_effect = ValueError("not valid JSON")
    mock_http = _make_mock_http_client(bad_resp)
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_metadata(b"%PDF...", "application/pdf")

    assert result == {}


# ---------------------------------------------------------------------------
# is_supported
# ---------------------------------------------------------------------------


def test_is_supported_returns_true_for_pdf(tika: TikaResource) -> None:
    """Return True for application/pdf."""
    assert tika.is_supported("application/pdf") is True


def test_is_supported_returns_false_for_zip(tika: TikaResource) -> None:
    """Return False for application/zip."""
    assert tika.is_supported("application/zip") is False


def test_supported_content_types_includes_common_formats() -> None:
    """SUPPORTED_CONTENT_TYPES covers the document formats used in OCW and OpenEdX."""
    assert "application/pdf" in SUPPORTED_CONTENT_TYPES
    assert "text/html" in SUPPORTED_CONTENT_TYPES
    assert "text/plain" in SUPPORTED_CONTENT_TYPES
    assert "text/markdown" in SUPPORTED_CONTENT_TYPES


# ---------------------------------------------------------------------------
# yield_for_execution (client lifecycle)
# ---------------------------------------------------------------------------


def test_yield_for_execution_closes_client_on_exit(tika: TikaResource) -> None:
    """yield_for_execution closes the HTTP client when the context exits."""
    mock_http = MagicMock(spec=httpx.Client)
    mock_context = MagicMock()
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        with tika.yield_for_execution(mock_context):
            _ = tika._client

    mock_http.close.assert_called_once()
    assert tika._http_client is None


def test_yield_for_execution_closes_client_on_exception(tika: TikaResource) -> None:
    """yield_for_execution closes the HTTP client even when an exception is raised."""
    mock_http = MagicMock(spec=httpx.Client)
    mock_context = MagicMock()
    with pytest.raises(RuntimeError), patch(
        "ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http
    ), tika.yield_for_execution(mock_context):
        _ = tika._client
        msg = "asset failed"
        raise RuntimeError(msg)

    mock_http.close.assert_called_once()
    assert tika._http_client is None
