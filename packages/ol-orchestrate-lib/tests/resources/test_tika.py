"""Tests for ol_orchestrate.resources.tika."""

from unittest.mock import MagicMock, patch

import httpx2 as httpx
import pytest
from ol_orchestrate.resources.tika import (
    RETRY_DELAYS_SECONDS,
    SUPPORTED_CONTENT_TYPES,
    TikaResource,
    _base_content_type,
)


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


def _text_response(*parts: str | None) -> MagicMock:
    """Build a /rmeta/text response: one entry per document part."""
    resp = _mock_response()
    resp.json.return_value = [
        {"Content-Type": "text/html"}
        if part is None
        else {"Content-Type": "text/html", "X-TIKA:content": part}
        for part in parts
    ]
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
    """extract_text returns the stripped content of /rmeta/text."""
    mock_http = _make_mock_http_client(_text_response("  Hello world  "))
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_text(b"%PDF-1.4 ...", "application/pdf")

    assert result == "Hello world"
    mock_http.put.assert_called_once()
    call_kwargs = mock_http.put.call_args
    assert call_kwargs.args[0] == "https://tika.example.com/rmeta/text"
    assert call_kwargs.kwargs["headers"]["X-Access-Token"] == "test-token"
    assert call_kwargs.kwargs["headers"]["Accept"] == "application/json"
    assert call_kwargs.kwargs["headers"]["Content-Type"] == "application/pdf"


def test_extract_text_returns_none_for_empty_response(tika: TikaResource) -> None:
    """extract_text returns None when Tika returns whitespace-only content."""
    mock_http = _make_mock_http_client(_text_response("   \n   "))
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_text(b"<html></html>", "text/html")

    assert result is None


def test_extract_text_returns_none_when_no_part_has_content(tika: TikaResource) -> None:
    """An image-only HTML file has no content key, as it has none for MIT Learn."""
    mock_http = _make_mock_http_client(_text_response(None))
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_text(b'<img alt="LEARN icon">', "text/html")

    assert result is None


def test_extract_text_joins_the_embedded_parts(tika: TikaResource) -> None:
    """The text of embedded files follows the document's, as in Learn's client."""
    mock_http = _make_mock_http_client(_text_response("\nbody\n", None, "attached\n"))
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_text(b"PK...", "application/pdf")

    assert result == "body\nattached"


def test_extract_text_skips_unsupported_content_type(tika: TikaResource) -> None:
    """extract_text returns None and makes no HTTP call for unsupported MIME types."""
    mock_http = MagicMock(spec=httpx.Client)
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_text(b"...", "application/zip")

    assert result is None
    mock_http.put.assert_not_called()


def test_extract_text_passes_ocr_strategy_header(tika: TikaResource) -> None:
    """extract_text includes X-Tika-PDFOcrStrategy when ocr_strategy is provided."""
    mock_http = _make_mock_http_client(_text_response("text from ocr"))
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        tika.extract_text(b"%PDF...", "application/pdf", ocr_strategy="no_ocr")

    headers = mock_http.put.call_args.kwargs["headers"]
    assert headers["X-Tika-PDFOcrStrategy"] == "no_ocr"


def test_extract_text_omits_ocr_header_when_not_specified(tika: TikaResource) -> None:
    """extract_text does not include X-Tika-PDFOcrStrategy by default."""
    mock_http = _make_mock_http_client(_text_response("text"))
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
    with (
        patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http),
        pytest.raises(httpx.HTTPStatusError),
    ):
        tika.extract_text(b"%PDF...", "application/pdf")


def _status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("PUT", "https://tika.example.com/rmeta/text")
    return httpx.HTTPStatusError(
        str(status_code),
        request=request,
        response=httpx.Response(status_code, request=request),
    )


def _failing_response(status_code: int) -> MagicMock:
    resp = _mock_response(status_code=status_code)
    resp.raise_for_status.side_effect = _status_error(status_code)
    return resp


@pytest.fixture
def slept(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record retry waits instead of sleeping through them."""
    waits: list[float] = []
    monkeypatch.setattr("ol_orchestrate.resources.tika.time.sleep", waits.append)
    monkeypatch.setattr(
        "ol_orchestrate.resources.tika.random.uniform", lambda _low, high: high
    )
    return waits


def test_extract_text_waits_out_a_parser_restart(
    tika: TikaResource, slept: list[float]
) -> None:
    """A 502, a 503 and a refused connection are a Tika pod restarting.

    Without the retry each of these failed the whole course partition, which
    is how one day of parser restarts became 3,751 failed runs.
    """
    mock_http = MagicMock(spec=httpx.Client)
    mock_http.put.side_effect = [
        _failing_response(502),
        httpx.ConnectError("refused"),
        _failing_response(503),
        _text_response("recovered"),
    ]
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_text(b"%PDF...", "application/pdf")

    assert result == "recovered"
    # The fixture pins the jitter to its upper bound.
    assert slept == [delay * 1.5 for delay in RETRY_DELAYS_SECONDS]


def test_extract_text_gives_up_after_the_last_retry(
    tika: TikaResource, slept: list[float]
) -> None:
    """A Tika that stays down still surfaces, as the status it answered with."""
    mock_http = _make_mock_http_client(_failing_response(502))
    with (
        patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http),
        pytest.raises(httpx.HTTPStatusError),
    ):
        tika.extract_text(b"%PDF...", "application/pdf")

    assert mock_http.put.call_count == len(RETRY_DELAYS_SECONDS) + 1
    assert len(slept) == len(RETRY_DELAYS_SECONDS)


def test_extract_text_raises_a_connection_error_that_outlasts_the_retries(
    tika: TikaResource, slept: list[float]
) -> None:
    """The last attempt's transport error reaches the caller unchanged."""
    mock_http = MagicMock(spec=httpx.Client)
    mock_http.put.side_effect = httpx.ConnectError("refused")
    with (
        patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http),
        pytest.raises(httpx.ConnectError),
    ):
        tika.extract_text(b"%PDF...", "application/pdf")

    assert mock_http.put.call_count == len(RETRY_DELAYS_SECONDS) + 1
    assert len(slept) == len(RETRY_DELAYS_SECONDS)


@pytest.mark.parametrize(
    "error",
    [
        _status_error(401),
        _status_error(422),
        _status_error(500),
        # One slow document: retrying it costs another full timeout each time.
        _status_error(504),
        httpx.ReadTimeout("slow parse"),
        httpx.ConnectTimeout("no route"),
        # Raised before anything is sent, so a wait cannot change it.
        httpx.UnsupportedProtocol("base_url has no scheme"),
    ],
    ids=["401", "422", "500", "504", "read-timeout", "connect-timeout", "no-scheme"],
)
def test_extract_text_does_not_retry_a_failure_about_the_request(
    tika: TikaResource, slept: list[float], error: Exception
) -> None:
    """Only a restarting Tika is retried; everything else fails on first answer."""
    mock_http = MagicMock(spec=httpx.Client)
    if isinstance(error, httpx.HTTPStatusError):
        mock_http.put.return_value = _failing_response(error.response.status_code)
    else:
        mock_http.put.side_effect = error
    with (
        patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http),
        pytest.raises(type(error)),
    ):
        tika.extract_text(b"%PDF...", "application/pdf")

    mock_http.put.assert_called_once()
    assert slept == []


def test_extract_metadata_waits_out_a_parser_restart(
    tika: TikaResource, slept: list[float]
) -> None:
    """/meta goes through the same retry as /tika."""
    mock_http = MagicMock(spec=httpx.Client)
    mock_http.put.side_effect = [_failing_response(502), _mock_response()]
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_metadata(b"%PDF...", "application/pdf")

    assert result == {"Content-Type": "application/pdf", "Author": "Test"}
    assert len(slept) == 1


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
# Client lifecycle (__del__)
# ---------------------------------------------------------------------------


def test_del_closes_client_when_initialised(tika: TikaResource) -> None:
    """__del__ closes the HTTP client if one was created."""
    mock_http = MagicMock(spec=httpx.Client)
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        _ = tika._client  # force client creation

    tika.__del__()

    mock_http.close.assert_called_once()


def test_del_is_safe_when_no_client_created(tika: TikaResource) -> None:
    """__del__ does not raise when no client was ever created."""
    assert tika._http_client is None
    tika.__del__()  # must not raise


# ---------------------------------------------------------------------------
# _base_content_type normalisation
# ---------------------------------------------------------------------------


def test_base_content_type_strips_parameters() -> None:
    """Strip charset and other parameters from a MIME type."""
    assert _base_content_type("text/html; charset=utf-8") == "text/html"


def test_base_content_type_lowercases() -> None:
    """Normalise mixed-case MIME types to lowercase."""
    assert _base_content_type("Application/PDF") == "application/pdf"


def test_base_content_type_strips_and_lowercases() -> None:
    """Handle both parameters and mixed casing together."""
    assert _base_content_type("Text/HTML; Charset=UTF-8") == "text/html"


def test_base_content_type_leaves_simple_type_unchanged() -> None:
    """Plain lowercase MIME type passes through unchanged."""
    assert _base_content_type("application/pdf") == "application/pdf"


def test_is_supported_accepts_mime_with_parameters(tika: TikaResource) -> None:
    """is_supported returns True for supported types carrying charset parameters."""
    assert tika.is_supported("text/html; charset=utf-8") is True


def test_is_supported_accepts_mixed_case(tika: TikaResource) -> None:
    """is_supported returns True for supported types with mixed casing."""
    assert tika.is_supported("Application/PDF") is True


def test_extract_text_accepts_mime_with_parameters(tika: TikaResource) -> None:
    """extract_text processes supported types that carry charset parameters."""
    mock_http = _make_mock_http_client(_text_response("hello"))
    with patch("ol_orchestrate.resources.tika.httpx.Client", return_value=mock_http):
        result = tika.extract_text(b"<html>hello</html>", "text/html; charset=utf-8")

    assert result == "hello"
    mock_http.put.assert_called_once()
