"""Apache Tika text extraction resource.

Wraps the OL-deployed Tika REST API service for extracting plain text and
metadata from binary documents (PDFs, HTML, Office files, etc.).

Deployment details:
  Production:  https://tika-production.ol.mit.edu
  QA:          https://tika-qa.ol.mit.edu
  Auth:        X-Access-Token header
  Vault path:  secret-operations/tika/access-token  key: value

    The same path in every environment -- the environment is selected by which
    Vault you authenticate to, not by the path. Do not use the env-scoped
    secret-operations/{production-apps,rc-apps}/tika/access-token: nothing
    writes those paths any more, so a token read there earns a 401.

Infrastructure source:
  ol-infrastructure/src/ol_infrastructure/applications/tika/
"""

import logging
import random
import time

import httpx2 as httpx
from dagster import ConfigurableResource
from pydantic import Field, PrivateAttr

log = logging.getLogger(__name__)

TIKA_CONTENT_KEY = "X-TIKA:content"
TIKA_CONTAINER_EXCEPTION_KEY = "X-TIKA:EXCEPTION:container_exception"


class TikaParseError(Exception):
    """Tika could not parse the document it was sent."""


# What a caller sees while a Tika pod restarts its parser process. Tika runs the
# parser in a forked JVM that the watchdog kills and restarts on a heap
# OutOfMemoryError or a task timeout, without the container restarting, so the
# pod stays Ready and APISIX keeps routing to it. On 2026-10-02 the three
# production pods did this 327 times against 9 container restarts, and each
# restart answered every request in flight on that pod with a 502.
#
# 504 is absent on purpose: it is the gateway timing out one slow parse, and
# sending the same document again costs another full timeout.
RETRYABLE_STATUSES = frozenset({502, 503})

# The forked process was back in 1 to 24 seconds in the restarts read from the
# 2026-10-02 logs. The second retry is sent 10 to 30 seconds after the first
# failure and the third 32 to 98 seconds after it, so the third always clears
# the slowest of them. Kept to three because the document that killed the
# parser is retried too, and each of its retries kills the parser again for
# every other caller on that pod.
RETRY_DELAYS_SECONDS = (5, 15, 45)


class TikaResource(ConfigurableResource[None]):
    """Dagster resource for extracting text and metadata from documents via Tika.

    Calls the OL-deployed Apache Tika REST API.  Each method is stateless —
    the underlying :class:`httpx.Client` is shared across calls within a single
    asset execution but is not reused across executions.

    Example usage in an asset::

        @asset
        def extract_text(tika: TikaResource) -> str:
            pdf_bytes = Path("example.pdf").read_bytes()
            return tika.extract_text(pdf_bytes, "application/pdf") or ""

    Configure in ``definitions.py``::

        from ol_orchestrate.resources.tika import TikaResource

        defs = Definitions(
            resources={
                "tika": TikaResource(
                    base_url="https://tika-production.ol.mit.edu",
                    access_token=vault_secret["value"],
                ),
            },
            ...
        )
    """

    base_url: str = Field(
        description=(
            "Base URL of the Tika service, e.g. "
            "'https://tika-production.ol.mit.edu'. No trailing slash."
        ),
    )
    access_token: str = Field(
        description=(
            "X-Access-Token value for the Tika service. "
            "Read from Vault at secret-operations/tika/access-token "
            "(key: value), the same path in every environment."
        ),
    )
    timeout: int = Field(
        default=300,
        description=(
            "HTTP timeout in seconds for Tika requests. "
            "Large PDFs can take 60-120 s; the default of 300 s provides headroom."
        ),
    )

    _http_client: httpx.Client | None = PrivateAttr(default=None)

    @property
    def _client(self) -> httpx.Client:
        if self._http_client is None:
            self._http_client = httpx.Client(
                timeout=httpx.Timeout(self.timeout, connect=10),
            )
        return self._http_client

    def __del__(self) -> None:
        """Close the underlying HTTP client when the resource is garbage collected."""
        if self._http_client is not None:
            self._http_client.close()

    def _put(
        self, endpoint: str, file_bytes: bytes, headers: dict[str, str]
    ) -> httpx.Response:
        """PUT a document to Tika, waiting out a parser restart.

        :param endpoint: Tika endpoint name, e.g. ``"rmeta/text"`` or ``"meta"``.
        :param file_bytes: Raw bytes of the document.
        :param headers: Request headers, including the access token.
        :returns: The successful response.
        :raises httpx.HTTPStatusError: On a non-2xx status that is not
            retryable, or a retryable one that outlasted every retry.
        :raises httpx.TransportError: On a timeout, or a connection failure
            that outlasted every retry.

        Blocks for up to 98 seconds of waits on top of the request timeouts
        when every attempt fails.
        """
        url = f"{self.base_url}/{endpoint}"
        for delay in RETRY_DELAYS_SECONDS:
            try:
                response = self._client.put(url, content=file_bytes, headers=headers)
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                if error.response.status_code not in RETRYABLE_STATUSES:
                    raise
                failure: Exception = error
            # Not TransportError: that also covers timeouts (one slow
            # document) and errors raised before anything is sent, such as a
            # base_url with no scheme, which no wait will fix.
            except (httpx.NetworkError, httpx.RemoteProtocolError) as error:
                failure = error
            else:
                return response
            # Every caller that lost a request to the same restart would
            # otherwise come back in the same instant.
            wait_seconds = delay * random.uniform(0.5, 1.5)  # noqa: S311
            log.warning(
                "Tika /%s failed (%s); retrying in %.0f s",
                endpoint,
                failure,
                wait_seconds,
            )
            time.sleep(wait_seconds)
        response = self._client.put(url, content=file_bytes, headers=headers)
        response.raise_for_status()
        return response

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_text(
        self,
        file_bytes: bytes,
        content_type: str | None,
        *,
        ocr_strategy: str | None = None,
    ) -> str | None:
        """Extract plain text from a document using Tika's ``/rmeta/text`` endpoint.

        Sends the file bytes via HTTP PUT and returns the text of the document
        and of everything embedded in it as one stripped string, or ``None`` if
        Tika finds no text. Which files to send is the caller's decision
        (ol_file_types for course content).

        ``/rmeta/text`` and not ``/tika``: this is the endpoint MIT Learn's
        client reads (``tika.parser.from_buffer``), and the two disagree.
        ``/tika`` writes ``[image: <alt>]`` for every ``<img>``, so an HTML
        file holding only an icon has text there and none here, and Learn
        loads no ContentFile for a file with no text.

        Args:
            file_bytes: Raw bytes of the document to process.
            content_type: MIME type of the document (e.g. ``"application/pdf"``),
                sent as a hint; ``None`` sends none and Tika detects the type.
            ocr_strategy: Optional ``X-Tika-PDFOcrStrategy`` header value
                (e.g. ``"ocr_only"``, ``"no_ocr"``, ``"ocr_and_text"``).
                Passed through to Tika when provided.

        Returns:
            Extracted text as a string, or ``None`` if no text was produced.

        Raises:
            httpx.HTTPStatusError: If Tika responds with a non-2xx status.
            httpx.TimeoutException: If the request exceeds :attr:`timeout` seconds.
            TikaParseError: If Tika could not parse the document.
        """
        headers = {
            "Accept": "application/json",
            "X-Access-Token": self.access_token,
        }
        if content_type:
            headers["Content-Type"] = content_type
        if ocr_strategy:
            headers["X-Tika-PDFOcrStrategy"] = ocr_strategy

        response = self._put("rmeta/text", file_bytes, headers)

        # One entry for the document and one per embedded file. An entry with
        # no text has no content key.
        parts = response.json()

        # /rmeta answers 200 for a document it cannot parse, where /tika
        # answered 422. Without this an unparseable document reads as an empty
        # one, and the caller's health check counts empty as a success.
        parse_error = parts[0].get(TIKA_CONTAINER_EXCEPTION_KEY)
        if parse_error:
            raise TikaParseError(parse_error.splitlines()[0])

        text = "".join(part.get(TIKA_CONTENT_KEY) or "" for part in parts).strip()
        return text if text else None

    def extract_metadata(
        self,
        file_bytes: bytes,
        content_type: str,
    ) -> dict[str, str | list[str]]:
        """Extract document metadata using the Tika ``/meta`` endpoint.

        Returns the parsed JSON response from Tika (a flat dict of metadata
        key → string or list of strings).  Returns an empty dict if Tika
        returns an empty or non-JSON response.

        Args:
            file_bytes: Raw bytes of the document to process.
            content_type: MIME type of the document.

        Returns:
            Dict of metadata key-value pairs from Tika.

        Raises:
            httpx.HTTPStatusError: If Tika responds with a non-2xx status.
            httpx.TimeoutException: If the request exceeds :attr:`timeout` seconds.
        """
        headers = {
            "Content-Type": content_type,
            "Accept": "application/json",
            "X-Access-Token": self.access_token,
        }
        response = self._put("meta", file_bytes, headers)

        try:
            return response.json()
        except Exception:  # noqa: BLE001
            log.warning(
                "Tika /meta returned non-JSON response for content_type=%s",
                content_type,
            )
            return {}
