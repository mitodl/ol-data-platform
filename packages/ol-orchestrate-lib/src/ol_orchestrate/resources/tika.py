"""Apache Tika text extraction resource.

Wraps the OL-deployed Tika REST API service for extracting plain text and
metadata from binary documents (PDFs, HTML, Office files, etc.).

Deployment details:
  Production:  https://tika-production.ol.mit.edu
  QA:          https://tika-qa.ol.mit.edu
  Auth:        X-Access-Token header
  Vault paths:
    production  secret-operations/production-apps/tika/access-token  key: value
    qa          secret-operations/rc-apps/tika/access-token           key: value

Infrastructure source:
  ol-infrastructure/src/ol_infrastructure/applications/tika/
"""

import logging
from collections.abc import Generator
from contextlib import contextmanager

import httpx
from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field, PrivateAttr

log = logging.getLogger(__name__)

# MIME types Tika handles well via the /tika endpoint.
# Anything outside this set is likely to return empty text or garbage.
SUPPORTED_CONTENT_TYPES: frozenset[str] = frozenset(
    [
        "application/pdf",
        "text/html",
        "text/plain",
        "text/markdown",
        "application/msword",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.ms-powerpoint",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/rtf",
        "application/epub+zip",
        "application/xml",
        "text/xml",
    ]
)


class TikaResource(ConfigurableResource):
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
            "Read from Vault at "
            "secret-operations/production-apps/tika/access-token (key: value) "
            "for production or "
            "secret-operations/rc-apps/tika/access-token for QA."
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

    @contextmanager
    def yield_for_execution(
        self,
        context: InitResourceContext,  # noqa: ARG002
    ) -> Generator["TikaResource", None, None]:
        """Yield the resource and close the HTTP client on teardown."""
        try:
            yield self
        finally:
            if self._http_client is not None:
                self._http_client.close()
                self._http_client = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_text(
        self,
        file_bytes: bytes,
        content_type: str,
        *,
        ocr_strategy: str | None = None,
    ) -> str | None:
        """Extract plain text from a document using the Tika ``/tika`` endpoint.

        Sends the file bytes via HTTP PUT and returns the response body as a
        stripped string, or ``None`` if Tika returns an empty response or the
        content type is not in :data:`SUPPORTED_CONTENT_TYPES`.

        Args:
            file_bytes: Raw bytes of the document to process.
            content_type: MIME type of the document (e.g. ``"application/pdf"``).
                Must be a value Tika can parse; unsupported types are skipped
                with a debug log rather than raising.
            ocr_strategy: Optional ``X-Tika-PDFOcrStrategy`` header value
                (e.g. ``"ocr_only"``, ``"no_ocr"``, ``"ocr_and_text"``).
                Passed through to Tika when provided.

        Returns:
            Extracted text as a string, or ``None`` if no text was produced.

        Raises:
            httpx.HTTPStatusError: If Tika responds with a non-2xx status.
            httpx.TimeoutException: If the request exceeds :attr:`timeout` seconds.
        """
        if content_type not in SUPPORTED_CONTENT_TYPES:
            log.debug(
                "Skipping Tika extraction for unsupported content type: %s",
                content_type,
            )
            return None

        headers = {
            "Content-Type": content_type,
            "Accept": "text/plain",
            "X-Access-Token": self.access_token,
        }
        if ocr_strategy:
            headers["X-Tika-PDFOcrStrategy"] = ocr_strategy

        response = self._client.put(
            f"{self.base_url}/tika",
            content=file_bytes,
            headers=headers,
        )
        response.raise_for_status()

        text = response.text.strip()
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
        response = self._client.put(
            f"{self.base_url}/meta",
            content=file_bytes,
            headers=headers,
        )
        response.raise_for_status()

        try:
            return response.json()
        except Exception:  # noqa: BLE001
            log.warning(
                "Tika /meta returned non-JSON response for content_type=%s",
                content_type,
            )
            return {}

    def is_supported(self, content_type: str) -> bool:
        """Return True if *content_type* is in the supported extraction set."""
        return content_type in SUPPORTED_CONTENT_TYPES
