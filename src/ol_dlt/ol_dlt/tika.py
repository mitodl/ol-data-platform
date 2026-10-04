"""Text extraction through the OL-deployed Apache Tika service.

The Dagster code locations use ``ol_orchestrate.resources.tika.TikaResource``.
That is a Dagster resource, which ol_dlt must not import, so this is the client
for a dlt source. It calls ``/rmeta/text`` and joins the text of the document
and everything embedded in it, which is what MIT Learn's tika-python client
does, so the text here is byte-for-byte the text Learn stores and the MD5 of
it equals ``ContentFile.checksum``.

The qa and production profiles read the token from Vault at
``secret-operations/tika/access-token``, the path the tika stack in
ol-infrastructure writes in every environment. Any other profile reads
``TIKA_ACCESS_TOKEN`` from the environment and talks to QA's Tika unless
``TIKA_BASE_URL`` says otherwise.
"""

import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ol_dlt import config, vault

TIKA_VAULT_MOUNT = "secret-operations"
TIKA_VAULT_PATH = "tika/access-token"

_BASE_URLS = {"production": "https://tika-production.ol.mit.edu"}
_DEFAULT_BASE_URL = "https://tika-qa.ol.mit.edu"

# Large PDFs take 60-120 s, as for TikaResource.
TIMEOUT_SECONDS = 300
RETRIES = 3

# What MIT Learn sends (TIKA_OCR_STRATEGY in main/settings_course_etl.py), so a
# scanned PDF yields the same nothing here as it does there.
OCR_STRATEGY = "no_ocr"

CONTENT_FIELD = "X-TIKA:content"


class TikaClient:
    """Extract plain text from document bytes.

    :param base_url: Tika service root, no trailing slash.
    :param access_token: ``X-Access-Token`` value.
    """

    def __init__(self, base_url: str, access_token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept": "application/json",
                "X-Access-Token": access_token,
                "X-Tika-PDFOcrStrategy": OCR_STRATEGY,
            }
        )
        # A read timeout is not retried: a hung Tika would hold each file for
        # four timeouts before it counted as failed.
        retry = Retry(
            total=RETRIES,
            read=0,
            backoff_factor=2,
            status_forcelist=(502, 503, 504),
            allowed_methods=("PUT",),
        )
        self._session.mount("https://", HTTPAdapter(max_retries=retry))
        self._session.mount("http://", HTTPAdapter(max_retries=retry))

    def extract_text(self, body: bytes, timeout: float = TIMEOUT_SECONDS) -> str | None:
        """Return the document's text, or None when Tika finds none.

        No Content-Type is sent, so Tika detects the format from the bytes.
        MIT Learn's OCW ETL does the same.

        :param body: The document.
        :param timeout: Seconds to wait for Tika.
        :returns: The text as Tika gives it, unstripped, or None when empty.
        :raises requests.RequestException: Tika refused or failed the document.
        """
        response = self._session.put(
            f"{self.base_url}/rmeta/text", data=body, timeout=timeout
        )
        response.raise_for_status()
        return "".join(part.get(CONTENT_FIELD, "") for part in response.json()) or None


def client_for_profile() -> TikaClient:
    """Build the client for the active profile, failing if it has no token."""
    profile = config.active_profile()
    if profile in config.ICEBERG_PROFILES:
        token = vault.read_kv_secret(TIKA_VAULT_MOUNT, TIKA_VAULT_PATH)["value"]
        return TikaClient(_BASE_URLS.get(profile, _DEFAULT_BASE_URL), token)
    token = config.require_secrets(
        TIKA_ACCESS_TOKEN=config.resolve_secret(None, "TIKA_ACCESS_TOKEN")
    )["TIKA_ACCESS_TOKEN"]
    return TikaClient(os.getenv("TIKA_BASE_URL", _DEFAULT_BASE_URL), token)
