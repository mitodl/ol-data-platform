"""A minimal OpenMetadata REST client shared by ``ol-dbt contracts`` and ``ol-dbt metrics``.

It reads ``OM_SERVER_URL`` (the API root, e.g. ``https://data.ol.mit.edu/api``)
and ``OM_BOT_JWT_TOKEN`` from the environment, the same pair the OpenMetadata
bootstrap Job in ol-infrastructure uses. The openmetadata-ingestion SDK is not
used: it pins boto3 and dbt-core versions the workspace cannot share.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from ol_dbt_cli.lib.data_contracts import ENTITY_COLLECTIONS
from ol_dbt_cli.lib.manifest import ManifestRegistry, find_manifest, load_manifest

HTTP_TIMEOUT_SECONDS = 60
HTTP_NOT_FOUND = 404
JSON_PATCH_CONTENT_TYPE = "application/json-patch+json"

_PRINCIPAL_COLLECTIONS = {"team": "teams", "user": "users"}


class OpenMetadataError(RuntimeError):
    """An OpenMetadata API call answered with an HTTP error status."""

    def __init__(self, message: str, status: int) -> None:
        """Carry the HTTP *status* so a caller can tell a missing entity (404) from a failure."""
        super().__init__(message)
        self.status = status


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse redirects: urllib would forward the bearer token to the new host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def load_catalog_manifest(dbt_dir: Path, manifest_path: str | None) -> ManifestRegistry:
    """Load the manifest that maps dbt bindings to the relations OpenMetadata catalogs.

    :param manifest_path: ``--manifest``; defaults to ``<dbt_dir>/target/manifest.json``.
    :raises ValueError: when no manifest is given or found.
    """
    path = Path(manifest_path) if manifest_path else find_manifest(dbt_dir)
    if path is None:
        msg = "dbt bindings need a manifest built for the catalogs OpenMetadata ingests (production). Pass --manifest."
        raise ValueError(msg)
    return load_manifest(path)


def quote_fqn(fqn: str) -> str:
    return urllib.parse.quote(fqn, safe="")


class OpenMetadataClient:
    def __init__(self, server_url: str, token: str) -> None:
        """Talk to the OpenMetadata API at *server_url* (the ``/api`` root) with a bot JWT."""
        self.server_url = server_url.rstrip("/")
        self._token = token

    @classmethod
    def from_env(cls) -> OpenMetadataClient:
        return cls(os.environ["OM_SERVER_URL"], os.environ["OM_BOT_JWT_TOKEN"])

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        query: dict[str, str] | None = None,
        content_type: str = "application/json",
    ) -> Any:
        """Send one request and return the decoded JSON response, or ``None`` when the body is empty.

        :raises OpenMetadataError: on any HTTP error status.
        """
        url = f"{self.server_url}{path}"
        if query:
            url = f"{url}?{urllib.parse.urlencode(query)}"
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Authorization": f"Bearer {self._token}", "Content-Type": content_type}
        req = urllib.request.Request(url, data=data, method=method, headers=headers)  # noqa: S310
        try:
            with _OPENER.open(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
                payload = resp.read()
        except urllib.error.HTTPError as exc:
            msg = f"{method} {path} -> {exc.code}: {exc.read().decode(errors='replace')[:500]}"
            raise OpenMetadataError(msg, exc.code) from exc
        # PUT /v1/lineage answers 200 with no body (LineageResource.addLineage).
        return json.loads(payload) if payload else None

    def get_or_none(self, path: str, query: dict[str, str] | None = None) -> Any:
        """GET *path*, returning ``None`` when OpenMetadata answers 404."""
        try:
            return self.request("GET", path, query=query)
        except OpenMetadataError as exc:
            if exc.status == HTTP_NOT_FOUND:
                return None
            raise

    def entity_id(self, entity_type: str, fqn: str) -> str:
        collection = ENTITY_COLLECTIONS[entity_type]
        return self.request("GET", f"/v1/{collection}/name/{quote_fqn(fqn)}")["id"]

    def named_refs(self, principals: list[dict[str, str]]) -> list[dict[str, str]]:
        """Resolve ``{type: team | user, name}`` owners or reviewers to the ``{id, type}`` references the API takes."""
        refs = []
        for principal in principals:
            collection = _PRINCIPAL_COLLECTIONS[principal["type"]]
            found = self.request("GET", f"/v1/{collection}/name/{quote_fqn(principal['name'])}")
            refs.append({"id": found["id"], "type": principal["type"]})
        return refs
