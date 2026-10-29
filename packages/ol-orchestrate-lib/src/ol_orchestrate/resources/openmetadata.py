"""OpenMetadata REST API client, authenticated as a bot."""

from typing import Any, Self

from pydantic import Field

from ol_orchestrate.resources.api_client import BaseApiClient


class OpenMetadataApiClient(BaseApiClient):
    """OpenMetadata's API at ``base_url`` (the ``/api`` root, e.g. ``https://data.ol.mit.edu/api``).

    The Vault secret holds ``server_url`` and ``token`` (a bot JWT), written by
    ol-infrastructure's substructure/open_metadata stack for dagster-bot.
    """

    token: str = Field(description="OpenMetadata bot JWT")

    @classmethod
    def from_secret(cls, raw_secret: dict[str, Any]) -> Self:
        return cls(base_url=raw_secret["server_url"], token=raw_secret["token"])

    def send(
        self, method: str, endpoint: str, body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Send *body* as JSON to ``<base_url>/<endpoint>`` and decode the response.

        :raises httpx.HTTPStatusError: on a non-2xx response.
        """
        response = self.http_client.request(
            method,
            f"{self.base_url}/{endpoint}",
            json=body,
            headers={"Authorization": f"{self.token_type} {self.token}"},
        )
        response.raise_for_status()
        return response.json()
