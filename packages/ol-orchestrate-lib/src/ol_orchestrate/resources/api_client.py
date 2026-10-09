from typing import Any

import httpx2 as httpx
from dagster import ConfigurableResource
from pydantic import Field, PrivateAttr


class BaseApiClient(ConfigurableResource):
    base_url: str = Field(description="Base URL of the API.")
    token_type: str = Field(
        default="Bearer",
        description="Token type to generate for use with authenticated requests",
    )
    http_timeout: int = Field(
        default=60,
        description="seconds to allow for requests to complete before timing out",
    )
    _http_client: httpx.Client | None = PrivateAttr(default=None)

    @property
    def http_client(self) -> httpx.Client:
        if not self._http_client:
            timeout = httpx.Timeout(self.http_timeout, connect=10)
            self._http_client = httpx.Client(timeout=timeout)
        return self._http_client

    @classmethod
    def from_secret(cls, raw_secret: dict[str, Any]) -> "BaseApiClient":
        return cls(**raw_secret)

    @classmethod
    def local_secret(cls) -> dict[str, Any] | None:
        """Return this client's secret from the environment, for local development.

        :returns: a dict of the shape ``from_secret`` reads, or ``None`` when the
            environment does not carry a complete one and Vault has to be read
        :rtype: dict[str, Any] | None
        """
        return None

    def get_request(
        self, endpoint: str, headers: dict[str, str] | None = None
    ) -> httpx.Response:
        url = f"{self.base_url}/{endpoint}"
        response = self.http_client.get(url, headers=headers)
        response.raise_for_status()
        return response

    def post_request(
        self,
        endpoint: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        url = f"{self.base_url}/{endpoint}"
        response = self.http_client.post(url, json=data, headers=headers)
        response.raise_for_status()
        return response
