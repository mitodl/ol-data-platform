from typing import Any, Optional

import httpx
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
    _http_client: Optional[httpx.Client] = PrivateAttr(default=None)

    @property
    def http_client(self) -> httpx.Client:
        if not self._http_client:
            timeout = httpx.Timeout(self.http_timeout, connect=10)
            self._http_client = httpx.Client(timeout=timeout)
        return self._http_client

    def get_request(
        self, endpoint: str, headers: Optional[dict[str, str]] = None
    ) -> httpx.Response:
        url = f"{self.base_url}/{endpoint}"
        response = self.http_client.get(url, headers=headers)
        response.raise_for_status()
        return response

    def post_request(
        self,
        endpoint: str,
        data: dict[str, Any],
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        url = f"{self.base_url}/{endpoint}"
        response = self.http_client.post(url, json=data, headers=headers)
        response.raise_for_status()
        return response
