import time
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Self

import httpx
from dagster import ConfigurableResource, InitResourceContext, ResourceDependency
from pydantic import Field, PrivateAttr

from ol_orchestrate.resources.secrets.vault import Vault

TOO_MANY_REQUESTS = 429


class EdxorgApiClient(ConfigurableResource):
    client_id: str = Field(description="OAuth client ID")
    client_secret: str = Field(description="OAuth client secret")
    token_url: str = Field(description="URL to fetch the OAuth token")
    token_type: str = Field(
        default="JWT",
        description="Token type to generate for use with authenticated requests, "
        "e.g. JWT or Bearer",
    )
    http_timeout: int = Field(
        default=60,
        description=(
            "Time (in seconds) to allow for requests to complete before timing out."
        ),
    )
    _access_token: str = PrivateAttr(default=None)
    _access_token_expires: datetime = PrivateAttr(default=None)
    _http_client: httpx.Client = PrivateAttr(default=None)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._initialize_client()

    def _initialize_client(self) -> None:
        if self._http_client is not None:
            return
        timeout = httpx.Timeout(self.http_timeout, connect=10)
        self._http_client = httpx.Client(timeout=timeout)

    def _get_access_token(self) -> str:
        """
        Get an access token from the edX.org access token endpoint

        Returns:
            str: the access token
        """
        now = datetime.now(tz=UTC)
        if self._access_token is None or self._access_token_expires < now:
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "token_type": self.token_type,
            }
            response = self._http_client.post(self.token_url, data=payload)
            response.raise_for_status()
            self._access_token = response.json()["access_token"]
            self._access_token_expires = now + timedelta(
                seconds=response.json()["expires_in"]
            )
        return self._access_token

    def _fetch_with_token(self, request_url: str) -> dict[Any, Any]:
        """
        Get the json results from the request_url with access token

        Args:
            request_url: the URL to fetch the data from

        Returns:
            dict: a list of results returned from the API
        """
        response = self._http_client.get(
            request_url,
            headers={"Authorization": f"JWT {self._get_access_token()}"},
        )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error_response:
            if error_response.response.status_code == TOO_MANY_REQUESTS:
                time.sleep(60)
                return self._get_request_with_token(request_url)
            raise
        return response.json()

    def get_edxorg_programs(self):
        """
        Retrieve the program metadata from the edX.org REST API by walking through
         the paginated results

        Yield: A generator for walking the paginated list of programs returned
        from the API

        """
        request_url = "https://discovery.edx.org/api/v1/programs/"
        response_data = self._fetch_with_token(request_url)
        results = response_data["results"]
        next_page = response_data["next"]
        yield results
        while next_page:
            response_data = self._fetch_with_token(next_page)
            next_page = response_data["next"]
            yield response_data["results"]


class EdxorgApiClientFactory(ConfigurableResource):
    _client: EdxorgApiClient = PrivateAttr()
    vault: ResourceDependency[Vault]

    def _initialize_client(self) -> EdxorgApiClient:
        client_secrets = self.vault.client.secrets.kv.v1.read_secret(
            mount_point="secret-data",
            path="pipelines/edx/org/edxorg_api",  # to be added on production
        )["data"]

        self._client = EdxorgApiClient(
            client_id=client_secrets["id"],
            client_secret=client_secrets["secret"],
            token_url=client_secrets["token_url"],
        )
        return self._client

    @property
    def client(self) -> EdxorgApiClient:
        return self._client

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self]:  # noqa: ARG002
        self._initialize_client()
        yield self
