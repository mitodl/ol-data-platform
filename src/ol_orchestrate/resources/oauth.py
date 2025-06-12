import time
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Optional, Self

import httpx
from dagster import ConfigurableResource, InitResourceContext, ResourceDependency
from pydantic import Field, PrivateAttr, ValidationError, validator

from ol_orchestrate.resources.secrets.vault import Vault

TOO_MANY_REQUESTS = 429


class OAuthApiClient(ConfigurableResource):
    client_id: str = Field(description="OAUTH2 client ID")
    client_secret: str = Field(description="OAUTH2 client secret")
    token_type: str = Field(
        default="JWT",
        description="Token type to generate for use with authenticated requests",
    )
    token_url: str = Field(
        description="URL to request token. e.g. https://lms.mitx.mit.edu/oauth2/access_token",
    )
    base_url: str = Field(
        description="Base URL of OAuth API client being queries. e.g. https://lms.mitx.mit.edu/",
    )
    http_timeout: int = Field(
        default=60,
        description=(
            "Time (in seconds) to allow for requests to complete before timing out."
        ),
    )
    _access_token: Optional[str] = PrivateAttr(default=None)
    _access_token_expires: Optional[datetime] = PrivateAttr(default=None)
    _http_client: Optional[httpx.Client] = PrivateAttr(default=None)

    @property
    def http_client(self) -> httpx.Client:
        if not self._http_client:
            timeout = httpx.Timeout(self.http_timeout, connect=10)
            self._http_client = httpx.Client(timeout=timeout)
        return self._http_client

    @validator("token_type")
    def validate_token_type(cls, token_type):  # noqa: N805
        if token_type.lower() not in ["jwt", "bearer"]:
            raise ValidationError
        return token_type

    def _fetch_access_token(self) -> Optional[str]:
        now = datetime.now(tz=UTC)
        if self._access_token is None or (self._access_token_expires or now) <= now:
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "token_type": self.token_type,
            }
            response = self.http_client.post(self.token_url, data=payload)
            response.raise_for_status()
            self._access_token = response.json()["access_token"]
            self._access_token_expires = now + timedelta(
                seconds=response.json()["expires_in"]
            )
        return self._access_token

    @property
    def _username(self) -> str:
        response = self.http_client.get(
            f"{self.base_url}/api/user/v1/me",
            headers={"Authorization": f"JWT {self._fetch_access_token()}"},
        )
        response.raise_for_status()
        return response.json()["username"]

    def fetch_with_auth(
        self,
        request_url: str,
        page_size: int = 100,
        extra_params: dict[str, Any] | None = None,
    ) -> dict[Any, Any] | tuple[dict[Any, Any], int]:
        if self.token_url == f"{self.base_url}/oauth2/access_token":
            request_params = {"username": self._username, "page_size": page_size}
        else:
            request_params = {}

        request_params.update(**(extra_params or {}))

        response = self.http_client.get(
            request_url,
            headers={
                "Authorization": f"{self.token_type} {self._fetch_access_token()}"
            },
            params=httpx.QueryParams(**request_params),
        )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error_response:
            if error_response.response.status_code == TOO_MANY_REQUESTS:
                retry_after = error_response.response.headers.get("Retry-After", 60)
                delay = int(retry_after) if retry_after.isdigit() else 60
                time.sleep(delay)
                return self.fetch_with_auth(
                    request_url, page_size=page_size, extra_params=extra_params
                )
            raise
        return response.json()


class OAuthApiClientFactory(ConfigurableResource):
    deployment: str = Field(description="The name of the deployment")
    _client: Optional[OAuthApiClient] = PrivateAttr(default=None)
    vault: ResourceDependency[Vault]

    def _initialize_client(self) -> OAuthApiClient:
        client_secrets = self.vault.client.secrets.kv.v1.read_secret(
            mount_point="secret-data",
            path=f"pipelines/{self.deployment}/oauth-client",
        )["data"]

        return OAuthApiClient(
            client_id=client_secrets["id"],
            client_secret=client_secrets["secret"],
            base_url=client_secrets["url"],
            token_url=client_secrets.get(
                "token_url", f"{client_secrets['url']}/oauth2/access_token"
            ),
        )

    @property
    def client(self) -> OAuthApiClient:
        if not self._client:
            self._client = self._initialize_client()
        return self._client

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self]:  # noqa: ARG002
        yield self
