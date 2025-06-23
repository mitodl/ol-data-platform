import base64
from collections.abc import Mapping
from typing import Any, Self

from dagster._annotations import beta
from dagster_airbyte.resources import AirbyteCloudClient, AirbyteCloudWorkspace
from dagster_shared.utils.cached_method import cached_method
from pydantic.fields import Field, PrivateAttr
from pydantic.functional_validators import field_validator, model_validator

AIRBYTE_REST_API_VERSION = "v1"
AIRBYTE_CONFIGURATION_API_VERSION = "v1"


@beta
class AirbyteOSSClient(AirbyteCloudClient):
    """Expose methods on top of the Airbyte APIs for Airbyte Community Edition."""

    workspace_id: str | None = Field(
        None, description="The ID of the Airbyte workspace to use"
    )
    api_server: str = Field(
        ..., description="The base URL of the API server, excluding the path"
    )
    username: str | None = Field(
        None, description="The username to authenticate to the API if using basic auth"
    )
    password: str | None = Field(
        None, description="The password to authenticate to the API if using basic auth"
    )
    client_id: str | None = Field(
        None, description="The Airbyte client ID if using OAuth."
    )
    client_secret: str | None = Field(
        None, description="The Airbyte client secret if using OAuth."
    )
    request_max_retries: int = Field(
        ...,
        description=(
            "The maximum number of times requests to the Airbyte API should be retried "
            "before failing."
        ),
    )
    request_retry_delay: float = Field(
        ...,
        description="Time (in seconds) to wait between each request retry.",
    )
    request_timeout: int = Field(
        ...,
        description=(
            "Time (in seconds) after which the requests to Airbyte "
            "are declared timed out."
        ),
    )

    _access_token_value: str | None = PrivateAttr(default=None)
    _access_token_timestamp: float | None = PrivateAttr(default=None)

    @field_validator("api_server")
    @classmethod
    def ensure_https_api(cls, api_server: str) -> str:
        if not api_server.startswith("https"):
            msg = "The API server should be accessed via HTTPS"
            raise ValueError(msg)
        return api_server

    @model_validator(mode="after")
    def ensure_workspace_id(self) -> Self:
        if not self.workspace_id:
            workspaces = self._make_request(
                method="GET",
                base_url=self.rest_api_base_url,
                endpoint="/workspaces",
                data={},
            ).get("data", [])
            return self.model_copy(
                update={"workspace_id": workspaces[0]["workspaceId"]}
            )
        return self

    @property
    def rest_api_base_url(self) -> str:
        return f"{self.api_server}/api/public/{AIRBYTE_REST_API_VERSION}"

    @property
    def configuration_api_base_url(self) -> str:
        return f"{self.api_server}/api/{AIRBYTE_CONFIGURATION_API_VERSION}"

    @property
    def authorization_request_params(self) -> Mapping[str, Any]:
        if self.password and self.username:
            basic_auth_creds = base64.b64encode(
                f"{self.username}:{self.password}".encode()
            ).decode("utf-8")
            auth_param = {"Authorization": f"Basic {basic_auth_creds}"}
        else:
            # Make sure the access token is refreshed before using it when calling the
            # API.
            if self._needs_refreshed_access_token():
                self._refresh_access_token()
            auth_param = {
                "Authorization": f"Bearer {self._access_token_value}",
            }
        return auth_param


@beta
class AirbyteOSSWorkspace(AirbyteCloudWorkspace):
    """This class represents a Airbyte Community workspace and provides utilities
    to interact with Airbyte APIs.
    """

    api_server: str = Field(
        ..., description="The base URL of the API server, excluding the path"
    )
    username: str | None = Field(
        None, description="The username to authenticate to the API if using basic auth"
    )
    password: str | None = Field(
        None, description="The password to authenticate to the API if using basic auth"
    )
    client_id: str | None = Field(
        None, description="The Airbyte client ID if using OAuth."
    )
    client_secret: str | None = Field(
        None, description="The Airbyte client secret if using OAuth."
    )
    workspace_id: str | None = None
    request_max_retries: int = Field(
        default=3,
        description=(
            "The maximum number of times requests to the Airbyte API should be retried "
            "before failing."
        ),
    )
    request_retry_delay: float = Field(
        default=0.25,
        description="Time (in seconds) to wait between each request retry.",
    )
    request_timeout: int = Field(
        default=15,
        description=(
            "Time (in seconds) after which the requests to Airbyte "
            "are declared timed out."
        ),
    )
    _client: AirbyteOSSClient = PrivateAttr(default=None)  # type: ignore[assignment]

    @cached_method
    def get_client(self) -> AirbyteCloudClient:
        return AirbyteOSSClient(
            api_server=self.api_server,
            username=self.username,
            password=self.password,
            client_id=self.client_id,
            client_secret=self.client_secret,
            request_max_retries=self.request_max_retries,
            request_retry_delay=self.request_retry_delay,
            request_timeout=self.request_timeout,
        )
