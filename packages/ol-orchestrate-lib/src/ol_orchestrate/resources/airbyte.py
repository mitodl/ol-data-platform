from collections.abc import Mapping, Sequence
from typing import Any, Self
from urllib.parse import urlparse, urlunparse

from dagster._annotations import beta
from dagster_airbyte.resources import AirbyteClient, AirbyteWorkspace
from dagster_shared.utils.cached_method import cached_method
from pydantic.fields import Field, PrivateAttr
from pydantic.functional_validators import model_validator

AIRBYTE_REST_API_VERSION = "v1"
AIRBYTE_CONFIGURATION_API_VERSION = "v1"


@beta
class AirbyteOSSClient(AirbyteClient):
    """Expose methods on top of the Airbyte APIs for Airbyte Community Edition."""

    workspace_id: str | None = Field(
        default=None, description="The ID of the workspace to interact with"
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
    request_page_size: int = Field(
        default=15,
        description=(
            "The number of records to include in paginated requests to the Airbyte API"
        ),
    )
    rest_api_base_url: str = Field(
        "", description="The full URL of the Airbyte REST API"
    )
    configuration_api_base_url: str = Field(
        "", description="The full URL of the Airbyte configuration API"
    )

    _access_token_value: str | None = PrivateAttr(default=None)
    _access_token_timestamp: float | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def ensure_workspace_id(self) -> Self:
        if not self.workspace_id:
            workspaces = self._single_request(
                method="GET",
                url=f"{self.rest_api_base_url}/workspaces",
            ).get("data", [])
            self.__dict__["workspace_id"] = workspaces[0]["workspaceId"]
        return self

    def _paginated_request(
        self,
        method: str,
        url: str,
        params: Mapping[str, Any],
        data: Mapping[str, Any] | None = None,
        include_additional_request_params: bool = True,  # noqa: FBT001, FBT002
    ) -> Sequence[Mapping[str, Any]]:
        """Execute paginated requests and yield all items."""
        result_data = []
        _url_parsed = urlparse(url)
        while url != "":
            response = self._single_request(
                method=method,
                url=url,
                data=data,
                params=params,
                include_additional_request_params=include_additional_request_params,
            )

            # Handle different response structures
            result_data.extend(response.get("data", []))
            # The `next` parameter in a self-hosted environment defaults to using
            # `localhost` in the host portion of the path, resulting in errors when
            # trying to fetch paginated data. (TMM 2025-09-22)
            if next_url := response.get("next", ""):
                next_parsed = urlparse(next_url)
                url = urlunparse(
                    (_url_parsed.scheme, _url_parsed.netloc, *next_parsed[2:])
                )
            else:
                url = ""
            params = {}

        return result_data


@beta
class AirbyteOSSWorkspace(AirbyteWorkspace):
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
    request_page_size: int = Field(
        default=15,
        description=(
            "The number of records to include in paginated requests to the Airbyte API"
        ),
    )
    rest_api_base_url: str = Field(
        "", description="The full URL of the Airbyte REST API"
    )
    configuration_api_base_url: str = Field(
        "", description="The full URL of the Airbyte configuration API"
    )

    _client: AirbyteOSSClient = PrivateAttr(default=None)  # type: ignore[assignment]

    @cached_method
    def get_client(self) -> AirbyteClient:
        return AirbyteOSSClient(
            api_server=self.api_server,
            username=self.username,
            password=self.password,
            client_id=self.client_id,
            client_secret=self.client_secret,
            request_max_retries=self.request_max_retries,
            request_retry_delay=self.request_retry_delay,
            request_timeout=self.request_timeout,
            rest_api_base_url=self.rest_api_base_url
            or f"{self.api_server}/api/public/{AIRBYTE_REST_API_VERSION}",
            configuration_api_base_url=self.configuration_api_base_url
            or f"{self.api_server}/api/{AIRBYTE_CONFIGURATION_API_VERSION}",
        )
