import time
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Optional, Self
from urllib.parse import parse_qs

import httpx
from dagster import ConfigurableResource, InitResourceContext, ResourceDependency
from pydantic import Field, PrivateAttr, ValidationError, validator

from ol_orchestrate.resources.secrets.vault import Vault

TOO_MANY_REQUESTS = 429


class OpenEdxApiClient(ConfigurableResource):
    client_id: str = Field(description="OAUTH2 client ID for Open edX installation")
    client_secret: str = Field(
        description="OAUTH2 client secret for Open edX installation"
    )
    lms_url: str = Field(
        description="Base URL of edX instance LMS being queried, including protocol. "
        "e.g. https://lms.mitx.mit.edu"
    )
    studio_url: Optional[str] = Field(
        default=None,
        description="Base URL of edx instance Studio being queried, including protocol."
        " e.g. https://studio.mitx.mit.edu",
    )
    token_type: str = Field(
        default="JWT",
        description="Token type to generate for use with authenticated requests",
    )
    token_url: str = Field(
        default=f"{lms_url}/oauth2/access_token",
        description="URL to request token. e.g. https://lms.mitx.mit.edu/oauth2/access_token",
    )
    http_timeout: int = Field(
        default=60,
        description=(
            "Time (in seconds) to allow for requests to complete before timing out."
        ),
    )
    _access_token: Optional[str] = PrivateAttr(default=None)
    _access_token_expires: Optional[datetime] = PrivateAttr(default=None)
    _http_client: httpx.Client = PrivateAttr(default=None)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._initialize_client()

    @validator("token_type")
    def validate_token_type(cls, token_type):  # noqa: N805
        if token_type.lower() not in ["jwt", "bearer"]:
            raise ValidationError
        return token_type

    def _initialize_client(self) -> None:
        if self._http_client is not None:
            return
        timeout = httpx.Timeout(self.http_timeout, connect=10)
        self._http_client = httpx.Client(timeout=timeout)

    def _fetch_access_token(self) -> Optional[str]:
        now = datetime.now(tz=UTC)
        if self._access_token is None or (self._access_token_expires or now) <= now:
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

    @property
    def _username(self) -> str:
        response = self._http_client.get(
            f"{self.lms_url}/api/user/v1/me",
            headers={"Authorization": f"JWT {self._fetch_access_token()}"},
        )
        response.raise_for_status()
        return response.json()["username"]

    def _fetch_with_auth(
        self,
        request_url: str,
        page_size: int = 100,
        extra_params: dict[str, Any] | None = None,
    ) -> dict[Any, Any]:
        request_params = {"username": self._username, "page_size": page_size}
        request_params.update(**(extra_params or {}))
        response = self._http_client.get(
            request_url,
            headers={"Authorization": f"JWT {self._fetch_access_token()}"},
            params=httpx.QueryParams(**request_params),
        )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error_response:
            if error_response.response.status_code == TOO_MANY_REQUESTS:
                time.sleep(60)
                return self._fetch_with_auth(
                    request_url, page_size=page_size, extra_params=extra_params
                )
            raise
        return response.json()

    def get_edx_course_ids(
        self, page_size: int = 100
    ) -> Generator[list[dict[str, str]], None, None]:
        """Retrieve all items from the edX courses REST API including pagination.

        :param page_size: The number of courses to retrieve per page via the API.
        :type page_size: int

        :yield: A generator for walking the paginated list of courses returned from
            the API
        """
        request_url = f"{self.lms_url}/api/courses/v1/courses/"
        response_data = self._fetch_with_auth(request_url, page_size=page_size)
        course_data = response_data["results"]
        next_page = response_data["pagination"].get("next")
        yield course_data
        while next_page:
            response_data = self._fetch_with_auth(
                request_url, page_size=page_size, extra_params=parse_qs(next_page)
            )
            next_page = response_data["pagination"].get("next")
            yield response_data["results"]

    def export_courses(self, course_ids: list[str]) -> dict[str, dict[str, str]]:
        """Trigger export of edX courses to an S3 bucket via an API request.

        :param course_ids: A list of course IDs to be exported
        :type course_ids: List[str]

        :returns: A dictionary of course IDs and the S3 URL where it will be exported
                  to.
        """
        request_url = f"{self.studio_url}/api/courses/v0/export/"
        response = self._http_client.post(
            request_url,
            json={"courses": course_ids},
            headers={"Authorization": f"JWT {self._fetch_access_token()}"},
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    def check_course_export_status(
        self, course_id: str, task_id: str
    ) -> dict[str, str]:
        response = self._http_client.get(
            f"{self.studio_url}/api/courses/v0/export/{course_id}/?task_id={task_id}",
            headers={"Authorization": f"JWT {self._fetch_access_token()}"},
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    def get_course_structure_document(self, course_id: str):
        """Retrieve the course structure for an active course as JSON.

        :param course_id: The ID of the course for which to retrieve the structure

        :returns: JSON document representing the hierarchical structure of the target
                  course.
        """
        request_url = f"{self.lms_url}/api/course-structure/v0/{course_id}/"
        return self._fetch_with_auth(request_url)

    def get_course_outline(self, course_id: str):
        request_url = (
            f"{self.lms_url}/api/learning_sequences/v1/course_outline/{course_id}"
        )
        return self._fetch_with_auth(request_url)

    def get_edxorg_programs(self):
        """
        Retrieve the program metadata from the edX.org REST API by walking through
         the paginated results

        Yield: A generator for walking the paginated list of programs returned
        from the API

        """
        request_url = "https://discovery.edx.org/api/v1/programs/"
        response_data = self._fetch_with_auth(request_url)
        results = response_data["results"]
        next_page = response_data["next"]
        yield results
        while next_page:
            response_data = self._fetch_with_auth(next_page)
            next_page = response_data["next"]
            yield response_data["results"]


class OpenEdxApiClientFactory(ConfigurableResource):
    deployment: str = Field(
        description="The canonical name of the Ope edX deployment managed by "
        "Open Learning engineering. Refer to https://github.com/mitodl/ol-infrastructure/blob/main/src/bridge/settings/openedx/types.py#L73"
    )
    _client: OpenEdxApiClient = PrivateAttr()
    vault: ResourceDependency[Vault]

    def _initialize_client(self) -> OpenEdxApiClient:
        client_secrets = self.vault.client.secrets.kv.v1.read_secret(
            mount_point="secret-data",
            path=f"pipelines/edx/{self.deployment}/edx-oauth-client",
        )["data"]

        self._client = OpenEdxApiClient(
            client_id=client_secrets["id"],
            client_secret=client_secrets["secret"],
            lms_url=client_secrets["url"],
            studio_url=client_secrets["studio_url"],
            token_url=client_secrets.get(
                "token_url", f"{client_secrets['url']}/oauth2/access_token"
            ),
        )
        return self._client

    @property
    def client(self) -> OpenEdxApiClient:
        return self._client

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self]:  # noqa: ARG002
        self._initialize_client()
        yield self
