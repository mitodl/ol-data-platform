from collections.abc import Generator
from contextlib import contextmanager
from typing import Optional, Self
from urllib.parse import parse_qs

from dagster import (
    AssetExecutionContext,
    AssetRecordsFilter,
    ConfigurableResource,
    InitResourceContext,
    Output,
    ResourceDependency,
)
from httpx import HTTPStatusError
from pydantic import Field, PrivateAttr

from ol_orchestrate.resources.oauth import OAuthApiClient
from ol_orchestrate.resources.secrets.vault import Vault


class OpenEdxApiClient(OAuthApiClient):
    studio_url: Optional[str] = Field(
        default=None,
        description="Base URL of edx instance Studio being queried, including protocol."
        " e.g. https://studio.mitx.mit.edu",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_edx_course_ids(
        self, page_size: int = 100
    ) -> Generator[list[dict[str, str]], None, None]:
        """Retrieve all items from the edX courses REST API including pagination.

        :param page_size: The number of courses to retrieve per page via the API.
        :type page_size: int

        :yield: A generator for walking the paginated list of courses returned from
            the API
        """
        request_url = f"{self.base_url}/api/courses/v1/courses/"
        response_data = self.fetch_with_auth(request_url, page_size=page_size)
        course_data = response_data["results"]
        next_page = response_data["pagination"].get("next")
        yield course_data
        while next_page:
            response_data = self.fetch_with_auth(
                request_url, page_size=page_size, extra_params=parse_qs(next_page)
            )
            next_page = response_data["pagination"].get("next")
            yield response_data["results"]

    def check_course_status(self, course_id: str) -> int:
        """Query the edX platform to determine if the course is active.
        :param course_id: The unique identifier of the course.
        :type course_id: str

        :return: A dictionary containing the course ID and the status of the course.
        """
        request_url = f"{self.base_url}/api/courses/v1/courses/{course_id}/"
        try:
            self.fetch_with_auth(request_url)
        except HTTPStatusError as e:
            return e.response.status_code
        return 200

    def backfill_asset(
        self, context: AssetExecutionContext, course_key: str
    ) -> Optional[Output]:
        """Query the edX platform to determine if the course is active.
        :param context: Provides additional context specific to the asset.
        :type context: AssetExecutionContext

        :param course_key: The ID of the course for which to retrieve the asset.
        :type course_key: str

        :return: The last successfully materialized asset.
        """
        context.log.exception(
            "Course %s not found in the Open edX platform.", course_key
        )
        try:
            return (
                context.instance.fetch_materializations(
                    records_filter=AssetRecordsFilter(
                        asset_key=context.asset_key,
                        asset_partitions=context.partition_key,
                    ),
                    limt=1,
                )
                .records[0]
                .asset_materialization
            )
        except IndexError:
            context.log.exception(
                "No previous materialization found for course %s", course_key
            )
            return None

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
        request_url = f"{self.studio_url}/api/courses/v0/export/{course_id}/"
        return self.fetch_with_auth(request_url, extra_params={"task_id": task_id})

    def get_course_structure_document(self, course_id: str):
        """Retrieve the course structure for an active course as JSON.

        :param course_id: The ID of the course for which to retrieve the structure

        :returns: JSON document representing the hierarchical structure of the target
                  course.
        """
        request_url = f"{self.base_url}/api/course-structure/v0/{course_id}/"
        return self.fetch_with_auth(request_url)

    def get_course_outline(self, course_id: str):
        request_url = (
            f"{self.base_url}/api/learning_sequences/v1/course_outline/{course_id}"
        )
        return self.fetch_with_auth(request_url)

    def get_edxorg_programs(self):
        """
        Retrieve the program metadata from the edX.org REST API by walking through
         the paginated results

        Yield: A generator for walking the paginated list of programs returned
        from the API

        """
        request_url = "https://discovery.edx.org/api/v1/programs/"
        response_data = self.fetch_with_auth(request_url)
        results = response_data["results"]
        next_page = response_data["next"]
        count = response_data["count"]
        yield count, results
        while next_page:
            response_data = self.fetch_with_auth(
                request_url, extra_params=parse_qs(next_page)
            )
            next_page = response_data["next"]
            yield response_data["results"]

    def get_edxorg_mitx_courses(self):
        """
        Retrieve a list of all the active courses in MITx catalog by walking through the
        paginated results

        Yield: A generator for walking the paginated list of courses
        """
        course_catalog_url = "https://discovery.edx.org/api/v1/catalogs/10/courses/"
        response_data = self.fetch_with_auth(course_catalog_url)
        results = response_data["results"]
        next_page = response_data["next"]
        count = response_data["count"]
        yield count, results
        while next_page:
            response_data = self.fetch_with_auth(
                course_catalog_url, extra_params=parse_qs(next_page)
            )
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
            base_url=client_secrets["url"],
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
