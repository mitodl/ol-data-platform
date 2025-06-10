from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Self

import httpx
from dagster import ConfigurableResource, InitResourceContext, ResourceDependency
from pydantic import Field, PrivateAttr

from ol_orchestrate.resources.secrets.vault import Vault


class CanvasApiClient(ConfigurableResource):
    token_type: str = Field(
        default="Bearer",
        description="Token type to generate for use with authenticated requests",
    )
    access_token: str = Field(
        description="Access token for the Canvas API",
    )
    base_url: str = Field(
        description="Base URL of the canvas API. e.g. https://canvas.mit.edu",
    )
    http_timeout: int = Field(
        default=60,
        description=(
            "Time (in seconds) to allow for requests to complete before timing out."
        ),
    )
    _http_client: Optional[httpx.Client] = PrivateAttr(default=None)

    @property
    def http_client(self) -> httpx.Client:
        if not self._http_client:
            timeout = httpx.Timeout(self.http_timeout, connect=10)
            self._http_client = httpx.Client(timeout=timeout)
        return self._http_client

    def get_course(self, course_id: int):
        request_url = f"{self.base_url}/api/v1/courses/{course_id}"
        response = self.http_client.get(
            request_url,
            headers={"Authorization": f"{self.token_type} {self.access_token}"},
        )
        response.raise_for_status()
        return response.json()

    def export_course_content(
        self, course_id: int, export_type: str = "common_cartridge"
    ) -> dict[str, str]:
        """Trigger export of canvas courses via an API request.

        :param course_id: The unique identifier of the course to be exported.
        :type course_id: int

        :param export_type: either "common_cartridge", "zip" or "qti"
        :type export_type: str


        returns: A dictionary containing information about the export, including
        the export ID.
        """
        request_url = f"{self.base_url}/api/v1/courses/{course_id}/content_exports"
        response = self.http_client.post(
            request_url,
            json={"export_type": export_type},
            headers={"Authorization": f"{self.token_type} {self.access_token}"},
        )
        response.raise_for_status()
        return response.json()

    def check_course_export_status(
        self, course_id: int, export_id: int
    ) -> dict[str, str]:
        """Trigger export of canvas courses via an API request.

        param course_id: The unique identifier of the course to be exported.
        type course_id: int

        returns: A dictionary containing information about the export, including
        the export ID.
        """
        request_url = (
            f"{self.base_url}/api/v1/courses/{course_id}/content_exports/{export_id}"
        )
        response = self.http_client.get(
            request_url,
            headers={"Authorization": f"{self.token_type} {self.access_token}"},
        )
        response.raise_for_status()
        return response.json()

    def download_course_export(self, url: str, output_path: Path) -> Path:
        """Download a file from a URL to a local path, following redirects and streaming
        to disk.

        :param url: The URL to download the file from.
        :type url: str
        :param output_path: The local path where the file will be saved.
        :type output_path: Path
        """
        with self.http_client.stream(
            "GET", url, follow_redirects=True, timeout=60
        ) as response:
            response.raise_for_status()
            with output_path.open("wb") as f:
                for chunk in response.iter_bytes():
                    if chunk:
                        f.write(chunk)
        return output_path


class CanvasApiClientFactory(ConfigurableResource):
    _client: Optional[CanvasApiClient] = PrivateAttr(default=None)
    vault: ResourceDependency[Vault]

    def _initialize_client(self) -> CanvasApiClient:
        client_secrets = self.vault.client.secrets.kv.v1.read_secret(
            mount_point="secret-data",
            path="pipelines/canvas",
        )["data"]

        return CanvasApiClient(
            base_url=client_secrets["base_url"],
            access_token=client_secrets["access_token"],
        )

    @property
    def client(self) -> CanvasApiClient:
        if not self._client:
            self._client = self._initialize_client()
        return self._client

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self]:  # noqa: ARG002
        yield self
