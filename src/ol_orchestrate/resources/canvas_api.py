from pathlib import Path

from pydantic import Field

from ol_orchestrate.resources.api_client import BaseApiClient


class CanvasApiClient(BaseApiClient):
    access_token: str = Field(
        description="Access token for the Canvas API",
    )

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
