from collections.abc import Generator
from http import HTTPStatus
from pathlib import Path
from typing import Any

import httpx2 as httpx
from pydantic import Field

from ol_orchestrate.resources.api_client import BaseApiClient

# Resumes after a dropped connection, not retries from zero: each one picks up
# where the file ends, so three covers three separate drops in one transfer.
DOWNLOAD_RESUME_ATTEMPTS = 3


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
        """Download a course export to a local path, resuming if the body is cut off.

        Exports run to gigabytes, and a connection dropped partway used to cost
        the whole transfer: DAGSTER-2X failed 2.0 GB into a 2.5 GB export. After
        a drop this asks for the remainder with a ``Range`` header and appends
        it. A server that answers the range with a full 200, or with a
        ``Content-Range`` that does not start where the file ends, gets the file
        rewritten from the start instead.

        The request asks for no content encoding. Range offsets count the bytes
        the server sends, and ``iter_bytes`` writes them decoded, so with a
        compressed body the size on disk would not be the offset to resume from.

        :param url: The URL to download the file from.
        :type url: str
        :param output_path: The local path where the file will be saved.
        :type output_path: Path
        :returns: ``output_path``.
        :rtype: Path
        """
        output_path.unlink(missing_ok=True)
        for attempt in range(DOWNLOAD_RESUME_ATTEMPTS + 1):
            written = output_path.stat().st_size if output_path.exists() else 0
            headers = {"Accept-Encoding": "identity"}
            if written:
                headers["Range"] = f"bytes={written}-"
            try:
                with self.http_client.stream(
                    "GET", url, follow_redirects=True, timeout=60, headers=headers
                ) as response:
                    # The connection dropped after the last byte but before the
                    # body was terminated: the file is already whole.
                    if (
                        response.status_code
                        == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE
                        and (
                            response.headers.get("Content-Range")
                            == f"bytes */{written}"
                        )
                    ):
                        return output_path
                    response.raise_for_status()
                    resumes = response.status_code == HTTPStatus.PARTIAL_CONTENT
                    if resumes and not response.headers.get(
                        "Content-Range", ""
                    ).startswith(f"bytes {written}-"):
                        output_path.unlink(missing_ok=True)
                        continue
                    with output_path.open("ab" if resumes else "wb") as f:
                        for chunk in response.iter_bytes():
                            f.write(chunk)
            except (httpx.RemoteProtocolError, httpx.ReadError, httpx.ReadTimeout):
                if attempt == DOWNLOAD_RESUME_ATTEMPTS:
                    raise
            else:
                return output_path
        msg = (
            f"Gave up on {output_path.name} after {DOWNLOAD_RESUME_ATTEMPTS + 1} "
            "requests: the last range request got a Content-Range that did not "
            "resume the file."
        )
        raise RuntimeError(msg)

    def get_all_items(
        self, course_id: int, endpoint: str, per_page: int = 100
    ) -> Generator[list[dict[str, Any]], None, None]:
        """
        Fetch all paginated results from a Canvas course endpoint.

        Example endpoints:
          - 'files'
          - 'pages'
          - 'assignments'
        """
        page = 1
        while True:
            request_url = (
                f"{self.base_url}/api/v1/courses/{course_id}/{endpoint}"
                f"?page={page}&per_page={per_page}"
            )
            response = self.http_client.get(
                request_url,
                headers={"Authorization": f"{self.token_type} {self.access_token}"},
            )
            response.raise_for_status()
            items = response.json()

            if not items:
                break

            yield items
            page += 1

    def get_course_files(
        self, course_id: int, per_page: int = 100
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Get a list of files in a course including pagination.

        :param course_id: The unique identifier of the course
        :type course_id: int
        :param per_page: Number of items per page
        :type per_page: int
        """
        yield from self.get_all_items(course_id, "files", per_page=per_page)

    def get_course_folders(self, course_id: int, folder_id: int) -> dict[str, str]:
        """Get details of a specific folder in a course.

        :param course_id: The unique identifier of the course
        :type course_id: int
        :param folder_id: The unique identifier of the folder
        :type folder_id: int

        """
        request_url = f"{self.base_url}/api/v1/courses/{course_id}/folders/{folder_id}"
        response = self.http_client.get(
            request_url,
            headers={"Authorization": f"{self.token_type} {self.access_token}"},
        )
        response.raise_for_status()
        return response.json()

    def get_course_assignments(
        self, course_id: int, per_page: int = 100
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Get a list of Assignments in a course including pagination.

        :param course_id: The unique identifier of the course
        :type course_id: int
        :param per_page: Number of items per page
        :type per_page: int
        """
        yield from self.get_all_items(course_id, "assignments", per_page=per_page)

    def get_course_pages(
        self, course_id: int, per_page: int = 100
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Get a list of Pages in a course including pagination.

        :param course_id: The unique identifier of the course
        :type course_id: int
        :param per_page: Number of items per page
        :type per_page: int
        """
        yield from self.get_all_items(course_id, "pages", per_page=per_page)
