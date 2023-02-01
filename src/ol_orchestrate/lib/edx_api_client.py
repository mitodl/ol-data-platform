import time
from collections.abc import Generator  # noqa: TCH003

import httpx

TOO_MANY_REQUESTS = 429


def get_access_token(
    client_id: str,
    client_secret: str,
    edx_url: str,
    token_type: str = "jwt",  # noqa: S107
) -> str:
    """Retrieve an access token from an Open edX site via OAUTH2 credentials.

    :param client_id: OAUTH2 client ID for Open edX installation
    :type client_id: str

    :param client_secret: OAUTH2 client secret for Open edX installation
    :type client_secret: str

    :param edx_url: Base URL of edX instance being queried, including protocol.  e.g.
        https://lms.mitx.mit.edu
    :type edx_url: str

    :param token_type: Whether to use a JWT or Bearer token.
    :type token_type: str

    :returns: The retrieved JWT access token for authenticating requests to Open edX API

    :rtype: str
    """
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "token_type": token_type,
    }
    response = httpx.post(f"{edx_url}/oauth2/access_token", data=payload)
    response.raise_for_status()
    return response.json()["access_token"]


def _get_username(edx_url: str, access_token: str):
    response = httpx.get(
        f"{edx_url}/api/user/v1/me",
        headers={"Authorization": f"JWT {access_token}"},
    )
    response.raise_for_status()
    return response.json()["username"]


def _fetch_with_auth(
    request_url: str, access_token: str, username: str, page_size: int
):
    response = httpx.get(
        request_url,
        headers={"Authorization": f"JWT {access_token}"},
        params={"username": username, "page_size": page_size},
    )

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as error_response:
        if error_response.response.status_code == TOO_MANY_REQUESTS:
            time.sleep(60)
            return _fetch_with_auth(request_url, access_token, username, page_size)
        raise
    return response.json()


def get_edx_course_ids(
    edx_url: str, access_token: str, page_size: int = 100
) -> Generator[list[dict], None, None]:
    """Retrieve all items from the edX courses REST API including pagination.

    :param edx_url: Base URL of edX instance being queried, including protocol.  e.g.
        https://lms.mitx.mit.edu
    :type edx_url: str

    :param access_token: A valid JWT access token for authenticating to the edX API
    :type access_token: str

    :param page_size: The number of courses to retrieve per page via the API.
    :type page_size: int

        :yield: A generator for walking the paginated list of courses returned from the
        API  # noqa: RST206
    """
    username = _get_username(edx_url, access_token)
    request_url = f"{edx_url}/api/courses/v1/courses/"
    response_data = _fetch_with_auth(request_url, access_token, username, page_size)
    course_data = response_data["results"]
    next_page = response_data["pagination"].get("next")
    yield course_data
    while next_page:
        response_data = _fetch_with_auth(next_page, access_token, username, page_size)
        next_page = response_data["pagination"].get("next")
        yield response_data["results"]


def export_courses(
    edx_studio_url: str, access_token: str, course_ids: list[str]
) -> dict[str, dict[str, str]]:
    """Trigger export of edX courses to an S3 bucket via an API request.

    :param edx_studio_url: The base URL of the edX studio, including protocol.  e.g.
        https://studio.mitx.mit.edu
    :type edx_studio_url: str

    :param access_token: A valid JWT access token for authenticating to the edX API
    :type access_token: str

    :param course_ids: A list of course IDs to be exported
    :type course_ids: List[str]

    :returns: A dictionary of course IDs and the S3 URL where it will be exported to.

    :rtype: Dict[str, str]
    """
    request_url = f"{edx_studio_url}/api/courses/v0/export/"
    response = httpx.post(
        request_url,
        json={"courses": course_ids},
        headers={"Authorization": f"JWT {access_token}"},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def check_course_export_status(
    edx_studio_url: str, access_token: str, course_id: str, task_id: str
) -> dict[str, str]:
    response = httpx.get(
        f"{edx_studio_url}/api/courses/v0/export/{course_id}/?task_id={task_id}",
        headers={"Authorization": f"JWT {access_token}"},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()
