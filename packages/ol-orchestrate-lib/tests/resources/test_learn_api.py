"""Tests for reading MIT Learn's published programs."""

import httpx2 as httpx
from ol_orchestrate.resources.learn_api import MITLearnApiClient

FIRST_PAGE = "https://learn.example.com/api/v1/programs/?platform=edx&limit=100"
SECOND_PAGE = f"{FIRST_PAGE}&offset=100"


def test_get_published_programs_follows_pagination() -> None:
    """Every page is read, and the platform filter goes on the first request."""
    requests: list[httpx.Request] = []
    pages = {
        FIRST_PAGE: {"results": [{"readable_id": "a"}], "next": SECOND_PAGE},
        SECOND_PAGE: {"results": [{"readable_id": "b"}], "next": None},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=pages[str(request.url)])

    client = MITLearnApiClient(base_url="https://learn.example.com", token="t")
    client._http_client = httpx.Client(transport=httpx.MockTransport(handler))

    programs = client.get_published_programs("edx")

    assert [p["readable_id"] for p in programs] == ["a", "b"]
    assert len(requests) == 2
