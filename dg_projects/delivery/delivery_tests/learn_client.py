"""A real MIT Learn client whose requests are answered in-process."""

import json
from collections.abc import Callable
from typing import Any

import httpx2 as httpx
from ol_orchestrate.resources.learn_api import MITLearnApiClient


def learn_client(
    response: httpx.Response,
    on_post: Callable[[int], None] | None = None,
) -> tuple[MITLearnApiClient, list[dict[str, Any]]]:
    """Build a client whose requests get ``response``, recording each body.

    :param response: what every request is answered with
    :param on_post: called with the count of bodies sent so far, before answering
    :returns: the client and the list the decoded bodies are appended to
    :rtype: tuple[MITLearnApiClient, list[dict[str, Any]]]
    """
    sent: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        if on_post is not None:
            on_post(len(sent))
        return response

    client = MITLearnApiClient(base_url="https://learn.example.com", token="t")
    client._http_client = httpx.Client(transport=httpx.MockTransport(handler))
    return client, sent
