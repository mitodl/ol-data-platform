"""Turn an HTTP error into a Dagster failure with the right retry semantics.

Assets that call an external API were all raising a bare ``Exception`` on any
non-2xx response. That has two costs: every failure lands in Sentry as the same
undifferentiated ``Exception`` type regardless of what went wrong, and Dagster's
``run_retries`` re-runs the step for status codes no number of attempts can
clear. A 405 means the endpoint does not accept the method we are sending; a
404 means the thing is not there. Retrying either just multiplies the alert.
"""

from typing import Any

from dagster import Failure, MetadataValue
from httpx2 import HTTPStatusError

HTTP_CLIENT_ERROR_FLOOR = 400
HTTP_SERVER_ERROR_FLOOR = 500

# The two 4xx codes that explicitly invite another attempt. Everything else in
# the 4xx range says the request itself is wrong, which a rerun does not change.
RETRYABLE_CLIENT_ERRORS = frozenset({408, 429})


def is_retryable(status_code: int) -> bool:
    """Whether re-running the step could plausibly get a different answer."""
    if status_code in RETRYABLE_CLIENT_ERRORS:
        return True
    return not (HTTP_CLIENT_ERROR_FLOOR <= status_code < HTTP_SERVER_ERROR_FLOOR)


def http_failure(
    error: HTTPStatusError,
    description: str,
    metadata: dict[str, Any] | None = None,
) -> Failure:
    """Build a Failure for ``error``, retryable only if the status code is.

    ``description`` should say what the caller was trying to do, in terms a
    reader of the Sentry issue can act on. The status code, method and URL are
    appended, so leave those out of it.

    Returned rather than raised so the call site keeps ``raise ... from error``
    and the original traceback survives.
    """
    status_code = error.response.status_code
    retryable = is_retryable(status_code)
    request = error.request
    verdict = (
        "Retrying may clear this."
        if retryable
        else "Retrying cannot clear this -- the request itself is being rejected."
    )
    return Failure(
        description=(
            f"{description}: HTTP {status_code} from "
            f"{request.method} {request.url}. {verdict}"
        ),
        metadata={
            "status_code": status_code,
            "url": MetadataValue.text(str(request.url)),
            "method": request.method,
            "retryable": retryable,
            **(metadata or {}),
        },
        allow_retries=retryable,
    )
