"""Turn an HTTP error into a Dagster failure with the right retry semantics.

Assets that call an external API were all raising a bare ``Exception`` on any
non-2xx response, so every failure landed in Sentry as the same
undifferentiated ``Exception`` type regardless of what went wrong. A 405 means
the endpoint does not accept the method we are sending and a 404 means the
thing is not there, neither of which a rerun changes; a 502 is worth another
attempt. Those deserve to be different issues.

Scope of ``allow_retries``, because it is narrower than it sounds: Dagster
consults it only when the op or asset carries a ``RetryPolicy`` (see
``dagster._core.execution.plan.utils.op_execution_error_boundary``). It has no
effect on the run-level auto-reexecution daemon, which decides purely from run
status, the ``dagster/max_retries`` and
``dagster/retry_on_asset_or_op_failure`` tags, and the run group size. So a
permanent failure raised here is still re-run by ``run_retries`` unless that is
suppressed at the run or instance level.
"""

from typing import Any

from dagster import Failure, MetadataValue
from httpx2 import HTTPStatusError

HTTP_SERVER_ERROR_FLOOR = 500
HTTP_SERVER_ERROR_CEILING = 600

# The two 4xx codes that explicitly invite another attempt. Everything else in
# the 4xx range says the request itself is wrong, which a rerun does not change.
RETRYABLE_CLIENT_ERRORS = frozenset({408, 429})


class PermanentHTTPFailure(Failure):
    """A response a rerun cannot change: the request itself is being rejected.

    A distinct class rather than a plain ``Failure`` so Sentry can separate it
    from the transient case. Both the in-process hook and the run failure
    sensor fingerprint on the exception's class name, so every ``Failure``
    raised from one step used to collapse into a single issue no matter which
    status produced it.
    """


class TransientHTTPFailure(Failure):
    """A response where another attempt could plausibly succeed."""


def is_retryable(status_code: int) -> bool:
    """Whether re-running the step could plausibly get a different answer.

    Only 5xx, plus the two 4xx codes that invite a retry. Deliberately not
    "anything that is not 4xx": that made redirects and every other non-4xx
    status retryable, which is not the documented policy.
    """
    if status_code in RETRYABLE_CLIENT_ERRORS:
        return True
    return HTTP_SERVER_ERROR_FLOOR <= status_code < HTTP_SERVER_ERROR_CEILING


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
    failure_cls = TransientHTTPFailure if retryable else PermanentHTTPFailure
    return failure_cls(
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
