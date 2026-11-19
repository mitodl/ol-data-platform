"""What the learning_resources webhook assets share."""

import httpx2 as httpx
from dagster import AssetExecutionContext, Backoff, Config, Failure, RetryPolicy
from ol_orchestrate.lib.failures import RETRY_ON_ASSET_OR_OP_FAILURE_TAG
from ol_orchestrate.lib.http_errors import (
    http_failure,
    is_retryable,
    response_body_excerpt,
)
from pydantic import Field

# The only retries a learning_resources webhook asset gets (see skip_run_retries).
# Waits of 20s, 60s and 140s span a Learn deploy or pod restart, which the
# run_retries daemon's reruns used to cover. Each attempt POSTs the full batch.
WEBHOOK_RETRY_POLICY = RetryPolicy(
    max_retries=3, delay=20.0, backoff=Backoff.EXPONENTIAL
)


def skip_run_retries(context: AssetExecutionContext) -> None:
    """Stop the run_retries daemon rerunning this run when it fails.

    A 5xx from Learn is a ``TransientHTTPFailure``, which the ``stop_run_retries``
    hook leaves alone, so after the step's own retries the daemon would rerun the
    run three more times, each with its step retries: up to 16 POSTs of the full
    batch for one persistent 5xx. The hook can't take this over, because the OVS
    assets raise the same failure and rely on run retries after step retries that
    span seconds.

    The tag is per run, so call it only in an asset its sensor runs alone. The
    next run sends what a failed one would have: a webhook delivery is a full
    sync, and the views-rebuilt notice resends what no run has recorded as sent.
    """
    context.instance.add_run_tags(
        context.run_id, {RETRY_ON_ASSET_OR_OP_FAILURE_TAG: "false"}
    )


class LearnDeliveryConfig(Config):
    """Run config for a learning_resources webhook asset.

    The default is what a sensor run gets, since it passes no run config. Setting
    this is a choice made for one hand materialization, so nothing should ever
    default it on.
    """

    allow_mass_unpublish: bool = Field(
        default=False,
        description=(
            "Send this batch even if it unpublishes more than MIT Learn's limit of "
            "10% of a source's published resources of a type. Only for a removal "
            "that has been explained."
        ),
    )


def webhook_failure(source: str, exc: httpx.HTTPStatusError) -> Failure:
    """Build the run failure for a rejected webhook batch.

    A rejection (a 409 for an unowned pair, a 422 over the unpublish limit) says why
    in the response body, and the same batch gets the same answer, so it is not
    retried and the reason goes in the description, which is what the failure alert
    shows. A 5xx's body is an error page, so that stays in the metadata
    ``http_failure`` writes.

    :param source: The name of the source for the message, e.g. ``MIT PE``.
    :param exc: The error ``notify_learning_resources`` raised.
    :returns: The failure to raise.
    :rtype: Failure
    """
    description = f"{source} webhook failed"
    body = response_body_excerpt(exc)
    if body and not is_retryable(exc.response.status_code):
        description = f"{description}: {body}"
    return http_failure(exc, description)
