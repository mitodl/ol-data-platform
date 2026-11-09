"""What the learning_resources webhook assets share."""

import httpx2 as httpx
from dagster import Config, Failure
from ol_orchestrate.lib.http_errors import (
    http_failure,
    is_retryable,
    response_body_excerpt,
)
from pydantic import Field


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
