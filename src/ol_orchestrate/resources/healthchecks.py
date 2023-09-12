import urllib.parse
from typing import Literal, Optional

import httpx
from dagster import Bool, Field, InitResourceContext, String, resource


class HealthchecksIO:
    def __init__(self, check_id: str, healthchecks_host: str = "https://hc-ping.com"):
        self.check_id = check_id
        self.host = healthchecks_host

    def send_update(self, method: Optional[Literal["start", "fail"]] = None):
        httpx.post(
            urllib.parse.urljoin(self.host, self.check_id, method)  # type: ignore  # noqa: E501, PGH003
        )


@resource(
    config_schema={
        "check_id": Field(
            String,
            is_required=True,
            description=(
                "UUID to identify the specific check in the Healthchecks "
                "application that will be updated."
            ),
        ),
        "measure_time": Field(
            Bool,
            is_required=False,
            default_value=False,
            description=(
                "Toggle whether to send a start event when the resource is "
                "initialized."
            ),
        ),
        "ping_host": Field(
            String,
            is_required=False,
            default_value="https://hc-ping.com",
            description="Base URL of host for sending check information.",
        ),
    }
)
def healthchecks_io_resource(resource_context: InitResourceContext):
    check = HealthchecksIO(
        check_id=resource_context.resource_config["check_id"],
        healthchecks_host=resource_context.resource_config["ping_host"],
    )
    if resource_context.resource_config["measure_time"]:
        check.send_update("start")
    yield check


@resource()
def healthchecks_dummy_resource(resource_context: InitResourceContext):  # noqa: ARG001
    yield
