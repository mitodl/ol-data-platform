import urllib.parse
from typing import Literal, Optional

import httpx
from dagster import (
    ConfigurableResource,
    InitResourceContext,
)
from pydantic import Field


class HealthchecksIO(ConfigurableResource):
    check_id: Optional[str] = Field(
        default=None,
        description=(
            "UUID to identify the specific check in the Healthchecks "
            "application that will be updated."
        ),
    )
    measure_time: bool = Field(
        default=False,
        description=(
            "Toggle whether to send a start event when the resource is initialized."
        ),
    )
    ping_host: str = Field(
        default="https://hc-ping.com",
        description="Base URL of host for sending check information.",
    )

    @property
    def host(self) -> str:
        return self.ping_host

    def send_update(self, method: Optional[Literal["start", "fail"]] = None):
        httpx.post(urllib.parse.urljoin(self.host, self.check_id, method))  # type: ignore[arg-type]  # noqa: E501

    def setup_for_execution(self, context: InitResourceContext) -> None:  # noqa: ARG002
        if self.measure_time:
            self.send_update("start")
