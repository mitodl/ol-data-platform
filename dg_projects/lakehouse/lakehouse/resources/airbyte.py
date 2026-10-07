import time
from collections.abc import Mapping, Sequence
from datetime import datetime
from http import HTTPStatus
from typing import Any, Self

from dagster import Failure
from dagster._annotations import beta
from dagster_airbyte.resources import AirbyteClient, AirbyteWorkspace
from dagster_airbyte.translator import AirbyteJob, AirbyteJobStatusType
from dagster_shared.utils.cached_method import cached_method
from pydantic.fields import Field, PrivateAttr
from pydantic.functional_validators import model_validator
from requests.exceptions import RequestException

AIRBYTE_REST_API_VERSION = "v1"
AIRBYTE_CONFIGURATION_API_VERSION = "v1"

# The statuses ``AirbyteClient.sync_and_poll`` counts as "a sync is already
# under way" when it decides whether to start a new job or attach to an
# existing one.  Mirrored here so the collapse below keys off the same set.
IN_FLIGHT_JOB_STATUSES = (
    AirbyteJobStatusType.RUNNING,
    AirbyteJobStatusType.PENDING,
    AirbyteJobStatusType.INCOMPLETE,
)

# Airbyte pages its list endpoints by offset over an ordering that is not stable
# between requests, so consecutive pages can overlap: a row shifts back onto the
# next page and appears twice while another is never returned. Probed against
# production on 2026-10-01 at 15 rows a page, one listing in six came back as 39
# rows holding 29 distinct connections. `max_items_per_page` (100, the most the
# API accepts) holds the whole workspace (39 connections, 50 sources), so there
# is no page boundary for a row to cross; the checks in `list_collection` are
# what still hold once the workspace outgrows it.
LISTING_ATTEMPTS = 4


@beta
class AirbyteOSSClient(AirbyteClient):
    """Expose methods on top of the Airbyte APIs for Airbyte Community Edition."""

    workspace_id: str | None = Field(
        default=None, description="The ID of the workspace to interact with"
    )
    api_server: str = Field(
        ..., description="The base URL of the API server, excluding the path"
    )
    username: str | None = Field(
        None, description="The username to authenticate to the API if using basic auth"
    )
    password: str | None = Field(
        None, description="The password to authenticate to the API if using basic auth"
    )
    client_id: str | None = Field(
        None, description="The Airbyte client ID if using OAuth."
    )
    client_secret: str | None = Field(
        None, description="The Airbyte client secret if using OAuth."
    )
    request_max_retries: int = Field(
        ...,
        description=(
            "The maximum number of times requests to the Airbyte API should be retried "
            "before failing."
        ),
    )
    request_retry_delay: float = Field(
        ...,
        description="Time (in seconds) to wait between each request retry.",
    )
    request_timeout: int = Field(
        ...,
        description=(
            "Time (in seconds) after which the requests to Airbyte "
            "are declared timed out."
        ),
    )
    rest_api_base_url: str = Field(
        "", description="The full URL of the Airbyte REST API"
    )
    configuration_api_base_url: str = Field(
        "", description="The full URL of the Airbyte configuration API"
    )

    _access_token_value: str | None = PrivateAttr(default=None)
    _access_token_timestamp: float | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def ensure_workspace_id(self) -> Self:
        if not self.workspace_id:
            workspaces = self._single_request(
                method="GET",
                url=f"{self.rest_api_base_url}/workspaces",
            ).get("data", [])
            self.__dict__["workspace_id"] = workspaces[0]["workspaceId"]
        return self

    def get_jobs_for_connection(
        self, connection_id: str, created_after: datetime | None = None
    ) -> Sequence[AirbyteJob]:
        """Return the connection's jobs with concurrent in-flight ones collapsed.

        ``sync_and_poll`` attaches to a single in-flight job and raises
        ``Found multiple running jobs`` on two or more.  That distinction does
        not hold here: ``definitions.py`` sets ``poll_previous_running_sync``
        precisely because the automation condition and Airbyte's own scheduler
        both launch syncs into the same connection, so two in-flight jobs is
        the same routine overlap as one, arriving twice.  Nine connections were
        failing nightly on it, each amplified fourfold by run retries that
        cannot succeed -- the condition is unchanged by re-running.

        Attaching to the newest is what the one-job branch already does, and
        Airbyte job ids increase monotonically, so the highest id is the most
        recently created.  Older in-flight jobs are dropped from the returned
        list; every terminal job is passed through untouched.

        This overrides the accessor rather than ``sync_and_poll`` because the
        library exposes no other seam: the decision is inline in a method too
        large to fork safely, and this is its only caller in dagster-airbyte
        0.29.
        """
        # Offset pages can overlap (see LISTING_ATTEMPTS), and one in-flight job
        # returned twice would survive the collapse below as two.
        jobs = list(
            {
                job.id: job
                for job in super().get_jobs_for_connection(
                    connection_id=connection_id, created_after=created_after
                )
            }.values()
        )
        in_flight = [job for job in jobs if job.status in IN_FLIGHT_JOB_STATUSES]
        if len(in_flight) <= 1:
            return jobs
        newest_id = max(job.id for job in in_flight)
        superseded = {job.id for job in in_flight if job.id != newest_id}
        return [job for job in jobs if job.id not in superseded]

    def list_collection(self, path: str, id_key: str) -> list[Mapping[str, Any]]:
        """List a collection until two consecutive reads agree and neither overlapped.

        Each page is a slice of whatever order the server used for that request. If
        the collection holds still, a listing has as many rows as the collection, so
        a skipped record shows up as another one duplicated. If a record is deleted
        between two page fetches, every later row shifts back and the one on the
        page boundary is skipped with nothing duplicated; the next read returns it,
        so it cannot match. Requiring two matching reads covers both.

        :param path: The collection's path under the REST API, e.g. ``connections``.
        :param id_key: The field that identifies a record in the collection.
        :returns: Every record in the workspace's collection, each exactly once.
        :raises Failure: When no two consecutive reads agree.
        """
        previous: set[str] | None = None
        for _ in range(LISTING_ATTEMPTS):
            items = list(
                self._paginated_request(
                    method="GET",
                    url=f"{self.rest_api_base_url}/{path}",
                    params={"workspaceIds": self.workspace_id},
                )
            )
            ids = {item[id_key] for item in items}
            if len(ids) != len(items):
                previous = None
                continue
            if ids == previous:
                return items
            previous = ids
        msg = (
            f"Airbyte's /{path} listing did not return the same complete set twice in "
            f"{LISTING_ATTEMPTS} attempts. Refusing to use it: a record skipped at a "
            "page boundary would be missing from the result."
        )
        raise Failure(description=msg)

    def get_connections(self) -> Sequence[Mapping[str, Any]]:
        """List the workspace's connections, each exactly once.

        The asset graph is built from this, so a connection skipped at a page
        boundary would leave the graph without its assets and raise nothing.
        """
        return self.list_collection("connections", "connectionId")

    def _single_request(
        self,
        method: str,
        url: str,
        data: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
        include_additional_request_headers: bool = True,  # noqa: FBT001, FBT002
    ) -> Mapping[str, Any]:
        """Execute a request, backing off between retries and failing fast on 4xx.

        The library sleeps a fixed ``request_retry_delay`` between attempts and
        retries every error alike. Here the delay doubles on each attempt, so
        the retries span an API outage instead of all landing inside it, and a
        4xx other than 429 raises at once with the response body: the request
        will be refused again, and the body is where Airbyte says why (a 409 on
        ``POST /jobs`` is "A sync is already running").

        :raises Failure: On a 4xx, or when the retries are used up.
        """
        for attempt in range(self.request_max_retries + 1):
            try:
                session = self._get_session(
                    include_additional_request_headers=include_additional_request_headers
                )
                response = session.request(
                    method=method,
                    url=url,
                    json=data,
                    params=params,
                    timeout=self.request_timeout,
                )
                response.raise_for_status()
                return response.json()
            except RequestException as e:
                self._log.error(
                    "Request to Airbyte API failed for url %s with method %s : %s",
                    url,
                    method,
                    e,
                )
                refused = e.response
                if (
                    refused is not None
                    and HTTPStatus(refused.status_code).is_client_error
                    and refused.status_code != HTTPStatus.TOO_MANY_REQUESTS
                ):
                    msg = (
                        f"Airbyte API answered {refused.status_code} to {method} "
                        f"{url}: {refused.text}"
                    )
                    raise Failure(description=msg) from e
                if attempt < self.request_max_retries:
                    time.sleep(self.request_retry_delay * 2**attempt)

        msg = f"Max retries ({self.request_max_retries}) exceeded with url: {url}."
        raise Failure(description=msg)


@beta
class AirbyteOSSWorkspace(AirbyteWorkspace):
    """This class represents a Airbyte Community workspace and provides utilities
    to interact with Airbyte APIs.
    """

    api_server: str = Field(
        ..., description="The base URL of the API server, excluding the path"
    )
    username: str | None = Field(
        None, description="The username to authenticate to the API if using basic auth"
    )
    password: str | None = Field(
        None, description="The password to authenticate to the API if using basic auth"
    )
    client_id: str | None = Field(
        None, description="The Airbyte client ID if using OAuth."
    )
    client_secret: str | None = Field(
        None, description="The Airbyte client secret if using OAuth."
    )
    workspace_id: str | None = None
    request_max_retries: int = Field(
        default=3,
        description=(
            "The maximum number of times requests to the Airbyte API should be retried "
            "before failing."
        ),
    )
    request_retry_delay: float = Field(
        default=0.25,
        description="Time (in seconds) to wait between each request retry.",
    )
    request_timeout: int = Field(
        default=15,
        description=(
            "Time (in seconds) after which the requests to Airbyte "
            "are declared timed out."
        ),
    )
    rest_api_base_url: str = Field(
        "", description="The full URL of the Airbyte REST API"
    )
    configuration_api_base_url: str = Field(
        "", description="The full URL of the Airbyte configuration API"
    )

    _client: AirbyteOSSClient = PrivateAttr(default=None)  # type: ignore[assignment]

    @cached_method
    def get_client(self) -> AirbyteOSSClient:
        """Build the OSS client, carrying every setting the base class carries.

        The polling four -- poll_interval, poll_timeout, cancel_on_termination
        and poll_previous_running_sync -- were missing from this list, so
        configuring them on the workspace set a field the client never saw and
        every sync silently ran on the library defaults. That is why
        ``poll_previous_running_sync`` did not take effect and ten connections
        raised "already running" instead of waiting (DAGSTER-D, S, V, W, Y, Z,
        11, 12, 19, 1W).
        """
        return AirbyteOSSClient(
            api_server=self.api_server,
            username=self.username,
            password=self.password,
            client_id=self.client_id,
            client_secret=self.client_secret,
            workspace_id=self.workspace_id,
            request_max_retries=self.request_max_retries,
            request_retry_delay=self.request_retry_delay,
            request_timeout=self.request_timeout,
            max_items_per_page=self.max_items_per_page,
            poll_interval=self.poll_interval,
            poll_timeout=self.poll_timeout,
            cancel_on_termination=self.cancel_on_termination,
            poll_previous_running_sync=self.poll_previous_running_sync,
            rest_api_base_url=self.rest_api_base_url
            or f"{self.api_server}/api/public/{AIRBYTE_REST_API_VERSION}",
            configuration_api_base_url=self.configuration_api_base_url
            or f"{self.api_server}/api/{AIRBYTE_CONFIGURATION_API_VERSION}",
        )
