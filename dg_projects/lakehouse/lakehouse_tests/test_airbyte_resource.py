"""Tests for the Airbyte Community Edition workspace override.

AirbyteOSSWorkspace.get_client() re-implements the base class's constructor call
with an explicit argument list, which means any setting the base class grows --
or that we simply forgot -- is silently dropped rather than failing loudly. That
is not hypothetical: the four polling settings were missing, so configuring
poll_previous_running_sync on the workspace set a field the client never read.
"""

from datetime import datetime
from http import HTTPStatus
from typing import Any

import pytest
from dagster import Failure
from dagster_airbyte.resources import AirbyteClient
from dagster_airbyte.translator import AirbyteJob, AirbyteJobStatusType
from lakehouse.resources import airbyte as airbyte_resource
from lakehouse.resources.airbyte import (
    LISTING_ATTEMPTS,
    AirbyteOSSClient,
    AirbyteOSSWorkspace,
)
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError

# Non-default values throughout, so a setting that fails to propagate shows up
# as the library default rather than coincidentally matching.
WORKSPACE_SETTINGS = {
    "api_server": "https://airbyte.example.invalid",
    "username": "dagster",
    "password": "not-a-real-password",  # pragma: allowlist secret
    "workspace_id": "workspace-1",
    "request_max_retries": 7,
    "request_retry_delay": 1.5,
    "request_timeout": 60,
    "max_items_per_page": 50,
    "poll_interval": 17.0,
    "poll_timeout": 1234.0,
    "cancel_on_termination": False,
    "poll_previous_running_sync": True,
}


@pytest.fixture
def client():
    return AirbyteOSSWorkspace(**WORKSPACE_SETTINGS).get_client()


@pytest.mark.parametrize(
    ("setting", "expected"),
    [
        # The polling four -- all of these were dropped.
        ("poll_previous_running_sync", True),
        ("poll_interval", 17.0),
        ("poll_timeout", 1234.0),
        ("cancel_on_termination", False),
        # And the rest, so a future edit to the argument list cannot quietly
        # drop one of these either.
        ("workspace_id", "workspace-1"),
        ("username", "dagster"),
        ("request_max_retries", 7),
        ("request_retry_delay", 1.5),
        ("request_timeout", 60),
        ("max_items_per_page", 50),
    ],
)
def test_workspace_settings_reach_the_client(client, setting, expected) -> None:
    assert getattr(client, setting) == expected


def test_poll_previous_running_sync_is_what_stops_the_already_running_failure(
    client,
) -> None:
    """The setting that turns ten Sentry issues into a wait.

    dagster_airbyte.sync_and_poll raises `Failure: Found sync job for
    connection_id=... already running` when it finds an in-flight sync and this
    is False. The connection id is in the message, so each connection became its
    own issue: DAGSTER-D, S, V, W, Y, Z, 11, 12, 19, 1W.
    """
    assert client.poll_previous_running_sync is True


def test_the_api_base_urls_are_derived_from_the_api_server(client) -> None:
    assert client.rest_api_base_url == "https://airbyte.example.invalid/api/public/v1"
    assert client.configuration_api_base_url == "https://airbyte.example.invalid/api/v1"


def test_explicit_base_urls_win_over_the_derived_ones() -> None:
    client = AirbyteOSSWorkspace(
        **WORKSPACE_SETTINGS,
        rest_api_base_url="https://proxy.example.invalid/public",
        configuration_api_base_url="https://proxy.example.invalid/config",
    ).get_client()

    assert client.rest_api_base_url == "https://proxy.example.invalid/public"
    assert client.configuration_api_base_url == "https://proxy.example.invalid/config"


def _job(job_id: int, status: AirbyteJobStatusType | str) -> AirbyteJob:
    status_value = status.value if isinstance(status, AirbyteJobStatusType) else status
    return AirbyteJob(id=job_id, status=status_value, type="sync")


def _stub_super_jobs(monkeypatch, jobs):
    """Make AirbyteClient.get_jobs_for_connection return *jobs* without HTTP."""
    monkeypatch.setattr(
        AirbyteClient,
        "get_jobs_for_connection",
        lambda self, connection_id, created_after=None: jobs,  # noqa: ARG005
    )


class TestConcurrentInFlightJobsAreCollapsed:
    """sync_and_poll fails outright on two in-flight jobs; one it attaches to.

    That distinction does not survive contact with a connection that two
    schedulers launch into. Nine connections failed nightly on `Found multiple
    running jobs`, each amplified fourfold by run retries that cannot change
    the condition (DAGSTER-2Q, 33, 39, 3A, 3C, 3E, 3F, 3N, 3R).
    """

    def test_the_newest_in_flight_job_is_the_one_kept(
        self, client, monkeypatch
    ) -> None:
        _stub_super_jobs(
            monkeypatch,
            [
                _job(10, AirbyteJobStatusType.RUNNING),
                _job(12, AirbyteJobStatusType.PENDING),
                _job(11, AirbyteJobStatusType.RUNNING),
            ],
        )
        kept = client.get_jobs_for_connection(connection_id="c")
        assert [job.id for job in kept] == [12]

    def test_terminal_jobs_are_passed_through_untouched(
        self, client, monkeypatch
    ) -> None:
        """Only the in-flight set is collapsed; history is not rewritten."""
        _stub_super_jobs(
            monkeypatch,
            [
                _job(1, AirbyteJobStatusType.SUCCEEDED),
                _job(2, AirbyteJobStatusType.FAILED),
                _job(3, AirbyteJobStatusType.RUNNING),
                _job(4, AirbyteJobStatusType.RUNNING),
                _job(5, AirbyteJobStatusType.CANCELLED),
            ],
        )
        kept = client.get_jobs_for_connection(connection_id="c")
        assert [job.id for job in kept] == [1, 2, 4, 5]

    def test_incomplete_counts_as_in_flight(self, client, monkeypatch) -> None:
        """sync_and_poll treats INCOMPLETE as in-flight, so this must agree.

        Disagreeing would leave two jobs in the set sync_and_poll counts and
        reproduce the failure this exists to remove.
        """
        _stub_super_jobs(
            monkeypatch,
            [
                _job(7, AirbyteJobStatusType.INCOMPLETE),
                _job(8, AirbyteJobStatusType.RUNNING),
            ],
        )
        kept = client.get_jobs_for_connection(connection_id="c")
        assert [job.id for job in kept] == [8]

    def test_a_single_in_flight_job_is_left_alone(self, client, monkeypatch) -> None:
        jobs = [
            _job(1, AirbyteJobStatusType.SUCCEEDED),
            _job(2, AirbyteJobStatusType.RUNNING),
        ]
        _stub_super_jobs(monkeypatch, jobs)
        assert client.get_jobs_for_connection(connection_id="c") == jobs

    def test_no_in_flight_job_is_left_alone(self, client, monkeypatch) -> None:
        jobs = [_job(1, AirbyteJobStatusType.SUCCEEDED)]
        _stub_super_jobs(monkeypatch, jobs)
        assert client.get_jobs_for_connection(connection_id="c") == jobs


class TestJobListing:
    """get_jobs_for_connection over HTTP pages, as sync_and_poll calls it."""

    CREATED_AFTER = datetime(2026, 10, 5, 0, 0, 0)  # noqa: DTZ001

    @pytest.fixture
    def requests_made(self, monkeypatch) -> list[tuple[str, dict[str, Any]]]:
        """Serve two pages that both hold in-flight job 8."""
        made: list[tuple[str, dict[str, Any]]] = []
        pages = [
            {
                "data": [
                    {"jobId": 8, "status": "running", "jobType": "sync"},
                    {"jobId": 7, "status": "succeeded", "jobType": "sync"},
                ],
                # A self-hosted server names localhost here and keeps only the
                # connection and the paging of the query it was sent.
                "next": "http://localhost:8006/api/public/v1/jobs?connectionId=c&limit=50&offset=50",
            },
            {
                "data": [
                    {"jobId": 8, "status": "running", "jobType": "sync"},
                    {"jobId": 6, "status": "succeeded", "jobType": "sync"},
                ]
            },
        ]

        def single_request(self, url, params, **_):  # noqa: ARG001
            made.append((url, dict(params)))
            return pages[len(made) - 1]

        monkeypatch.setattr(AirbyteOSSClient, "_single_request", single_request)
        return made

    def test_every_page_keeps_the_filter_and_our_host(
        self, client, requests_made
    ) -> None:
        """Following `next` verbatim dropped createdAtStart after page 1.

        Every sync start then read the connection's whole job history, 100 jobs
        a request, instead of the two days sync_and_poll asks for.
        """
        client.get_jobs_for_connection("c", created_after=self.CREATED_AFTER)

        url = "https://airbyte.example.invalid/api/public/v1/jobs"
        query = {
            "workspaceIds": "workspace-1",
            "connectionId": "c",
            "createdAtStart": "2026-10-05T00:00:00Z",
        }
        assert requests_made == [
            (url, {"limit": 50, **query}),
            (url, {"limit": "50", "offset": "50", **query}),
        ]

    @pytest.mark.usefixtures("requests_made")
    def test_a_job_repeated_across_pages_is_counted_once(self, client) -> None:
        """Two copies of one in-flight id read as "Found multiple running jobs"."""
        jobs = client.get_jobs_for_connection("c", created_after=self.CREATED_AFTER)
        assert [job.id for job in jobs] == [8, 7, 6]


class _Response:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= HTTPStatus.BAD_REQUEST:
            msg = f"{self.status_code} Error"
            raise HTTPError(msg, response=self)  # type: ignore[arg-type]

    def json(self) -> dict[str, Any]:
        return {"jobId": 1}


class TestRequestRetries:
    @pytest.fixture
    def responses(self, monkeypatch) -> list[_Response | Exception]:
        """Answer each request with the next queued response, recording sleeps."""
        queued: list[_Response | Exception] = []
        self.slept: list[float] = []
        self.sent = 0

        class Session:
            def request(_self, **_):  # noqa: N805
                self.sent += 1
                answer = queued.pop(0)
                if isinstance(answer, Exception):
                    raise answer
                return answer

        monkeypatch.setattr(
            AirbyteOSSClient, "_get_session", lambda *_, **__: Session()
        )
        monkeypatch.setattr(airbyte_resource.time, "sleep", self.slept.append)
        return queued

    def test_a_502_is_retried_with_a_doubling_delay(self, client, responses) -> None:
        responses.extend(
            [_Response(502), RequestsConnectionError(), _Response(503), _Response(200)]
        )
        assert client._single_request("POST", "https://a/jobs") == {"jobId": 1}
        assert self.slept == [1.5, 3.0, 6.0]

    def test_retries_stop_at_the_configured_count(self, client, responses) -> None:
        responses.extend([_Response(502)] * 8)
        with pytest.raises(Failure, match=r"Max retries \(7\) exceeded"):
            client._single_request("GET", "https://a/jobs")
        assert self.sent == 8
        assert len(self.slept) == 7

    def test_a_409_fails_at_once_with_airbytes_reason(self, client, responses) -> None:
        """Retrying a conflict cannot clear it, and the body names the cause."""
        responses.append(_Response(409, "A sync is already running for: c"))
        with pytest.raises(Failure, match=r"409 .* A sync is already running"):
            client._single_request("POST", "https://a/jobs")
        assert self.sent == 1
        assert self.slept == []

    def test_a_long_refusal_body_is_cut(self, client, responses) -> None:
        responses.append(_Response(404, "x" * 5000))
        with pytest.raises(Failure) as raised:
            client._single_request("GET", "https://a/jobs")
        assert len(raised.value.description or "") < 1000

    def test_a_status_outside_the_standard_set_is_retried(
        self, client, responses
    ) -> None:
        # A load balancer's 561 or a CDN's 520 has no HTTPStatus member.
        responses.extend([_Response(561), _Response(520), _Response(200)])
        assert client._single_request("GET", "https://a/jobs") == {"jobId": 1}
        assert self.sent == 3

    @pytest.mark.parametrize("status", [408, 429])
    def test_a_4xx_that_invites_a_retry_gets_one(
        self, client, responses, status
    ) -> None:
        responses.extend([_Response(status), _Response(200)])
        assert client._single_request("GET", "https://a/jobs") == {"jobId": 1}
        assert self.sent == 2


class TestOverlappingPages:
    """Airbyte offset paging can repeat one record and skip another."""

    OVERLAPPED = (
        # Same row count as the clean read, one connection in it twice: what
        # production returned on 2026-10-01, at 39 rows and 29 distinct ids.
        {"connectionId": "conn-1"},
        {"connectionId": "conn-1"},
    )
    CLEAN = (
        {"connectionId": "conn-1", "status": "active"},
        {"connectionId": "conn-2", "status": "active"},
    )

    @pytest.fixture
    def listings(self, monkeypatch) -> list[tuple[dict[str, Any], ...]]:
        """Serve the appended listings in order, repeating the last one."""
        served: list[tuple[dict[str, Any], ...]] = []
        self.reads = 0

        def paginated_request(_self, **request):
            self.request = request
            self.reads += 1
            return list(served[min(self.reads, len(served)) - 1])

        monkeypatch.setattr(AirbyteOSSClient, "_paginated_request", paginated_request)
        return served

    def test_a_stable_listing_is_read_twice(self, client, listings) -> None:
        listings.append(self.CLEAN)
        rows = client.list_collection("connections", "connectionId")

        assert self.reads == 2
        assert [row["connectionId"] for row in rows] == ["conn-1", "conn-2"]
        assert self.request == {
            "method": "GET",
            "url": "https://airbyte.example.invalid/api/public/v1/connections",
            "params": {"workspaceIds": "workspace-1"},
        }

    def test_an_overlapping_listing_is_read_again(self, client, listings) -> None:
        listings.extend([self.OVERLAPPED, self.CLEAN])
        client.list_collection("connections", "connectionId")
        assert self.reads == 3

    def test_a_skip_without_a_duplicate_is_caught_by_the_next_read(
        self, client, listings
    ) -> None:
        # A deletion between page fetches shifts the rest back a row, so one
        # record is skipped and nothing repeats. Only a second read shows it.
        listings.extend([self.CLEAN[:1], self.CLEAN])
        assert len(client.list_collection("connections", "connectionId")) == 2

    def test_a_listing_that_keeps_overlapping_is_refused(
        self, client, listings
    ) -> None:
        listings.append(self.OVERLAPPED)
        with pytest.raises(Failure, match="/connections listing did not return"):
            client.list_collection("connections", "connectionId")
        assert self.reads == LISTING_ATTEMPTS

    def test_the_asset_load_lists_connections_the_same_way(
        self, client, listings
    ) -> None:
        # build_airbyte_assets_definitions reads the workspace through
        # get_connections, so a connection skipped here has no assets.
        listings.extend([self.OVERLAPPED, self.CLEAN])
        rows = client.get_connections()
        assert [row["connectionId"] for row in rows] == ["conn-1", "conn-2"]

    @pytest.mark.parametrize("status", ["inactive", "deprecated"])
    def test_the_asset_load_leaves_out_a_connection_airbyte_will_not_sync(
        self, client, listings, status
    ) -> None:
        listings.append((*self.CLEAN, {"connectionId": "conn-3", "status": status}))
        rows = client.get_connections()
        assert [row["connectionId"] for row in rows] == ["conn-1", "conn-2"]

    def test_the_drift_check_listing_keeps_inactive_connections(
        self, client, listings
    ) -> None:
        listings.append((*self.CLEAN, {"connectionId": "conn-3", "status": "inactive"}))
        rows = client.list_collection("connections", "connectionId")
        assert len(rows) == len(self.CLEAN) + 1
