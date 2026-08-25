"""Tests for the Airbyte Community Edition workspace override.

AirbyteOSSWorkspace.get_client() re-implements the base class's constructor call
with an explicit argument list, which means any setting the base class grows --
or that we simply forgot -- is silently dropped rather than failing loudly. That
is not hypothetical: the four polling settings were missing, so configuring
poll_previous_running_sync on the workspace set a field the client never read.
"""

import pytest
from dagster_airbyte.resources import AirbyteClient
from dagster_airbyte.translator import AirbyteJob, AirbyteJobStatusType
from lakehouse.resources.airbyte import AirbyteOSSWorkspace

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
    the condition (DAGSTER-2R, 3A, 3C, 3E, 3F, 3N, 3R, 33, 39).
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
