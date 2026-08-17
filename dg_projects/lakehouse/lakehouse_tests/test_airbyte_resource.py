"""Tests for the Airbyte Community Edition workspace override.

AirbyteOSSWorkspace.get_client() re-implements the base class's constructor call
with an explicit argument list, which means any setting the base class grows --
or that we simply forgot -- is silently dropped rather than failing loudly. That
is not hypothetical: the four polling settings were missing, so configuring
poll_previous_running_sync on the workspace set a field the client never read.
"""

import pytest
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
