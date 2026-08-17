"""Unit tests for ol_orchestrate.lib.sentry."""

from __future__ import annotations

from typing import Any

import pytest
import sentry_sdk
from dagster import AssetSpec, Definitions, asset, materialize
from ol_orchestrate.lib import sentry as sentry_lib
from ol_orchestrate.lib.constants import DAGSTER_ENV
from sentry_sdk.transport import Transport

# ── Helpers ───────────────────────────────────────────────────────────────────


class RecordingTransport(Transport):
    """Stands in for the Sentry HTTP transport and keeps every event."""

    def __init__(self) -> None:
        super().__init__()
        self.envelopes: list[Any] = []

    def capture_envelope(self, envelope: Any) -> None:
        self.envelopes.append(envelope)

    def events(self) -> list[dict[str, Any]]:
        return [
            item.payload.json
            for envelope in self.envelopes
            for item in envelope.items
            if item.headers.get("type") == "event"
        ]


@pytest.fixture
def recorded_events(monkeypatch: pytest.MonkeyPatch) -> RecordingTransport:
    """Initialize Sentry against a recording transport for one test."""
    transport = RecordingTransport()
    # init_sentry short-circuits once it has run, and pytest shares a process
    # across tests, so reset the guard to get a real init here.
    monkeypatch.setattr(sentry_lib, "_initialized_location", None)
    monkeypatch.setenv("SENTRY_DSN", "https://key@example.ingest.sentry.io/1")
    monkeypatch.setenv("SENTRY_RELEASE", "test-release")
    sentry_sdk.init(
        dsn="https://key@example.ingest.sentry.io/1",
        transport=transport,
        release="test-release",
    )
    return transport


# ── with_sentry_hooks ─────────────────────────────────────────────────────────


def test_with_sentry_hooks_attaches_hook_to_assets_definition() -> None:
    @asset
    def some_asset() -> int:
        return 1

    (hooked,) = sentry_lib.with_sentry_hooks([some_asset])

    assert {hook.name for hook in hooked.hook_defs} == {"capture_exception_to_sentry"}


def test_with_sentry_hooks_passes_through_non_assets_definitions() -> None:
    """AssetSpec has no ops to hook and must survive untouched."""
    spec = AssetSpec("an_external_asset")

    (passed_through,) = sentry_lib.with_sentry_hooks([spec])

    assert passed_through is spec


def test_with_sentry_hooks_preserves_order_of_mixed_input() -> None:
    @asset
    def first() -> int:
        return 1

    spec = AssetSpec("second")

    result = sentry_lib.with_sentry_hooks([first, spec])

    assert len(result) == 2
    assert result[1] is spec


# ── init_sentry ───────────────────────────────────────────────────────────────


def test_init_sentry_without_dsn_is_a_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local dagster dev and test collection run with no DSN set."""
    monkeypatch.setattr(sentry_lib, "_initialized_location", None)
    monkeypatch.delenv("SENTRY_DSN", raising=False)

    assert sentry_lib.init_sentry("some_location") is False


def test_init_sentry_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sentry_lib, "_initialized_location", None)
    monkeypatch.setenv("SENTRY_DSN", "https://key@example.ingest.sentry.io/1")

    assert sentry_lib.init_sentry("some_location") is True
    assert sentry_lib.init_sentry("some_location") is True


def test_init_sentry_retags_when_a_different_location_initializes(
    monkeypatch: pytest.MonkeyPatch,
    recorded_events: RecordingTransport,
) -> None:
    """Importing two code locations into one process must not mis-tag events.

    Each location gets its own process in the deployment, so this only arises
    locally and in tests -- but silently keeping the first location's name was
    misleading in exactly those places.
    """
    # The fixture has already built a client around the recording transport.
    # Pretend the process was initialized for first_location so init_sentry
    # takes the re-tag path rather than rebuilding that client away.
    monkeypatch.setattr(sentry_lib, "_initialized_location", "first_location")

    assert sentry_lib.init_sentry("second_location") is True

    sentry_sdk.capture_message("something broke")

    event = recorded_events.events()[-1]
    assert event["tags"]["dagster_code_location"] == "second_location"


# ── capture_exception_to_sentry ───────────────────────────────────────────────


def test_failing_asset_reports_real_exception_to_sentry(
    recorded_events: RecordingTransport,
) -> None:
    """The whole point: a failed step yields a Sentry event with real frames."""

    @asset
    def exploding_asset() -> None:
        message = "the warehouse is on fire"
        raise ValueError(message)

    defs = Definitions(assets=sentry_lib.with_sentry_hooks([exploding_asset]))
    job = defs.resolve_implicit_global_asset_job_def()

    result = job.execute_in_process(raise_on_error=False)
    assert not result.success

    events = recorded_events.events()
    assert len(events) == 1, f"expected exactly one Sentry event, got {len(events)}"

    event = events[0]
    exception = event["exception"]["values"][0]
    assert exception["type"] == "ValueError"
    assert exception["value"] == "the warehouse is on fire"
    # A real stack trace, not a string that was pasted into the message.
    assert exception["stacktrace"]["frames"], "event carries no stack frames"

    assert event["tags"]["captured_by"] == "hook"
    assert event["tags"]["dagster_step"] == "exploding_asset"
    assert event["fingerprint"] == [
        DAGSTER_ENV,
        # No init_sentry() in this fixture, so no location has been recorded.
        "unknown",
        "exploding_asset",
        "ValueError",
    ]
    assert job.name not in event["fingerprint"], (
        "job_name describes the launch path, not the defect"
    )


def test_qa_and_production_do_not_share_an_issue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One Sentry project holds both, so the environment has to group them apart.

    Without it a QA-only defect and a production outage merge into one issue and
    the list cannot tell you which you are looking at.
    """
    # Both environments set explicitly. Letting the first one inherit the
    # process's DAGSTER_ENV makes the test a tautology when that is `ci` or
    # `dev`, and an outright failure when it is `qa`.
    monkeypatch.setattr(sentry_lib, "DAGSTER_ENV", "production")
    production = sentry_lib.failure_fingerprint("lakehouse", "dbt_build", "Failure")
    monkeypatch.setattr(sentry_lib, "DAGSTER_ENV", "qa")
    qa = sentry_lib.failure_fingerprint("lakehouse", "dbt_build", "Failure")

    assert production[0] == "production"
    assert qa[0] == "qa"
    assert production != qa


def test_a_terminated_run_worker_is_not_reported() -> None:
    """SIGTERM during a deploy or an eviction is not a defect (DAGSTER-1R/1S/1T)."""
    event = {"exception": {"values": [{"type": "DagsterExecutionInterruptedError"}]}}

    assert sentry_lib.drop_interruptions(event, {}) is None


def test_a_real_exception_still_gets_through() -> None:
    event = {"exception": {"values": [{"type": "ValueError"}]}}

    assert sentry_lib.drop_interruptions(event, {}) is event


def test_successful_asset_reports_nothing(
    recorded_events: RecordingTransport,
) -> None:
    @asset
    def calm_asset() -> int:
        return 1

    result = materialize(sentry_lib.with_sentry_hooks([calm_asset]))

    assert result.success
    assert recorded_events.events() == []
