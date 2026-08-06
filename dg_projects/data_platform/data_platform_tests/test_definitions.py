"""Unit tests for the platform run failure notification sensor."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from data_platform.definitions import (
    MAX_RETRIES_TAG,
    RETRY_NUMBER_TAG,
    WILL_RETRY_TAG,
    asset_check_failure_message,
    defs,
    error_message,
    get_exception,
    truncate_text,
    will_be_retried,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _run(**tags: str) -> Any:
    return SimpleNamespace(
        tags=tags,
        run_id="0123abcd-4567-89ef-0123-456789abcdef",
        job_name="a_job",
    )


def _instance(*, enabled: bool = True, max_retries: int = 3) -> Any:
    return SimpleNamespace(
        run_retries_enabled=enabled,
        run_retries_max_retries=max_retries,
    )


def _context(step_failure_events: list[Any], failure_message: str = "") -> Any:
    return SimpleNamespace(
        dagster_run=_run(),
        get_step_failure_events=lambda: step_failure_events,
        failure_event=SimpleNamespace(message=failure_message),
    )


def _step_failure(step_key: str, error_text: str) -> Any:
    return SimpleNamespace(
        step_key=step_key,
        event_specific_data=SimpleNamespace(
            error=SimpleNamespace(to_string=lambda: error_text)
        ),
    )


def _block_text(blocks: list[dict[str, Any]]) -> str:
    return "\n".join(
        block.get("text", {}).get("text", "")
        for block in blocks
        if isinstance(block.get("text"), dict)
    )


# ── Retry suppression ─────────────────────────────────────────────────────────


def test_will_be_retried_trusts_the_daemon_tag() -> None:
    assert will_be_retried(_run(**{WILL_RETRY_TAG: "true"}), _instance()) is True
    assert will_be_retried(_run(**{WILL_RETRY_TAG: "false"}), _instance()) is False


def test_will_be_retried_falls_back_when_tag_not_yet_written() -> None:
    """The auto-reexecution daemon can lag this sensor's tick."""
    assert will_be_retried(_run(), _instance(max_retries=3)) is True
    assert (
        will_be_retried(_run(**{RETRY_NUMBER_TAG: "3"}), _instance(max_retries=3))
        is False
    )


def test_will_be_retried_is_false_when_retries_disabled() -> None:
    assert will_be_retried(_run(), _instance(enabled=False)) is False


def test_will_be_retried_prefers_per_run_max_retries_tag() -> None:
    run = _run(**{RETRY_NUMBER_TAG: "1", MAX_RETRIES_TAG: "1"})

    assert will_be_retried(run, _instance(max_retries=99)) is False


# ── Message formatting ────────────────────────────────────────────────────────


def test_error_message_falls_back_when_no_step_failures() -> None:
    """OOM kills and run-monitoring reaps produce no step failure events."""
    context = _context([], failure_message="Run worker terminated unexpectedly")

    blocks = error_message(context)

    text = _block_text(blocks)
    assert "No step failure recorded" in text
    assert "Run worker terminated unexpectedly" in text


def test_error_message_includes_step_detail_when_present() -> None:
    context = _context([_step_failure("some_model", "boom")])

    text = _block_text(error_message(context))

    assert "some_model" in text
    assert "boom" in text


def test_error_message_includes_sentry_event_id_when_given() -> None:
    context = _context([_step_failure("some_model", "boom")])

    blocks = error_message(context, sentry_event_id="cafebabe")

    context_blocks = [b for b in blocks if b["type"] == "context"]
    assert "cafebabe" in context_blocks[0]["elements"][0]["text"]


def test_error_message_omits_sentry_block_when_sentry_disabled() -> None:
    context = _context([_step_failure("some_model", "boom")])

    blocks = error_message(context, sentry_event_id=None)

    assert not [b for b in blocks if b["type"] == "context"]


def test_error_message_always_links_back_to_the_run() -> None:
    blocks = error_message(_context([]))

    actions = [b for b in blocks if b["type"] == "actions"]
    assert actions, "message carries no link back to Dagster"
    assert "/runs/" in actions[0]["elements"][0]["url"]


# ── dbt error extraction ──────────────────────────────────────────────────────


def test_get_exception_extracts_dbt_errors() -> None:
    text = (
        "dagster_dbt.errors.DagsterDbtCliRuntimeError: oh no\n"
        "Errors parsed from dbt logs:\n\n"
        "Model my_model failed to compile\n\nStack Trace:\nirrelevant"
    )

    result = get_exception(text)

    assert "*DBT Error:*" in result
    assert "Model my_model failed to compile" in result
    assert "irrelevant" not in result


def test_get_exception_formats_generic_errors() -> None:
    result = get_exception("ValueError: nope\n\nStack Trace:\nirrelevant")

    assert "*Error:*" in result
    assert "ValueError: nope" in result
    assert "irrelevant" not in result


@pytest.mark.parametrize(("length", "expected"), [(10, 10), (5000, 3000)])
def test_truncate_text_bounds_slack_payloads(length: int, expected: int) -> None:
    assert len(truncate_text("x" * length)) == expected


# ── Definitions ───────────────────────────────────────────────────────────────


def test_sensors_are_defined_without_vault_or_sentry() -> None:
    """Regression: the sensor used to vanish if Vault was unreachable at import."""
    assert sorted(sensor.name for sensor in defs.sensors) == [
        "asset_check_failure_sensor",
        "run_failure_notification_sensor",
    ]


# ── Asset check alerting ──────────────────────────────────────────────────────


def test_asset_check_failure_message_lists_each_failed_check() -> None:
    evaluations = [
        SimpleNamespace(
            asset_key=SimpleNamespace(to_user_string=lambda: "mart/enrollments"),
            check_name="freshness_check",
        ),
        SimpleNamespace(
            asset_key=SimpleNamespace(to_user_string=lambda: "reporting/revenue"),
            check_name="freshness_check",
        ),
    ]

    blocks = asset_check_failure_message(evaluations)

    text = _block_text(blocks)
    assert "mart/enrollments" in text
    assert "reporting/revenue" in text
    assert "(2)" in blocks[0]["text"]["text"]
