"""Unit tests for the platform run failure notification sensor."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from dagster import AssetCheckSeverity
from data_platform.definitions import (
    MAX_CHECK_EVALUATIONS_PER_TICK,
    MAX_RETRIES_TAG,
    RETRY_NUMBER_TAG,
    WILL_RETRY_TAG,
    asset_check_failure_message,
    collect_new_check_failures,
    defs,
    error_message,
    get_exception,
    truncate_text,
    will_be_retried,
)

ERROR = AssetCheckSeverity.ERROR
WARN = AssetCheckSeverity.WARN

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


def _evaluation(
    *, asset: str, check: str = "freshness_check", passed: bool, severity: Any
) -> Any:
    return SimpleNamespace(
        asset_key=SimpleNamespace(to_user_string=lambda: asset),
        check_name=check,
        passed=passed,
        severity=severity,
    )


def _record(storage_id: int, evaluation: Any) -> Any:
    return SimpleNamespace(
        storage_id=storage_id,
        event_log_entry=SimpleNamespace(
            dagster_event=SimpleNamespace(event_specific_data=evaluation)
        ),
    )


def _instance_returning(records: list[Any], captured: dict[str, Any]) -> Any:
    # limit/ascending are keyword-only here because the production call site
    # passes them by keyword; positional use would not match reality.
    def get_event_records(_filter: Any, *, limit: int, ascending: bool) -> list[Any]:
        captured["limit"] = limit
        captured["ascending"] = ascending
        return records

    return SimpleNamespace(get_event_records=get_event_records)


def test_collect_keeps_only_error_severity_failures() -> None:
    captured: dict[str, Any] = {}
    records = [
        _record(1, _evaluation(asset="a", passed=False, severity=ERROR)),
        _record(2, _evaluation(asset="b", passed=False, severity=WARN)),
        _record(3, _evaluation(asset="c", passed=True, severity=ERROR)),
    ]

    failures, _ = collect_new_check_failures(
        _instance_returning(records, captured), None
    )

    assert [f.asset_key.to_user_string() for f in failures] == ["a"]


def test_collect_reads_oldest_first_so_a_backlog_cannot_be_skipped() -> None:
    """Regression: a full batch must not strand the events older than it.

    Reading descending returns the *newest* batch above the cursor; advancing
    past it permanently drops every older event once a backlog exceeds
    MAX_CHECK_EVALUATIONS_PER_TICK.
    """
    captured: dict[str, Any] = {}
    backlog = [
        _record(
            storage_id,
            _evaluation(asset=f"asset_{storage_id}", passed=False, severity=ERROR),
        )
        for storage_id in range(1, MAX_CHECK_EVALUATIONS_PER_TICK + 1)
    ]

    failures, next_cursor = collect_new_check_failures(
        _instance_returning(backlog, captured), 0
    )

    assert captured["ascending"] is True, "descending order silently drops backlog"
    assert captured["limit"] == MAX_CHECK_EVALUATIONS_PER_TICK
    assert len(failures) == MAX_CHECK_EVALUATIONS_PER_TICK
    # Newest of the ascending batch: nothing older is stranded, nothing newer
    # is re-delivered.
    assert next_cursor == str(backlog[-1].storage_id)


def test_collect_advances_cursor_even_when_nothing_failed() -> None:
    """A batch of passing checks must not be re-scanned forever."""
    captured: dict[str, Any] = {}
    records = [_record(7, _evaluation(asset="a", passed=True, severity=ERROR))]

    failures, next_cursor = collect_new_check_failures(
        _instance_returning(records, captured), None
    )

    assert failures == []
    assert next_cursor == "7"


def test_collect_reports_no_cursor_when_there_is_nothing_new() -> None:
    failures, next_cursor = collect_new_check_failures(_instance_returning([], {}), 42)

    assert failures == []
    assert next_cursor is None


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
