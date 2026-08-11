"""Unit tests for the platform run failure notification sensor."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from dagster import AssetCheckSeverity, RetryPolicy
from data_platform.definitions import (
    MAX_CHECK_EVALUATIONS_PER_TICK,
    RETRY_NUMBER_TAG,
    asset_check_failure_message,
    collect_new_check_failures,
    dagster_url,
    defs,
    error_message,
    get_exception,
    is_reported_by_the_run_failure_sensor,
    is_retry_of_a_reported_failure,
    sentry_fingerprint,
    truncate_text,
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


def _context(step_failure_events: list[Any], failure_message: str = "") -> Any:
    return SimpleNamespace(
        dagster_run=_run(),
        get_step_failure_events=lambda: step_failure_events,
        failure_event=SimpleNamespace(message=failure_message),
    )


def _step_failure(
    step_key: str,
    error_text: str,
    cls_name: str = "ValueError",
    cause: Any = None,
) -> Any:
    return SimpleNamespace(
        step_key=step_key,
        event_specific_data=SimpleNamespace(
            error=SimpleNamespace(
                to_string=lambda: error_text, cls_name=cls_name, cause=cause
            )
        ),
    )


def _block_text(blocks: list[dict[str, Any]]) -> str:
    return "\n".join(
        block.get("text", {}).get("text", "")
        for block in blocks
        if isinstance(block.get("text"), dict)
    )


# ── Retry suppression ─────────────────────────────────────────────────────────


def test_the_first_attempt_of_a_failure_is_reported() -> None:
    """Regression: suppressing every attempt Dagster might retry meant a
    transient failure that passed on the next attempt was never reported.

    An Iceberg split on a __dbt_tmp path or a staging table that vanished
    mid-test clears on rerun, so each of those runs had all of its failed
    attempts suppressed and the only surviving alert was the asset check
    message, which carries no error text.
    """
    assert is_retry_of_a_reported_failure(_run()) is False


@pytest.mark.parametrize("retry_number", ["1", "2", "3"])
def test_automatic_retries_of_a_reported_failure_are_suppressed(
    retry_number: str,
) -> None:
    """One alert per broken run, not one per attempt: under the deployment's
    run_retries.max_retries of 3 a persistent failure would otherwise announce
    itself four times.
    """
    run = _run(**{RETRY_NUMBER_TAG: retry_number})

    assert is_retry_of_a_reported_failure(run) is True


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


# ── Sentry fingerprinting ─────────────────────────────────────────────────────


def test_sensor_fingerprint_matches_the_in_process_hook() -> None:
    """The two reporting paths must group into one Sentry issue, not two.

    ol_orchestrate.lib.sentry's hook uses
    ``[job_name, step_key, type(exception).__name__]``; a mismatch here raises a
    duplicate issue for every single failure.
    """
    context = _context([_step_failure("some_model", "boom", cls_name="ValueError")])

    assert sentry_fingerprint(context) == ["a_job", "some_model", "ValueError"]


def test_sensor_fingerprint_groups_process_deaths_separately() -> None:
    """No step failure means the hook never ran, so there is nothing to match."""
    context = _context([])

    assert sentry_fingerprint(context) == ["a_job", "run", "run_failure"]


def test_sensor_fingerprint_survives_a_missing_error_payload() -> None:
    failure = SimpleNamespace(
        step_key="some_model", event_specific_data=SimpleNamespace()
    )
    context = _context([failure])

    assert sentry_fingerprint(context) == ["a_job", "some_model", "run_failure"]


def test_sensor_fingerprint_unwraps_dagsters_user_code_wrapper() -> None:
    """The serialized error is Dagster's wrapper, not what user code raised.

    A step raising FileNotFoundError serializes with
    ``cls_name="DagsterExecutionStepExecutionError"`` and the real exception one
    level down in ``cause``. Fingerprinting the wrapper never matched the hook's
    ``type(exception).__name__``, so every step failure raised two Sentry issues
    instead of one -- DAGSTER-3 from the hook and DAGSTER-5 from this sensor,
    for the identical failures.
    """
    context = _context(
        [
            _step_failure(
                "extract_edxorg_courserun_metadata",
                "boom",
                cls_name="DagsterExecutionStepExecutionError",
                cause=SimpleNamespace(cls_name="FileNotFoundError", cause=None),
            )
        ]
    )

    assert sentry_fingerprint(context) == [
        "a_job",
        "extract_edxorg_courserun_metadata",
        "FileNotFoundError",
    ]


def test_sensor_fingerprint_unwraps_only_the_dagster_layer() -> None:
    """Unwrapping stops at the first non-Dagster class.

    The hook captures the exception user code raised, not whatever that was
    chained from, so descending the whole cause chain would miss it in the
    other direction.
    """
    context = _context(
        [
            _step_failure(
                "some_model",
                "boom",
                cls_name="DagsterExecutionLoadInputError",
                cause=SimpleNamespace(
                    cls_name="FileNotFoundError",
                    cause=SimpleNamespace(cls_name="NoSuchKey", cause=None),
                ),
            )
        ]
    )

    assert sentry_fingerprint(context)[2] == "FileNotFoundError"


def test_sensor_fingerprint_keeps_a_wrapper_with_no_cause() -> None:
    """A wrapper that serialized without a cause still names something."""
    context = _context(
        [
            _step_failure(
                "some_model", "boom", cls_name="DagsterExecutionStepExecutionError"
            )
        ]
    )

    assert sentry_fingerprint(context)[2] == "DagsterExecutionStepExecutionError"


@pytest.mark.parametrize(
    "retry_policy", [None, RetryPolicy(max_retries=1)], ids=["plain", "retried"]
)
def test_sensor_fingerprint_matches_a_real_dagster_step_failure(
    retry_policy: RetryPolicy | None,
) -> None:
    """End-to-end: the sensor and the hook must agree on a real failure.

    The hand-built fixtures above are only as good as our model of Dagster's
    serialization -- and the original bug survived precisely because the old
    fixture set cls_name to the user exception directly, which is not what
    Dagster produces. This executes a real job and compares the sensor's
    fingerprint against what the *actual* hook records, rather than against a
    hardcoded string, so neither side can drift without failing here.

    The ``retried`` case covers an op whose RetryPolicy is exhausted, which
    Dagster serializes as ``RetryRequestedFromPolicy -> <user exception>``.
    Several learning_resources assets carry a RetryPolicy, so that wrapper is
    on a live path.
    """
    from dagster import (  # noqa: PLC0415
        DagsterEventType,
        HookContext,
        failure_hook,
        job,
        op,
    )

    hook_recorded: dict[str, str] = {}

    @failure_hook(name="record")
    def record(context: HookContext) -> None:
        # Mirrors ol_orchestrate.lib.sentry.capture_exception_to_sentry.
        hook_recorded[context.step_key] = type(context.op_exception).__name__

    @op(retry_policy=retry_policy)
    def raises_file_not_found():
        msg = "The specified key does not exist."
        raise FileNotFoundError(msg)

    @job(hooks={record})
    def a_job():
        raises_file_not_found()

    result = a_job.execute_in_process(raise_on_error=False)
    step_failures = [
        event
        for event in result.all_events
        if event.event_type == DagsterEventType.STEP_FAILURE
    ]
    fingerprint = sentry_fingerprint(_context(step_failures))

    assert fingerprint[2] == hook_recorded["raises_file_not_found"]
    assert fingerprint == ["a_job", "raises_file_not_found", "FileNotFoundError"]


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


def test_get_exception_counts_the_failed_dbt_nodes() -> None:
    text = (
        "dagster_dbt.errors.DagsterDbtCliRuntimeError: oh no\n"
        "Errors parsed from dbt logs:\n\n"
        "46 of 65 ERROR accepted_values_gender  [ERROR in 260.14s]\n\n"
        "59 of 65 ERROR unique_program_enrollment  [ERROR in 262.23s]\n"
    )

    assert "*DBT Error* (2 failed):" in get_exception(text)


def test_the_failed_count_reflects_nodes_truncation_drops() -> None:
    """The count is what tells you a truncated list is not the whole story."""
    text = (
        "dagster_dbt.errors.DagsterDbtCliRuntimeError: oh no\n"
        "Errors parsed from dbt logs:\n\n"
        + "".join(
            f"{n} of 99 ERROR test_{n}  [ERROR in 1s]\n" + "x" * 200 + "\n"
            for n in range(1, 41)
        )
    )

    result = get_exception(text)

    assert "*DBT Error* (40 failed):" in result
    assert result.endswith("...```")


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
    *,
    asset: str,
    check: str = "freshness_check",
    passed: bool,
    severity: Any,
    metadata: dict[str, Any] | None = None,
) -> Any:
    return SimpleNamespace(
        asset_key=SimpleNamespace(to_user_string=lambda: asset),
        check_name=check,
        passed=passed,
        severity=severity,
        metadata=metadata or {},
    )


def _record(storage_id: int, evaluation: Any, run_id: str = "a-run-id") -> Any:
    return SimpleNamespace(
        storage_id=storage_id,
        event_log_entry=SimpleNamespace(
            run_id=run_id,
            dagster_event=SimpleNamespace(event_specific_data=evaluation),
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

    assert [e.asset_key.to_user_string() for _, e in failures] == ["a"]


def test_collect_pairs_each_failure_with_its_run_id() -> None:
    """The run_id must survive alongside the evaluation for Slack to link to it."""
    captured: dict[str, Any] = {}
    records = [
        _record(1, _evaluation(asset="a", passed=False, severity=ERROR), run_id="r1"),
        _record(2, _evaluation(asset="b", passed=False, severity=ERROR), run_id="r2"),
    ]

    failures, _ = collect_new_check_failures(
        _instance_returning(records, captured), None
    )

    assert [run_id for run_id, _ in failures] == ["r1", "r2"]


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


def test_a_full_batch_of_failures_stays_within_slacks_block_limit() -> None:
    """Regression: Slack rejects chat.postMessage payloads over 50 blocks.

    asset_check_failure_message adds one header block on top of one detail
    block per failure, so a full MAX_CHECK_EVALUATIONS_PER_TICK-sized batch
    of failures must leave room for that header.
    """
    failures = [
        _check_failure(f"run-{i}", f"asset_{i}")
        for i in range(MAX_CHECK_EVALUATIONS_PER_TICK)
    ]

    blocks = asset_check_failure_message(failures)

    assert len(blocks) <= 50


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


def _check_failure(
    run_id: str,
    asset: str,
    metadata: dict[str, Any] | None = None,
    check_name: str = "freshness_check",
) -> tuple[str, Any]:
    return (
        run_id,
        SimpleNamespace(
            asset_key=SimpleNamespace(to_user_string=lambda: asset),
            check_name=check_name,
            metadata=metadata or {},
        ),
    )


@pytest.mark.parametrize("status", ["error", "fail"])
def test_dbt_test_failures_are_left_to_the_run_failure_sensor(status: str) -> None:
    """Regression: a failing dbt test was announced twice, and the second
    message named the check without the cause.

    An accepted_values check on a gender column read as invalid production data
    when the test had errored on ICEBERG_CANNOT_OPEN_SPLIT before evaluating
    anything. dbt build exits non-zero on either status, so the run fails and
    the run failure sensor reports it with dbt's own error text.
    """
    evaluation = _check_failure("run-1", "mart/x", {"status": status})[1]

    assert is_reported_by_the_run_failure_sensor(evaluation) is True


def test_freshness_checks_are_still_reported_here() -> None:
    """These have no run behind them at all, so nothing else will report them.

    Their passed=False genuinely means the asset is stale, and dagster-dbt's
    node status -- the marker for a dbt-sourced check -- is absent.
    """
    evaluation = _check_failure("run-1", "mart/enrollments")[1]

    assert is_reported_by_the_run_failure_sensor(evaluation) is False


def test_a_check_with_no_metadata_at_all_is_reported_here() -> None:
    """A native Dagster asset check need not attach metadata, and a failing one
    does not fail its run the way a dbt test does.
    """
    evaluation = SimpleNamespace(metadata=None)

    assert is_reported_by_the_run_failure_sensor(evaluation) is False


def test_collect_drops_dbt_tests_but_keeps_freshness_checks() -> None:
    """The filter has to run inside the collector, not the formatter, or the
    sensor posts an empty message for a batch of nothing but dbt tests.
    """
    records = [
        _record(1, _evaluation(asset="mart/x", passed=False, severity=ERROR)),
        _record(
            2,
            _evaluation(
                asset="mart/y",
                passed=False,
                severity=ERROR,
                metadata={"status": "error"},
            ),
        ),
    ]

    failures, next_cursor = collect_new_check_failures(
        _instance_returning(records, {}), None
    )

    assert [e.asset_key.to_user_string() for _, e in failures] == ["mart/x"]
    # The cursor still advances past the dropped record, or it is rescanned.
    assert next_cursor == "2"


def test_asset_check_failure_message_lists_each_failed_check() -> None:
    failures = [
        _check_failure("run-1", "mart/enrollments"),
        _check_failure("run-2", "reporting/revenue"),
    ]

    blocks = asset_check_failure_message(failures)

    text = _block_text(blocks)
    assert "mart/enrollments" in text
    assert "reporting/revenue" in text
    assert "(2)" in blocks[0]["text"]["text"]


def test_asset_check_failure_message_links_each_check_to_its_own_run() -> None:
    """Regression: each check must link to the run that surfaced it, not the
    generic asset graph -- a batch of failures can span multiple runs.
    """
    failures = [
        _check_failure("run-1", "mart/enrollments"),
        _check_failure("run-2", "reporting/revenue"),
    ]

    blocks = asset_check_failure_message(failures)

    detail_blocks = [b for b in blocks if b["type"] == "section"]
    urls = [b["accessory"]["url"] for b in detail_blocks]
    assert urls == [
        f"{dagster_url}/runs/run-1",
        f"{dagster_url}/runs/run-2",
    ]
