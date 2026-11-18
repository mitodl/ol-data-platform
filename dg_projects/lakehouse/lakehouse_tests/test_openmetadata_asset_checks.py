"""Tests for pushing Dagster asset check evaluations to OpenMetadata."""

import logging
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import httpx2 as httpx
import pytest
from dagster import AssetCheckEvaluation, AssetCheckSeverity, AssetKey
from dagster._core.definitions.freshness import FreshnessState, FreshnessStateChange
from dagster_dbt import DagsterDbtTranslator
from lakehouse.lib.openmetadata_asset_checks import (
    TEST_DEFINITION_NAME,
    push_check_evaluations,
    push_freshness_changes,
    reconcile_freshness_test_cases,
    table_fqns_by_asset_key,
)

DIM_USER = AssetKey(["dimensional", "dim_user"])
DIM_USER_FQN = (
    "Starburst Galaxy.ol_data_lake_production"
    ".ol_warehouse_production_dimensional.dim_user"
)
LOG = logging.getLogger(__name__)


class _NameTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        return AssetKey(
            [dbt_resource_props["config"]["schema"], dbt_resource_props["name"]]
        )


def _node(
    name: str, resource_type: str = "model", schema: str = "dimensional"
) -> dict[str, Any]:
    return {
        "name": name,
        "resource_type": resource_type,
        "config": {"schema": schema},
        "database": "ol_data_lake_production",
        "schema": f"ol_warehouse_production_{schema}",
        "alias": name,
    }


def test_table_fqns_cover_only_the_given_table_nodes() -> None:
    manifest = {
        "nodes": {
            "model.p.dim_user": _node("dim_user"),
            "test.p.not_null_dim_user": _node("not_null_dim_user", "test"),
            "seed.p.platforms": _node("platforms", "seed", "staging"),
            "model.p.b2b_cohort": _node("b2b_cohort", schema="b2b_analytics"),
        }
    }
    platforms = AssetKey(["staging", "platforms"])
    fqns = table_fqns_by_asset_key(
        manifest, _NameTranslator(), {DIM_USER, platforms}, "Starburst Galaxy"
    )
    assert fqns == {
        DIM_USER: DIM_USER_FQN,
        platforms: (
            "Starburst Galaxy.ol_data_lake_production"
            ".ol_warehouse_production_staging.platforms"
        ),
    }


class _FakeClient:
    """OpenMetadata where nothing exists yet; *fail* makes one call return a status."""

    def __init__(
        self, fail: tuple[str, str, int] | None = None, existing: set[str] | None = None
    ) -> None:
        self.fail = fail
        self.existing = existing or set()
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []

    def send(
        self, method: str, endpoint: str, body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        self.calls.append((method, endpoint, body))
        status = None
        if self.fail and (method, endpoint) == self.fail[:2]:
            status = self.fail[2]
        elif method == "GET" and endpoint not in self.existing:
            status = 404
        if status is not None:
            request = httpx.Request(method, f"https://om/api/{endpoint}")
            response = httpx.Response(status, request=request)
            msg = f"{status} from {endpoint}"
            raise httpx.HTTPStatusError(msg, request=request, response=response)
        return {}


TEST_DEFINITION_GET = f"v1/dataQuality/testDefinitions/name/{TEST_DEFINITION_NAME}"
TEST_CASE_ENCODED = (
    "Starburst%20Galaxy.ol_data_lake_production.ol_warehouse_production_dimensional"
    ".dim_user.dagster_freshness_check"
)
TEST_CASE_GET = f"v1/dataQuality/testCases/name/{TEST_CASE_ENCODED}"
RESULT_ENDPOINT = f"v1/dataQuality/testCases/testCaseResults/{TEST_CASE_ENCODED}"


def _record(
    check_name: str,
    *,
    storage_id: int = 1,
    passed: bool = True,
    asset_key: AssetKey = DIM_USER,
    metadata: dict[str, Any] | None = None,
    timestamp: float = 1_760_000_000.5,
) -> SimpleNamespace:
    evaluation = AssetCheckEvaluation(
        asset_key=asset_key,
        check_name=check_name,
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata=metadata or {},
        description="Asset is overdue." if not passed else None,
    )
    entry = SimpleNamespace(
        timestamp=timestamp,
        dagster_event=SimpleNamespace(event_specific_data=evaluation),
    )
    return SimpleNamespace(storage_id=storage_id, event_log_entry=entry)


def _push(
    records: Sequence[SimpleNamespace],
    client: _FakeClient,
    deadline: float = math.inf,
) -> tuple[int, int]:
    return push_check_evaluations(
        records, {DIM_USER: DIM_USER_FQN}, client, LOG, deadline
    )


def test_push_writes_results_and_skips_dbt_tests_and_unmapped_assets() -> None:
    client = _FakeClient()
    records = [
        _record("freshness_check", storage_id=1, passed=False),
        _record("freshness_check", storage_id=2, timestamp=1_760_003_600.0),
        # dagster-dbt's conversion of a dbt test: already in OM via run_results.json.
        _record(
            "not_null_dim_user_user_pk",
            storage_id=3,
            metadata={"unique_id": "test.x", "invocation_id": "i", "status": "pass"},
        ),
        _record(
            "freshness_check", storage_id=4, asset_key=AssetKey(["raw", "unmapped"])
        ),
    ]

    assert _push(records, client) == (2, 4)
    assert [(method, endpoint) for method, endpoint, _ in client.calls] == [
        ("GET", TEST_DEFINITION_GET),
        ("PUT", "v1/dataQuality/testDefinitions"),
        # The test case is looked up once per batch, not once per evaluation.
        ("GET", TEST_CASE_GET),
        ("POST", "v1/dataQuality/testCases"),
        ("POST", RESULT_ENDPOINT),
        ("POST", RESULT_ENDPOINT),
    ]
    test_case = client.calls[3][2]
    assert test_case is not None
    assert test_case["testDefinition"] == TEST_DEFINITION_NAME
    assert test_case["entityLink"] == f"<#E::table::{DIM_USER_FQN}>"
    failed, passed = client.calls[4][2], client.calls[5][2]
    assert failed is not None
    assert passed is not None
    assert failed["testCaseStatus"] == "Failed"
    assert failed["timestamp"] == 1_760_000_000_500
    assert failed["result"] == "Dagster check failed (ERROR).\nAsset is overdue."
    assert passed["testCaseStatus"] == "Success"


def test_existing_definition_and_test_case_are_not_rewritten() -> None:
    """So an edit made in OpenMetadata, e.g. disabling the definition, sticks."""
    client = _FakeClient(existing={TEST_DEFINITION_GET, TEST_CASE_GET})
    _push([_record("freshness_check")], client)
    assert [(method, endpoint) for method, endpoint, _ in client.calls] == [
        ("GET", TEST_DEFINITION_GET),
        ("GET", TEST_CASE_GET),
        ("POST", RESULT_ENDPOINT),
    ]


def test_result_already_written_counts_as_pushed() -> None:
    """A retried batch re-posts results OpenMetadata already has; it answers 409."""
    client = _FakeClient(fail=("POST", RESULT_ENDPOINT, 409))
    assert _push([_record("freshness_check")], client) == (1, 1)


def test_nothing_to_push_makes_no_calls() -> None:
    client = _FakeClient()
    records = [
        _record(
            "freshness_check", storage_id=7, asset_key=AssetKey(["raw", "unmapped"])
        )
    ]
    assert _push(records, client) == (0, 7)
    assert client.calls == []


def test_table_missing_from_openmetadata_is_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _FakeClient(fail=("POST", "v1/dataQuality/testCases", 404))
    with caplog.at_level(logging.WARNING):
        assert _push([_record("freshness_check")], client) == (0, 1)
    assert "OpenMetadata has no table" in caplog.text


def test_other_api_errors_raise() -> None:
    client = _FakeClient(fail=("POST", RESULT_ENDPOINT, 500))
    with pytest.raises(httpx.HTTPStatusError):
        _push([_record("freshness_check")], client)


def test_a_passed_deadline_leaves_the_rest_of_the_batch_for_the_next_tick() -> None:
    """Regression: a tick that runs past the sensor timeout advances no cursor.

    Each write takes OpenMetadata a second or more, so a batch with many
    pushable evaluations has to be spread over several ticks.
    """
    client = _FakeClient(existing={TEST_DEFINITION_GET, TEST_CASE_GET})
    records = [
        _record("freshness_check", storage_id=1, asset_key=AssetKey(["raw", "x"])),
        _record("freshness_check", storage_id=2),
        _record("freshness_check", storage_id=3, timestamp=1_760_003_600.0),
    ]

    # The first pushable record always goes ahead, so a tick never ends with no
    # progress. Here the unmapped record 1 is handled first, so the deadline
    # stops the push before record 2.
    assert _push(records, client, deadline=-math.inf) == (0, 1)
    assert client.calls == []
    assert _push(records[1:], client, deadline=-math.inf) == (1, 2)
    assert [call[:2] for call in client.calls][-1] == ("POST", RESULT_ENDPOINT)


def test_the_deadline_also_bounds_checks_on_tables_openmetadata_lacks() -> None:
    """Regression: the deadline was gated on a written result, so a batch of
    checks on missing tables (two requests each, nothing written) ran unbounded
    into the sensor timeout.
    """
    client = _FakeClient(fail=("POST", "v1/dataQuality/testCases", 404))
    other = AssetKey(["dimensional", "dim_course"])
    records = [
        _record("freshness_check", storage_id=1),
        _record("freshness_check", storage_id=2, asset_key=other),
    ]

    pushed, handled = push_check_evaluations(
        records,
        {DIM_USER: DIM_USER_FQN, other: f"{DIM_USER_FQN}_other"},
        client,
        LOG,
        -math.inf,
    )

    assert (pushed, handled) == (0, 1)


def _freshness_record(
    new_state: FreshnessState,
    previous_state: FreshnessState,
    *,
    storage_id: int,
    asset_key: AssetKey = DIM_USER,
) -> SimpleNamespace:
    change = FreshnessStateChange(
        key=asset_key,
        new_state=new_state,
        previous_state=previous_state,
        state_change_timestamp=1_760_000_000.25,
    )
    entry = SimpleNamespace(
        timestamp=1_760_000_001.0,
        dagster_event=SimpleNamespace(event_specific_data=change),
    )
    return SimpleNamespace(storage_id=storage_id, event_log_entry=entry)


def test_freshness_changes_continue_the_freshness_check_test_case() -> None:
    client = _FakeClient(existing={TEST_DEFINITION_GET, TEST_CASE_GET})
    records = [
        _freshness_record(FreshnessState.FAIL, FreshnessState.PASS, storage_id=1),
        # Never materialized: says nothing about freshness, so no result.
        _freshness_record(
            FreshnessState.UNKNOWN, FreshnessState.NOT_APPLICABLE, storage_id=2
        ),
        _freshness_record(FreshnessState.PASS, FreshnessState.FAIL, storage_id=3),
    ]

    assert push_freshness_changes(
        records, {DIM_USER: DIM_USER_FQN}, client, LOG, math.inf
    ) == (2, 3)
    # The test case the replaced freshness_check wrote to already exists.
    assert [(method, endpoint) for method, endpoint, _ in client.calls] == [
        ("GET", TEST_DEFINITION_GET),
        ("GET", TEST_CASE_GET),
        ("POST", RESULT_ENDPOINT),
        ("POST", RESULT_ENDPOINT),
    ]
    failed, passed = client.calls[2][2], client.calls[3][2]
    assert failed == {
        "timestamp": 1_760_000_000_250,
        "testCaseStatus": "Failed",
        "result": "Dagster freshness policy FAIL (was PASS).",
        "testResultValue": [],
    }
    assert passed is not None
    assert passed["testCaseStatus"] == "Success"


def _state(state: FreshnessState) -> SimpleNamespace:
    return SimpleNamespace(
        freshness_state=state,
        updated_at=datetime(2026, 11, 18, 12, 0, tzinfo=UTC),
    )


def test_reconcile_creates_only_the_missing_freshness_test_cases() -> None:
    covered = AssetKey(["dimensional", "dim_course"])
    covered_fqn = f"{DIM_USER_FQN[: -len('dim_user')]}dim_course"
    never_built = AssetKey(["mart", "never_built"])
    # dim_user already has the test case the state-change sensor keeps current.
    client = _FakeClient(existing={TEST_CASE_GET})
    states = {
        DIM_USER: _state(FreshnessState.FAIL),
        covered: _state(FreshnessState.PASS),
        never_built: _state(FreshnessState.UNKNOWN),
        AssetKey(["raw", "unmapped"]): _state(FreshnessState.PASS),
    }
    fqns = {
        DIM_USER: DIM_USER_FQN,
        covered: covered_fqn,
        never_built: f"{covered_fqn}_never_built",
    }

    assert reconcile_freshness_test_cases(states, fqns, client, LOG) == 1

    covered_case = (
        "Starburst%20Galaxy.ol_data_lake_production.ol_warehouse_production_dimensional"
        ".dim_course.dagster_freshness_check"
    )
    assert [(method, endpoint) for method, endpoint, _ in client.calls] == [
        ("GET", TEST_CASE_GET),
        ("GET", f"v1/dataQuality/testCases/name/{covered_case}"),
        ("GET", TEST_DEFINITION_GET),
        ("PUT", "v1/dataQuality/testDefinitions"),
        ("GET", f"v1/dataQuality/testCases/name/{covered_case}"),
        ("POST", "v1/dataQuality/testCases"),
        ("POST", f"v1/dataQuality/testCases/testCaseResults/{covered_case}"),
    ]
    result = client.calls[-1][2]
    assert result is not None
    assert result["testCaseStatus"] == "Success"
    assert result["timestamp"] == int(
        datetime(2026, 11, 18, 12, 0, tzinfo=UTC).timestamp() * 1000
    )
