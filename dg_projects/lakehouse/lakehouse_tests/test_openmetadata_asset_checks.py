"""Tests for pushing Dagster asset check evaluations to OpenMetadata."""

import logging
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

import httpx2 as httpx
import pytest
from dagster import AssetCheckEvaluation, AssetCheckSeverity, AssetKey
from dagster_dbt import DagsterDbtTranslator
from lakehouse.lib.openmetadata_asset_checks import (
    TEST_DEFINITION_NAME,
    push_check_evaluations,
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
    return SimpleNamespace(event_log_entry=entry)


def test_push_writes_results_and_skips_dbt_tests_and_unmapped_assets() -> None:
    client = _FakeClient()
    records = [
        _record("freshness_check", passed=False),
        _record("freshness_check", timestamp=1_760_003_600.0),
        # dagster-dbt's conversion of a dbt test: already in OM via run_results.json.
        _record(
            "not_null_dim_user_user_pk",
            metadata={"unique_id": "test.x", "invocation_id": "i", "status": "pass"},
        ),
        _record("freshness_check", asset_key=AssetKey(["raw", "unmapped"])),
    ]
    pushed = push_check_evaluations(records, {DIM_USER: DIM_USER_FQN}, client, LOG)

    assert pushed == 2
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
    push_check_evaluations(
        [_record("freshness_check")], {DIM_USER: DIM_USER_FQN}, client, LOG
    )
    assert [(method, endpoint) for method, endpoint, _ in client.calls] == [
        ("GET", TEST_DEFINITION_GET),
        ("GET", TEST_CASE_GET),
        ("POST", RESULT_ENDPOINT),
    ]


def test_result_already_written_counts_as_pushed() -> None:
    """A retried batch re-posts results OpenMetadata already has; it answers 409."""
    client = _FakeClient(fail=("POST", RESULT_ENDPOINT, 409))
    records = [_record("freshness_check")]
    assert push_check_evaluations(records, {DIM_USER: DIM_USER_FQN}, client, LOG) == 1


def test_nothing_to_push_makes_no_calls() -> None:
    client = _FakeClient()
    records = [_record("freshness_check", asset_key=AssetKey(["raw", "unmapped"]))]
    assert push_check_evaluations(records, {DIM_USER: DIM_USER_FQN}, client, LOG) == 0
    assert client.calls == []


def test_table_missing_from_openmetadata_is_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _FakeClient(fail=("POST", "v1/dataQuality/testCases", 404))
    records = [_record("freshness_check")]
    with caplog.at_level(logging.WARNING):
        assert (
            push_check_evaluations(records, {DIM_USER: DIM_USER_FQN}, client, LOG) == 0
        )
    assert "OpenMetadata has no table" in caplog.text


def test_other_api_errors_raise() -> None:
    client = _FakeClient(fail=("POST", RESULT_ENDPOINT, 500))
    with pytest.raises(httpx.HTTPStatusError):
        push_check_evaluations(
            [_record("freshness_check")], {DIM_USER: DIM_USER_FQN}, client, LOG
        )
