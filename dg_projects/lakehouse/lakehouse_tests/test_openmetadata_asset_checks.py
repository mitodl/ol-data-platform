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
    def __init__(self, test_case_status: int = 200) -> None:
        self.test_case_status = test_case_status
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def send(self, method: str, endpoint: str, body: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((method, endpoint, body))
        if endpoint == "v1/dataQuality/testCases" and self.test_case_status != 200:
            request = httpx.Request(method, f"https://om/api/{endpoint}")
            response = httpx.Response(self.test_case_status, request=request)
            msg = f"{self.test_case_status} from {endpoint}"
            raise httpx.HTTPStatusError(msg, request=request, response=response)
        if endpoint == "v1/dataQuality/testCases":
            return {"fullyQualifiedName": f"{DIM_USER_FQN}.{body['name']}"}
        return {}


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
    methods = [(method, endpoint) for method, endpoint, _ in client.calls]
    result_endpoint = (
        "v1/dataQuality/testCases/testCaseResults/"
        "Starburst%20Galaxy.ol_data_lake_production.ol_warehouse_production_dimensional"
        ".dim_user.dagster_freshness_check"
    )
    assert methods == [
        ("PUT", "v1/dataQuality/testDefinitions"),
        # The test case is upserted once per batch, not once per evaluation.
        ("PUT", "v1/dataQuality/testCases"),
        ("POST", result_endpoint),
        ("POST", result_endpoint),
    ]
    test_case = client.calls[1][2]
    assert test_case["testDefinition"] == TEST_DEFINITION_NAME
    assert test_case["entityLink"] == f"<#E::table::{DIM_USER_FQN}>"
    failed, passed = client.calls[2][2], client.calls[3][2]
    assert failed["testCaseStatus"] == "Failed"
    assert failed["timestamp"] == 1_760_000_000_500
    assert failed["result"] == "Dagster check failed (ERROR).\nAsset is overdue."
    assert passed["testCaseStatus"] == "Success"


def test_nothing_to_push_makes_no_calls() -> None:
    client = _FakeClient()
    records = [_record("freshness_check", asset_key=AssetKey(["raw", "unmapped"]))]
    assert push_check_evaluations(records, {DIM_USER: DIM_USER_FQN}, client, LOG) == 0
    assert client.calls == []


def test_table_missing_from_openmetadata_is_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _FakeClient(test_case_status=404)
    records = [_record("freshness_check")]
    with caplog.at_level(logging.WARNING):
        assert (
            push_check_evaluations(records, {DIM_USER: DIM_USER_FQN}, client, LOG) == 0
        )
    assert "OpenMetadata has no table" in caplog.text


def test_other_api_errors_raise() -> None:
    client = _FakeClient(test_case_status=500)
    with pytest.raises(httpx.HTTPStatusError):
        push_check_evaluations(
            [_record("freshness_check")], {DIM_USER: DIM_USER_FQN}, client, LOG
        )
