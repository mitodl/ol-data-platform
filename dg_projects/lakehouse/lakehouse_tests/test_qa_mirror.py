"""Tests for the QA mirror assets.

Rendering is tested in `src/ol_dbt_cli/tests/test_qa_mirror.py`. What is left
here is the Dagster side: the statements run in an order that leaves no stale
copy behind, and the plan check copies nothing.
"""

from pathlib import Path
from typing import Any

import pytest
from dagster import (
    Definitions,
    ExecutorDefinition,
    Failure,
    ResourceDefinition,
    build_asset_check_context,
    build_asset_context,
    validate_run_config,
)
from dagster_k8s import k8s_job_executor
from lakehouse.assets.qa_mirror import (
    QA_MIRROR_REFRESH_MAX_CONCURRENT,
    _mirror_asset,
    _plan_check,
    build_qa_mirror_assets,
    build_qa_mirror_checks,
    qa_mirror_plan_schedules,
    qa_mirror_refresh_schedules,
)
from ol_dbt_cli.lib.qa_mirror import MirrorDeclarationError, MirrorTable
from ol_orchestrate.lib.failures import with_failure_hooks
from pymysql.err import OperationalError

TABLE = MirrorTable(
    unit="emeritus/bigquery",
    raw_table="raw__emeritus__bigquery__api_enrollments",
    columns={"_airbyte_extracted_at": "copy", "email": "hash"},
    where=None,
)
TARGET = (
    "ol_data_lake_qa.ol_warehouse_qa_raw.`raw__emeritus__bigquery__api_enrollments`"
)


class FakeStarRocks:
    def __init__(self, described: list[dict[str, str]]) -> None:
        self.described = described
        self.statements: list[str] = []
        self.non_idempotent: list[str] = []

    def fetch(self, sql: str) -> list[dict[str, Any]]:
        self.statements.append(sql)
        if sql.startswith("DESCRIBE"):
            return self.described
        if sql.startswith("EXPLAIN"):
            return [{"Explain String": "OlapScanNode"}]
        return [{"row_count": 42}]

    def execute(self, sql: str, *, idempotent: bool = True) -> None:
        self.statements.append(sql)
        if not idempotent:
            self.non_idempotent.append(sql)


PRODUCTION = [
    {"Field": "_airbyte_extracted_at", "Type": "bigint"},
    {"Field": "email", "Type": "varchar(1048576)"},
    {"Field": "first_name", "Type": "varchar(1048576)"},
]


def _materialise(
    starrocks: FakeStarRocks, tables: list[MirrorTable] | None = None
) -> Any:
    asset = _mirror_asset(TABLE.unit, tables or [TABLE])
    return asset(build_asset_context(), starrocks)


def test_describes_explains_drops_then_copies() -> None:
    starrocks = FakeStarRocks(PRODUCTION)
    result = _materialise(starrocks)

    describe, explain, drop, ctas, count = starrocks.statements
    assert describe.startswith("DESCRIBE ol_data_lake_production.")
    assert explain.startswith("EXPLAIN SELECT")
    # The plan is of the copy's own query, so a `where` StarRocks cannot plan
    # fails here rather than after the DROP below.
    assert explain.removeprefix("EXPLAIN ") in ctas
    assert drop == f"DROP TABLE IF EXISTS {TARGET} FORCE"
    assert ctas.startswith(f"CREATE TABLE {TARGET}\nAS SELECT")
    assert count.endswith(f"FROM {TARGET}")
    # A retried CTAS that died after creating the table would fail on "already
    # exists" and hide the real error.
    assert starrocks.non_idempotent == [ctas]
    copied = result.metadata["tables"].data["raw__emeritus__bigquery__api_enrollments"]
    assert copied == {
        "rows": 42,
        "masked": {"email": "hash"},
        "dropped": ["first_name"],
        "where": None,
    }


def test_a_json_keys_mode_is_reported_as_masked() -> None:
    # The one mode that is a mapping rather than a name has to survive the
    # masked-column report and its JSON metadata.
    mode = {"json_keys": ["channel"]}
    table = MirrorTable(
        unit=TABLE.unit,
        raw_table=TABLE.raw_table,
        columns={"_airbyte_extracted_at": "copy", "email": mode},
        where=None,
    )
    result = _materialise(FakeStarRocks(PRODUCTION), [table])
    assert result.metadata["tables"].data[table.name]["masked"] == {"email": mode}


def test_a_stale_declaration_fails_before_any_qa_copy_in_the_unit_is_dropped() -> None:
    stale = MirrorTable(
        unit=TABLE.unit,
        raw_table="raw__emeritus__bigquery__other",
        columns={"gone": "copy"},
        where=None,
    )
    starrocks = FakeStarRocks(PRODUCTION)
    with pytest.raises(MirrorDeclarationError):
        _materialise(starrocks, [TABLE, stale])
    assert [s.split()[0] for s in starrocks.statements] == [
        "DESCRIBE",
        "EXPLAIN",
        "DESCRIBE",
    ]


def test_a_query_starrocks_cannot_plan_fails_before_any_qa_copy_is_dropped() -> None:
    # A `where` naming a column production has renamed, or calling a function
    # no signature matches, gets past render_mirror: that compares
    # `mirror.columns` with DESCRIBE and never resolves the predicate's own
    # columns. Only planning the query catches it.
    class FailingExplain(FakeStarRocks):
        def fetch(self, sql: str) -> list[dict[str, Any]]:
            rows = super().fetch(sql)
            if sql.startswith("EXPLAIN"):
                raise OperationalError(1064, "Getting analyzing error")
            return rows

    starrocks = FailingExplain(PRODUCTION)
    with pytest.raises(OperationalError):
        _materialise(starrocks, [TABLE, TABLE])
    assert [s.split()[0] for s in starrocks.statements] == ["DESCRIBE", "EXPLAIN"]


def test_the_last_table_in_a_unit_is_planned_before_the_first_one_is_dropped() -> None:
    # The table whose plan fails is the second, so a loop that rendered and
    # copied one table at a time would already have dropped and rebuilt the
    # first. Only planning the whole unit up front leaves it untouched.
    class FailsOnTheSecondExplain(FakeStarRocks):
        def fetch(self, sql: str) -> list[dict[str, Any]]:
            rows = super().fetch(sql)
            explains = [s for s in self.statements if s.startswith("EXPLAIN")]
            if sql.startswith("EXPLAIN") and len(explains) > 1:
                raise OperationalError(1064, "Getting analyzing error")
            return rows

    starrocks = FailsOnTheSecondExplain(PRODUCTION)
    with pytest.raises(OperationalError):
        _materialise(starrocks, [TABLE, TABLE])
    assert [s.split()[0] for s in starrocks.statements] == [
        "DESCRIBE",
        "EXPLAIN",
        "DESCRIBE",
        "EXPLAIN",
    ]


def test_a_failed_ctas_fails_the_run_rather_than_counting_what_it_left() -> None:
    # The CTAS is the one statement the resource will not retry, so a failure
    # here ends the run. Were it swallowed, the row count below it would read
    # the empty table StarRocks leaves behind and report a successful refresh
    # of nothing, which the qa_branch_contract staleness check cannot catch.
    class FailingCtas(FakeStarRocks):
        def execute(self, sql: str, *, idempotent: bool = True) -> None:
            super().execute(sql, idempotent=idempotent)
            if not idempotent:
                raise OperationalError(2013, "Lost connection to MySQL server")

    starrocks = FailingCtas(PRODUCTION)
    with pytest.raises(OperationalError):
        _materialise(starrocks)
    assert [s.split()[0] for s in starrocks.statements] == [
        "DESCRIBE",
        "EXPLAIN",
        "DROP",
        "CREATE",
    ]


def test_each_unit_gets_its_own_concurrency_pool() -> None:
    # The refresh drops the table before it recreates it, so two runs of one
    # unit must not overlap. Per unit rather than one shared pool: different
    # units touch disjoint tables, and queueing them together would put a small
    # unit behind edxorg's 760 GB program_learner_report copy.
    pools = {
        next(iter(asset.keys)).to_user_string(): asset.op.pool
        for asset in build_qa_mirror_assets()
    }
    assert pools["qa_mirror/emeritus/bigquery"] == "qa_mirror_emeritus_bigquery"
    assert len(set(pools.values())) == len(pools)


def test_the_plan_check_describes_and_explains_but_copies_nothing() -> None:
    starrocks = FakeStarRocks(PRODUCTION)
    check = _plan_check(TABLE.unit, [TABLE, TABLE])
    result = check(build_asset_check_context(), starrocks)

    assert result.passed
    assert [s.split()[0] for s in starrocks.statements] == [
        "DESCRIBE",
        "EXPLAIN",
        "DESCRIBE",
        "EXPLAIN",
    ]
    assert starrocks.non_idempotent == []


def test_a_declaration_production_no_longer_fits_fails_the_plan_check() -> None:
    # Raised, not passed=False: the Sentry failure hook fires on a failed step.
    renamed = MirrorTable(
        unit=TABLE.unit, raw_table=TABLE.raw_table, columns={"gone": "copy"}, where=None
    )
    with pytest.raises(MirrorDeclarationError):
        _plan_check(TABLE.unit, [renamed])(
            build_asset_check_context(), FakeStarRocks(PRODUCTION)
        )


def test_the_failure_hooks_attach_to_the_plan_checks() -> None:
    # The check reports a broken declaration by raising, which reaches Sentry
    # only through this hook. with_failure_hooks wraps AssetsDefinition, and
    # AssetChecksDefinition is one.
    for check in with_failure_hooks(build_qa_mirror_checks()):
        assert {hook.name for hook in check.hook_defs} == {
            "capture_exception_to_sentry",
            "stop_run_retries",
        }


def test_every_mirror_asset_has_a_plan_check() -> None:
    assets = {next(iter(asset.keys)) for asset in build_qa_mirror_assets()}
    checked = {
        key.asset_key for check in build_qa_mirror_checks() for key in check.check_keys
    }
    assert checked == assets


def test_the_real_inventory_builds_one_unpartitioned_asset_per_mirrored_unit() -> None:
    assets = build_qa_mirror_assets()
    keys = {key.to_user_string() for asset in assets for key in asset.keys}
    assert "qa_mirror/zendesk/api" in keys
    assert "qa_mirror/edxorg/program_reports" in keys
    for asset in assets:
        # The refresh is the monthly schedule's job (QA_DATA_TOPOLOGY_SPEC.md
        # §1). An AutomationCondition would refresh it on some other cadence.
        assert asset.partitions_def is None
        assert all(
            condition is None
            for condition in asset.automation_conditions_by_key.values()
        )


@pytest.mark.parametrize("executor", [None, k8s_job_executor])
def test_the_schedules_resolve_and_validate_under_either_executor(
    executor: ExecutorDefinition | None,
) -> None:
    # Production leaves the executor to Dagster's default, QA uses k8s, and the
    # two take differently shaped executor run config. The refresh job names its
    # own executor, so its config validates under both.
    assets, checks = build_qa_mirror_assets(), build_qa_mirror_checks()
    defs = Definitions(
        assets=assets,
        asset_checks=checks,
        schedules=[
            *qa_mirror_refresh_schedules(assets),
            *qa_mirror_plan_schedules(checks),
        ],
        resources={"starrocks": ResourceDefinition.none_resource()},
        executor=executor,
    )
    refresh = validate_run_config(defs.resolve_job_def("qa_mirror_monthly_job"), {})
    assert (
        refresh["execution"]["multiprocess"]["max_concurrent"]
        == QA_MIRROR_REFRESH_MAX_CONCURRENT
    )
    assert validate_run_config(defs.resolve_job_def("qa_mirror_plan_daily_job"), {})


def test_a_missing_inventory_keeps_the_monthly_refresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The refresh then runs qa_mirror_inventory_missing, which fails naming the
    # path. Dropping the schedule would leave nothing to report it.
    monkeypatch.setattr("lakehouse.assets.qa_mirror.INVENTORY_DIR", tmp_path)
    assert [s.name for s in qa_mirror_refresh_schedules(build_qa_mirror_assets())] == [
        "qa_mirror_monthly_schedule"
    ]
    assert qa_mirror_plan_schedules(build_qa_mirror_checks()) == []


def test_a_missing_inventory_is_a_failing_asset_not_an_empty_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("lakehouse.assets.qa_mirror.INVENTORY_DIR", tmp_path)
    (asset,) = build_qa_mirror_assets()
    assert asset.key.to_user_string() == "qa_mirror/inventory_missing"
    with pytest.raises(Failure, match="No inventory units found"):
        asset()
