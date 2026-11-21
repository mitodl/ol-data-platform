"""Which raw tables the load-side history guard checks, and reading dbt's answer."""

import importlib
import json
import sys
import types
from collections.abc import Iterator
from contextlib import nullcontext
from typing import Any

import pytest
from dagster import (
    AssetCheckKey,
    AssetCheckSeverity,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Definitions,
    ExecuteInProcessResult,
    define_asset_job,
)
from lakehouse.lib.history_load_guard import (
    CHECK_NAME,
    LOG_PREFIX,
    GuardedTable,
    guarded_tables,
    parse_load_counts,
)

SOURCE = "source.open_learning.ol_warehouse_raw_data."
DBT_MODULE = "lakehouse.assets.lakehouse.dbt"
ASSET_MODULE = "lakehouse.assets.history_load_guard"


@pytest.fixture
def asset_module() -> Iterator[types.ModuleType]:
    """Import the check module without a dbt manifest.

    It imports dbt_credentials and dbt_dev_vars from the dbt asset module,
    which reads target/manifest.json at import, and the pytest job has none.
    Running a check calls both, so they are stood in for: no credentials to
    inject, and no dev vars.
    """
    import lakehouse.assets  # noqa: PLC0415

    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(
            sys.modules,
            DBT_MODULE,
            types.SimpleNamespace(
                dbt_credentials=lambda _starrocks: nullcontext(), dbt_dev_vars=list
            ),
        )
        patch.delitem(sys.modules, ASSET_MODULE, raising=False)
        try:
            yield importlib.import_module(ASSET_MODULE)
        finally:
            sys.modules.pop(ASSET_MODULE, None)
            if hasattr(lakehouse.assets, "history_load_guard"):
                del lakehouse.assets.history_load_guard


def _snapshot(
    name: str,
    raw_tables: list[str],
    *,
    absence: str = "deleted",
    unique_key: str | list[str] = "id",
    keyset_table: str | None = None,
) -> dict[str, Any]:
    raw_history: dict[str, Any] = {
        "unit": "x/app_postgres",
        "absence": absence,
        "delete_capture": "keyset" if keyset_table else "full_read",
    }
    if keyset_table:
        raw_history |= {"keyset_table": keyset_table, "keyset_lakes": ["qa"]}
    return {
        "name": name,
        "resource_type": "snapshot",
        "config": {"meta": {"raw_history": raw_history}, "unique_key": unique_key},
        "depends_on": {"nodes": [SOURCE + table for table in raw_tables]},
    }


def _manifest(*nodes: dict[str, Any]) -> dict[str, Any]:
    return {
        "metadata": {"project_name": "open_learning"},
        "nodes": {f"snapshot.open_learning.{node['name']}": node for node in nodes},
    }


def test_a_live_delete_recording_snapshot_guards_its_raw_table() -> None:
    manifest = _manifest(_snapshot("snapshot_raw__x__a", ["raw__x__a"]))

    assert guarded_tables(manifest) == [
        GuardedTable(
            snapshot="snapshot_raw__x__a", raw_table="raw__x__a", unique_key=("id",)
        )
    ]


def test_a_keyset_snapshot_guards_the_raw_table_not_the_keyset() -> None:
    manifest = _manifest(
        _snapshot(
            "snapshot_raw__x__a",
            ["raw__x__a", "raw__x__a__keyset"],
            keyset_table="raw__x__a__keyset",
        )
    )

    assert [table.raw_table for table in guarded_tables(manifest)] == ["raw__x__a"]


def test_a_composite_key_is_kept_whole() -> None:
    manifest = _manifest(
        _snapshot("snapshot_raw__x__a", ["raw__x__a"], unique_key=["a", "b"])
    )

    assert guarded_tables(manifest)[0].unique_key == ("a", "b")


def test_a_key_the_snapshot_quoted_is_passed_bare() -> None:
    """The macro quotes it again for its own engine; twice breaks the SQL."""
    manifest = _manifest(
        _snapshot(
            "snapshot_raw__x__a",
            ["raw__x__a"],
            unique_key=['"Email"', '"Program Title"'],
        )
    )

    assert guarded_tables(manifest)[0].unique_key == ("Email", "Program Title")


@pytest.mark.parametrize(
    "node",
    [
        _snapshot("snapshot_raw__x__a", ["raw__x__a"], absence="aged_out"),
        {**_snapshot("model_a", ["raw__x__a"]), "resource_type": "model"},
        {
            **_snapshot("snapshot_hand_written", ["raw__x__a"]),
            "config": {"meta": {}, "unique_key": "id"},
        },
    ],
    ids=["aged-out", "model", "hand-written-snapshot"],
)
def test_other_nodes_are_not_guarded(node: dict[str, Any]) -> None:
    assert guarded_tables(_manifest(node)) == []


def test_a_snapshot_reading_two_raw_tables_is_refused() -> None:
    manifest = _manifest(_snapshot("snapshot_raw__x__a", ["raw__x__a", "raw__x__b"]))

    with pytest.raises(ValueError, match="expected exactly one"):
        guarded_tables(manifest)


def _line(**overrides: Any) -> str:
    fields = {
        "snapshot": "snapshot_raw__x__a",
        "raw_table": "raw__x__a",
        "loaded_keys": 40,
        "snapshot_open_records": 100,
        "missing_keys": 60,
        "max_fraction": 0.1,
        "min_count": 10,
        "short": True,
    } | overrides
    return LOG_PREFIX + json.dumps(fields)


def test_the_macro_lines_are_read_and_other_messages_skipped() -> None:
    counts = parse_load_counts(
        ["Running with dbt=1.10", _line(), _line(raw_table="raw__x__b", short=False)]
    )

    assert set(counts) == {"raw__x__a", "raw__x__b"}
    assert counts["raw__x__a"].short
    assert not counts["raw__x__b"].short


def test_a_snapshot_not_yet_run_says_so() -> None:
    count = parse_load_counts(
        [_line(snapshot_open_records=None, missing_keys=0, short=False)]
    )["raw__x__a"]

    assert "has not run yet" in count.describe()


def test_checks_are_built_per_airbyte_group_for_the_tables_lakehouse_loads(
    asset_module: types.ModuleType,
) -> None:
    tables = [
        GuardedTable("snapshot_raw__x__a", "raw__x__a", ("id",)),
        GuardedTable("snapshot_raw__x__b", "raw__x__b", ("id",)),
        GuardedTable("snapshot_raw__y__c", "raw__y__c", ("id",)),
        # Loaded by dlt in data_loading: no Airbyte group, so no check here.
        GuardedTable("snapshot_raw__z__d", "raw__z__d", ("id",)),
    ]
    groups = {
        AssetKey(["ol_warehouse_raw_data", "raw__x__a"]): "x_app",
        AssetKey(["ol_warehouse_raw_data", "raw__x__b"]): "x_app",
        AssetKey(["ol_warehouse_raw_data", "raw__y__c"]): "y_app",
    }

    checks = asset_module.build_history_load_guard_checks(tables, groups)

    by_group = {
        check.node_def.name: {spec.key for spec in check.check_specs}
        for check in checks
    }
    assert by_group == {
        "history_load_guard_x_app": {
            AssetCheckKey(AssetKey(["ol_warehouse_raw_data", "raw__x__a"]), CHECK_NAME),
            AssetCheckKey(AssetKey(["ol_warehouse_raw_data", "raw__x__b"]), CHECK_NAME),
        },
        "history_load_guard_y_app": {
            AssetCheckKey(AssetKey(["ol_warehouse_raw_data", "raw__y__c"]), CHECK_NAME)
        },
    }
    # Blocking would hold back staging for the whole connection on one failure.
    assert not any(spec.blocking for check in checks for spec in check.check_specs)


GUARDED = [
    GuardedTable("snapshot_raw__x__a", "raw__x__a", ("id",)),
    GuardedTable("snapshot_raw__x__b", "raw__x__b", ("id",)),
]


class _Event:
    def __init__(self, msg: str) -> None:
        self.raw_event = {"info": {"msg": msg}}


class FakeDbt:
    """Stands in for DbtCliResource: replays log lines, then optionally fails."""

    def __init__(self, lines: list[str], error: Exception | None = None) -> None:
        """Replay ``lines`` as dbt log messages, then raise ``error`` if given."""
        self.lines = lines
        self.error = error
        self.calls: list[list[str]] = []

    def cli(self, args: list[str], *, raise_on_error: bool) -> "FakeDbt":
        """Record the invocation; a dbt error must fail the step."""
        assert raise_on_error
        self.calls.append(args)
        return self

    def stream_raw_events(self) -> Iterator[_Event]:
        """Yield the lines, then fail as a dbt process dying mid-stream would."""
        yield from (_Event(line) for line in self.lines)
        if self.error:
            raise self.error

    def tables_asked_for(self) -> list[str]:
        """Return the raw tables passed to the macro in the one invocation."""
        (args,) = self.calls
        return [
            t["raw_table"] for t in json.loads(args[args.index("--args") + 1])["tables"]
        ]


def _count(table: str, *, short: bool) -> str:
    return _line(
        snapshot=f"snapshot_{table}",
        raw_table=table,
        short=short,
        missing_keys=60 if short else 0,
    )


def _run_check(
    asset_module: types.ModuleType,
    dbt: FakeDbt,
    *,
    tables: list[GuardedTable] = GUARDED,
    allow_short: tuple[str, ...] = (),
) -> ExecuteInProcessResult:
    (check,) = asset_module.build_history_load_guard_checks(
        GUARDED, {table.raw_key: "x_app" for table in GUARDED}
    )
    defs = Definitions(
        assets=[AssetSpec(table.raw_key) for table in GUARDED],
        asset_checks=[check],
        resources={"dbt": dbt, "starrocks": object()},
        jobs=[
            define_asset_job(
                "guard",
                selection=AssetSelection.checks(
                    *(AssetCheckKey(table.raw_key, CHECK_NAME) for table in tables)
                ),
            )
        ],
    )
    run_config = {
        "ops": {
            "history_load_guard_x_app": {"config": {"allow_short": list(allow_short)}}
        }
    }
    return defs.resolve_job_def("guard").execute_in_process(
        run_config=run_config, raise_on_error=False
    )


def _evaluations(result: ExecuteInProcessResult) -> dict[str, Any]:
    return {
        evaluation.asset_key.path[-1]: evaluation
        for evaluation in result.get_asset_check_evaluations()
    }


def test_a_short_load_fails_at_error_and_a_full_one_passes(
    asset_module: types.ModuleType,
) -> None:
    dbt = FakeDbt([_count("raw__x__a", short=True), _count("raw__x__b", short=False)])

    evaluations = _evaluations(_run_check(asset_module, dbt))

    assert not evaluations["raw__x__a"].passed
    assert evaluations["raw__x__a"].severity == AssetCheckSeverity.ERROR
    assert evaluations["raw__x__b"].passed


def test_only_the_selected_tables_are_counted(asset_module: types.ModuleType) -> None:
    dbt = FakeDbt([_count("raw__x__b", short=False)])

    result = _run_check(asset_module, dbt, tables=GUARDED[1:])

    assert dbt.tables_asked_for() == ["raw__x__b"]
    assert set(_evaluations(result)) == {"raw__x__b"}


def test_allow_short_passes_a_short_load_and_says_so(
    asset_module: types.ModuleType,
) -> None:
    dbt = FakeDbt([_count("raw__x__a", short=True), _count("raw__x__b", short=False)])

    evaluations = _evaluations(
        _run_check(asset_module, dbt, allow_short=("raw__x__a",))
    )

    assert evaluations["raw__x__a"].passed
    assert "(allowed for this run)" in evaluations["raw__x__a"].description


@pytest.mark.parametrize(
    "dbt",
    [
        FakeDbt([_count("raw__x__a", short=False)]),
        FakeDbt([_count("raw__x__a", short=False)], error=RuntimeError("dbt died")),
    ],
    ids=["a-table-without-a-count", "dbt-fails-after-a-count"],
)
def test_an_incomplete_answer_records_no_result_for_any_table(
    asset_module: types.ModuleType, dbt: FakeDbt
) -> None:
    """No result for the latest load keeps every snapshot in the group waiting."""
    result = _run_check(asset_module, dbt)

    assert not result.success
    assert _evaluations(result) == {}
