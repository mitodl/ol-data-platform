"""Which raw tables the load-side history guard checks, and reading dbt's answer."""

import importlib
import json
import sys
import types
from collections.abc import Iterator
from typing import Any

import pytest
from dagster import AssetCheckKey, AssetKey
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
    Building the checks calls neither.
    """
    import lakehouse.assets  # noqa: PLC0415

    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(
            sys.modules,
            DBT_MODULE,
            types.SimpleNamespace(dbt_credentials=None, dbt_dev_vars=None),
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
