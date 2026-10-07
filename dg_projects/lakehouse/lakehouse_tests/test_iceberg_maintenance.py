"""Tests for what the iceberg_raw_layer_maintenance asset counts as a failure.

Expiry is tested in
`packages/ol-orchestrate-lib/tests/lib/test_iceberg_maintenance.py`. What is
left here is the accounting: a table the scan could not load and a table whose
expiry raised are both failed tables.
"""

import importlib
import sys
import types
from collections.abc import Iterator
from typing import Any

import pytest
from dagster import build_asset_context
from ol_orchestrate.lib.iceberg_maintenance import RawLayerScan, RawLayerTableInfo

DBT_MODULE = "lakehouse.assets.lakehouse.dbt"
ASSET_MODULE = "lakehouse.assets.iceberg_maintenance"
BROKEN = "raw__mitxonline__app__postgres__broken"
# Enough tables that one failure stays under the 5% threshold.
HEALTHY_TABLES = 41


@pytest.fixture
def module() -> Iterator[types.ModuleType]:
    """Import the asset module without a dbt manifest.

    The module imports `dbt_project`, which reads target/manifest.json at
    import, and the pytest job has none. The raw asset never touches it.
    """
    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(sys.modules, DBT_MODULE, types.SimpleNamespace(dbt_project=None))
        patch.delitem(sys.modules, ASSET_MODULE, raising=False)
        yield importlib.import_module(ASSET_MODULE)


def _tables(database: str, count: int = HEALTHY_TABLES) -> list[RawLayerTableInfo]:
    return [
        RawLayerTableInfo(
            table_name=f"raw__mitxonline__app__postgres__t{n}",
            database=database,
            snapshot_count=3,
            eligible_snapshot_count=2,
        )
        for n in range(count)
    ]


def _run(
    module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    scan: RawLayerScan,
    failing: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    def fake_expire(*, table_name: str, **_kwargs: Any) -> dict[str, Any]:
        if table_name in failing:
            msg = "commit lost to a concurrent write"
            raise RuntimeError(msg)
        return {"skipped": False, "eligible_count": 2, "stale_branch_count": 1}

    monkeypatch.setattr(module, "load_raw_layer_maintenance_work", lambda **_: scan)
    monkeypatch.setattr(module, "get_glue_catalog", lambda: None)
    monkeypatch.setattr(module, "expire_snapshots", fake_expire)
    output = module.iceberg_raw_layer_maintenance(build_asset_context())
    return {key: value.value for key, value in output.metadata.items()}


def test_a_table_the_scan_could_not_load_is_a_failed_table(
    module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    scan = RawLayerScan(
        tables=_tables(module.RAW_GLUE_DATABASE),
        failures=[f"{BROKEN}: metadata.json not found"],
    )

    metadata = _run(module, monkeypatch, scan)

    assert metadata["tables_scanned"] == HEALTHY_TABLES + 1
    assert metadata["tables_cleaned"] == HEALTHY_TABLES
    assert metadata["failure_count"] == 1
    assert metadata["failure_details"] == [f"{BROKEN}: metadata.json not found"]


def test_scan_and_expiry_failures_count_toward_the_same_threshold(
    module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    tables = _tables(module.RAW_GLUE_DATABASE)
    scan = RawLayerScan(tables=tables, failures=[f"{BROKEN}: metadata.json not found"])

    with pytest.raises(RuntimeError, match=rf"failed for 2/{HEALTHY_TABLES + 1}"):
        _run(module, monkeypatch, scan, failing=frozenset({tables[0].table_name}))


def test_a_scan_that_loads_nothing_fails_the_asset(
    module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    scan = RawLayerScan(failures=[f"{BROKEN}: access denied"])

    with pytest.raises(RuntimeError, match="failed for 1/1"):
        _run(module, monkeypatch, scan)
