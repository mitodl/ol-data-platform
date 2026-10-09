"""Tests for what the Iceberg maintenance assets count and log as a failure.

Expiry is tested in
`packages/ol-orchestrate-lib/tests/lib/test_iceberg_maintenance.py`. What is
left here is the accounting: a table the scan could not load and a table whose
expiry raised are both failed tables, and each is named in the run's log.
"""

import importlib
import logging
import sys
import types
from collections.abc import Iterator
from typing import Any

import pytest
from dagster import build_asset_context
from ol_orchestrate.lib.iceberg_maintenance import (
    RawLayerScan,
    RawLayerTableInfo,
    TableMaintenanceConfig,
)

DBT_MODULE = "lakehouse.assets.lakehouse.dbt"
ASSET_MODULE = "lakehouse.assets.iceberg_maintenance"
BROKEN = "raw__mitxonline__app__postgres__broken"
# With the unloadable table that is 42 scanned, so the third failure is the
# first one over 5%.
HEALTHY_TABLES = 41
# The metadata keeps this many failures; past it the log is the only record.
FAILURE_DETAILS_KEPT = 20


@pytest.fixture
def module() -> Iterator[types.ModuleType]:
    """Import the asset module without a dbt manifest.

    The module imports `dbt_project`, which reads target/manifest.json at
    import, and the pytest job has none. The raw asset never touches it.
    """
    import lakehouse.assets  # noqa: PLC0415

    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(sys.modules, DBT_MODULE, types.SimpleNamespace(dbt_project=None))
        patch.delitem(sys.modules, ASSET_MODULE, raising=False)
        try:
            yield importlib.import_module(ASSET_MODULE)
        finally:
            # The copy imported under the stub must not outlive the test.
            sys.modules.pop(ASSET_MODULE, None)
            if hasattr(lakehouse.assets, "iceberg_maintenance"):
                del lakehouse.assets.iceberg_maintenance


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
    nothing_to_expire: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    def fake_expire(*, table_name: str, **_kwargs: Any) -> dict[str, Any]:
        if table_name in failing:
            msg = "commit lost to a concurrent write"
            raise RuntimeError(msg)
        if table_name in nothing_to_expire:
            return {"skipped": True, "eligible_count": 0, "stale_branch_count": 0}
        return {"skipped": False, "eligible_count": 2, "stale_branch_count": 1}

    monkeypatch.setattr(module, "load_raw_layer_maintenance_work", lambda **_: scan)
    monkeypatch.setattr(module, "get_glue_catalog", lambda: None)
    monkeypatch.setattr(module, "expire_snapshots", fake_expire)
    with build_asset_context() as context:
        output = module.iceberg_raw_layer_maintenance(context)
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

    failing = frozenset(table.table_name for table in tables[:2])

    with pytest.raises(RuntimeError, match=rf"failed for 3/{HEALTHY_TABLES + 1}"):
        _run(module, monkeypatch, scan, failing=failing)


def test_failures_at_five_percent_or_under_do_not_fail_the_asset(
    module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    tables = _tables(module.RAW_GLUE_DATABASE)
    scan = RawLayerScan(tables=tables, failures=[f"{BROKEN}: metadata.json not found"])

    metadata = _run(
        module, monkeypatch, scan, failing=frozenset({tables[0].table_name})
    )

    assert metadata["failure_count"] == 2


def test_a_table_with_nothing_to_expire_is_not_counted_as_cleaned(
    module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    tables = _tables(module.RAW_GLUE_DATABASE)

    metadata = _run(
        module,
        monkeypatch,
        RawLayerScan(tables=tables),
        nothing_to_expire=frozenset({tables[0].table_name}),
    )

    assert metadata["tables_scanned"] == HEALTHY_TABLES
    assert metadata["tables_cleaned"] == HEALTHY_TABLES - 1


def test_a_scan_that_loads_nothing_fails_the_asset(
    module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    scan = RawLayerScan(failures=[f"{BROKEN}: access denied"])

    with pytest.raises(RuntimeError, match="failed for 1/1"):
        _run(module, monkeypatch, scan)


def test_a_glue_listing_with_no_iceberg_tables_fails_the_asset(
    module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(RuntimeError, match="lists no Iceberg tables"):
        _run(module, monkeypatch, RawLayerScan())


def test_every_failed_raw_table_is_logged_past_the_metadata_cap(
    module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    # 21 expiry failures of 500 tables stay under the 5% threshold.
    tables = _tables(module.RAW_GLUE_DATABASE, count=500)
    failing = frozenset(t.table_name for t in tables[: FAILURE_DETAILS_KEPT + 1])
    scan = RawLayerScan(tables=tables, failures=[f"{BROKEN}: metadata.json not found"])

    metadata = _run(module, monkeypatch, scan, failing=failing)

    assert len(metadata["failure_details"]) == FAILURE_DETAILS_KEPT
    # context.log does not propagate to the root logger, so caplog sees none of
    # it. Its console handler writes to stderr.
    logged = capfd.readouterr().err
    for table_name in [BROKEN, *failing]:
        assert logged.count(f"{table_name}:") == 1


class _NoMaterializations:
    def get_event_records(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []


class _FailingTrino:
    def optimize(self, **_kwargs: Any) -> None:
        msg = "OPTIMIZE rejected"
        raise RuntimeError(msg)

    def analyze(self, **_kwargs: Any) -> None:
        msg = "ANALYZE rejected"
        raise RuntimeError(msg)


def test_dbt_layer_operation_failures_go_to_the_run_log(
    module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def failing_expire(**_kwargs: Any) -> dict[str, Any]:
        msg = "commit lost to a concurrent write"
        raise RuntimeError(msg)

    monkeypatch.setattr(module, "expire_snapshots", failing_expire)
    cfg = TableMaintenanceConfig(
        model_name="dim_user",
        schema_name="ol_warehouse_production_dimensional",
        materialized="table",
        asset_key=["dimensional", "dim_user"],
    )
    run_log = logging.getLogger("the_run_log")

    with caplog.at_level(logging.WARNING, logger=run_log.name):
        context = types.SimpleNamespace(instance=_NoMaterializations(), log=run_log)
        summary = module._run_table_maintenance(
            cfg, context, None, _FailingTrino(), None
        )

    assert [error.split(":")[0] for error in summary["errors"]] == [
        "expire_snapshots",
        "optimize",
        "analyze",
    ]
    messages = [
        record.getMessage() for record in caplog.records if record.name == run_log.name
    ]
    assert [message.split(" failed for ")[0] for message in messages] == [
        "EXPIRE SNAPSHOTS",
        "OPTIMIZE",
        "ANALYZE",
    ]
    assert all(
        "ol_warehouse_production_dimensional.dim_user" in message
        for message in messages
    )
