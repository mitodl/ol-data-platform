"""Tests for the qa_observation_drift asset.

The comparison is tested in src/ol_dbt_cli/tests/test_qa_contract.py. What is
left here is the Dagster side: what it reads, and when a run fails.
"""

import dataclasses
from datetime import timedelta
from pathlib import Path

import pytest
from dagster import build_asset_context
from lakehouse.assets import qa_observation_drift as module
from lakehouse.assets.qa_observation_drift import qa_observation_drift
from lakehouse.lib.scheduled_automation import SCHEDULE_ENVIRONMENTS
from ol_dbt_cli.lib.inventory import load_units
from ol_dbt_cli.lib.qa_mirror import mirror_tables
from ol_dbt_cli.lib.qa_observation import (
    OBSERVATION_FILENAME,
    Observation,
    TableState,
    holds_rows,
    load_observation,
)
from ol_orchestrate.lib.failures import PermanentFailure


def _committed() -> Observation:
    observation = load_observation(module.INVENTORY_DIR / OBSERVATION_FILENAME)
    assert observation is not None
    return observation


COMMITTED = _committed()


def _observing(monkeypatch: pytest.MonkeyPatch, live: Observation) -> list[str]:
    asked: list[str] = []

    def fake_observe(_tables: set[str], database: str) -> Observation:
        asked.append(database)
        return live

    monkeypatch.setattr(module, "observe_glue", fake_observe)
    return asked


def test_the_committed_observation_is_in_the_inventory_directory():
    assert COMMITTED.glue_database == "ol_warehouse_qa_raw"


def test_scheduled_in_qa_only():
    assert SCHEDULE_ENVIRONMENTS["qa_observation_drift_daily"] == frozenset({"qa"})


def test_a_matching_observation_passes(monkeypatch: pytest.MonkeyPatch):
    asked = _observing(monkeypatch, COMMITTED)

    output = qa_observation_drift(build_asset_context())

    assert asked == [COMMITTED.glue_database]
    assert output.value["drifted_tables"] == 0
    assert output.value["tables_observed"] == len(COMMITTED.tables)


def test_a_table_that_landed_fails_the_run(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    empty = next(
        name for name, state in sorted(COMMITTED.tables.items()) if not state.rows
    )
    landed = TableState(present=True, iceberg=True, rows=5)
    live = dataclasses.replace(COMMITTED, tables={**COMMITTED.tables, empty: landed})
    _observing(monkeypatch, live)

    with pytest.raises(PermanentFailure, match="1 QA raw table") as raised:
        qa_observation_drift(build_asset_context())

    assert "ol-dbt inventory observe" in str(raised.value)
    assert f"{empty}: committed observation holds no rows, QA now holds 5 rows" in (
        capsys.readouterr().err
    )


def test_a_mirror_copy_past_its_max_age_fails_the_run(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    # Ageing changes no table's emptiness, so without this a copy could go
    # stale indefinitely while the committed observation still matched QA.
    mirrored = next(
        table.raw_table
        for tables in mirror_tables(load_units(module.INVENTORY_DIR)).values()
        for table in tables
        if holds_rows(COMMITTED.tables[table.raw_table])
    )
    later = dataclasses.replace(
        COMMITTED, observed_at=COMMITTED.observed_at + timedelta(days=365)
    )
    _observing(monkeypatch, later)

    with pytest.raises(PermanentFailure, match="mirror_max_age_days") as raised:
        qa_observation_drift(build_asset_context())

    assert "qa_mirror_monthly_job" in str(raised.value)
    assert "QA raw table(s) differ" not in str(raised.value)
    assert f"{mirrored}: copied " in capsys.readouterr().err


def test_a_missing_observation_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    (tmp_path / "units").symlink_to(module.INVENTORY_DIR / "units")
    monkeypatch.setattr(module, "INVENTORY_DIR", tmp_path)

    with pytest.raises(PermanentFailure, match=f"No {OBSERVATION_FILENAME}"):
        qa_observation_drift(build_asset_context())
