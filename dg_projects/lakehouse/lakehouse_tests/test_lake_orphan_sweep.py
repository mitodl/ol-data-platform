"""Tests for the lake_orphan_sweep asset.

Finding and deleting orphans is tested in
`packages/ol-orchestrate-lib/tests/lib/test_lake_orphan_sweep.py`. What is left
here is the Dagster side: where deletion may run, and what a run reports.
"""

from datetime import UTC, datetime
from typing import Any

import pytest
from dagster import build_asset_context
from lakehouse.assets import lake_orphan_sweep as module
from lakehouse.assets.lake_orphan_sweep import (
    LakeOrphanSweepConfig,
    lake_orphan_sweep,
)
from ol_orchestrate.lib.lake_orphan_sweep import PrefixOutcome, SweepResult

UUID = "b" * 32
ORPHANS = [
    {
        "bucket": "lake-mart-qa",
        "prefix": f"gone-{UUID}",
        "objects": 2,
        "bytes": 105,
        "newest": datetime(2026, 9, 1, tzinfo=UTC),
        "age_days": 35,
        "eligible": True,
    },
    {
        "bucket": "lake-mart-qa",
        "prefix": f"building-{UUID}",
        "objects": 1,
        "bytes": 7,
        "newest": datetime(2026, 10, 4, tzinfo=UTC),
        "age_days": 2,
        "eligible": False,
    },
]


def _result(outcomes: list[PrefixOutcome] | None, **overrides: Any) -> SweepResult:
    fields: dict[str, Any] = {
        "targets": [("lake-mart-qa", "")],
        "prefixes_scanned": 40,
        "orphans": ORPHANS,
        "unsuffixed": ["lake-mart-qa/student_risk_probability"],
        "unreadable_databases": ["ol_warehouse_production_mart"],
        "outcomes": outcomes,
    }
    return SweepResult(**(fields | overrides))


def _returning(
    monkeypatch: pytest.MonkeyPatch, result: SweepResult
) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(module, "_aws_clients", lambda: ("glue", "s3"))

    def fake_sweep(_glue: str, _s3: str, **kwargs: Any) -> SweepResult:
        calls.append(kwargs)
        return result

    monkeypatch.setattr(module, "sweep_warehouse", fake_sweep)
    return calls


def _metadata(config: LakeOrphanSweepConfig) -> dict[str, Any]:
    output = lake_orphan_sweep(build_asset_context(), config)
    return {key: value.value for key, value in output.metadata.items()}


def test_no_environment_deletes_yet():
    assert frozenset() == module.LAKE_ORPHAN_SWEEP_DELETE_ENVIRONMENTS


def test_config_has_no_default_minimum_age():
    with pytest.raises(Exception, match="min_age_days"):
        LakeOrphanSweepConfig()


def test_delete_is_refused_where_it_is_not_enabled(monkeypatch: pytest.MonkeyPatch):
    calls = _returning(monkeypatch, _result(None))

    with pytest.raises(ValueError, match="Deletion is not enabled"):
        lake_orphan_sweep(
            build_asset_context(), LakeOrphanSweepConfig(min_age_days=7, delete=True)
        )

    assert calls == []


def test_report_run_says_delete_did_not_run(monkeypatch: pytest.MonkeyPatch):
    calls = _returning(monkeypatch, _result(None))

    metadata = _metadata(LakeOrphanSweepConfig(min_age_days=7))

    assert calls[0]["delete"] is False
    assert calls[0]["min_age_days"] == 7
    assert metadata["delete_status"] == "not run (report only)"
    assert not [key for key in metadata if key.startswith("deleted_")]
    assert metadata["orphan_prefixes"] == len(ORPHANS)
    assert metadata["eligible_prefixes"] == 1
    assert metadata["eligible_bytes"] == ORPHANS[0]["bytes"]
    assert metadata["orphan_details"][0]["path"] == f"s3://lake-mart-qa/gone-{UUID}/"
    assert metadata["unsuffixed_unreferenced_prefixes"] == 1
    assert metadata["unreadable_glue_databases"] == ["ol_warehouse_production_mart"]


def test_delete_run_reports_what_it_deleted_and_what_it_kept(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        module, "LAKE_ORPHAN_SWEEP_DELETE_ENVIRONMENTS", frozenset({module.DAGSTER_ENV})
    )
    outcomes = [
        PrefixOutcome("lake-mart-qa", f"gone-{UUID}", "deleted", objects=2, bytes=105),
        PrefixOutcome(
            "lake-mart-qa", f"late-{UUID}", "skipped", "now referenced by Glue"
        ),
    ]
    calls = _returning(monkeypatch, _result(outcomes))

    metadata = _metadata(LakeOrphanSweepConfig(min_age_days=7, delete=True))

    assert calls[0]["delete"] is True
    assert metadata["delete_status"] == "ran"
    assert metadata["deleted_prefixes"] == 1
    assert metadata["deleted_bytes"] == ORPHANS[0]["bytes"]
    assert [row["path"] for row in metadata["deleted_details"]] == [
        f"s3://lake-mart-qa/gone-{UUID}/"
    ]
    assert metadata["kept_at_delete_time"] == [
        f"s3://lake-mart-qa/late-{UUID}/: now referenced by Glue"
    ]


def test_delete_errors_fail_the_run(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        module, "LAKE_ORPHAN_SWEEP_DELETE_ENVIRONMENTS", frozenset({module.DAGSTER_ENV})
    )
    outcomes = [
        PrefixOutcome(
            "lake-mart-qa",
            f"gone-{UUID}",
            "deleted",
            objects=2,
            bytes=105,
            errors=["k: AccessDenied no"],
        )
    ]
    _returning(monkeypatch, _result(outcomes))

    with pytest.raises(RuntimeError, match="could not be deleted"):
        lake_orphan_sweep(
            build_asset_context(), LakeOrphanSweepConfig(min_age_days=7, delete=True)
        )


def test_a_scope_that_resolves_to_nothing_fails(monkeypatch: pytest.MonkeyPatch):
    _returning(monkeypatch, _result(None, targets=[], orphans=[], unsuffixed=[]))

    with pytest.raises(RuntimeError, match="nothing to scan"):
        lake_orphan_sweep(build_asset_context(), LakeOrphanSweepConfig(min_age_days=7))
