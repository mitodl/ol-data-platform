"""Tests for the export ignore-list (.exportignore) pruning."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ol_superset.lib.export_ignore import (
    load_ignore_patterns,
    plan_prune,
    prune_ignored_assets,
)


def _write_yaml(path: Path, data: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data))


def _dashboard(uuid: str, title: str, chart_uuids: list[str]) -> dict[str, object]:
    position = {
        f"CHART-{cu}": {"type": "CHART", "meta": {"uuid": cu}} for cu in chart_uuids
    }
    return {"uuid": uuid, "dashboard_title": title, "position": position}


def _chart(uuid: str, name: str, dataset_uuid: str = "ds-1") -> dict[str, object]:
    return {
        "uuid": uuid,
        "slice_name": name,
        "dataset_uuid": dataset_uuid,
        "params": {},
    }


@pytest.fixture
def assets_dir(tmp_path: Path) -> Path:
    base = tmp_path / "assets"
    # A tracked dashboard sharing one chart with the ignored dashboard.
    _write_yaml(
        base / "dashboards" / "Keep_Me_d1.yaml",
        _dashboard("d1", "Keep Me", ["c_keep", "c_shared"]),
    )
    # The ignored MM_Fin dashboard, referencing an orphan chart + a shared chart.
    _write_yaml(
        base / "dashboards" / "MM_Fin_Main_d2.yaml",
        _dashboard("d2", "MM_Fin_Main_Dashboard", ["c_orphan", "c_shared"]),
    )
    _write_yaml(base / "charts" / "Keep_c_keep.yaml", _chart("c_keep", "Keep Chart"))
    _write_yaml(base / "charts" / "Shared_c_shared.yaml", _chart("c_shared", "Shared"))
    _write_yaml(
        base / "charts" / "MM_Fin_Orphan_c_orphan.yaml",
        _chart("c_orphan", "MM_Fin_Orphan_Chart"),
    )
    # A dataset must never be pruned.
    _write_yaml(
        base / "datasets" / "Trino" / "orders_ds1.yaml",
        {"uuid": "ds-1", "table_name": "marts__orders", "schema": "s", "catalog": "c"},
    )
    return base


def test_load_ignore_patterns(tmp_path: Path) -> None:
    ignore = tmp_path / ".exportignore"
    ignore.write_text("# a comment\nMM_Fin_*\n\n  Foo_*  \n")
    assert load_ignore_patterns(ignore) == ["MM_Fin_*", "Foo_*"]


def test_load_ignore_patterns_missing(tmp_path: Path) -> None:
    assert load_ignore_patterns(tmp_path / "nope") == []


def test_empty_patterns_prune_nothing(assets_dir: Path) -> None:
    report = plan_prune(assets_dir, [])
    assert report.total == 0


def test_prunes_matched_dashboard_and_orphan_chart(assets_dir: Path) -> None:
    report = plan_prune(assets_dir, ["MM_Fin_*"])
    removed = {p.name for p in (*report.removed_dashboards, *report.removed_charts)}
    # Ignored dashboard removed.
    assert "MM_Fin_Main_d2.yaml" in removed
    # Its orphan chart removed (matched by name and orphaned).
    assert "MM_Fin_Orphan_c_orphan.yaml" in removed
    # Shared chart KEPT — still referenced by the surviving dashboard.
    assert "Shared_c_shared.yaml" not in removed
    # Unrelated dashboard/chart kept.
    assert "Keep_Me_d1.yaml" not in removed
    assert "Keep_c_keep.yaml" not in removed


def test_orphan_only_when_all_parents_removed(tmp_path: Path) -> None:
    # A chart named benignly, referenced only by the ignored dashboard, is
    # removed as an orphan even though its own name does not match.
    base = tmp_path / "assets"
    _write_yaml(
        base / "dashboards" / "MM_Fin_d.yaml",
        _dashboard("d", "MM_Fin_Board", ["benign"]),
    )
    _write_yaml(base / "charts" / "Benign_benign.yaml", _chart("benign", "Benign"))
    report = plan_prune(base, ["MM_Fin_*"])
    names = {p.name for p in report.removed_charts}
    assert "Benign_benign.yaml" in names
    assert report.orphaned_charts
    assert report.orphaned_charts[0].name == "Benign_benign.yaml"


def test_surviving_reference_keeps_directly_matched_chart(tmp_path: Path) -> None:
    # A chart whose own name matches the pattern but which is still referenced
    # by a surviving dashboard must be KEPT — pruning it would leave a dangling
    # Dashboard -> Chart reference and fail `ol-superset validate`.
    base = tmp_path / "assets"
    _write_yaml(
        base / "dashboards" / "MM_Fin_ignored_d.yaml",
        _dashboard("d_ignored", "MM_Fin_Board", ["c_match"]),
    )
    _write_yaml(
        base / "dashboards" / "Keep_d.yaml",
        _dashboard("d_keep", "Keep Me", ["c_match"]),
    )
    _write_yaml(
        base / "charts" / "MM_Fin_Shared_c_match.yaml",
        _chart("c_match", "MM_Fin_Shared_Chart"),
    )
    report = plan_prune(base, ["MM_Fin_*"])
    removed = {p.name for p in report.removed_charts}
    # Ignored dashboard removed, but the shared chart survives with its keeper.
    assert {p.name for p in report.removed_dashboards} == {"MM_Fin_ignored_d.yaml"}
    assert "MM_Fin_Shared_c_match.yaml" not in removed


def test_match_by_uuid(tmp_path: Path) -> None:
    base = tmp_path / "assets"
    _write_yaml(
        base / "charts" / "some_chart.yaml",
        _chart("abcd-1234-uuid", "Innocent Name"),
    )
    report = plan_prune(base, ["abcd-1234-uuid"])
    assert {p.name for p in report.removed_charts} == {"some_chart.yaml"}


def test_datasets_never_pruned(assets_dir: Path) -> None:
    # Even a pattern that would match the dataset table/uuid leaves it in place.
    prune_ignored_assets(assets_dir, ["ds-1", "marts__orders", "*"])
    assert (assets_dir / "datasets" / "Trino" / "orders_ds1.yaml").exists()


def test_prune_deletes_files_and_dry_run_does_not(assets_dir: Path) -> None:
    orphan = assets_dir / "charts" / "MM_Fin_Orphan_c_orphan.yaml"
    dash = assets_dir / "dashboards" / "MM_Fin_Main_d2.yaml"

    dry = prune_ignored_assets(assets_dir, ["MM_Fin_*"], dry_run=True)
    assert dry.total > 0
    assert orphan.exists() and dash.exists()  # dry-run leaves files

    prune_ignored_assets(assets_dir, ["MM_Fin_*"])
    assert not orphan.exists()
    assert not dash.exists()
    assert (assets_dir / "dashboards" / "Keep_Me_d1.yaml").exists()
