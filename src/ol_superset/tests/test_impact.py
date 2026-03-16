"""Tests for the impact command logic (non-git parts)."""

from __future__ import annotations

from pathlib import Path

from ol_superset.commands.impact import (
    _build_reverse_index,
    _ChangedAssets,
    _classify_changed_files,
    _extract_uuid,
    _find_orphans,
    _parse_dbt_schema_model_names,
    _trace_impact,
)
from ol_superset.lib.asset_index import (
    AssetIndex,
    ChartAsset,
    DashboardAsset,
    DatasetAsset,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mk_dataset(
    uuid: str, table_name: str, schema: str = "s", sql: str | None = None
) -> DatasetAsset:
    return DatasetAsset(
        uuid=uuid,
        table_name=table_name,
        schema=schema,
        catalog="c",
        database="Trino",
        sql=sql,
        path=Path(f"{table_name}_{uuid}.yaml"),
    )


def _mk_chart(uuid: str, name: str, dataset_uuid: str | None) -> ChartAsset:
    return ChartAsset(
        uuid=uuid,
        name=name,
        dataset_uuid=dataset_uuid,
        path=Path(f"{name}_{uuid}.yaml"),
    )


def _mk_dashboard(uuid: str, title: str, chart_uuids: set[str]) -> DashboardAsset:
    return DashboardAsset(
        uuid=uuid,
        title=title,
        chart_uuids=chart_uuids,
        path=Path(f"{title}_{uuid}.yaml"),
    )


def _mk_index() -> AssetIndex:
    """
    Dataset A (model_a) → Chart 1, Chart 2 → Dashboard X
    Dataset B (model_b) → Chart 3          → Dashboard X, Dashboard Y
    Dataset C (virtual) → Chart 4          → Dashboard Y
    Dataset D (orphan)  → (no charts)
    Chart 5 (orphan)    → (not in any dashboard)
    """
    ds_a = _mk_dataset("aaaa-0000-0000-0000-000000000001", "model_a")
    ds_b = _mk_dataset("bbbb-0000-0000-0000-000000000002", "model_b")
    ds_c = _mk_dataset(
        "cccc-0000-0000-0000-000000000003", "virtual_ds", sql="SELECT 1 AS x"
    )
    ds_d = _mk_dataset("dddd-0000-0000-0000-000000000004", "orphan_ds")

    ch1 = _mk_chart("1111-0000-0000-0000-000000000001", "Chart 1", ds_a.uuid)
    ch2 = _mk_chart("2222-0000-0000-0000-000000000002", "Chart 2", ds_a.uuid)
    ch3 = _mk_chart("3333-0000-0000-0000-000000000003", "Chart 3", ds_b.uuid)
    ch4 = _mk_chart("4444-0000-0000-0000-000000000004", "Chart 4", ds_c.uuid)
    ch5 = _mk_chart("5555-0000-0000-0000-000000000005", "Chart 5 (orphan)", ds_b.uuid)

    dash_x = _mk_dashboard(
        "xxxx-0000-0000-0000-000000000001",
        "Dashboard X",
        {ch1.uuid, ch2.uuid, ch3.uuid},
    )
    dash_y = _mk_dashboard(
        "yyyy-0000-0000-0000-000000000002",
        "Dashboard Y",
        {ch3.uuid, ch4.uuid},
    )

    return AssetIndex(
        datasets={ds.uuid: ds for ds in [ds_a, ds_b, ds_c, ds_d]},
        charts={ch.uuid: ch for ch in [ch1, ch2, ch3, ch4, ch5]},
        dashboards={dash.uuid: dash for dash in [dash_x, dash_y]},
    )


# ---------------------------------------------------------------------------
# _extract_uuid
# ---------------------------------------------------------------------------


class TestExtractUuid:
    def test_extracts_uuid_from_stem(self) -> None:
        stem = "marts__combined_enrollment_detail_abcdef12-1234-5678-abcd-ef1234567890"
        assert _extract_uuid(stem) == "abcdef12-1234-5678-abcd-ef1234567890"

    def test_returns_none_when_no_uuid(self) -> None:
        assert _extract_uuid("no_uuid_here") is None

    def test_extracts_uuid_with_underscores_in_name(self) -> None:
        stem = "some_long_model_name_with_parts_aabbccdd-aabb-ccdd-eeff-001122334455"
        assert _extract_uuid(stem) == "aabbccdd-aabb-ccdd-eeff-001122334455"


# ---------------------------------------------------------------------------
# _build_reverse_index
# ---------------------------------------------------------------------------


class TestBuildReverseIndex:
    def test_model_to_datasets(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        assert "aaaa-0000-0000-0000-000000000001" in ri.model_to_datasets["model_a"]
        assert "bbbb-0000-0000-0000-000000000002" in ri.model_to_datasets["model_b"]

    def test_virtual_dataset_not_in_model_to_datasets(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        # virtual_ds has sql set, so it should NOT appear in model_to_datasets
        assert "cccc-0000-0000-0000-000000000003" not in ri.model_to_datasets.get(
            "virtual_ds", []
        )

    def test_dataset_to_charts(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        charts_for_a = ri.dataset_to_charts["aaaa-0000-0000-0000-000000000001"]
        assert "1111-0000-0000-0000-000000000001" in charts_for_a
        assert "2222-0000-0000-0000-000000000002" in charts_for_a

    def test_chart_to_dashboards(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        # Chart 3 appears in both dashboards
        dashes = ri.chart_to_dashboards["3333-0000-0000-0000-000000000003"]
        assert "xxxx-0000-0000-0000-000000000001" in dashes
        assert "yyyy-0000-0000-0000-000000000002" in dashes

    def test_referenced_sets(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        # Chart 5 is NOT in any dashboard → not in referenced_chart_uuids
        assert "5555-0000-0000-0000-000000000005" not in ri.referenced_chart_uuids
        # Dataset D is not referenced by any chart
        assert "dddd-0000-0000-0000-000000000004" not in ri.referenced_dataset_uuids


# ---------------------------------------------------------------------------
# _trace_impact
# ---------------------------------------------------------------------------


class TestTraceImpact:
    def test_dbt_model_change_propagates_to_dashboards(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        changed = _ChangedAssets(dbt_models=["model_a"])
        impact = _trace_impact(changed, index, ri)

        assert "aaaa-0000-0000-0000-000000000001" in impact.datasets
        assert "1111-0000-0000-0000-000000000001" in impact.charts
        assert "2222-0000-0000-0000-000000000002" in impact.charts
        assert "xxxx-0000-0000-0000-000000000001" in impact.dashboards

    def test_dbt_model_change_does_not_affect_unrelated_dashboard(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        changed = _ChangedAssets(dbt_models=["model_a"])
        impact = _trace_impact(changed, index, ri)
        # Dashboard Y only has charts from model_b and virtual_ds
        assert "yyyy-0000-0000-0000-000000000002" not in impact.dashboards

    def test_dataset_change_traces_to_two_dashboards(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        # model_b → Chart 3 → Dashboard X + Dashboard Y
        changed = _ChangedAssets(dbt_models=["model_b"])
        impact = _trace_impact(changed, index, ri)
        assert "xxxx-0000-0000-0000-000000000001" in impact.dashboards
        assert "yyyy-0000-0000-0000-000000000002" in impact.dashboards

    def test_direct_dataset_uuid_change(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        changed = _ChangedAssets(dataset_uuids=["aaaa-0000-0000-0000-000000000001"])
        impact = _trace_impact(changed, index, ri)
        assert "1111-0000-0000-0000-000000000001" in impact.charts
        assert "xxxx-0000-0000-0000-000000000001" in impact.dashboards

    def test_direct_chart_uuid_change(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        changed = _ChangedAssets(chart_uuids=["1111-0000-0000-0000-000000000001"])
        impact = _trace_impact(changed, index, ri)
        assert "xxxx-0000-0000-0000-000000000001" in impact.dashboards
        assert "yyyy-0000-0000-0000-000000000002" not in impact.dashboards

    def test_direct_dashboard_uuid_change(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        changed = _ChangedAssets(dashboard_uuids=["xxxx-0000-0000-0000-000000000001"])
        impact = _trace_impact(changed, index, ri)
        assert "xxxx-0000-0000-0000-000000000001" in impact.dashboards

    def test_no_changes_yields_empty_impact(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        changed = _ChangedAssets()
        impact = _trace_impact(changed, index, ri)
        assert not impact.datasets
        assert not impact.charts
        assert not impact.dashboards

    def test_unknown_model_yields_empty_impact(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        changed = _ChangedAssets(dbt_models=["nonexistent_model"])
        impact = _trace_impact(changed, index, ri)
        assert not impact.datasets


# ---------------------------------------------------------------------------
# _find_orphans
# ---------------------------------------------------------------------------


class TestFindOrphans:
    def test_finds_orphaned_chart(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        orphans = _find_orphans(index, ri)
        orphan_uuids = {ch.uuid for ch in orphans.charts}
        assert "5555-0000-0000-0000-000000000005" in orphan_uuids

    def test_finds_orphaned_dataset(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        orphans = _find_orphans(index, ri)
        orphan_uuids = {ds.uuid for ds in orphans.datasets}
        assert "dddd-0000-0000-0000-000000000004" in orphan_uuids

    def test_referenced_chart_is_not_orphan(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        orphans = _find_orphans(index, ri)
        orphan_uuids = {ch.uuid for ch in orphans.charts}
        # Chart 1 is in Dashboard X
        assert "1111-0000-0000-0000-000000000001" not in orphan_uuids

    def test_referenced_dataset_is_not_orphan(self) -> None:
        index = _mk_index()
        ri = _build_reverse_index(index)
        orphans = _find_orphans(index, ri)
        orphan_uuids = {ds.uuid for ds in orphans.datasets}
        assert "aaaa-0000-0000-0000-000000000001" not in orphan_uuids

    def test_no_orphans_in_clean_index(self) -> None:
        ds = _mk_dataset("aaaa-0000-0000-0000-aaaaaaaaaaaa", "model_a")
        ch = _mk_chart("bbbb-0000-0000-0000-bbbbbbbbbbbb", "Chart", ds.uuid)
        db = _mk_dashboard("cccc-0000-0000-0000-cccccccccccc", "Dash", {ch.uuid})
        index = AssetIndex(
            datasets={ds.uuid: ds},
            charts={ch.uuid: ch},
            dashboards={db.uuid: db},
        )
        ri = _build_reverse_index(index)
        orphans = _find_orphans(index, ri)
        assert not orphans.charts
        assert not orphans.datasets


# ---------------------------------------------------------------------------
# _classify_changed_files
# ---------------------------------------------------------------------------


class TestClassifyChangedFiles:
    def _run(
        self,
        files: list[str],
        repo_root: Path = Path("/repo"),
        assets_dir: Path = Path("/repo/src/ol_superset/assets"),
        dbt_dir: Path | None = Path("/repo/src/ol_dbt"),
    ) -> _ChangedAssets:
        return _classify_changed_files(files, repo_root, assets_dir, dbt_dir)

    def test_classifies_dataset_yaml(self) -> None:
        files = [
            "src/ol_superset/assets/datasets/Trino/"
            "marts__combined_course_bbbb1234-0000-0000-0000-000000000001.yaml"
        ]
        changed = self._run(files)
        assert "bbbb1234-0000-0000-0000-000000000001" in changed.dataset_uuids

    def test_classifies_chart_yaml(self) -> None:
        files = [
            "src/ol_superset/assets/charts/"
            "My_Chart_cccc5678-0000-0000-0000-000000000002.yaml"
        ]
        changed = self._run(files)
        assert "cccc5678-0000-0000-0000-000000000002" in changed.chart_uuids

    def test_classifies_dashboard_yaml(self) -> None:
        files = [
            "src/ol_superset/assets/dashboards/"
            "My_Dash_dddd9012-0000-0000-0000-000000000003.yaml"
        ]
        changed = self._run(files)
        assert "dddd9012-0000-0000-0000-000000000003" in changed.dashboard_uuids

    def test_classifies_dbt_sql_model(self) -> None:
        files = ["src/ol_dbt/models/marts/combined/marts__orders.sql"]
        changed = self._run(files)
        assert "marts__orders" in changed.dbt_models

    def test_other_files_go_to_other(self) -> None:
        files = ["pyproject.toml", "README.md"]
        changed = self._run(files)
        assert len(changed.other_files) == 2
        assert not changed.dbt_models
        assert not changed.dataset_uuids

    def test_no_dbt_dir_skips_dbt_sql(self) -> None:
        files = ["src/ol_dbt/models/marts/combined/marts__orders.sql"]
        changed = self._run(files, dbt_dir=None)
        assert not changed.dbt_models
        expected = "src/ol_dbt/models/marts/combined/marts__orders.sql"
        assert expected in changed.other_files


# ---------------------------------------------------------------------------
# _parse_dbt_schema_model_names
# ---------------------------------------------------------------------------


class TestParseDbtSchemaModelNames:
    def test_parses_model_names(self, tmp_path: Path) -> None:
        yaml_content = """
version: 2
models:
  - name: model_one
    description: First model
  - name: model_two
    description: Second model
"""
        p = tmp_path / "_models.yml"
        p.write_text(yaml_content)
        names = _parse_dbt_schema_model_names(p)
        assert "model_one" in names
        assert "model_two" in names

    def test_returns_empty_on_invalid_yaml(self, tmp_path: Path) -> None:
        p = tmp_path / "_bad.yml"
        p.write_text(":\nthis: {is: [broken")
        assert _parse_dbt_schema_model_names(p) == []

    def test_returns_empty_when_no_models_key(self, tmp_path: Path) -> None:
        p = tmp_path / "_sources.yml"
        p.write_text("version: 2\nsources:\n  - name: raw\n")
        assert _parse_dbt_schema_model_names(p) == []
