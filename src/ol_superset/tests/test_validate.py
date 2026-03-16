"""Tests for column case-sensitivity checks in the validate command."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ol_superset.commands.validate import (
    _validate_asset_references,
    _validate_dbt_chain,
)
from ol_superset.lib.asset_index import build_asset_index

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data))


def _make_dbt_project(tmp_path: Path, model_columns: list[str]) -> Path:
    """Minimal dbt project with one reporting model."""
    dbt_dir = tmp_path / "ol_dbt"
    models_dir = dbt_dir / "models" / "reporting"
    models_dir.mkdir(parents=True)

    _write_yaml(
        dbt_dir / "dbt_project.yml",
        {
            "name": "open_learning",
            "models": {"open_learning": {"reporting": {"+schema": "reporting"}}},
        },
    )
    _write_yaml(
        dbt_dir / "profiles.yml",
        {
            "open_learning": {
                "outputs": {"production": {"schema": "ol_warehouse_production"}}
            }
        },
    )
    _write_yaml(
        models_dir / "_reporting__models.yml",
        {
            "version": 2,
            "models": [
                {
                    "name": "my_report",
                    "columns": [{"name": c} for c in model_columns],
                }
            ],
        },
    )
    return dbt_dir


def _make_assets(
    tmp_path: Path,
    dataset_columns: list[str],
    chart_groupby: list[str],
    *,
    dataset_sql: str | None = None,
) -> Path:
    """Minimal assets dir: one dataset and one chart linked to it."""
    assets_dir = tmp_path / "assets"
    (assets_dir / "dashboards").mkdir(parents=True)

    _write_yaml(
        assets_dir / "datasets" / "Trino" / "my_report_aaaa1111.yaml",
        {
            "uuid": "aaaa-1111-1111-1111-111111111111",
            "table_name": "my_report",
            "schema": "ol_warehouse_production_reporting",
            "catalog": "ol_data_lake_production",
            "sql": dataset_sql,
            "columns": [{"column_name": c} for c in dataset_columns],
        },
    )
    _write_yaml(
        assets_dir / "charts" / "My_Chart_bbbb2222.yaml",
        {
            "uuid": "bbbb-2222-2222-2222-222222222222",
            "slice_name": "My Chart",
            "dataset_uuid": "aaaa-1111-1111-1111-111111111111",
            "params": {"groupby": chart_groupby},
        },
    )
    return assets_dir


# ---------------------------------------------------------------------------
# Step 4a — dataset column case vs dbt model
# ---------------------------------------------------------------------------


class TestDatasetColumnCaseVsDbt:
    def test_wrong_case_dataset_col_vs_dbt_model_is_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Dataset YAML with mixed-case column vs lowercase dbt model → error."""
        dbt_dir = _make_dbt_project(tmp_path, model_columns=["dedp_course_cert_count"])
        assets_dir = _make_assets(
            tmp_path,
            dataset_columns=["DEDP_Course_Cert_Count"],  # wrong case in dataset YAML
            chart_groupby=["DEDP_Course_Cert_Count"],
        )
        index = build_asset_index(assets_dir)
        errors, _warnings = _validate_dbt_chain(index, dbt_dir, 0)

        out = capsys.readouterr().out
        assert errors > 0
        assert "DEDP_Course_Cert_Count" in out
        assert "dedp_course_cert_count" in out
        assert "wrong case" in out

    def test_correct_case_dataset_col_vs_dbt_model_is_clean(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Dataset YAML with lowercase column matching dbt model → no error."""
        dbt_dir = _make_dbt_project(tmp_path, model_columns=["dedp_course_cert_count"])
        assets_dir = _make_assets(
            tmp_path,
            dataset_columns=["dedp_course_cert_count"],  # correct case
            chart_groupby=["dedp_course_cert_count"],
        )
        index = build_asset_index(assets_dir)
        errors, _warnings = _validate_dbt_chain(index, dbt_dir, 0)

        assert errors == 0

    def test_undocumented_col_not_in_dbt_is_warning_not_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Dataset column with no match in dbt model at all → warning, not error."""
        dbt_dir = _make_dbt_project(tmp_path, model_columns=["user_email"])
        assets_dir = _make_assets(
            tmp_path,
            dataset_columns=["user_email", "completely_unknown_column"],
            chart_groupby=["user_email"],
        )
        index = build_asset_index(assets_dir)
        errors, warnings = _validate_dbt_chain(index, dbt_dir, 0)

        out = capsys.readouterr().out
        assert errors == 0
        assert warnings > 0
        assert "completely_unknown_column" in out
        assert "not documented" in out


# ---------------------------------------------------------------------------
# Step 3 — chart column refs vs dataset (always-running, no dbt dir needed)
# ---------------------------------------------------------------------------


class TestChartColVsSimpleDataset:
    def test_wrong_case_chart_col_vs_simple_dataset_is_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Chart groupby with wrong-case column vs lowercase dataset → error."""
        assets_dir = _make_assets(
            tmp_path,
            dataset_columns=["dedp_course_cert_count"],
            chart_groupby=["DEDP_Course_Cert_Count"],  # wrong case in chart
        )
        errors, _warnings, _index = _validate_asset_references(assets_dir, 0)

        out = capsys.readouterr().out
        assert errors > 0
        assert "DEDP_Course_Cert_Count" in out
        assert "dedp_course_cert_count" in out

    def test_multiple_wrong_case_chart_cols_all_reported(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """All wrong-case column refs are each reported as separate errors."""
        assets_dir = _make_assets(
            tmp_path,
            dataset_columns=[
                "any_dedp_program_cert_ind",
                "dedp_international_program_cert_ind",
            ],
            chart_groupby=[
                "Any_DEDP_Program_Cert_Ind",
                "DEDP_International_Program_Cert_Ind",
            ],
        )
        errors, _warnings, _index = _validate_asset_references(assets_dir, 0)

        out = capsys.readouterr().out
        assert errors >= 2
        assert "Any_DEDP_Program_Cert_Ind" in out
        assert "DEDP_International_Program_Cert_Ind" in out

    def test_correct_case_chart_col_vs_simple_dataset_is_clean(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Chart groupby with exact-match column → no error."""
        assets_dir = _make_assets(
            tmp_path,
            dataset_columns=["dedp_course_cert_count"],
            chart_groupby=["dedp_course_cert_count"],
        )
        errors, _warnings, _index = _validate_asset_references(assets_dir, 0)

        assert errors == 0

    def test_truly_missing_chart_col_vs_simple_dataset_is_warning(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Chart refs column absent from dataset entirely → warning, not error."""
        assets_dir = _make_assets(
            tmp_path,
            dataset_columns=["user_email"],
            chart_groupby=["nonexistent_column"],
        )
        errors, warnings, _index = _validate_asset_references(assets_dir, 0)

        out = capsys.readouterr().out
        assert errors == 0
        assert warnings > 0
        assert "nonexistent_column" in out
        assert "not found in dataset" in out


class TestChartColVsVirtualDataset:
    def test_wrong_case_chart_col_vs_virtual_dataset_is_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Chart groupby with wrong-case column vs virtual dataset SQL → error."""
        virtual_sql = (
            "SELECT u.user_email, "
            "COUNT(DISTINCT c.course_readable_id) AS dedp_course_cert_count "
            "FROM ol_warehouse_production_mart.marts__combined__users u "
            "LEFT JOIN ol_warehouse_production_mart.my_report c "
            "ON u.user_hashed_id = c.user_hashed_id "
            "GROUP BY u.user_email"
        )
        assets_dir = _make_assets(
            tmp_path,
            dataset_columns=["user_email", "dedp_course_cert_count"],
            chart_groupby=["DEDP_Course_Cert_Count"],  # wrong case
            dataset_sql=virtual_sql,
        )
        errors, _warnings, _index = _validate_asset_references(assets_dir, 0)

        out = capsys.readouterr().out
        assert errors > 0
        assert "DEDP_Course_Cert_Count" in out
        assert "dedp_course_cert_count" in out

    def test_correct_case_chart_col_vs_virtual_dataset_is_clean(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Chart groupby lowercase column matching virtual dataset SQL → no error."""
        virtual_sql = (
            "SELECT u.user_email "
            "FROM ol_warehouse_production_mart.marts__combined__users u "
        )
        assets_dir = _make_assets(
            tmp_path,
            dataset_columns=["user_email"],
            chart_groupby=["user_email"],
            dataset_sql=virtual_sql,
        )
        errors, _warnings, _index = _validate_asset_references(assets_dir, 0)

        assert errors == 0

    def test_truly_missing_chart_col_vs_virtual_dataset_is_warning(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Chart refs column absent from virtual SQL output → warning, not error."""
        virtual_sql = (
            "SELECT u.user_email "
            "FROM ol_warehouse_production_mart.marts__combined__users u"
        )
        assets_dir = _make_assets(
            tmp_path,
            dataset_columns=["user_email"],
            chart_groupby=["nonexistent_column"],
            dataset_sql=virtual_sql,
        )
        errors, warnings, _index = _validate_asset_references(assets_dir, 0)

        out = capsys.readouterr().out
        assert errors == 0
        assert warnings > 0
        assert "nonexistent_column" in out
        assert "not found in virtual dataset" in out
