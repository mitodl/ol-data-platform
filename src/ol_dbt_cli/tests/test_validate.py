"""Tests for commands/validate.py — structural validation checks."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from ol_dbt_cli.commands.validate import (
    Severity,
    ValidationReport,
    _check_docs_coverage,
    _check_select_star,
    _check_yaml_integrity,
    _check_yaml_sql_sync,
    _print_json_report,
    _print_text_report,
    _resolve_model_targets,
    _resolve_star_with_registry,
)
from ol_dbt_cli.lib.sql_parser import ParsedModel
from ol_dbt_cli.lib.yaml_registry import (
    YamlColumn,
    YamlModel,
    YamlRegistry,
    YamlSource,
    YamlSourceTable,
    build_yaml_registry,
)


def _simple_registry(*col_names: str, model_name: str = "stg_users") -> YamlRegistry:
    """Build a minimal YamlRegistry with one model."""
    return YamlRegistry(
        models={
            model_name: YamlModel(
                name=model_name,
                source_file=Path("_models.yml"),
                columns={n: YamlColumn(name=n) for n in col_names},
            )
        }
    )


@pytest.fixture()
def models_dir(tmp_path: Path) -> Path:
    staging = tmp_path / "staging"
    staging.mkdir()

    (staging / "_models.yml").write_text(
        textwrap.dedent("""
        version: 2
        models:
        - name: stg_users
          description: Users
          columns:
          - name: user_id
            description: int
          - name: user_email
            description: str
          - name: stale_col
            description: this column no longer exists in SQL
        """).strip()
    )

    (staging / "stg_users.sql").write_text("select id as user_id, email as user_email, name as user_name from raw")
    return tmp_path


class TestYamlSqlSync:
    def test_detects_phantom_column(self, models_dir: Path) -> None:
        yaml_registry = build_yaml_registry(models_dir)
        parsed = ParsedModel(
            name="stg_users",
            output_columns={"user_id", "user_email", "user_name"},
        )
        report = ValidationReport()
        _check_yaml_sql_sync("stg_users", yaml_registry, parsed, report)

        errors = [i for i in report.issues if i.severity == Severity.ERROR]
        assert any("stale_col" in i.message for i in errors)

    def test_detects_undocumented_column(self, models_dir: Path) -> None:
        yaml_registry = build_yaml_registry(models_dir)
        parsed = ParsedModel(
            name="stg_users",
            output_columns={"user_id", "user_email", "user_name"},
        )
        report = ValidationReport()
        _check_yaml_sql_sync("stg_users", yaml_registry, parsed, report)

        warnings = [i for i in report.issues if i.severity == Severity.WARNING]
        assert any("user_name" in i.message for i in warnings)

    def test_no_issues_when_in_sync(self) -> None:
        registry = _simple_registry("user_id", "user_email")
        parsed = ParsedModel(name="stg_users", output_columns={"user_id", "user_email"})
        report = ValidationReport()
        _check_yaml_sql_sync("stg_users", registry, parsed, report)
        assert len(report.errors) == 0
        assert len(report.warnings) == 0

    def test_unresolved_star_skips_phantom_check(self) -> None:
        """When SELECT * is unresolved, yaml_sql_sync must NOT flag YAML cols as phantom."""
        registry = _simple_registry("user_id", "user_email", "user_name")
        # has_star=True, output_columns empty -> 3 YAML cols would all appear phantom.
        # The check should skip and emit one WARNING instead.
        parsed = ParsedModel(name="stg_users", output_columns=set(), has_star=True)
        report = ValidationReport()
        _check_yaml_sql_sync("stg_users", registry, parsed, report)

        errors = [i for i in report.issues if i.severity == Severity.ERROR]
        assert len(errors) == 0, "Must not produce phantom-column errors for unresolved SELECT *"
        warnings = [i for i in report.issues if i.severity == Severity.WARNING]
        assert len(warnings) == 1
        assert "SELECT *" in warnings[0].message

    def test_resolved_star_runs_normal_checks(self) -> None:
        """When SELECT * was resolved via CTE analysis, yaml_sql_sync runs normally."""
        registry = _simple_registry("user_id", "user_email")
        parsed = ParsedModel(
            name="stg_users",
            output_columns={"user_id", "user_email"},
            has_star=False,
            star_resolved=True,
        )
        report = ValidationReport()
        _check_yaml_sql_sync("stg_users", registry, parsed, report)
        assert len(report.issues) == 0

    def test_resolved_star_still_detects_phantom(self) -> None:
        """Resolved SELECT * + phantom YAML col -> ERROR."""
        registry = _simple_registry("user_id", "user_email", "ghost_col")
        parsed = ParsedModel(
            name="stg_users",
            output_columns={"user_id", "user_email"},
            has_star=False,
            star_resolved=True,
        )
        report = ValidationReport()
        _check_yaml_sql_sync("stg_users", registry, parsed, report)
        errors = [i for i in report.issues if i.severity == Severity.ERROR]
        assert any("ghost_col" in i.message for i in errors)

    def test_unresolved_star_records_source(self) -> None:
        """star_source name appears in the WARNING message."""
        registry = _simple_registry("user_id")
        parsed = ParsedModel(
            name="stg_users",
            output_columns=set(),
            has_star=True,
            star_source="ref_stg_upstream",
        )
        report = ValidationReport()
        _check_yaml_sql_sync("stg_users", registry, parsed, report)
        assert any("ref_stg_upstream" in i.message for i in report.issues)


class TestSelectStar:
    def test_warns_on_unresolved_star(self) -> None:
        parsed = ParsedModel(name="stg_users", output_columns=set(), has_star=True)
        report = ValidationReport()
        _check_select_star("stg_users", parsed, report)
        assert len(report.issues) == 1
        assert report.issues[0].severity == Severity.WARNING

    def test_info_on_resolved_star(self) -> None:
        parsed = ParsedModel(
            name="stg_users",
            output_columns={"user_id"},
            has_star=False,
            star_resolved=True,
        )
        report = ValidationReport()
        _check_select_star("stg_users", parsed, report)
        assert len(report.issues) == 1
        assert report.issues[0].severity == Severity.INFO

    def test_no_issue_for_explicit_columns(self) -> None:
        parsed = ParsedModel(name="stg_users", output_columns={"user_id", "user_email"})
        report = ValidationReport()
        _check_select_star("stg_users", parsed, report)
        assert len(report.issues) == 0

    def test_star_source_in_warning(self) -> None:
        parsed = ParsedModel(
            name="stg_users",
            output_columns=set(),
            has_star=True,
            star_source="raw_users",
        )
        report = ValidationReport()
        _check_select_star("stg_users", parsed, report)
        assert any("raw_users" in i.message for i in report.issues)


class TestDocsCoverage:
    def test_flags_missing_yaml(self) -> None:
        registry = YamlRegistry()
        report = ValidationReport()
        _check_docs_coverage("undocumented_model", registry, report)
        assert len(report.issues) == 1
        assert report.issues[0].severity == Severity.WARNING

    def test_no_issue_when_documented(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        report = ValidationReport()
        _check_docs_coverage("stg_users", registry, report)
        assert len(report.issues) == 0


class TestYamlIntegrity:
    def test_flags_yaml_without_sql(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        report = ValidationReport()
        _check_yaml_integrity(registry, sql_model_names=set(), report=report)
        errors = [i for i in report.issues if i.severity == Severity.ERROR]
        assert any("stg_users" in i.model for i in errors)

    def test_no_issue_when_sql_exists(self, models_dir: Path) -> None:
        registry = build_yaml_registry(models_dir)
        report = ValidationReport()
        _check_yaml_integrity(registry, sql_model_names={"stg_users"}, report=report)
        assert len(report.issues) == 0


class TestErrorsOnly:
    def _mixed_report(self) -> ValidationReport:
        """Report with one ERROR and one WARNING."""
        report = ValidationReport()
        report.add("yaml_sql_sync", Severity.ERROR, "model_a", "phantom column: foo")
        report.add("docs_coverage", Severity.WARNING, "model_b", "no YAML definition")
        report.add("select_star", Severity.INFO, "model_c", "SELECT * resolved via CTE")
        return report

    def test_json_errors_only_filters_non_errors(self, capsys) -> None:
        report = self._mixed_report()
        _print_json_report(report, errors_only=True)
        import json

        out = json.loads(capsys.readouterr().out)
        assert all(item["severity"] == "ERROR" for item in out)
        assert len(out) == 1

    def test_json_no_filter_includes_all(self, capsys) -> None:
        report = self._mixed_report()
        _print_json_report(report, errors_only=False)
        import json

        out = json.loads(capsys.readouterr().out)
        assert len(out) == 3

    def test_text_errors_only_hides_warnings(self, capsys) -> None:
        report = self._mixed_report()
        _print_text_report(report, errors_only=True)
        captured = capsys.readouterr().out
        assert "phantom column" in captured  # ERROR is shown
        assert "no YAML definition" not in captured  # WARNING suppressed
        assert "SELECT * resolved" not in captured  # INFO suppressed

    def test_text_all_clean_with_errors_only(self, capsys) -> None:
        """When only warnings exist and --errors-only is set, show the clean message."""
        report = ValidationReport()
        report.add("docs_coverage", Severity.WARNING, "model_b", "no YAML definition")
        _print_text_report(report, errors_only=True)
        captured = capsys.readouterr().out
        assert "No errors" in captured or "suppressed" in captured or "✅" in captured


class TestResolveModelTargets:
    """Tests for _resolve_model_targets."""

    def _make_tree(self, tmp_path: Path) -> tuple[Path, dict[str, Path]]:
        """Create a small models directory tree and return (models_dir, sql_file_map)."""
        staging = tmp_path / "staging" / "mitlearn"
        staging.mkdir(parents=True)
        marts = tmp_path / "marts"
        marts.mkdir()

        files = {
            "stg_users": staging / "stg_users.sql",
            "stg_orders": staging / "stg_orders.sql",
            "fct_revenue": marts / "fct_revenue.sql",
        }
        for path in files.values():
            path.write_text("select 1")

        sql_file_map = {stem: path for stem, path in files.items()}
        return tmp_path, sql_file_map

    def test_exact_name_match(self, tmp_path: Path) -> None:

        models_dir, sql_file_map = self._make_tree(tmp_path)
        resolved, unknown = _resolve_model_targets(("stg_users",), models_dir, sql_file_map)
        assert resolved == ["stg_users"]
        assert unknown == []

    def test_multiple_names(self, tmp_path: Path) -> None:

        models_dir, sql_file_map = self._make_tree(tmp_path)
        resolved, unknown = _resolve_model_targets(("stg_users", "fct_revenue"), models_dir, sql_file_map)
        assert set(resolved) == {"stg_users", "fct_revenue"}
        assert unknown == []

    def test_comma_separated_names(self, tmp_path: Path) -> None:

        models_dir, sql_file_map = self._make_tree(tmp_path)
        resolved, unknown = _resolve_model_targets(("stg_users,stg_orders",), models_dir, sql_file_map)
        assert set(resolved) == {"stg_users", "stg_orders"}
        assert unknown == []

    def test_directory_relative_to_models(self, tmp_path: Path) -> None:

        models_dir, sql_file_map = self._make_tree(tmp_path)
        # "staging/mitlearn" relative to models_dir
        resolved, unknown = _resolve_model_targets(("staging/mitlearn",), models_dir, sql_file_map)
        assert set(resolved) == {"stg_users", "stg_orders"}
        assert unknown == []

    def test_subdirectory_name_only(self, tmp_path: Path) -> None:

        models_dir, sql_file_map = self._make_tree(tmp_path)
        # bare "mitlearn" fuzzy-matches the staging/mitlearn subdir
        resolved, unknown = _resolve_model_targets(("mitlearn",), models_dir, sql_file_map)
        assert set(resolved) == {"stg_users", "stg_orders"}
        assert unknown == []

    def test_unknown_token_reported(self, tmp_path: Path) -> None:

        models_dir, sql_file_map = self._make_tree(tmp_path)
        resolved, unknown = _resolve_model_targets(("does_not_exist",), models_dir, sql_file_map)
        assert resolved == []
        assert unknown == ["does_not_exist"]

    def test_deduplication(self, tmp_path: Path) -> None:

        models_dir, sql_file_map = self._make_tree(tmp_path)
        # stg_users specified twice (once by name, once via its directory)
        resolved, _ = _resolve_model_targets(("stg_users", "staging/mitlearn"), models_dir, sql_file_map)
        assert resolved.count("stg_users") == 1


def _make_yaml_registry_with_source(source_name: str, table_name: str, cols: list[str]) -> YamlRegistry:
    """Build a YamlRegistry containing one source table with given columns."""
    yaml_cols = {c: YamlColumn(name=c) for c in cols}
    table = YamlSourceTable(name=table_name, source_name=source_name, columns=yaml_cols)
    source = YamlSource(name=source_name, source_file=Path("_sources.yml"), tables={table_name: table})
    return YamlRegistry(sources={source_name: source})


def _make_yaml_registry_with_model(model_name: str, cols: list[str]) -> YamlRegistry:
    """Build a YamlRegistry containing one model with given columns."""
    yaml_cols = {c: YamlColumn(name=c) for c in cols}
    model = YamlModel(name=model_name, source_file=Path("_models.yml"), columns=yaml_cols)
    return YamlRegistry(models={model_name: model})


class TestResolveStarWithRegistry:
    def test_resolves_via_source_yaml(self) -> None:
        """SELECT * from a source() is resolved using the source YAML declarations."""
        parsed = ParsedModel(
            name="stg_users",
            has_star=True,
            star_source="source_raw_users",
            source_placeholder_map={"source_raw_users": "raw.users"},
        )
        registry = _make_yaml_registry_with_source("raw", "users", ["id", "email", "is_active"])
        result = _resolve_star_with_registry(parsed, registry, manifest=None, sql_models_by_name={})
        assert result == {"id", "email", "is_active"}

    def test_resolves_via_ref_yaml(self) -> None:
        """SELECT * from a ref() is resolved using the upstream model's YAML columns."""
        parsed = ParsedModel(
            name="int_users",
            has_star=True,
            star_source="ref_stg_users",
            ref_placeholder_map={"ref_stg_users": "stg_users"},
        )
        registry = _make_yaml_registry_with_model("stg_users", ["user_id", "user_email"])
        result = _resolve_star_with_registry(parsed, registry, manifest=None, sql_models_by_name={})
        assert result == {"user_id", "user_email"}

    def test_resolves_via_upstream_parsed_sql(self) -> None:
        """When YAML has no columns, fall back to the upstream model's parsed SQL output."""
        parsed = ParsedModel(
            name="int_users",
            has_star=True,
            star_source="ref_stg_users",
            ref_placeholder_map={"ref_stg_users": "stg_users"},
        )
        # Registry has the model but no columns
        registry = _make_yaml_registry_with_model("stg_users", [])
        upstream = ParsedModel(name="stg_users", output_columns={"user_id", "user_email"})
        result = _resolve_star_with_registry(
            parsed, registry, manifest=None, sql_models_by_name={"stg_users": upstream}
        )
        assert result == {"user_id", "user_email"}

    def test_returns_none_when_source_unknown(self) -> None:
        """Returns None when the source is not declared in YAML."""
        parsed = ParsedModel(
            name="stg_users",
            has_star=True,
            star_source="source_raw_users",
            source_placeholder_map={"source_raw_users": "raw.users"},
        )
        result = _resolve_star_with_registry(parsed, YamlRegistry(), manifest=None, sql_models_by_name={})
        assert result is None

    def test_returns_none_for_plain_table(self) -> None:
        """A star from a bare table name (no ref/source mapping) stays unresolved."""
        parsed = ParsedModel(
            name="stg_users",
            has_star=True,
            star_source="raw_users",
        )
        result = _resolve_star_with_registry(parsed, YamlRegistry(), manifest=None, sql_models_by_name={})
        assert result is None


class TestBrokenRefColumns:
    """Tests for _check_broken_ref_columns — existing breakage detection."""

    def _make_parsed(
        self,
        name: str,
        sql_file: Path,
        refs: list[str],
        placeholder_map: dict[str, str],
        sql: str,
    ) -> ParsedModel:
        from ol_dbt_cli.lib.sql_parser import ParsedModel

        sql_file.write_text(sql)
        m = ParsedModel(name=name, refs=refs, ref_placeholder_map=placeholder_map)
        m.source_path = sql_file
        return m

    def test_detects_missing_column_qualified(self, tmp_path: Path) -> None:
        """ERROR when a qualified column read from ref() is absent from upstream output."""
        from ol_dbt_cli.commands.validate import _check_broken_ref_columns
        from ol_dbt_cli.lib.sql_parser import ParsedModel
        from ol_dbt_cli.lib.yaml_registry import YamlRegistry

        sql = """
        with src as (select * from ref_stg_users)
        select src.user_id, src.deleted_col from src
        """
        downstream = self._make_parsed(
            "downstream",
            tmp_path / "downstream.sql",
            refs=["stg_users"],
            placeholder_map={"ref_stg_users": "stg_users"},
            sql=sql,
        )
        upstream = ParsedModel(name="stg_users", output_columns={"user_id", "user_email"})
        report_obj = __import__("ol_dbt_cli.commands.validate", fromlist=["ValidationReport"]).ValidationReport()
        _check_broken_ref_columns(
            "downstream",
            downstream,
            YamlRegistry(),
            manifest=None,
            sql_models_by_name={"stg_users": upstream},
            report=report_obj,
        )
        errors = report_obj.errors
        assert len(errors) == 1
        assert errors[0].check == "broken_ref_columns"
        assert "deleted_col" in errors[0].message

    def test_detects_missing_column_unqualified(self, tmp_path: Path) -> None:
        """ERROR for unqualified column in passthrough CTE that doesn't exist upstream."""
        from ol_dbt_cli.commands.validate import _check_broken_ref_columns
        from ol_dbt_cli.lib.sql_parser import ParsedModel
        from ol_dbt_cli.lib.yaml_registry import YamlRegistry

        sql = """
        with src as (select * from ref_stg_users)
        select user_id, removed_col from src
        """
        downstream = self._make_parsed(
            "downstream",
            tmp_path / "downstream.sql",
            refs=["stg_users"],
            placeholder_map={"ref_stg_users": "stg_users"},
            sql=sql,
        )
        upstream = ParsedModel(name="stg_users", output_columns={"user_id", "user_email"})
        report_obj = __import__("ol_dbt_cli.commands.validate", fromlist=["ValidationReport"]).ValidationReport()
        _check_broken_ref_columns(
            "downstream",
            downstream,
            YamlRegistry(),
            manifest=None,
            sql_models_by_name={"stg_users": upstream},
            report=report_obj,
        )
        errors = report_obj.errors
        assert any(e.check == "broken_ref_columns" and "removed_col" in e.message for e in errors)

    def test_no_error_when_all_columns_exist(self, tmp_path: Path) -> None:
        """No error when all consumed columns are present in upstream output."""
        from ol_dbt_cli.commands.validate import _check_broken_ref_columns
        from ol_dbt_cli.lib.sql_parser import ParsedModel
        from ol_dbt_cli.lib.yaml_registry import YamlRegistry

        sql = """
        with src as (select * from ref_stg_users)
        select src.user_id, src.user_email from src
        """
        downstream = self._make_parsed(
            "downstream",
            tmp_path / "downstream.sql",
            refs=["stg_users"],
            placeholder_map={"ref_stg_users": "stg_users"},
            sql=sql,
        )
        upstream = ParsedModel(name="stg_users", output_columns={"user_id", "user_email"})
        report_obj = __import__("ol_dbt_cli.commands.validate", fromlist=["ValidationReport"]).ValidationReport()
        _check_broken_ref_columns(
            "downstream",
            downstream,
            YamlRegistry(),
            manifest=None,
            sql_models_by_name={"stg_users": upstream},
            report=report_obj,
        )
        assert not report_obj.errors

    def test_skips_when_upstream_unknown(self, tmp_path: Path) -> None:
        """Silently skips when the upstream model has no resolvable output columns."""
        from ol_dbt_cli.commands.validate import _check_broken_ref_columns
        from ol_dbt_cli.lib.yaml_registry import YamlRegistry

        sql = "select src.col_a from ref_stg_users src"
        downstream = self._make_parsed(
            "downstream",
            tmp_path / "downstream.sql",
            refs=["stg_users"],
            placeholder_map={"ref_stg_users": "stg_users"},
            sql=sql,
        )
        # Upstream not in sql_models_by_name and no manifest/YAML
        report_obj = __import__("ol_dbt_cli.commands.validate", fromlist=["ValidationReport"]).ValidationReport()
        _check_broken_ref_columns(
            "downstream",
            downstream,
            YamlRegistry(),
            manifest=None,
            sql_models_by_name={},
            report=report_obj,
        )
        assert not report_obj.issues  # no warning or error — skip silently

    def test_skips_when_consumed_columns_unknown(self, tmp_path: Path) -> None:
        """Silently skips when the downstream SQL cannot determine which columns are read."""
        from ol_dbt_cli.commands.validate import _check_broken_ref_columns
        from ol_dbt_cli.lib.sql_parser import ParsedModel
        from ol_dbt_cli.lib.yaml_registry import YamlRegistry

        # No SQL file path → get_columns_read_from_ref returns None
        downstream = ParsedModel(
            name="downstream",
            refs=["stg_users"],
            ref_placeholder_map={"ref_stg_users": "stg_users"},
        )
        upstream = ParsedModel(name="stg_users", output_columns={"user_id"})
        report_obj = __import__("ol_dbt_cli.commands.validate", fromlist=["ValidationReport"]).ValidationReport()
        _check_broken_ref_columns(
            "downstream",
            downstream,
            YamlRegistry(),
            manifest=None,
            sql_models_by_name={"stg_users": upstream},
            report=report_obj,
        )
        assert not report_obj.issues
