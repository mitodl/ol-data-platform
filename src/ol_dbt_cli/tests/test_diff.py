"""Tests for commands/diff.py — audit_helper diff-tool helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ol_dbt_cli.commands.diff import (
    DEFAULT_MATCH_THRESHOLD,
    DiffError,
    RelationsCompareResult,
    build_compare_relations_inline_sql,
    build_dbt_show_command,
    build_relation_expr,
    build_row_count_inline_sql,
    evaluate_compare_result,
    find_dbt_dir,
    is_true,
    parse_dbt_show_json,
    parse_relation_identifier,
)


class TestIsTrue:
    def test_python_bool_true(self) -> None:
        assert is_true(True) is True

    def test_python_bool_false(self) -> None:
        assert is_true(False) is False

    def test_int_one(self) -> None:
        assert is_true(1) is True

    def test_int_zero(self) -> None:
        assert is_true(0) is False

    def test_string_true_variants(self) -> None:
        for value in ("true", "True", "TRUE", "t", "1", "yes"):
            assert is_true(value) is True

    def test_string_false_variants(self) -> None:
        for value in ("false", "False", "f", "0", "no", ""):
            assert is_true(value) is False

    def test_none_is_false(self) -> None:
        assert is_true(None) is False

    def test_unrelated_type_is_false(self) -> None:
        assert is_true([1, 2, 3]) is False


class TestParseRelationIdentifier:
    def test_two_part_identifier(self) -> None:
        assert parse_relation_identifier("my_schema.my_table") == (None, "my_schema", "my_table")

    def test_three_part_identifier(self) -> None:
        assert parse_relation_identifier("my_db.my_schema.my_table") == ("my_db", "my_schema", "my_table")

    def test_invalid_identifier_raises(self) -> None:
        with pytest.raises(ValueError, match="schema.table"):
            parse_relation_identifier("just_a_table")

    def test_too_many_parts_raises(self) -> None:
        with pytest.raises(ValueError, match="schema.table"):
            parse_relation_identifier("a.b.c.d")


class TestBuildRelationExpr:
    def test_model_resolves_via_ref(self) -> None:
        assert build_relation_expr("my_model", None) == "ref('my_model')"

    def test_two_part_relation_resolves_via_adapter_get_relation(self) -> None:
        expr = build_relation_expr(None, "my_schema.my_table")
        assert "adapter.get_relation(" in expr
        assert "database=target.database" in expr
        assert "schema='my_schema'" in expr
        assert "identifier='my_table'" in expr

    def test_three_part_relation_uses_explicit_database(self) -> None:
        expr = build_relation_expr(None, "my_db.my_schema.my_table")
        assert "database='my_db'" in expr
        assert "schema='my_schema'" in expr
        assert "identifier='my_table'" in expr

    def test_both_given_raises(self) -> None:
        with pytest.raises(ValueError, match="Exactly one"):
            build_relation_expr("my_model", "my_schema.my_table")

    def test_neither_given_raises(self) -> None:
        with pytest.raises(ValueError, match="Exactly one"):
            build_relation_expr(None, None)


class TestBuildRowCountInlineSql:
    def test_contains_both_relation_exprs(self) -> None:
        sql = build_row_count_inline_sql("ref('old_model')", "ref('new_model')")
        assert "ref('old_model')" in sql
        assert "ref('new_model')" in sql
        assert "audit_helper.compare_row_counts" in sql

    def test_is_wrapped_in_jinja_braces(self) -> None:
        sql = build_row_count_inline_sql("ref('a')", "ref('b')")
        assert sql.startswith("{{")
        assert sql.endswith("}}")

    def test_supports_adapter_get_relation_exprs(self) -> None:
        a_expr = build_relation_expr(None, "old_schema.my_table")
        b_expr = build_relation_expr(None, "new_schema.my_table")
        sql = build_row_count_inline_sql(a_expr, b_expr)
        assert "old_schema" in sql
        assert "new_schema" in sql


class TestBuildCompareRelationsInlineSql:
    def test_basic_call_has_no_primary_key(self) -> None:
        sql = build_compare_relations_inline_sql("ref('old_model')", "ref('new_model')")
        assert "a_relation=ref('old_model')" in sql
        assert "b_relation=ref('new_model')" in sql
        assert "primary_key" not in sql
        assert "exclude_columns" not in sql

    def test_includes_primary_key_when_given(self) -> None:
        sql = build_compare_relations_inline_sql("ref('old_model')", "ref('new_model')", primary_key="user_id")
        assert "primary_key='user_id'" in sql

    def test_includes_exclude_columns_when_given(self) -> None:
        sql = build_compare_relations_inline_sql(
            "ref('old_model')", "ref('new_model')", exclude_columns=["updated_at", "loaded_at"]
        )
        assert "exclude_columns=['updated_at', 'loaded_at']" in sql

    def test_includes_both_primary_key_and_exclude_columns(self) -> None:
        sql = build_compare_relations_inline_sql(
            "ref('old_model')", "ref('new_model')", primary_key="id", exclude_columns=["loaded_at"]
        )
        assert "primary_key='id'" in sql
        assert "exclude_columns=['loaded_at']" in sql

    def test_supports_mixed_ref_and_adapter_get_relation(self) -> None:
        a_expr = build_relation_expr(None, "old_schema.my_table")
        b_expr = build_relation_expr("my_model", None)
        sql = build_compare_relations_inline_sql(a_expr, b_expr)
        assert "old_schema" in sql
        assert "ref('my_model')" in sql


class TestBuildDbtShowCommand:
    def test_minimal_command(self) -> None:
        cmd = build_dbt_show_command("{{ some_macro() }}", Path("/repo/src/ol_dbt"))
        assert cmd[:3] == ["dbt", "show", "--inline"]
        assert "{{ some_macro() }}" in cmd
        assert "--profiles-dir" in cmd
        assert "/repo/src/ol_dbt" in cmd
        assert "--output" in cmd
        assert "json" in cmd
        assert "--target" not in cmd
        assert "--vars" not in cmd

    def test_includes_target_when_given(self) -> None:
        cmd = build_dbt_show_command("{{ x() }}", Path("/repo/src/ol_dbt"), target="dev_production")
        idx = cmd.index("--target")
        assert cmd[idx + 1] == "dev_production"

    def test_includes_vars_when_given(self) -> None:
        cmd = build_dbt_show_command("{{ x() }}", Path("/repo/src/ol_dbt"), vars_arg='{"schema_suffix": "alice"}')
        idx = cmd.index("--vars")
        assert cmd[idx + 1] == '{"schema_suffix": "alice"}'

    def test_limit_defaults_to_unlimited(self) -> None:
        cmd = build_dbt_show_command("{{ x() }}", Path("/repo/src/ol_dbt"))
        idx = cmd.index("--limit")
        assert cmd[idx + 1] == "-1"


class TestParseDbtShowJson:
    def test_parses_show_payload_after_log_noise(self) -> None:
        stdout = "\n".join(
            [
                "12:00:00  Running with dbt=1.10.0",
                "12:00:01  Registered adapter: trino",
                json.dumps({"show": [{"in_a": True, "in_b": True, "percent_of_total": 100.0}]}),
            ]
        )
        rows = parse_dbt_show_json(stdout)
        assert rows == [{"in_a": True, "in_b": True, "percent_of_total": 100.0}]

    def test_raises_when_no_show_key_present(self) -> None:
        stdout = "\n".join(
            [
                "12:00:00  Running with dbt=1.10.0",
                json.dumps({"some_other_key": []}),
            ]
        )
        with pytest.raises(DiffError):
            parse_dbt_show_json(stdout)

    def test_raises_on_completely_unparseable_output(self) -> None:
        with pytest.raises(DiffError):
            parse_dbt_show_json("not json at all\njust log lines\n")

    def test_ignores_lines_that_look_like_json_but_arent(self) -> None:
        stdout = "\n".join(
            [
                "{not valid json",
                json.dumps({"show": [{"total_records": 42}]}),
            ]
        )
        rows = parse_dbt_show_json(stdout)
        assert rows == [{"total_records": 42}]


class TestRelationsCompareResult:
    def test_percent_matched_full_match(self) -> None:
        result = RelationsCompareResult(rows=[{"in_a": True, "in_b": True, "percent_of_total": 100.0}])
        assert result.percent_matched == 100.0
        assert result.only_in_a_percent is None
        assert result.only_in_b_percent is None

    def test_percent_matched_partial_match(self) -> None:
        rows = [
            {"in_a": True, "in_b": True, "percent_of_total": 95.5},
            {"in_a": True, "in_b": False, "percent_of_total": 3.0},
            {"in_a": False, "in_b": True, "percent_of_total": 1.5},
        ]
        result = RelationsCompareResult(rows=rows)
        assert result.percent_matched == 95.5
        assert result.only_in_a_percent == 3.0
        assert result.only_in_b_percent == 1.5

    def test_percent_matched_none_when_missing(self) -> None:
        result = RelationsCompareResult(rows=[{"in_a": True, "in_b": False, "percent_of_total": 100.0}])
        assert result.percent_matched is None

    def test_percent_matched_none_when_no_rows(self) -> None:
        result = RelationsCompareResult(rows=[])
        assert result.percent_matched is None

    def test_handles_string_booleans_from_json(self) -> None:
        rows = [{"in_a": "true", "in_b": "true", "percent_of_total": "100.0"}]
        result = RelationsCompareResult(rows=rows)
        assert result.percent_matched == 100.0


class TestEvaluateCompareResult:
    def test_passes_at_default_threshold_on_full_match(self) -> None:
        rows = [{"in_a": True, "in_b": True, "percent_of_total": 100.0}]
        passed, percent = evaluate_compare_result(rows)
        assert passed is True
        assert percent == 100.0

    def test_fails_below_default_threshold(self) -> None:
        rows = [
            {"in_a": True, "in_b": True, "percent_of_total": 99.0},
            {"in_a": True, "in_b": False, "percent_of_total": 1.0},
        ]
        passed, percent = evaluate_compare_result(rows)
        assert passed is False
        assert percent == 99.0

    def test_passes_with_custom_lower_threshold(self) -> None:
        rows = [
            {"in_a": True, "in_b": True, "percent_of_total": 99.0},
            {"in_a": True, "in_b": False, "percent_of_total": 1.0},
        ]
        passed, percent = evaluate_compare_result(rows, threshold=95.0)
        assert passed is True
        assert percent == 99.0

    def test_fails_when_percent_cannot_be_determined(self) -> None:
        passed, percent = evaluate_compare_result([])
        assert passed is False
        assert percent is None

    def test_default_threshold_constant_is_100(self) -> None:
        assert DEFAULT_MATCH_THRESHOLD == 100.0


class TestFindDbtDir:
    def test_uses_explicit_project_dir_when_given(self, tmp_path: Path) -> None:
        result = find_dbt_dir(str(tmp_path))
        assert result == tmp_path.resolve()

    def test_raises_when_nothing_found(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "ol_dbt_cli.commands.diff.get_repo_root",
            lambda: (_ for _ in ()).throw(RuntimeError("not a repo")),
        )
        with pytest.raises(RuntimeError, match="dbt project not found"):
            find_dbt_dir(None)
