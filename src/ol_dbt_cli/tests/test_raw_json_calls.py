"""Tests for the raw_json_calls validate check."""

from __future__ import annotations

from pathlib import Path

import pytest

from ol_dbt_cli.lib.raw_json_calls import RAW_JSON_CALLS_CHECK, check_raw_json_calls
from ol_dbt_cli.lib.validation import Severity, ValidationReport

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def dbt_dir(tmp_path: Path) -> Path:
    for name in ("models", "macros"):
        (tmp_path / name).mkdir()
    (tmp_path / "dbt_project.yml").write_text("name: p\n")
    return tmp_path


def _messages(dbt_dir: Path) -> list[str]:
    report = ValidationReport()
    check_raw_json_calls(dbt_dir, report)
    assert all(i.check == RAW_JSON_CALLS_CHECK and i.severity == Severity.ERROR for i in report.issues)
    return [i.message for i in report.issues]


def test_reports_a_call_with_its_line_and_the_macros_to_use(dbt_dir: Path) -> None:
    (dbt_dir / "models" / "m.sql").write_text("select\n    json_query(payload, 'lax $.a') as a\nfrom t\n")
    assert _messages(dbt_dir) == [
        "models/m.sql:2 calls json_query() directly; use a macro: "
        "json_query_string, json_path_values or json_array_field_values"
    ]


@pytest.mark.parametrize(
    "sql",
    ["select json_parse (x)", "select JSON_PARSE(x)", "select json_array_length(x)", "select is_json_scalar(x)"],
)
def test_reports_any_json_function(dbt_dir: Path, sql: str) -> None:
    (dbt_dir / "models" / "m.sql").write_text(sql)
    assert len(_messages(dbt_dir)) == 1


@pytest.mark.parametrize(
    "sql",
    [
        "select json_payload from t",
        "select my_json_parse(x) from t",
        "select 'json_parse(x)' as text",
        "select 1 -- json_parse(x)\n",
        "select 1 /* json_parse(x) */",
        "select 1 {# json_parse(x) #}",
        """select {{ json_query_string("payload", "'$.a'") }}""",
        "select {{ json_extract_scalar('payload', \"'$.a'\") }}",
    ],
)
def test_ignores_macro_calls_comments_and_quoted_text(dbt_dir: Path, sql: str) -> None:
    (dbt_dir / "models" / "m.sql").write_text(sql)
    assert _messages(dbt_dir) == []


def test_reports_a_call_passed_to_a_macro_as_sql(dbt_dir: Path) -> None:
    (dbt_dir / "models" / "m.sql").write_text("""select {{ json_to_varchar("json_parse(payload)") }}""")
    assert _messages(dbt_dir) == [
        "models/m.sql:1 calls json_parse() directly; use a macro: "
        "json_strict_value, json_extract_json_array, json_parse_nested_varchar_array or unnest_json_array"
    ]


def test_does_not_read_macros(dbt_dir: Path) -> None:
    (dbt_dir / "macros" / "x.sql").write_text("{% macro default__f(c) %}json_parse({{ c }}){% endmacro %}")
    assert _messages(dbt_dir) == []


def test_reads_tests_and_yaml_expressions(dbt_dir: Path) -> None:
    (dbt_dir / "tests").mkdir()
    (dbt_dir / "tests" / "t.sql").write_text("select * from t where json_format(x) = '[]'")
    (dbt_dir / "models" / "_m.yml").write_text(
        "models: [{name: m, description: 'json_parse(x) is not called', "
        "data_tests: [{t: {expression: 'json_extract(x, ''$.a'') is not null'}}]}]\n"
    )
    assert _messages(dbt_dir) == [
        "tests/t.sql:1 calls json_format() directly; use a macro: json_to_varchar or array_to_json_string",
        "models/_m.yml calls json_extract() directly in models[0].data_tests[0].t.expression; "
        "use a macro: json_strict_value, json_extract_json_array or json_extract_value",
    ]


def test_the_project_has_no_raw_json_calls() -> None:
    assert _messages(REPO_ROOT / "src" / "ol_dbt") == []
