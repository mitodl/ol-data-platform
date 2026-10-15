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


@pytest.mark.parametrize(
    "sql",
    [
        "with json_rows(id) as (select 1) select * from json_rows",
        "with a as (select 1), json_rows (id) as (select 1) select * from json_rows",
        "with recursive json_rows(id) as (select 1) select * from json_rows",
        "insert into json_log(id, payload) values (1, '{}')",
        "create table json_log(id int)",
        "select e from t cross join unnest(t.items) as json_items(e)",
    ],
)
def test_ignores_a_relation_named_json_with_a_column_list(dbt_dir: Path, sql: str) -> None:
    (dbt_dir / "models" / "m.sql").write_text(sql)
    assert _messages(dbt_dir) == []


@pytest.mark.parametrize(
    "sql",
    [
        # Trino and DuckDB do not escape a quote with a backslash.
        "select '\\' as slash, json_extract_scalar(payload, '$.id') as id from t",
        # The quotes inside Jinja inside a literal do not end the literal.
        """select 'lax {{ p | replace("'", "") }}' as a, json_parse(x) as "b" from t""",
    ],
)
def test_reports_a_call_after_a_literal_the_quotes_could_misread(dbt_dir: Path, sql: str) -> None:
    (dbt_dir / "models" / "m.sql").write_text(sql)
    assert len(_messages(dbt_dir)) == 1


def test_reports_a_call_passed_to_a_macro_as_sql(dbt_dir: Path) -> None:
    (dbt_dir / "models" / "m.sql").write_text("""select {{ json_to_varchar("json_parse(payload)") }}""")
    assert _messages(dbt_dir) == [
        "models/m.sql:1 calls json_parse() directly; use a macro: "
        "json_strict_value, json_extract_json_array, json_parse_nested_varchar_array or unnest_json_array"
    ]


def test_reads_macros_except_the_engine_bodies(dbt_dir: Path) -> None:
    (dbt_dir / "macros" / "x.sql").write_text(
        "{% macro f(c) %}{{ return(adapter.dispatch('f')(c)) }}{% endmacro %}\n"
        "{% macro default__f(c) %}json_parse({{ c }}){% endmacro %}\n"
        "{% macro duckdb__f(c) %}json_extract_string({{ c }}, '$'){% endmacro %}\n"
        "{% macro starrocks__f(c) %}json_query(parse_json({{ c }}), '$'){% endmacro %}\n"
        "{% macro trino__f(c) %}json_format({{ c }}){% endmacro %}\n"
        "{% macro g(c) %}json_parse({{ c }}){% endmacro %}\n"
    )
    assert _messages(dbt_dir) == [
        "macros/x.sql:6 calls json_parse() directly; use a macro: "
        "json_strict_value, json_extract_json_array, json_parse_nested_varchar_array or unnest_json_array"
    ]


def test_reports_the_line_of_a_call_in_a_multiline_jinja_string(dbt_dir: Path) -> None:
    (dbt_dir / "models" / "m.sql").write_text(
        '{{ config(\n    post_hook="update {{ this }}\n    set v = json_format(x)"\n) }}\nselect 1'
    )
    assert _messages(dbt_dir) == [
        "models/m.sql:3 calls json_format() directly; use a macro: json_to_varchar or array_to_json_string"
    ]


def test_reads_tests_snapshots_analyses_and_yaml_expressions(dbt_dir: Path) -> None:
    for name in ("tests", "snapshots", "analyses", "seeds"):
        (dbt_dir / name).mkdir()
    (dbt_dir / "tests" / "t.sql").write_text("select * from t where json_format(x) = '[]'")
    (dbt_dir / "snapshots" / "s.sql").write_text("select json_size(x, '$') from t")
    (dbt_dir / "analyses" / "a.sql").write_text("select json_value(x, 'lax $.a') from t")
    (dbt_dir / "seeds" / "_s.yml").write_text("seeds: [{name: s, data_tests: [{t: {expression: 'json_exists(x)'}}]}]\n")
    (dbt_dir / "dbt_project.yml").write_text("on-run-end: ['select json_array_get(x, 0)']\n")
    (dbt_dir / "models" / "_m.yml").write_text(
        "models: [{name: m, description: 'json_parse(x) is not called', "
        "data_tests: [{t: {expression: 'json_extract(x, ''$.a'') is not null'}}]}]\n"
    )
    assert _messages(dbt_dir) == [
        "analyses/a.sql:1 calls json_value() directly",
        "snapshots/s.sql:1 calls json_size() directly",
        "tests/t.sql:1 calls json_format() directly; use a macro: json_to_varchar or array_to_json_string",
        "dbt_project.yml calls json_array_get() directly in on-run-end[0]",
        "models/_m.yml calls json_extract() directly in models[0].data_tests[0].t.expression; "
        "use a macro: json_strict_value, json_extract_json_array or json_extract_value",
        "seeds/_s.yml calls json_exists() directly in seeds[0].data_tests[0].t.expression",
    ]


def test_the_project_has_no_raw_json_calls() -> None:
    assert _messages(REPO_ROOT / "src" / "ol_dbt") == []
