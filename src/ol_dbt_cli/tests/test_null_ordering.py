"""Tests for the null_ordering validate check."""

from __future__ import annotations

from pathlib import Path

import pytest

from ol_dbt_cli.lib.null_ordering import NULL_ORDERING_CHECK, ascending_sort_keys, check_null_ordering
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
    check_null_ordering(dbt_dir, report)
    assert all(i.check == NULL_ORDERING_CHECK and i.severity == Severity.ERROR for i in report.issues)
    return [i.message for i in report.issues]


def _keys(sql: str) -> list[str]:
    return [sql[key.start : key.end] for key in sorted(ascending_sort_keys(sql))]


def test_reports_an_ascending_window_key_with_its_line(dbt_dir: Path) -> None:
    (dbt_dir / "models" / "m.sql").write_text(
        "select\n    row_number() over (\n        partition by a\n        order by created_on\n    ) as n\nfrom t\n"
    )
    assert _messages(dbt_dir) == [
        "models/m.sql:4 has an ascending sort key with no NULLS clause: created_on",
    ]


@pytest.mark.parametrize(
    ("sql", "keys"),
    [
        ("select row_number() over (order by a, b desc, c asc) from t", ["a", "c asc"]),
        ("select lag(x) over (partition by p order by ts rows between 1 preceding and current row) from t", ["ts"]),
        ("select array_agg(x order by y, z) from t", ["y", "z"]),
        ("select group_concat(x order by y separator ',') from t", ["y"]),
        ("select listagg(x, ',') within group (order by y) from t", ["y"]),
        ("select row_number() over w from t window w as (order by y)", ["y"]),
        (
            "select row_number() over (order by case when a then 0 else 1 end, coalesce(b, 0)) from t",
            ["case when a then 0 else 1 end", "coalesce(b, 0)"],
        ),
        ("select row_number() over (order by a -- tie\n, b) from t", ["a", "b"]),
        (
            "select row_number() over (order by {{ json_query_string('c', \"'$.k'\") }}) from t",
            ["{{ json_query_string('c', \"'$.k'\") }}"],
        ),
        (
            "select row_number() over (order by {{ json_query_string('c', \"'$.k'\") }} asc -- noqa\n) from t",
            ["{{ json_query_string('c', \"'$.k'\") }} asc"],
        ),
    ],
)
def test_finds_each_ascending_key(sql: str, keys: list[str]) -> None:
    assert _keys(sql) == keys


@pytest.mark.parametrize(
    "sql",
    [
        "select row_number() over (order by a nulls last, b desc, c asc nulls first, d desc nulls last) from t",
        "select array_agg(x order by y NULLS LAST) from t",
        "select * from t order by a",
        "select * from (select a from t order by a) as s",
        "with s as (select a from t order by a desc limit 1) select * from s",
        "select a in (select b from u order by b) from t",
        "select 1 -- over (order by a)\n",
        "select 'over (order by a)'",
        "select row_number() over (order by {{ order_by }}) from t",
        "select row_number() over (order by max(ts) desc {%- if c %}, max({{ c }}) desc nulls last{% endif %}) from t",
    ],
)
def test_ignores_explicit_keys_and_query_order(sql: str) -> None:
    assert _keys(sql) == []


@pytest.mark.parametrize(
    ("sql", "keys"),
    [
        ('select {{ array_join("array_agg(x order by y)", "\',\'") }}', ["y"]),
        ("select {{ array_agg_filter('x', 'c', order_by='a desc, b') }}", ["b"]),
        ("select {{ array_agg_filter(\n  'cast(x as varchar)'\n  , 'c'\n  , order_by=\"t.id\"\n) }}", ["t.id"]),
        ("{% set o = 'a desc, b ASC' %}\nselect {{ array_agg_filter('x', 'c', order_by=o) }}", ["b ASC"]),
        ("select {{ array_agg_filter('x', 'c', order_by='x nulls last', distinct=true) }}", []),
        ("select {{ array_agg_filter('x', 'c', order_by=unknown_name) }}", []),
        ("select {{ array_agg_filter('coalesce(cast(x as varchar), y)', 'c', order_by='a') }}", ["a"]),
    ],
)
def test_reads_sql_and_order_by_passed_as_jinja_strings(sql: str, keys: list[str]) -> None:
    assert _keys(sql) == keys


@pytest.mark.parametrize(
    ("sql", "keys"),
    [
        ("with s as (select id from t order by created_on, id desc limit 1) select * from s", ["created_on"]),
        ("select (select id from t order by created_on limit 1) as first_id", ["created_on"]),
        ("select id from t\norder by created_on\nlimit 10\n", ["created_on"]),
    ],
)
def test_reads_a_query_order_by_that_a_limit_follows(sql: str, keys: list[str]) -> None:
    assert _keys(sql) == keys


def test_a_qualified_column_named_like_a_frame_word_is_a_key() -> None:
    assert _keys("select row_number() over (order by t.range, b) from t") == ["t.range", "b"]


def test_exempts_macro_bodies_written_for_another_engine(dbt_dir: Path) -> None:
    (dbt_dir / "macros" / "x.sql").write_text(
        "{% macro trino__f(a) %}array_agg(a order by a){% endmacro %}\n"
        "{% macro default__g(a) %}array_agg(a order by a){% endmacro %}\n"
        "{% macro starrocks__g(a) %}array_agg(a order by a nulls last){% endmacro %}\n"
        "{% macro h(a) %}\narray_agg(a order by a){% endmacro %}\n"
        "{% macro default__k(a) %}array_agg(a order by a){% endmacro %}\n"
    )
    assert _messages(dbt_dir) == [
        f"macros/x.sql:{line} has an ascending sort key with no NULLS clause: a" for line in (5, 6)
    ]


def test_reads_sql_expressions_in_yaml(dbt_dir: Path) -> None:
    (dbt_dir / "models" / "_models.yml").write_text(
        """
version: 2
models:
- name: m
  description: ranks rows with row_number() over (order by a)
  data_tests:
  - dbt_utils.expression_is_true:
      expression: "rn = row_number() over (order by a)"
"""
    )
    assert _messages(dbt_dir) == [
        "models/_models.yml has an ascending sort key with no NULLS clause "
        "in models[0].data_tests[0].dbt_utils.expression_is_true.expression: a",
    ]


def test_the_project_is_clean() -> None:
    assert _messages(REPO_ROOT / "src" / "ol_dbt") == []
