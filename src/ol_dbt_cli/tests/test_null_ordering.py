"""Tests for the null_ordering validate check."""

from __future__ import annotations

from pathlib import Path

import pytest

from ol_dbt_cli.lib.null_ordering import NULL_ORDERING_CHECK, ascending_sort_keys, check_null_ordering
from tests.source_checks import error_messages, make_dbt_dir

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def dbt_dir(tmp_path: Path) -> Path:
    return make_dbt_dir(tmp_path)


def _messages(dbt_dir: Path) -> list[str]:
    return error_messages(check_null_ordering, NULL_ORDERING_CHECK, dbt_dir)


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
        (
            "{% set o = 'a' %}\nselect {{ array_agg_filter('x', 'c', order_by=o) }}\n"
            "{% set o = 'b nulls last' %}\nselect {{ array_agg_filter('x', 'c', order_by=o) }}",
            ["a"],
        ),
        ("{{ array_agg_filter('x', 'c', order_by=o) }}\n{% set o = 'a' %}", []),
        (
            "{% set o = 'a' %}\n{% macro m() %}{{ array_agg_filter('x', 'c', order_by=o) }}{% endmacro %}",
            [],
        ),
        (
            "{% macro m() %}{% set o = 'a' %}{{ array_agg_filter('x', 'c', order_by=o) }}{% endmacro %}",
            ["a"],
        ),
        ("{% set o = 'a' %}\n{# {{ array_agg_filter('x', 'c', order_by=o) }} #}", []),
        ("{# {% set o = 'a' %} #}{% set o = 'b nulls last' %}{{ array_agg_filter('x', 'c', order_by=o) }}", []),
        ("{# {{ array_agg_filter('x', 'c', order_by='a') }} #}", []),
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
        ("select id from t order by a, b nulls last offset 5 limit 1", ["a"]),
        ("select id from t order by a offset 5", ["a"]),
        ("select id from t order by a {% if is_incremental() %}limit 10{% endif %}", ["a"]),
        ("select id from t order by a\n{% if target.name == 'dev' %}limit 10{% endif %}", ["a"]),
        ("select id from t order by a {% if x %}, b{% endif %} limit 10", ["a", "b"]),
        ("select * from t order by a nulls last {% if target.name == 'dev' %} limit 10 {% endif %}", []),
        ("select * from t order by a {# limit 1 #}", []),
        ("select * from t order by a, '{% if x %}'", []),
        (
            "{% macro a() %}select * from t order by x{% endmacro %}"
            "{% macro b() %}select y, z from u limit 1{% endmacro %}",
            [],
        ),
        ("select * from t order by x union all select * from u limit 1", []),
        ("select * from t order by x intersect select * from u limit 1", []),
        ("select * from t order by x except select * from u limit 1", []),
        ("select * from t order by x;\nselect * from u limit 1", []),
        ("select * from t order by a {# {% x %} #} limit 10", ["a"]),
        ("select * from t order by a, '{% x %}' limit 10", ["a"]),
        ("{% macro a() %}select * from t order by x{% endmacro %}{% macro b() %}limit 1{% endmacro %}", []),
        ("select * from t order by a\n{% macro m() %}\nlimit 10\n{% endmacro %}", []),
        (
            "select * from t\n{% if x %}\norder by a\n{% else %}\norder by b nulls last\n{% endif %}\nlimit 10",
            ["a"],
        ),
        (
            "select * from t\n{% if x %}\norder by a nulls last\n{% else %}\norder by b\n{% endif %}\nlimit 10",
            ["b"],
        ),
        ("select * from t\n{% if x %}\norder by a\n{% else %}\norder by b\n{% endif %}", []),
        ("""{% set ob = "order by a" %}{% if target.name == 'dev' %}limit 10{% endif %}""", []),
    ],
)
def test_reads_a_query_order_by_that_a_limit_follows(sql: str, keys: list[str]) -> None:
    assert _keys(sql) == keys


def test_a_jinja_statement_string_is_not_part_of_a_window_key() -> None:
    sql = "select row_number() over (order by a nulls last {% if target.name == 'dev' %}, b{% endif %}) from t"
    assert _keys(sql) == ["b"]


def test_a_window_list_ends_at_another_branchs_order_by() -> None:
    sql = "select row_number() over (partition by x {% if y %}order by a{% else %}order by b nulls last{% endif %})"
    assert _keys(sql) == ["a"]


def test_a_window_list_does_not_end_at_offset() -> None:
    assert _keys("select row_number() over (partition by p order by offset) from t") == ["offset"]


@pytest.mark.parametrize(
    ("sql", "keys"),
    [
        ('select row_number() over (order by "created_on") from t', ['"created_on"']),
        ('select row_number() over (order by "created_on" desc, "id") from t', ['"id"']),
        ('select array_agg(x order by "created_on") from t', ['"created_on"']),
        ('select * from t order by "created_on" limit 1', ['"created_on"']),
        ("select row_number() over (order by `created_on`) from t", ["`created_on`"]),
        ('select row_number() over (order by "created_on" nulls last) from t', []),
        ('select row_number() over (order by "t"."range", b) from t', ['"t"."range"', "b"]),
        ("""select row_number() over (order by 'literal', "order by") from t""", ['"order by"']),
    ],
)
def test_a_quoted_identifier_is_a_key(sql: str, keys: list[str]) -> None:
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
