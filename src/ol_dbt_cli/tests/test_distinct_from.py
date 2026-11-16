"""Tests for the distinct_from validate check."""

from __future__ import annotations

from pathlib import Path

import pytest

from ol_dbt_cli.lib.distinct_from import DISTINCT_FROM_CHECK, check_distinct_from
from tests.source_checks import error_messages

REPO_ROOT = Path(__file__).resolve().parents[3]


def _messages(dbt_dir: Path) -> list[str]:
    return error_messages(check_distinct_from, DISTINCT_FROM_CHECK, dbt_dir)


@pytest.mark.parametrize(
    "predicate",
    [
        "a is distinct from b",
        "a IS NOT DISTINCT FROM b",
        "a is\n    distinct from b",
    ],
)
def test_reports_either_form_with_the_line_it_starts_on(dbt_dir: Path, predicate: str) -> None:
    (dbt_dir / "models" / "m.sql").write_text(f"select 1\nfrom t\nwhere {predicate}\n")
    assert _messages(dbt_dir) == ["models/m.sql:3 uses IS DISTINCT FROM"]


@pytest.mark.parametrize(
    "sql",
    [
        "select 'is distinct from' as label",
        "select 1 -- a is distinct from b\n",
        "select 1 /* a is distinct from b */",
        "select 1 {# a is distinct from b #}",
        "select {{ is_distinct_from('a', 'b') }}",
        "select count(distinct a) from t",
    ],
)
def test_ignores_comments_quoted_text_and_the_macro(dbt_dir: Path, sql: str) -> None:
    (dbt_dir / "models" / "m.sql").write_text(sql)
    assert _messages(dbt_dir) == []


def test_exempts_the_default_body_of_a_macro_starrocks_overrides(dbt_dir: Path) -> None:
    (dbt_dir / "macros" / "x.sql").write_text(
        "{% macro default__f(a, b) %}{{ a }} is distinct from {{ b }}{% endmacro %}\n"
        "{% macro starrocks__f(a, b) %}not ({{ a }} <=> {{ b }}){% endmacro %}\n"
        "{% macro g(a, b) %}{{ a }} is distinct from {{ b }}{% endmacro %}\n"
    )
    assert _messages(dbt_dir) == ["macros/x.sql:3 uses IS DISTINCT FROM"]


def test_reports_a_yaml_test_expression(dbt_dir: Path) -> None:
    (dbt_dir / "models" / "_models.yml").write_text(
        "models: [{name: m, data_tests: [{t: {expression: 'a is distinct from b'}}]}]\n"
    )
    assert _messages(dbt_dir) == [
        "models/_models.yml uses IS DISTINCT FROM in models[0].data_tests[0].t.expression: a is distinct from b"
    ]


def test_the_project_is_clean() -> None:
    assert _messages(REPO_ROOT / "src" / "ol_dbt") == []
