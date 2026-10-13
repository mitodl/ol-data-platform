"""Tests for the pipe_concat validate check."""

from __future__ import annotations

from pathlib import Path

import pytest

from ol_dbt_cli.lib.pipe_concat import PIPE_CONCAT_CHECK, check_pipe_concat
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
    check_pipe_concat(dbt_dir, report)
    assert all(i.check == PIPE_CONCAT_CHECK and i.severity == Severity.ERROR for i in report.issues)
    return [i.message for i in report.issues]


def test_reports_the_operator_in_a_model_with_its_line(dbt_dir: Path) -> None:
    (dbt_dir / "models" / "m.sql").write_text("select\n    a || b as ab\nfrom t\n")
    assert _messages(dbt_dir) == ["models/m.sql:2 uses the || operator"]


@pytest.mark.parametrize(
    "sql",
    [
        "select '||' as delimiter",
        "select 'it''s || here' as text",
        "select 1 -- the report's a || b\n",
        "select 1 /* a || b */",
        "select 1 {# a || b #}",
        """select {{ dbt.concat(["a", "'||'", "b"]) }}""",
        'select "a || b" from t',
    ],
)
def test_ignores_the_operator_in_comments_and_quoted_text(dbt_dir: Path, sql: str) -> None:
    (dbt_dir / "models" / "m.sql").write_text(sql)
    assert _messages(dbt_dir) == []


@pytest.mark.parametrize(
    "sql",
    [
        """select {{ from_iso8601_timestamp("created_on || 'Z'") }}""",
        "select {{ dbt.safe_cast('a || b', api.Column.translate_type('string')) }}",
        """{% set key = "first_name || last_name" %}\nselect 1""",
        """{{ config(post_hook="update {{ this }} set k = a || b") }}\nselect 1""",
        """{{ config(post_hook="update t set value = a || {{ this }}") }}\nselect 1""",
    ],
)
def test_reports_the_operator_in_sql_passed_as_a_jinja_string(dbt_dir: Path, sql: str) -> None:
    (dbt_dir / "models" / "m.sql").write_text(sql)
    assert _messages(dbt_dir) == ["models/m.sql:1 uses the || operator"]


def test_a_named_endmacro_ends_the_exempt_body(dbt_dir: Path) -> None:
    (dbt_dir / "macros" / "x.sql").write_text(
        "{% macro trino__f(a) %}x{% endmacro trino__f %}\n{% macro g(a) %}a || b{% endmacro %}\n"
    )
    assert _messages(dbt_dir) == ["macros/x.sql:2 uses the || operator"]


def test_reads_dbt_project_and_seed_yaml(dbt_dir: Path) -> None:
    (dbt_dir / "dbt_project.yml").write_text("on-run-end: ['insert into t select a || b']\n")
    (dbt_dir / "seeds").mkdir()
    (dbt_dir / "seeds" / "_s.yml").write_text("seeds: [{name: s, data_tests: [{t: {expression: \"|| b = 'x'\"}}]}]\n")
    assert len(_messages(dbt_dir)) == 2


def test_a_comment_apostrophe_does_not_hide_a_later_operator(dbt_dir: Path) -> None:
    (dbt_dir / "models" / "m.sql").write_text("-- the model's key\nselect a || b, 'x' from t\n")
    assert _messages(dbt_dir) == ["models/m.sql:2 uses the || operator"]


def test_exempts_macro_bodies_written_for_another_engine(dbt_dir: Path) -> None:
    (dbt_dir / "macros" / "x.sql").write_text(
        "{% macro trino__f(a) %}{{ a }} || 'z'{% endmacro %}\n"
        "{% macro duckdb__f(a) %}{{ a }} || 'z'{% endmacro %}\n"
        "{% macro default__g(a) -%}{{ a }} || 'z'{%- endmacro %}\n"
        "{% macro starrocks__g(a) %}concat({{ a }}, 'z'){% endmacro %}\n"
    )
    assert _messages(dbt_dir) == []


def test_a_commented_out_starrocks_macro_does_not_exempt_the_default(dbt_dir: Path) -> None:
    (dbt_dir / "macros" / "x.sql").write_text(
        "{# {% macro starrocks__f(a) %}concat({{ a }}, 'z'){% endmacro %} #}\n"
        "{% macro default__f(a) %}{{ a }} || 'z'{% endmacro %}\n"
    )
    assert _messages(dbt_dir) == ["macros/x.sql:2 uses the || operator"]


def test_reports_macro_bodies_starrocks_renders(dbt_dir: Path) -> None:
    (dbt_dir / "macros" / "x.sql").write_text(
        "{% macro default__f(a) %}{{ a }} || 'z'{% endmacro %}\n"
        "{% macro g(a) %}\n{{ a }} || 'z'\n{% endmacro %}\n"
        "{% macro starrocks__h(a) %}{{ a }} || 'z'{% endmacro %}\n"
    )
    assert _messages(dbt_dir) == [f"macros/x.sql:{line} uses the || operator" for line in (1, 3, 5)]


def test_reports_sql_expressions_in_yaml_but_not_prose_or_fixture_rows(dbt_dir: Path) -> None:
    (dbt_dir / "models" / "_models.yml").write_text(
        """
version: 2
sources:
- name: s
  tables:
  - name: t
    description: pipe-delimited, run_id|||true
    config:
      loaded_at_field: "from_iso8601_timestamp(created_on || 'Z')"
unit_tests:
- name: u
  given:
  - input: ref('a')
    rows:
    - {dates: "2000-02-01||2099-02-01"}
  - input: ref('b')
    format: sql
    rows: select 'a' || 'b' as ab
"""
    )
    assert _messages(dbt_dir) == [
        "models/_models.yml uses the || operator in sources[0].tables[0].config.loaded_at_field: "
        "from_iso8601_timestamp(created_on || 'Z')",
        "models/_models.yml uses the || operator in unit_tests[0].given[1].rows: select 'a' || 'b' as ab",
    ]


def test_the_project_is_clean() -> None:
    assert _messages(REPO_ROOT / "src" / "ol_dbt") == []
