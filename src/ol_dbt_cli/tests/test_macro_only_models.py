"""Tests for models whose whole body is a macro call.

Such a model has no SQL of its own, so the raw parser expands the project's
macros for it (and only for it).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from ol_dbt_cli.commands.impact import _analyse_model
from ol_dbt_cli.lib.git_utils import get_macro_sources_at_ref, resolve_merge_base
from ol_dbt_cli.lib.sql_parser import (
    get_columns_read_from_ref,
    parse_model_file,
    parse_model_sql,
    read_macro_sources,
    strip_jinja,
)
from ol_dbt_cli.lib.yaml_registry import YamlRegistry

ACTIVITY_MACROS = (
    "{% macro activity_source(relation, filter_null=true) %}\n"
    "with activities as (\n"
    "    select * from {{ relation }}\n"
    "    {% if filter_null -%}\n"
    "    where courserun_id is not null\n"
    "    {%- endif %}\n"
    ")\n"
    "{% endmacro %}\n"
    "{% macro activity_events(relation, user_id_column='openedx_user_id') %}\n"
    "{{ activity_source(relation) }}\n"
    "select\n"
    "    {{ user_id_column }}\n"
    "    , courserun_id\n"
    "    , {{ json_query_string('event', \"'$.id'\") }} as event_id\n"
    "from activities\n"
    "{% endmacro %}\n"
)

MACROS = {
    "macros/activity.sql": ACTIVITY_MACROS,
    "macros/json_query_string.sql": (
        "{%- macro json_query_string(json_col, json_path) -%}\n"
        "  {{ return(adapter.dispatch('json_query_string', 'open_learning')(json_col, json_path)) }}\n"
        "{%- endmacro -%}\n"
    ),
    "macros/override_ref.sql": "{% macro ref(model_name) %}{{ return(builtins.ref(model_name)) }}{% endmacro %}\n",
    "macros/generic_test.sql": "{% test is_positive(model, column_name) %}select 1{% endtest %}\n",
}

MODEL = (
    "{{ config(materialized='view') }}\n"
    "\n"
    "{{ activity_events(\n"
    "    ref('stg_activity'),\n"
    "    user_id_column='user_id'\n"
    ") }}\n"
)


def _git(args: list[str], cwd: Path) -> None:
    subprocess.run(  # noqa: S603
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false", *args],  # noqa: S607
        cwd=cwd,
        check=True,
        capture_output=True,
    )


class TestParseMacroOnlyModel:
    def test_without_macros_nothing_parses(self) -> None:
        parsed = parse_model_sql("m", MODEL)
        assert parsed.parse_error is not None
        assert parsed.output_columns == set()

    def test_expands_the_macro_body(self) -> None:
        parsed = parse_model_sql("m", MODEL, MACROS)
        assert parsed.parse_error is None
        assert parsed.output_columns == {"user_id", "courserun_id", "event_id"}

    def test_ref_passed_as_argument_keeps_lineage(self) -> None:
        """The project's own ``ref`` override must not replace the lineage collector."""
        parsed = parse_model_sql("m", MODEL, MACROS)
        assert parsed.refs == ["stg_activity"]
        assert parsed.ref_placeholder_map == {"ref_stg_activity": "stg_activity"}

    def test_model_with_its_own_sql_is_rendered_as_before(self) -> None:
        sql = "select id, {{ json_query_string('event', \"'$.id'\") }} as event_id from {{ ref('stg_activity') }}"
        assert strip_jinja(sql, MACROS).clean_sql == strip_jinja(sql).clean_sql

    def test_unknown_macro_still_leaves_placeholders(self) -> None:
        parsed = parse_model_sql("m", "{{ not_a_project_macro(ref('stg_activity')) }}\n", MACROS)
        assert parsed.parse_error is not None
        assert parsed.refs == ["stg_activity"]

    def test_columns_read_from_ref_see_the_expanded_sql(self, tmp_path: Path) -> None:
        model = tmp_path / "m.sql"
        model.write_text(MODEL)
        parsed = parse_model_file(model, macro_sources=MACROS)
        assert get_columns_read_from_ref(parsed, "stg_activity") == {"user_id", "courserun_id"}


class TestMacroSources:
    def test_read_macro_sources_keys_by_project_relative_path(self, tmp_path: Path) -> None:
        (tmp_path / "macros" / "nested").mkdir(parents=True)
        (tmp_path / "macros" / "a.sql").write_text("{% macro a() %}select 1{% endmacro %}")
        (tmp_path / "macros" / "nested" / "b.sql").write_text("{% macro b() %}select 2{% endmacro %}")
        (tmp_path / "macros" / "notes.md").write_text("not a macro")
        assert sorted(read_macro_sources(tmp_path)) == ["macros/a.sql", "macros/nested/b.sql"]


class TestImpactOnMacroOnlyModel:
    def _repo(self, tmp_path: Path) -> tuple[Path, Path, Path]:
        """Return (repo, dbt_dir, model file) with MODEL and its macros committed on main."""
        repo = tmp_path / "repo"
        dbt_dir = repo / "src" / "ol_dbt"
        (dbt_dir / "models").mkdir(parents=True)
        (dbt_dir / "macros").mkdir()
        _git(["init", "-b", "main"], cwd=repo)
        for path, content in MACROS.items():
            (dbt_dir / path).write_text(content)
        sql_file = dbt_dir / "models" / "m.sql"
        sql_file.write_text(MODEL)
        _git(["add", "-A"], cwd=repo)
        _git(["commit", "-m", "c0"], cwd=repo)
        return repo, dbt_dir, sql_file

    def test_macro_sources_at_ref_ignore_working_tree_edits(self, tmp_path: Path) -> None:
        repo, dbt_dir, _ = self._repo(tmp_path)
        merge_base = resolve_merge_base("main", repo_root=repo)
        (dbt_dir / "macros" / "activity.sql").write_text("{% macro activity_events(relation) %}select 1{% endmacro %}")
        assert get_macro_sources_at_ref(dbt_dir, merge_base, repo_root=repo) == MACROS

    def test_unchanged_model_and_macros_raise_no_alert(self, tmp_path: Path) -> None:
        repo, dbt_dir, sql_file = self._repo(tmp_path)
        merge_base = resolve_merge_base("main", repo_root=repo)
        base_macros = get_macro_sources_at_ref(dbt_dir, merge_base, repo_root=repo)
        alert = _analyse_model(
            "m", sql_file, merge_base, YamlRegistry(), None, {}, repo, read_macro_sources(dbt_dir), base_macros
        )
        assert alert is None

    def test_column_removed_inside_the_macro_is_reported(self, tmp_path: Path) -> None:
        repo, dbt_dir, sql_file = self._repo(tmp_path)
        merge_base = resolve_merge_base("main", repo_root=repo)
        base_macros = get_macro_sources_at_ref(dbt_dir, merge_base, repo_root=repo)
        (dbt_dir / "macros" / "activity.sql").write_text(ACTIVITY_MACROS.replace("    , courserun_id\n", ""))

        alert = _analyse_model(
            "m", sql_file, merge_base, YamlRegistry(), None, {}, repo, read_macro_sources(dbt_dir), base_macros
        )
        assert alert is not None
        assert [(c.column, c.change_type) for c in alert.column_changes] == [("courserun_id", "removed")]

    def test_model_replaced_by_a_macro_call_raises_no_alert(self, tmp_path: Path) -> None:
        """Moving a model's SQL into a macro changes no column."""
        repo, dbt_dir, sql_file = self._repo(tmp_path)
        sql_file.write_text(
            "with activities as (select * from {{ ref('stg_activity') }})\n"
            "select user_id, courserun_id, {{ json_query_string('event', \"'$.id'\") }} as event_id from activities\n"
        )
        _git(["commit", "-am", "inline"], cwd=repo)
        merge_base = resolve_merge_base("main", repo_root=repo)
        base_macros = get_macro_sources_at_ref(dbt_dir, merge_base, repo_root=repo)
        sql_file.write_text(MODEL)

        alert = _analyse_model(
            "m", sql_file, merge_base, YamlRegistry(), None, {}, repo, read_macro_sources(dbt_dir), base_macros
        )
        assert alert is None
