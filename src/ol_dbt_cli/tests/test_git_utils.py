"""Tests for lib/git_utils.py — git diff helpers for changed-model detection."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from ol_dbt_cli.lib.git_utils import (
    get_changed_files,
    get_changed_macro_files,
    get_changed_sql_models,
    get_changed_yaml_models,
    get_file_at_ref,
    resolve_merge_base,
)


def _git(args: list[str], cwd: Path) -> str:
    result = subprocess.run(  # noqa: S603, S607
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
        env={
            "GIT_AUTHOR_NAME": "Test",
            "GIT_AUTHOR_EMAIL": "test@example.com",
            "GIT_COMMITTER_NAME": "Test",
            "GIT_COMMITTER_EMAIL": "test@example.com",
            "PATH": "/usr/bin:/bin:/usr/local/bin",
        },
    )
    return result.stdout


def _commit(repo: Path, message: str) -> str:
    _git(["add", "-A"], cwd=repo)
    _git(["commit", "-m", message], cwd=repo)
    return _git(["rev-parse", "HEAD"], cwd=repo).strip()


@pytest.fixture()
def scripted_repo(tmp_path: Path) -> Path:
    r"""Build a repo with a PR branch forked from main, plus later main-only commits.

    History shape:
        main:    C0 --- C1 (models/other.sql added; models/foo.sql gets an
                   \        unrelated edit -- both AFTER the fork)
        feature:    C0a (models/foo.sql: A,B -> A,C; models/bar.sql added)

    C0 is the merge-base of main and feature. C1 exists only on main, after the
    branch point:
    - a two-dot diff (main..feature) would incorrectly show other.sql as
      changed (it exists on main's tip but not on feature); a three-dot /
      merge-base diff correctly excludes it.
    - fetching foo.sql's "base" content via the raw base_ref tip (main) would
      read C1's content (main's own later edit), not the C0 fork-point content
      feature actually diverged from.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(["init", "-b", "main"], cwd=repo)

    models = repo / "models"
    models.mkdir()
    (models / "foo.sql").write_text("select a, b\n")
    (models / "_schema.yml").write_text("version: 2\n")
    merge_base = _commit(repo, "C0: initial models")

    _git(["checkout", "-b", "feature"], cwd=repo)
    (models / "foo.sql").write_text("select a, c\n")
    (models / "bar.sql").write_text("select x\n")
    _commit(repo, "C0a: feature changes foo.sql, adds bar.sql")

    _git(["checkout", "main"], cwd=repo)
    (models / "other.sql").write_text("select y\n")
    (models / "foo.sql").write_text("select a, b, extra\n")
    _commit(repo, "C1: main-only changes after the fork")

    _git(["checkout", "feature"], cwd=repo)
    repo.joinpath(".merge_base_sha").write_text(merge_base)
    return repo


class TestResolveMergeBase:
    def test_returns_fork_point_not_base_tip(self, scripted_repo: Path) -> None:
        expected = scripted_repo.joinpath(".merge_base_sha").read_text()
        assert resolve_merge_base("main", repo_root=scripted_repo) == expected

    def test_differs_from_base_ref_tip(self, scripted_repo: Path) -> None:
        base_tip = _git(["rev-parse", "main"], cwd=scripted_repo).strip()
        merge_base = resolve_merge_base("main", repo_root=scripted_repo)
        assert merge_base != base_tip


class TestGetChangedFiles:
    def test_excludes_main_only_changes_after_fork(self, scripted_repo: Path) -> None:
        changed = get_changed_files(base_ref="main", repo_root=scripted_repo, include_untracked=False)
        names = {p.name for p in changed}
        assert "other.sql" not in names, "main-only commit after the fork leaked into the changed set"

    def test_includes_feature_branch_changes(self, scripted_repo: Path) -> None:
        changed = get_changed_files(base_ref="main", repo_root=scripted_repo, include_untracked=False)
        names = {p.name for p in changed}
        assert "foo.sql" in names
        assert "bar.sql" in names

    def test_includes_untracked_files_by_default(self, scripted_repo: Path) -> None:
        (scripted_repo / "models" / "baz.sql").write_text("select z\n")
        changed = get_changed_files(base_ref="main", repo_root=scripted_repo)
        assert "baz.sql" in {p.name for p in changed}

    def test_excludes_untracked_files_when_disabled(self, scripted_repo: Path) -> None:
        (scripted_repo / "models" / "baz.sql").write_text("select z\n")
        changed = get_changed_files(base_ref="main", repo_root=scripted_repo, include_untracked=False)
        assert "baz.sql" not in {p.name for p in changed}

    def test_includes_untracked_files_with_special_characters_in_name(self, scripted_repo: Path) -> None:
        # git's default porcelain quoting C-escapes paths containing whitespace or
        # non-ASCII bytes (e.g. `?? "my model.sql"`); without -z that garbles the
        # path into one that doesn't exist on disk and silently drops the file.
        (scripted_repo / "models" / "my model.sql").write_text("select 1\n")
        (scripted_repo / "models" / "café.sql").write_text("select 2\n")
        changed = get_changed_files(base_ref="main", repo_root=scripted_repo)
        names = {p.name for p in changed}
        assert "my model.sql" in names
        assert "café.sql" in names


class TestGetChangedSqlModels:
    def test_changed_set_matches_three_dot_semantics(self, scripted_repo: Path) -> None:
        names = get_changed_sql_models(scripted_repo, base_ref="main", repo_root=scripted_repo)
        assert set(names) == {"foo", "bar"}


@pytest.fixture()
def repo_with_macro_and_yaml_change(tmp_path: Path) -> Path:
    """Build a repo whose feature branch edits a macro and a YAML file, but no model .sql."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(["init", "-b", "main"], cwd=repo)

    (repo / "models").mkdir()
    (repo / "models" / "foo.sql").write_text("select a\n")
    (repo / "models" / "_schema.yml").write_text("version: 2\n")
    (repo / "macros").mkdir()
    (repo / "macros" / "helper.sql").write_text("{% macro helper() %}1{% endmacro %}\n")
    _commit(repo, "C0: initial")

    _git(["checkout", "-b", "feature"], cwd=repo)
    (repo / "macros" / "helper.sql").write_text("{% macro helper() %}2{% endmacro %}\n")
    (repo / "models" / "_schema.yml").write_text("version: 2\nmodels: []\n")
    _commit(repo, "C0a: change macro and yaml only")
    return repo


class TestGetChangedMacroFiles:
    def test_detects_changed_macro(self, repo_with_macro_and_yaml_change: Path) -> None:
        repo = repo_with_macro_and_yaml_change
        changed = get_changed_macro_files(repo, base_ref="main", repo_root=repo)
        assert {p.name for p in changed} == {"helper.sql"}

    def test_excludes_model_sql(self, repo_with_macro_and_yaml_change: Path) -> None:
        repo = repo_with_macro_and_yaml_change
        changed = get_changed_macro_files(repo, base_ref="main", repo_root=repo)
        assert all("models" not in p.parts for p in changed)


class TestGetChangedYamlModels:
    def test_detects_changed_yaml(self, repo_with_macro_and_yaml_change: Path) -> None:
        repo = repo_with_macro_and_yaml_change
        changed = get_changed_yaml_models(repo, base_ref="main", repo_root=repo)
        assert {p.name for p in changed} == {"_schema.yml"}

    def test_no_sql_models_changed(self, repo_with_macro_and_yaml_change: Path) -> None:
        repo = repo_with_macro_and_yaml_change
        assert get_changed_sql_models(repo, base_ref="main", repo_root=repo) == []


class TestGetFileAtRef:
    def test_merge_base_content_is_fork_point_not_base_tip(self, scripted_repo: Path) -> None:
        merge_base = resolve_merge_base("main", repo_root=scripted_repo)
        content = get_file_at_ref(scripted_repo / "models" / "foo.sql", merge_base, repo_root=scripted_repo)
        assert content == "select a, b\n"

    def test_base_ref_tip_content_is_polluted_by_later_main_commits(self, scripted_repo: Path) -> None:
        # Demonstrates the bug this task fixes: fetching against the raw
        # base_ref tip (the old behaviour) reads main's own later edit to
        # foo.sql (C1), not the C0 fork-point content feature actually
        # diverged from -- masking or misrepresenting the PR's real diff.
        content = get_file_at_ref(scripted_repo / "models" / "foo.sql", "main", repo_root=scripted_repo)
        assert content == "select a, b, extra\n"
