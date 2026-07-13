"""Git diff helpers for detecting changed dbt models vs a base branch."""

from __future__ import annotations

import subprocess
from pathlib import Path


def _run_git(args: list[str], cwd: Path) -> str:
    """Run a git command and return stdout, raising on non-zero exit."""
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        msg = f"git {' '.join(args)} failed: {result.stderr.strip()}"
        raise RuntimeError(msg)
    return result.stdout


def get_repo_root(start: Path | None = None) -> Path:
    """Return the absolute path of the git repository root."""
    cwd = start or Path.cwd()
    try:
        root = _run_git(["rev-parse", "--show-toplevel"], cwd=cwd).strip()
        return Path(root)
    except RuntimeError as exc:
        msg = "Not inside a git repository"
        raise RuntimeError(msg) from exc


def resolve_merge_base(base_ref: str, repo_root: Path | None = None) -> str:
    """Return the merge-base commit SHA of *base_ref* and HEAD.

    This is the fork point where the current branch diverged from *base_ref* —
    diffing/fetching against it (rather than *base_ref* directly) gives
    three-dot diff semantics, matching what a GitHub PR diff shows. Once
    *base_ref* advances past the branch point (e.g. other PRs merge to main
    while this one is open), a plain two-dot diff against *base_ref* would
    incorrectly include those unrelated changes in the changed set.
    """
    root = repo_root or get_repo_root()
    return _run_git(["merge-base", base_ref, "HEAD"], cwd=root).strip()


def get_changed_files(
    base_ref: str = "origin/main",
    repo_root: Path | None = None,
    *,
    include_untracked: bool = True,
) -> list[Path]:
    """Return absolute paths of files changed since diverging from *base_ref*.

    Diffs against ``merge-base(base_ref, HEAD)`` rather than *base_ref* directly
    (three-dot semantics; see :func:`resolve_merge_base`), so commits landed on
    *base_ref* after the branch point are excluded. Includes tracked files
    changed since the branch point (committed), staged for commit, or with
    unstaged modifications. New files that have not yet been ``git add``-ed are
    also included by default (*include_untracked*) so local runs see
    in-progress new models; set ``include_untracked=False`` to match a strict
    porcelain diff (e.g. CI, where checkouts have no untracked files).
    """
    root = repo_root or get_repo_root()
    merge_base = resolve_merge_base(base_ref, repo_root=root)

    def _diff_names(*args: str) -> set[str]:
        # -z: NUL-delimited, unquoted paths. Without it, git's default porcelain
        # quoting (C-style escapes for whitespace/non-ASCII, e.g. "my model.sql"
        # or "caf\303\251.sql") corrupts any such path into one that doesn't
        # exist on disk, silently dropping it from the changed set.
        raw = _run_git(["diff", "--name-only", "-z", *args], cwd=root)
        return {p for p in raw.split("\0") if p}

    # Files changed since the branch point (committed + staged + unstaged)
    all_relative = _diff_names(merge_base, "HEAD") | _diff_names("--cached") | _diff_names()

    if include_untracked:
        status = _run_git(["status", "--porcelain", "--untracked-files=all", "-z"], cwd=root)
        all_relative.update(entry[3:] for entry in status.split("\0") if entry.startswith("??"))

    return [root / p for p in sorted(all_relative)]


def get_changed_sql_models(
    dbt_dir: Path,
    base_ref: str = "origin/main",
    repo_root: Path | None = None,
) -> list[str]:
    """Return model names (no extension) for .sql files under *dbt_dir*/models/ that changed."""
    root = repo_root or get_repo_root(dbt_dir)
    models_dir = dbt_dir / "models"
    changed = get_changed_files(base_ref=base_ref, repo_root=root)
    names: list[str] = []
    for path in changed:
        if path.suffix == ".sql" and _is_under(path, models_dir):
            names.append(path.stem)
    return names


def get_changed_yaml_models(
    dbt_dir: Path,
    base_ref: str = "origin/main",
    repo_root: Path | None = None,
) -> list[Path]:
    """Return paths of YAML schema files (``_*.yml``) under *dbt_dir*/models/ that changed.

    Returns file :class:`~pathlib.Path` objects, not model names.  Callers that
    need model names should parse the returned paths through
    :func:`yaml_registry.build_yaml_registry` or read the ``models:`` key directly.
    """
    root = repo_root or get_repo_root(dbt_dir)
    models_dir = dbt_dir / "models"
    changed = get_changed_files(base_ref=base_ref, repo_root=root)
    yaml_files: list[Path] = []
    for path in changed:
        if path.suffix in {".yml", ".yaml"} and _is_under(path, models_dir):
            yaml_files.append(path)
    return yaml_files


def get_file_at_ref(path: Path, ref: str, repo_root: Path | None = None) -> str | None:
    """Return the content of *path* at git *ref*, or None if it didn't exist there."""
    root = repo_root or get_repo_root(path.parent)
    try:
        rel = path.relative_to(root)
    except ValueError:
        rel = path
    try:
        return _run_git(["show", f"{ref}:{rel!s}"], cwd=root)
    except RuntimeError:
        return None


def _is_under(path: Path, directory: Path) -> bool:
    """Return True if *path* is inside *directory*."""
    try:
        path.relative_to(directory)
        return True
    except ValueError:
        return False
