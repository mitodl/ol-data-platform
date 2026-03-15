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


def get_changed_files(
    base_ref: str = "origin/main",
    repo_root: Path | None = None,
) -> list[Path]:
    """Return absolute paths of files changed vs *base_ref* (staged + unstaged + untracked)."""
    root = repo_root or get_repo_root()
    # Files changed relative to base ref (committed + staged)
    committed = _run_git(
        ["diff", "--name-only", base_ref, "HEAD"],
        cwd=root,
    ).splitlines()
    # Staged but not yet committed
    staged = _run_git(
        ["diff", "--name-only", "--cached"],
        cwd=root,
    ).splitlines()
    # Unstaged modifications (tracked files)
    unstaged = _run_git(
        ["diff", "--name-only"],
        cwd=root,
    ).splitlines()

    all_relative = set(committed) | set(staged) | set(unstaged)
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
) -> list[str]:
    """Return model names whose YAML schema files changed.

    Returns the *model names* declared inside changed _*.yml files, not just the
    file names — callers should use :func:`yaml_registry.YamlRegistry` to expand.
    """
    root = repo_root or get_repo_root(dbt_dir)
    models_dir = dbt_dir / "models"
    changed = get_changed_files(base_ref=base_ref, repo_root=root)
    yaml_files: list[Path] = []
    for path in changed:
        if path.suffix in {".yml", ".yaml"} and _is_under(path, models_dir):
            yaml_files.append(path)
    return yaml_files  # type: ignore[return-value]  # callers resolve model names from these


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
