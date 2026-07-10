#!/usr/bin/env python3
"""Discover pytest suites for the Python Tests CI matrix.

Scans the repo for every directory that (a) contains a ``pyproject.toml`` and
(b) has a test directory — ``tests/`` or the Dagster ``<project>_tests/``
convention used under ``dg_projects/`` — holding at least one ``test_*.py``
file. Emits a JSON matrix on stdout as ``matrix=<json>`` for ``$GITHUB_OUTPUT``.

Discovering suites instead of hardcoding them means new code locations (e.g.
``dg_projects/*``) are covered automatically as soon as they add tests, and the
standalone uv projects (``src/ol_dlt``, ``src/ol_superset``) that are excluded
from / not members of the root workspace each get their own ``uv sync``.

Each matrix entry:
  - ``name``      unique, human-readable (dir path with ``/`` → ``-``); codecov flag
  - ``directory`` where ``uv sync`` / ``uv run pytest`` execute
  - ``testpath``  the test directory, relative to ``directory``
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Directories that never contain first-party test suites we want to run.
PRUNE = {
    ".git",
    ".venv",
    "node_modules",
    "target",
    "dbt_packages",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
}


def _has_test_files(directory: Path) -> bool:
    """Return True if *directory* contains any ``test_*.py`` file (recursively)."""
    return any(directory.rglob("test_*.py"))


def _test_dir_for(project_dir: Path) -> Path | None:
    """Return the test directory for *project_dir*, or None.

    Prefers a conventional ``tests/`` dir; otherwise the first ``*_tests/`` dir
    (Dagster code-location convention). Only returns a dir that has test files,
    so projects with a placeholder test dir but no tests yet are skipped until
    they add one.
    """
    candidates: list[Path] = []
    tests_dir = project_dir / "tests"
    if tests_dir.is_dir():
        candidates.append(tests_dir)
    candidates.extend(sorted(p for p in project_dir.glob("*_tests") if p.is_dir()))
    for candidate in candidates:
        if _has_test_files(candidate):
            return candidate
    return None


def discover(repo_root: Path) -> list[dict[str, str]]:
    """Return the matrix of pytest suites found under *repo_root*."""
    suites: list[dict[str, str]] = []
    for pyproject in sorted(repo_root.rglob("pyproject.toml")):
        project_dir = pyproject.parent
        rel_parts = project_dir.relative_to(repo_root).parts
        if any(part in PRUNE for part in rel_parts):
            continue
        test_dir = _test_dir_for(project_dir)
        if test_dir is None:
            continue
        rel_dir = project_dir.relative_to(repo_root)
        # str(rel_dir) for the root is "." (truthy), so handle it explicitly.
        name = "root" if rel_dir == Path() else str(rel_dir).replace("/", "-")
        suites.append(
            {
                "name": name,
                "directory": str(rel_dir),
                "testpath": str(test_dir.relative_to(project_dir)),
            }
        )
    return suites


def main() -> int:
    """Emit ``matrix=<json>`` for $GITHUB_OUTPUT and a summary to stderr."""
    repo_root = Path(os.environ.get("GITHUB_WORKSPACE", ".")).resolve()
    suites = discover(repo_root)
    # matrix=<json> line is consumed via >> "$GITHUB_OUTPUT".
    sys.stdout.write(f"matrix={json.dumps(suites)}\n")
    # Human-readable summary to stderr for the CI log.
    summary = [f"Discovered {len(suites)} pytest suite(s):"]
    summary.extend(f"  - {s['name']}: {s['directory']}/{s['testpath']}" for s in suites)
    sys.stderr.write("\n".join(summary) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
