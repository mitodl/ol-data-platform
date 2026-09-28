"""Locate the dbt executable belonging to the interpreter running ol-dbt."""

from __future__ import annotations

import sys
from pathlib import Path


def dbt_executable() -> str:
    """Return the ``dbt`` script installed alongside this interpreter, else ``dbt``.

    A bare ``dbt`` resolves through PATH, so any other install earlier on PATH
    (e.g. a ``pip --user`` dbt-core without dbt-duckdb) silently replaces the
    project's pinned dbt and fails with a misleading "Could not find adapter
    type" error. In the repo-root venv the console script sits in the same bin
    directory as ``sys.executable``, so prefer it there. ol-dbt-cli does not
    depend on dbt-core itself, so an environment synced from the member
    directory (or a ``uv tool`` install) has no sibling ``dbt`` and falls back
    to PATH. ``-m dbt.cli.main`` is avoided because runpy prints a
    RuntimeWarning to stderr on every invocation.

    :returns: Absolute path to the venv's ``dbt`` script, or ``"dbt"``.
    :rtype: str
    """
    sibling = Path(sys.executable).parent / "dbt"
    return str(sibling) if sibling.is_file() else "dbt"
