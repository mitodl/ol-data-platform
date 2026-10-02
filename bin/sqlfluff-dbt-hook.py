#!/usr/bin/env python3
"""Run sqlfluff on the dbt models without a warehouse; fail if any file goes unlinted.

The sqlfluff-lint and sqlfluff-fix hooks call this with sqlfluff's arguments,
e.g. ``python bin/sqlfluff-dbt-hook.py lint <files>``.

On its own, sqlfluff's dbt templater can exit 0 without linting a file:

- Before linting, it asks the warehouse which tables exist. With no warehouse,
  it stops at "Fatal linting error". Here that question gets the answer "none",
  so nothing connects, and incremental models render as a full refresh.
- It skips any file dbt can't compile. Here a skipped file fails the hook,
  unless the model is disabled or listed in ALLOWED_COMPILE_FAILURES.
"""

import re
import subprocess
import sys
from pathlib import Path

# Models that query the warehouse while compiling.
ALLOWED_COMPILE_FAILURES = frozenset(
    {
        "src/ol_dbt/models/dimensional/dim_date.sql",  # dbt_utils.date_spine
    }
)

_RUN_SQLFLUFF = """
import sys

from dbt.adapters.sql.impl import SQLAdapter

if not hasattr(SQLAdapter, "list_relations_without_caching"):
    sys.exit("bin/sqlfluff-dbt-hook.py needs updating: dbt-adapters no longer has "
             "SQLAdapter.list_relations_without_caching")
SQLAdapter.list_relations_without_caching = lambda self, schema_relation: []

from sqlfluff.cli.commands import cli

cli()
"""

_SKIPPED = re.compile(r"Skipped file (\S+) because (.+)")


def _repo_path(path: str) -> str:
    resolved = Path(path).resolve()
    if resolved.is_relative_to(Path.cwd()):
        return str(resolved.relative_to(Path.cwd()))
    return path


def _unlinted(output: str) -> list[str]:
    problems = []
    for line in output.splitlines():
        if "Fatal linting error" in line or "Skipping to avoid parser lock" in line:
            problems.append(line.strip())
            continue
        match = _SKIPPED.search(line)
        if match is None:
            continue
        path, reason = _repo_path(match.group(1)), match.group(2)
        if reason.startswith("it is disabled"):
            continue
        if path in ALLOWED_COMPILE_FAILURES and reason.startswith(
            "dbt raised a fatal exception during compilation"
        ):
            continue
        problems.append(line.strip())
    return problems


def main() -> int:
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _RUN_SQLFLUFF, *sys.argv[1:]],
        capture_output=True,
        text=True,
        check=False,
    )
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    # sqlfluff logs to stdout in its default format, and to stderr with --format json.
    problems = _unlinted(result.stdout + result.stderr)
    if problems:
        sys.stderr.write("sqlfluff did not lint every file:\n")
        sys.stderr.writelines(f"  {problem}\n" for problem in problems)
        return 1
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
