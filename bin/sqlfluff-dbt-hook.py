#!/usr/bin/env python3
"""Run sqlfluff on the dbt models; fail if any file goes unlinted.

The sqlfluff-lint and sqlfluff-fix hooks call this with sqlfluff's arguments,
e.g. ``python bin/sqlfluff-dbt-hook.py lint <files>``.

On its own, sqlfluff's dbt templater exits 0 when it skips a file, logging only
a warning, e.g. for any file dbt can't compile. Here any warning fails the hook,
unless it is the skip of a disabled model.
"""

import re
import subprocess
import sys

# sqlfluff reports every skipped file, and every early stop, in a log line at
# one of these levels.
_LOG_LINE = re.compile(r"^\s*(?:WARNING|ERROR|CRITICAL)\s+(.*)")
_DISABLED = re.compile(r"Skipped file \S+ because it is disabled")


def _unlinted(output: str) -> list[str]:
    return [
        line.strip()
        for line in output.splitlines()
        if (match := _LOG_LINE.match(line)) and not _DISABLED.match(match.group(1))
    ]


def main() -> int:
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "sqlfluff", *sys.argv[1:]],
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
