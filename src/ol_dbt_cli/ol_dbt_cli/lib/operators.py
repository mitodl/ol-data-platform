"""Report an operator StarRocks misreads or rejects wherever the dbt project's SQL uses it.

Shared by ``pipe_concat`` and ``distinct_from``. The scan reads source files, not
compiled SQL, because PR CI compiles for DuckDB only and what matters is what
StarRocks would be sent. A macro body written for another engine is exempt:
``trino__x`` and ``duckdb__x`` always, and ``default__x`` when the project also
defines ``starrocks__x``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import yaml

from ol_dbt_cli.lib.sql_source import other_engine_spans, sql_code, starrocks_macro_names, yaml_sql_strings
from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    import re
    from pathlib import Path

# Directories of the dbt project whose SQL reaches the warehouse. Seeds carry
# no SQL files, but their YAML can hold test expressions.
_SQL_DIRS = ("models", "macros", "tests", "snapshots", "analyses")
_YAML_DIRS = (*_SQL_DIRS, "seeds")


def check_operator(
    dbt_dir: Path,
    report: ValidationReport,
    *,
    check: str,
    pattern: re.Pattern[str],
    operator: str,
    detail: str,
) -> None:
    """Report every match of *pattern* in the project's SQL and YAML that StarRocks would evaluate.

    :param dbt_dir: The dbt project directory.
    :param report: The report to add an ERROR to for each occurrence.
    :param check: The check name the issues are reported under.
    :param pattern: The operator, matched against SQL with comments and quoted text blanked.
    :param operator: How a message names the operator, e.g. ``the || operator``.
    :param detail: What StarRocks does with it and what to write instead.
    """
    sql_files = sorted(path for name in _SQL_DIRS for path in (dbt_dir / name).rglob("*.sql"))
    yaml_files = [
        dbt_dir / "dbt_project.yml",
        *sorted(path for name in _YAML_DIRS for glob in ("*.yml", "*.yaml") for path in (dbt_dir / name).rglob(glob)),
    ]
    sources = {path: path.read_text() for path in sql_files}
    starrocks_macros = starrocks_macro_names(sources.values())

    for path, source in sources.items():
        exempt = other_engine_spans(source, starrocks_macros)
        for match in pattern.finditer(sql_code(source)):
            if any(start <= match.start() < end for start, end in exempt):
                continue
            line = source.count("\n", 0, match.start()) + 1
            report.add(
                check,
                Severity.ERROR,
                path.stem,
                f"{path.relative_to(dbt_dir).as_posix()}:{line} uses {operator}",
                detail,
            )

    for path in yaml_files:
        for key_path, value in yaml_sql_strings(yaml.safe_load(path.read_text())):
            if pattern.search(sql_code(value)):
                report.add(
                    check,
                    Severity.ERROR,
                    path.stem,
                    f"{path.relative_to(dbt_dir).as_posix()} uses {operator} in {key_path}: {value.strip()}",
                    detail,
                )
