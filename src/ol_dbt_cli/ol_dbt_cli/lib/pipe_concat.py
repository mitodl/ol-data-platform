"""Refuse the ``||`` operator in the dbt project's SQL.

StarRocks reads ``||`` as a logical OR at its default ``sql_mode``, so
``'a' || 'b'`` returns NULL there and nothing errors: a model that concatenates
with it builds and holds wrong values. Trino and DuckDB concatenate. Strings go
through ``dbt.concat``, which renders ``a || b`` on Trino and DuckDB and
``concat(a, b)`` on StarRocks.

The check reads source files, not compiled SQL, because PR CI compiles for
DuckDB only and what matters is what StarRocks would be sent. A macro body
written for another engine is exempt: ``trino__x`` and ``duckdb__x`` always,
and ``default__x`` when the project also defines ``starrocks__x``.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import yaml

from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

PIPE_CONCAT_CHECK = "pipe_concat"

# Directories of the dbt project whose SQL reaches the warehouse.
_SQL_DIRS = ("models", "macros", "tests", "snapshots", "analyses")
_OTHER_ENGINE_PREFIXES = ("trino__", "duckdb__")
_DETAIL = (
    "StarRocks evaluates || as a logical OR and returns NULL or 1 without an error. "
    "Concatenate strings with {{ dbt.concat([a, b]) }}; append to an array with a dispatched macro."
)

# Comments and quoted text are consumed whole so a `||` inside them is not reported.
_TOKEN = re.compile(
    r"""
      \{\#.*?\#\}              # Jinja comment
    | /\*.*?\*/                # SQL block comment
    | --[^\n]*                 # SQL line comment
    | '(?:[^'\\]|\\.|'')*'     # single-quoted string
    | "(?:[^"\\]|\\.|"")*"     # double-quoted identifier, or a Jinja string
    | (?P<pipes>\|\|)
    """,
    re.DOTALL | re.VERBOSE,
)
_MACRO = re.compile(
    r"\{%-?\s*macro\s+(?P<name>\w+)\s*\(.*?%\}(?P<body>.*?)\{%-?\s*endmacro\s*-?%\}",
    re.DOTALL,
)
# YAML keys whose values are prose or fixture data, never SQL.
_NON_SQL_KEYS = frozenset({"description", "rows", "meta", "tags"})


def _pipe_offsets(sql: str) -> Iterator[int]:
    for match in _TOKEN.finditer(sql):
        if match.group("pipes"):
            yield match.start()


def _other_engine_spans(source: str, starrocks_macros: frozenset[str]) -> list[tuple[int, int]]:
    """Spans of the macro bodies in *source* that StarRocks never renders."""
    spans = []
    for match in _MACRO.finditer(source):
        name = match.group("name")
        other_engine = name.startswith(_OTHER_ENGINE_PREFIXES) or (
            name.startswith("default__") and name.removeprefix("default__") in starrocks_macros
        )
        if other_engine:
            spans.append(match.span("body"))
    return spans


def _yaml_sql_strings(node: Any, path: str = "") -> Iterator[tuple[str, str]]:
    """Yield ``(key path, value)`` for every string in *node* that may hold SQL."""
    if isinstance(node, str):
        yield path, node
    elif isinstance(node, list):
        for index, item in enumerate(node):
            yield from _yaml_sql_strings(item, f"{path}[{index}]")
    elif isinstance(node, dict):
        for key, value in node.items():
            # A unit test fixture written as SQL is the one `rows` that is not data.
            if key in _NON_SQL_KEYS and not (key == "rows" and node.get("format") == "sql"):
                continue
            yield from _yaml_sql_strings(value, f"{path}.{key}" if path else str(key))


def check_pipe_concat(dbt_dir: Path, report: ValidationReport) -> None:
    """Report every ``||`` in the project's SQL and YAML that StarRocks would evaluate.

    :param dbt_dir: The dbt project directory.
    :param report: The report to add an ERROR to for each occurrence.
    """
    sql_files = sorted(path for name in _SQL_DIRS for path in (dbt_dir / name).rglob("*.sql"))
    yaml_files = sorted(
        path for name in _SQL_DIRS for pattern in ("*.yml", "*.yaml") for path in (dbt_dir / name).rglob(pattern)
    )
    sources = {path: path.read_text() for path in sql_files}
    starrocks_macros = frozenset(
        match.group("name").removeprefix("starrocks__")
        for source in sources.values()
        for match in _MACRO.finditer(source)
        if match.group("name").startswith("starrocks__")
    )

    for path, source in sources.items():
        exempt = _other_engine_spans(source, starrocks_macros)
        for offset in _pipe_offsets(source):
            if any(start <= offset < end for start, end in exempt):
                continue
            line = source.count("\n", 0, offset) + 1
            report.add(
                PIPE_CONCAT_CHECK,
                Severity.ERROR,
                path.stem,
                f"{path.relative_to(dbt_dir).as_posix()}:{line} uses the || operator",
                _DETAIL,
            )

    for path in yaml_files:
        for key_path, value in _yaml_sql_strings(yaml.safe_load(path.read_text())):
            if any(_pipe_offsets(value)):
                report.add(
                    PIPE_CONCAT_CHECK,
                    Severity.ERROR,
                    path.stem,
                    f"{path.relative_to(dbt_dir).as_posix()} uses the || operator in {key_path}: {value.strip()}",
                    _DETAIL,
                )
