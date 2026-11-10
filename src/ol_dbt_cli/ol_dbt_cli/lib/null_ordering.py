"""Require an explicit NULLS clause on every ascending sort key in a window or an ordered aggregate.

An ascending ORDER BY sorts NULLs last on Trino and DuckDB and first on
StarRocks, so ``row_number() over (order by created_on)`` picks a row with a
NULL ``created_on`` first there, and ``array_agg(x order by x)`` starts with the
NULLs. Nothing errors. Descending keys agree on all three at StarRocks' default
``sql_mode`` (NULLs last). The ``SORT_NULLS_LAST`` mode makes ascending agree and
descending disagree, so the project writes ``nulls last`` instead.

The check reads source files, not compiled SQL, for the reason ``pipe_concat``
gives. It reads the ORDER BY of an ``over (...)`` clause, a named window and an
aggregate call (``array_agg``, ``group_concat``, ``... within group``), SQL handed
to a macro as a Jinja string, and the ``order_by`` argument of ``array_agg_filter``
when it is a string literal or a variable the file sets to one. A sort key
written wholly in Jinja (``order by {{ order_by }}``) is the caller's to check.
A query's own ORDER BY (top-level or in a subquery) is read only when a LIMIT or
OFFSET follows it before any Jinja statement; otherwise it only orders output rows.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, NamedTuple

import yaml

from ol_dbt_cli.lib.sql_source import other_engine_spans, sql_code, starrocks_macro_names, yaml_sql_strings
from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

NULL_ORDERING_CHECK = "null_ordering"

_SQL_DIRS = ("models", "macros", "tests", "snapshots", "analyses")
_YAML_DIRS = (*_SQL_DIRS, "seeds")
_DETAIL = (
    "An ascending sort puts NULLs last on Trino and DuckDB and first on StarRocks. "
    "Write `nulls last` after the key (`order by created_on nulls last`)."
)

_ORDER_BY = re.compile(r"\border\s+by\b", re.IGNORECASE)
_SUBQUERY = re.compile(r"\s*(?:select|with)\b", re.IGNORECASE)
# Words that end a sort list at depth 0: a window frame, a LIMIT, group_concat's SEPARATOR.
# `t.range` is a column.
_LIST_END = re.compile(r"(?<![\w.])(?:rows|range|groups|limit|offset|separator)(?!\w)", re.IGNORECASE)
_ROW_LIMIT = re.compile(r"(?:limit|offset)\b", re.IGNORECASE)
_JINJA_STATEMENT = re.compile(r"\{%")
_EXPLICIT = re.compile(r"(?:\bdesc|\bnulls\s+(?:first|last))\s*\Z", re.IGNORECASE)
_EXPRESSION = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_ORDER_BY_ARG = re.compile(
    r"""\barray_agg_filter\s*\((?:[^()]|\((?:[^()]|\([^()]*\))*\))*?\border_by\s*=\s*"""
    r"""(?:(?P<q>['"])(?P<literal>.*?)(?P=q)|(?P<name>\w+))""",
    re.DOTALL,
)
_SET_STRING = re.compile(r"""\{%-?\s*set\s+(?P<name>\w+)\s*=\s*(?P<q>['"])(?P<value>.*?)(?P=q)\s*-?%\}""", re.DOTALL)


class SortKey(NamedTuple):
    """An ascending sort key with no NULLS clause.

    :param start: Offset of the key's first character.
    :param end: Offset just past its last character, where ``nulls last`` goes.
    """

    start: int
    end: int


def _enclosing_paren(code: str, offset: int) -> int:
    depth = 0
    for index in range(offset - 1, -1, -1):
        if code[index] == ")":
            depth += 1
        elif code[index] == "(":
            if depth == 0:
                return index
            depth -= 1
    return -1


def _sorts_rows_into_a_value(code: str, order_by: int) -> bool:
    """Whether the ORDER BY at *order_by* is a window's or an aggregate's, not a query's."""
    paren = _enclosing_paren(code, order_by)
    if paren < 0 or _SUBQUERY.match(code, paren + 1, order_by):
        return False
    # `over (`, `window w as (`, `array_agg(`, `within group (`: a word, not `((` or `, (`.
    preceding = code[:paren].rstrip()[-1:]
    return preceding.isalnum() or preceding == "_"


def _list_spans(code: str, start: int, end: int) -> Iterator[tuple[int, int]]:
    """Split the sort list from *start* at its depth-0 commas, up to *end* or its closing paren."""
    depth = 0
    item_start = start
    index = start
    while index < end:
        char = code[index]
        if char in "([":
            depth += 1
        elif char in ")]":
            if depth == 0:
                break
            depth -= 1
        elif depth == 0 and char == ",":
            yield item_start, index
            item_start = index + 1
        elif depth == 0 and _LIST_END.match(code, index):
            break
        index += 1
    yield item_start, index


def _ascending_keys(source: str, code: str, start: int, end: int) -> Iterator[SortKey]:
    for item_start, item_end in _list_spans(code, start, end):
        key = code[item_start:item_end]
        if not key.strip() or _EXPLICIT.search(key):
            continue
        # A Jinja expression is blank in *code*, so a key can start at `{{` or end at `}}`.
        key_start = item_start + len(key) - len(key.lstrip())
        key_end = item_start + len(key.rstrip())
        for expression in _EXPRESSION.finditer(source, item_start, item_end):
            key_start = min(key_start, expression.start())
            key_end = max(key_end, expression.end())
        yield SortKey(key_start, key_end)


def ascending_sort_keys(source: str) -> Iterator[SortKey]:
    """Yield the ascending window and ordered-aggregate sort keys in *source* with no NULLS clause.

    :param source: The text of a SQL file, or a SQL string from YAML.
    :returns: The span of each key, in no particular order.
    :rtype: Iterator[SortKey]
    """
    code = sql_code(source)
    for match in _ORDER_BY.finditer(code):
        if _sorts_rows_into_a_value(code, match.start()):
            yield from _ascending_keys(source, code, match.end(), len(code))
        else:
            # A query's own ORDER BY only orders its output, unless a LIMIT picks rows by it.
            # Its list ends at the next Jinja statement: an `{% if %}` around the LIMIT or
            # an `{% endmacro %}` would otherwise run it on into code that is not a sort key.
            statement = _JINJA_STATEMENT.search(source, match.end())
            bound = statement.start() if statement else len(code)
            *_, (_, list_end) = _list_spans(code, match.end(), bound)
            if _ROW_LIMIT.match(code, list_end):
                yield from _ascending_keys(source, code, match.end(), list_end)

    variables = {m.group("name"): m.span("value") for m in _SET_STRING.finditer(source)}
    for call in _ORDER_BY_ARG.finditer(source):
        span = call.span("literal") if call.group("literal") is not None else variables.get(call.group("name"))
        if span is not None:
            yield from _ascending_keys(source, code, *span)


def check_null_ordering(dbt_dir: Path, report: ValidationReport) -> None:
    """Report every ascending window or ordered-aggregate sort key with no NULLS clause.

    :param dbt_dir: The dbt project directory.
    :param report: The report to add an ERROR to for each key.
    """
    sql_files = sorted(path for name in _SQL_DIRS for path in (dbt_dir / name).rglob("*.sql"))
    yaml_files = [
        dbt_dir / "dbt_project.yml",
        *sorted(
            path for name in _YAML_DIRS for pattern in ("*.yml", "*.yaml") for path in (dbt_dir / name).rglob(pattern)
        ),
    ]
    sources = {path: path.read_text() for path in sql_files}
    starrocks_macros = starrocks_macro_names(sources.values())

    for path, source in sources.items():
        exempt = other_engine_spans(source, starrocks_macros)
        keys = {key for key in ascending_sort_keys(source) if not any(s <= key.start < e for s, e in exempt)}
        for key in sorted(keys):
            line = source.count("\n", 0, key.start) + 1
            report.add(
                NULL_ORDERING_CHECK,
                Severity.ERROR,
                path.stem,
                f"{path.relative_to(dbt_dir).as_posix()}:{line} has an ascending sort key with no NULLS clause: "
                f"{' '.join(source[key.start : key.end].split())}",
                _DETAIL,
            )

    for path in yaml_files:
        for key_path, value in yaml_sql_strings(yaml.safe_load(path.read_text())):
            for key in sorted(set(ascending_sort_keys(value))):
                report.add(
                    NULL_ORDERING_CHECK,
                    Severity.ERROR,
                    path.stem,
                    f"{path.relative_to(dbt_dir).as_posix()} has an ascending sort key with no NULLS clause "
                    f"in {key_path}: {' '.join(value[key.start : key.end].split())}",
                    _DETAIL,
                )
