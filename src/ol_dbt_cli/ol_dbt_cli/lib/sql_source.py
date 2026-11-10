"""Read the SQL in the dbt project's source files past comments, quoted text and Jinja.

The checks that read source files rather than compiled SQL (``pipe_concat``,
``raw_json_calls``, ``null_ordering``) look for an operator, a function call or a
sort key in the SQL a file hands to the warehouse, and a match inside a comment or
a string literal is not one.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

# A Jinja block ends only at a closing delimiter outside its strings, so a hook
# such as `post_hook="update t set v = a || {{ this }}"` is consumed whole.
_JINJA_QUOTED = r"""'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*\""""
_JINJA_COMMENT_PATTERN = r"\{\#.*?\#\}"
_JINJA_BLOCK = rf"""\{{\{{(?:{_JINJA_QUOTED}|[^'"}}]|\}}(?!\}}))*\}}\}}|\{{%(?:{_JINJA_QUOTED}|[^'"%]|%(?!\}}))*%\}}"""
# Trino and DuckDB escape a quote only by doubling it, so `'\'` is a whole literal.
# A literal may hold Jinja, whose own quotes do not end it:
# `'lax {{ json_path | replace("'", "") }}'`.
_SQL_QUOTED = rf"""'(?:{_JINJA_BLOCK}|{_JINJA_COMMENT_PATTERN}|[^'{{]|\{{(?![{{%\#])|'')*'|"(?:[^"]|"")*\""""
_TOKEN = re.compile(
    rf"""
      {_JINJA_COMMENT_PATTERN}
    | (?P<jinja>{_JINJA_BLOCK})
    | /\*.*?\*/                # SQL block comment
    | --[^\n]*                 # SQL line comment
    | {_SQL_QUOTED}            # string literal or double-quoted identifier
    """,
    re.DOTALL | re.VERBOSE,
)
_JINJA_STRING = re.compile(r"""'(?P<single>(?:[^'\\]|\\.)*)'|"(?P<double>(?:[^"\\]|\\.)*)\"""")
_NOT_NEWLINE = re.compile(r"[^\n]")
_JINJA_COMMENT = re.compile(_JINJA_COMMENT_PATTERN, re.DOTALL)
_MACRO = re.compile(
    r"\{%-?\s*macro\s+(?P<name>\w+)\s*\(.*?%\}(?P<body>.*?)\{%-?\s*endmacro\b[^%]*%\}",
    re.DOTALL,
)
_OTHER_ENGINE_PREFIXES = ("trino__", "duckdb__")
# YAML keys whose values are prose or fixture data, never SQL.
_NON_SQL_KEYS = frozenset({"description", "rows", "meta", "tags"})


def sql_code(source: str) -> str:
    """Return *source* with its comments, quoted text and Jinja blanked out.

    Blanking keeps every offset and newline, so a match in the result indexes
    *source*. The strings inside a Jinja expression or statement are kept, and
    read the same way, because that is how SQL is handed to a macro:
    ``{{ from_iso8601_timestamp("created_on || 'Z'") }}``.

    :param source: The text of a SQL file, or a SQL string from YAML.
    :returns: *source* with only its SQL code left in place.
    :rtype: str
    """
    chars = list(source)
    for match in _TOKEN.finditer(source):
        start, end = match.span()
        chars[start:end] = _NOT_NEWLINE.sub(" ", match.group())
        if match.group("jinja"):
            for string in _JINJA_STRING.finditer(match.group("jinja")):
                group = "single" if string.group("single") is not None else "double"
                string_start, string_end = string.span(group)
                chars[start + string_start : start + string_end] = sql_code(string.group(group))
    return "".join(chars)


def jinja_statement_spans(source: str) -> list[tuple[int, int]]:
    """Return the spans of the ``{% ... %}`` statements in *source*.

    A statement inside a comment or a quoted SQL string is part of that token, so it is not one.

    :param source: The text of a SQL file, or a SQL string from YAML.
    :returns: The ``(start, end)`` offsets of each statement, tag delimiters included.
    :rtype: list[tuple[int, int]]
    """
    return [
        match.span() for match in _TOKEN.finditer(source) if match.group("jinja") and match.group().startswith("{%")
    ]


def macro_declarations(source: str) -> Iterator[re.Match[str]]:
    """Find the macros *source* defines. A macro inside a Jinja comment defines nothing.

    The comments are blanked rather than removed so offsets still index *source*.

    :param source: The text of a SQL file.
    :returns: A match per macro, with ``name`` and ``body`` groups.
    :rtype: Iterator[re.Match[str]]
    """
    return _MACRO.finditer(_JINJA_COMMENT.sub(lambda m: _NOT_NEWLINE.sub(" ", m.group()), source))


def starrocks_macro_names(sources: Iterable[str]) -> frozenset[str]:
    """Return the names of the macros *sources* give a ``starrocks__`` body, without the prefix.

    :param sources: The text of every SQL file in the project.
    :returns: The dispatched macro names StarRocks has its own body for.
    :rtype: frozenset[str]
    """
    return frozenset(
        match.group("name").removeprefix("starrocks__")
        for source in sources
        for match in macro_declarations(source)
        if match.group("name").startswith("starrocks__")
    )


def other_engine_spans(source: str, starrocks_macros: frozenset[str]) -> list[tuple[int, int]]:
    """Return the spans of the macro bodies in *source* that StarRocks never renders.

    That is ``trino__x`` and ``duckdb__x`` always, and ``default__x`` when the
    project also defines ``starrocks__x``.

    :param source: The text of a SQL file.
    :param starrocks_macros: The result of :func:`starrocks_macro_names` for the project.
    :returns: The ``(start, end)`` offsets of each exempt body.
    :rtype: list[tuple[int, int]]
    """
    return [
        match.span("body")
        for match in macro_declarations(source)
        if match.group("name").startswith(_OTHER_ENGINE_PREFIXES)
        or (
            match.group("name").startswith("default__")
            and match.group("name").removeprefix("default__") in starrocks_macros
        )
    ]


def yaml_sql_strings(node: Any, path: str = "") -> Iterator[tuple[str, str]]:
    """Yield ``(key path, value)`` for every string in *node* that may hold SQL.

    :param node: A parsed YAML document, or a node of one.
    :param path: The key path of *node*, prefixed to the paths yielded.
    :returns: The key path and value of each string outside prose and fixture data.
    :rtype: Iterator[tuple[str, str]]
    """
    if isinstance(node, str):
        yield path, node
    elif isinstance(node, list):
        for index, item in enumerate(node):
            yield from yaml_sql_strings(item, f"{path}[{index}]")
    elif isinstance(node, dict):
        for key, value in node.items():
            # A unit test fixture written as SQL is the one `rows` that is not data.
            if key in _NON_SQL_KEYS and not (key == "rows" and node.get("format") == "sql"):
                continue
            yield from yaml_sql_strings(value, f"{path}.{key}" if path else str(key))
