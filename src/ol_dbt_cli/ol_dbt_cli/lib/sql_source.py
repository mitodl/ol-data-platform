"""Read the SQL in the dbt project's source files past comments, quoted text and Jinja.

The checks that read source files rather than compiled SQL (``pipe_concat``,
``raw_json_calls``) look for an operator or a function call in the SQL a file
hands to the warehouse, and a match inside a comment or a string literal is not one.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

# A Jinja block ends only at a closing delimiter outside its strings, so a hook
# such as `post_hook="update t set v = a || {{ this }}"` is consumed whole.
_JINJA_QUOTED = r"""'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*\""""
_TOKEN = re.compile(
    rf"""
      \{{\#.*?\#\}}              # Jinja comment
    | (?P<jinja>
          \{{\{{(?:{_JINJA_QUOTED}|[^'"}}]|\}}(?!\}}))*\}}\}}
        | \{{%(?:{_JINJA_QUOTED}|[^'"%]|%(?!\}}))*%\}}
      )
    | /\*.*?\*/                # SQL block comment
    | --[^\n]*                 # SQL line comment
    | '(?:[^'\\]|\\.|'')*'     # single-quoted string
    | "(?:[^"\\]|\\.|"")*"     # double-quoted identifier, or a Jinja string
    """,
    re.DOTALL | re.VERBOSE,
)
_JINJA_STRING = re.compile(r"""'(?P<single>(?:[^'\\]|\\.)*)'|"(?P<double>(?:[^"\\]|\\.)*)\"""")
_NOT_NEWLINE = re.compile(r"[^\n]")
_JINJA_COMMENT = re.compile(r"\{#.*?#\}", re.DOTALL)
_MACRO = re.compile(
    r"\{%-?\s*macro\s+(?P<name>\w+)\s*\(.*?%\}(?P<body>.*?)\{%-?\s*endmacro\b[^%]*%\}",
    re.DOTALL,
)
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


def macro_declarations(source: str) -> Iterator[re.Match[str]]:
    """Find the macros *source* defines. A macro inside a Jinja comment defines nothing.

    The comments are blanked rather than removed so offsets still index *source*.

    :param source: The text of a SQL file.
    :returns: A match per macro, with ``name`` and ``body`` groups.
    :rtype: Iterator[re.Match[str]]
    """
    return _MACRO.finditer(_JINJA_COMMENT.sub(lambda m: _NOT_NEWLINE.sub(" ", m.group()), source))


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
