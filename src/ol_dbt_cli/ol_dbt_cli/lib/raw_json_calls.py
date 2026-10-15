"""Refuse a JSON function called outside a macro in the dbt project's SQL.

Trino, DuckDB and StarRocks name their JSON functions differently and treat
malformed input differently: Trino's ``json_parse`` raises where its
``json_query`` returns NULL, and DuckDB has no ``json_parse``. A model that calls
one directly builds on one engine and fails, or returns other values, on the
others. The macros in ``macros/`` dispatch to a body per engine, so a JSON function is
called only from there. #2837 and #2978 converted every direct call in the
models; nothing kept them out while that was under way, and raw
``json_extract_scalar`` calls went from 14 to 22 between July and September 2026.

Like ``pipe_concat`` the check reads source files, not compiled SQL, because PR
CI compiles for DuckDB only. The body of a macro named for an engine
(``default__x``, ``duckdb__x``, ``starrocks__x``, ``trino__x``) is exempt: that
is where these calls belong. The rest of ``macros/`` is read like a model.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import yaml

from ol_dbt_cli.lib.sql_source import macro_declarations, sql_code, yaml_sql_strings
from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

RAW_JSON_CALLS_CHECK = "raw_json_calls"

_SQL_DIRS = ("models", "macros", "tests", "snapshots", "analyses")
_YAML_DIRS = (*_SQL_DIRS, "seeds")
_ENGINE_PREFIXES = ("default__", "duckdb__", "starrocks__", "trino__")
_CALL = re.compile(r"(?<!\w)(?P<name>json_\w+|is_json_scalar)\s*\(", re.IGNORECASE)
# The macros that replace the calls the conversions met. Any other JSON function
# gets the generic advice.
_MACROS = {
    "json_parse": "json_strict_value, json_extract_json_array, json_parse_nested_varchar_array or unnest_json_array",
    "json_extract": "json_strict_value, json_extract_json_array or json_extract_value",
    "json_extract_scalar": (
        "json_extract_scalar (as {{ json_extract_scalar(col, path) }}), "
        "or json_strict_value to keep raising on malformed JSON"
    ),
    "json_query": "json_query_string, json_path_values or json_array_field_values",
    "json_format": "json_to_varchar or array_to_json_string",
    "json_object": "json_object_pair or json_object_from_pairs",
}
_DETAIL = (
    "JSON functions differ by name and by malformed-input behaviour across Trino, DuckDB and StarRocks. "
    "Call a dispatch macro; add one to macros/cross_db_functions.sql if none fits."
)


def _calls(sql: str) -> Iterator[re.Match[str]]:
    return _CALL.finditer(sql_code(sql))


def _engine_spans(source: str) -> list[tuple[int, int]]:
    return [
        match.span("body") for match in macro_declarations(source) if match.group("name").startswith(_ENGINE_PREFIXES)
    ]


def _advice(name: str) -> str:
    macros = _MACROS.get(name.lower())
    return f"; use a macro: {macros}" if macros else ""


def check_raw_json_calls(dbt_dir: Path, report: ValidationReport) -> None:
    """Report every JSON function called directly in the project's SQL and YAML outside an engine body.

    :param dbt_dir: The dbt project directory.
    :param report: The report to add an ERROR to for each call.
    """
    sql_files = sorted(path for name in _SQL_DIRS for path in (dbt_dir / name).rglob("*.sql"))
    yaml_files = [
        dbt_dir / "dbt_project.yml",
        *sorted(
            path for name in _YAML_DIRS for pattern in ("*.yml", "*.yaml") for path in (dbt_dir / name).rglob(pattern)
        ),
    ]

    for path in sql_files:
        source = path.read_text()
        exempt = _engine_spans(source)
        for call in _calls(source):
            if any(start <= call.start() < end for start, end in exempt):
                continue
            name = call.group("name")
            line = source.count("\n", 0, call.start()) + 1
            report.add(
                RAW_JSON_CALLS_CHECK,
                Severity.ERROR,
                path.stem,
                f"{path.relative_to(dbt_dir).as_posix()}:{line} calls {name}() directly{_advice(name)}",
                _DETAIL,
            )

    for path in yaml_files:
        for key_path, value in yaml_sql_strings(yaml.safe_load(path.read_text())):
            for call in _calls(value):
                name = call.group("name")
                report.add(
                    RAW_JSON_CALLS_CHECK,
                    Severity.ERROR,
                    path.stem,
                    f"{path.relative_to(dbt_dir).as_posix()} calls {name}() directly in {key_path}{_advice(name)}",
                    _DETAIL,
                )
