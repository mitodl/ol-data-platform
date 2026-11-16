"""Refuse a staging model that reads a delete-filtered raw table without the filter.

A raw table synced incrementally from a database keeps every row the source
deletes, and a dlt merge feed keeps every record the source dropped.
``raw_delete_filter_map()`` names those tables, and ``live_raw_rows`` selects one
less its deleted records, which is what the raw history snapshot reads. A staging
model that reads such a table through ``source()`` alone serves the deleted
records the snapshot has closed, so staging and history disagree on what exists
(witan task tk-filter-deleted-keys-out-of-staging-for-keyset-ta-0bda08).

The table list comes from the generated macro rather than the inventory, because
that file is what dbt reads at run time, and `ol-dbt inventory history` already
fails CI when it and the inventory disagree. A ``{{ ... }}`` expression that
calls ``source()`` for such a table is always reported, since it reads the table
unfiltered. ``source()`` in a ``{% set %}``, e.g. to hand a relation to
``relation_has_column``, passes when the model also calls
``live_raw_rows('<table>'``. SQL and Jinja comments are ignored.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from ol_dbt_cli.lib.raw_history import DELETE_FILTER_MACRO_PATH
from ol_dbt_cli.lib.sql_source import without_jinja_comments
from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from pathlib import Path

RAW_DELETE_FILTER_CHECK = "raw_delete_filter"

_MAP_ENTRY = re.compile(r"'(?P<table>raw__\w+)':\s*\{'kind'")
_SOURCE = re.compile(r"source\(\s*['\"][^'\"]+['\"]\s*,\s*['\"](?P<table>raw__\w+)['\"]\s*\)")
_EXPRESSION = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_SQL_COMMENT = re.compile(r"--[^\n]*")
_DETAIL = (
    "The table is in raw_delete_filter_map(), so raw still holds records the source deleted. "
    "Read it with {{ live_raw_rows('<raw table>') }} (or live_raw_rows('<raw table>', <relation>)) "
    "so staging drops them as the raw history snapshot does."
)


def delete_filtered_tables(dbt_dir: Path) -> set[str]:
    """Return the raw tables ``raw_delete_filter_map()`` filters.

    :param dbt_dir: The dbt project directory.
    :returns: The raw table names, empty when the generated macro is absent.
    :rtype: set[str]
    """
    path = dbt_dir / "macros" / DELETE_FILTER_MACRO_PATH.name
    if not path.exists():
        return set()
    return {match.group("table") for match in _MAP_ENTRY.finditer(path.read_text())}


def check_raw_delete_filter(dbt_dir: Path, report: ValidationReport) -> None:
    """Report each staging model that reads a delete-filtered raw table without ``live_raw_rows``.

    :param dbt_dir: The dbt project directory.
    :param report: The report to add an ERROR to for each table a model reads unfiltered.
    """
    tables = delete_filtered_tables(dbt_dir)
    if not tables:
        return
    for path in sorted((dbt_dir / "models" / "staging").rglob("*.sql")):
        code = _SQL_COMMENT.sub("", without_jinja_comments(path.read_text()))
        direct = {
            m.group("table")
            for e in _EXPRESSION.finditer(code)
            if "live_raw_rows(" not in e.group(0)
            for m in _SOURCE.finditer(e.group(0))
        }
        for table in sorted({m.group("table") for m in _SOURCE.finditer(code)} & tables):
            if table not in direct and re.search(rf"live_raw_rows\(\s*['\"]{table}['\"]", code):
                continue
            report.add(
                RAW_DELETE_FILTER_CHECK,
                Severity.ERROR,
                path.stem,
                f"{path.relative_to(dbt_dir).as_posix()} reads {table} through source() without live_raw_rows",
                _DETAIL,
            )
