"""Refuse ``IS [NOT] DISTINCT FROM`` in the dbt project's SQL.

StarRocks (4.1.7) rejects both forms as a syntax error, and DuckDB, which PR CI
compiles and unit tests on, accepts them, so a model that writes one passes CI
and fails only when StarRocks builds it. Null-safe inequality goes through
``is_distinct_from``, which renders ``a is distinct from b`` on Trino and DuckDB
and ``not (a <=> b)`` on StarRocks.

What is scanned and what is exempt is described in ``ol_dbt_cli.lib.operators``.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from ol_dbt_cli.lib.operators import check_operator

if TYPE_CHECKING:
    from pathlib import Path

    from ol_dbt_cli.lib.validation import ValidationReport

DISTINCT_FROM_CHECK = "distinct_from"

_DETAIL = (
    "StarRocks has no IS DISTINCT FROM or IS NOT DISTINCT FROM and fails the build with a syntax error. "
    "Write {{ is_distinct_from(a, b) }}, or not {{ is_distinct_from(a, b) }} for null-safe equality."
)

_DISTINCT_FROM = re.compile(r"\bis\s+(?:not\s+)?distinct\s+from\b", re.IGNORECASE)


def check_distinct_from(dbt_dir: Path, report: ValidationReport) -> None:
    """Report every ``IS [NOT] DISTINCT FROM`` in the project's SQL and YAML that StarRocks would evaluate.

    :param dbt_dir: The dbt project directory.
    :param report: The report to add an ERROR to for each occurrence.
    """
    check_operator(
        dbt_dir,
        report,
        check=DISTINCT_FROM_CHECK,
        pattern=_DISTINCT_FROM,
        operator="IS DISTINCT FROM",
        detail=_DETAIL,
    )
