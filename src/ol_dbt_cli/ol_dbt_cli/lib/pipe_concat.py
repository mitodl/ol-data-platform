"""Refuse the ``||`` operator in the dbt project's SQL.

StarRocks reads ``||`` as a logical OR at its default ``sql_mode``, so
``'a' || 'b'`` returns NULL there and nothing errors: a model that concatenates
with it builds and holds wrong values. Trino and DuckDB concatenate. Strings go
through ``dbt.concat``, which renders ``a || b`` on Trino and DuckDB and
``concat(a, b)`` on StarRocks.

What is scanned and what is exempt is described in ``ol_dbt_cli.lib.operators``.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from ol_dbt_cli.lib.operators import check_operator

if TYPE_CHECKING:
    from pathlib import Path

    from ol_dbt_cli.lib.validation import ValidationReport

PIPE_CONCAT_CHECK = "pipe_concat"

_DETAIL = (
    "StarRocks evaluates || as a logical OR and returns NULL or 1 without an error. "
    "Concatenate strings with {{ dbt.concat([a, b]) }}; append to an array with a dispatched macro."
)

_PIPES = re.compile(r"\|\|")


def check_pipe_concat(dbt_dir: Path, report: ValidationReport) -> None:
    """Report every ``||`` in the project's SQL and YAML that StarRocks would evaluate.

    :param dbt_dir: The dbt project directory.
    :param report: The report to add an ERROR to for each occurrence.
    """
    check_operator(dbt_dir, report, check=PIPE_CONCAT_CHECK, pattern=_PIPES, operator="the || operator", detail=_DETAIL)
