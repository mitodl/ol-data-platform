"""Harness for the tests of the validate checks that read the dbt project's source files."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def make_dbt_dir(tmp_path: Path) -> Path:
    """Lay out an empty dbt project with ``models/`` and ``macros/`` in *tmp_path*.

    :param tmp_path: The directory to use as the project root.
    :returns: *tmp_path*.
    :rtype: Path
    """
    for name in ("models", "macros"):
        (tmp_path / name).mkdir()
    (tmp_path / "dbt_project.yml").write_text("name: p\n")
    return tmp_path


def error_messages(check: Callable[[Path, ValidationReport], None], check_name: str, dbt_dir: Path) -> list[str]:
    """Run *check* over *dbt_dir* and return its messages, asserting each is an ERROR of *check_name*.

    :param check: The check function, e.g. ``check_pipe_concat``.
    :param check_name: The name the check reports its issues under.
    :param dbt_dir: The dbt project directory.
    :returns: The message of each issue, in report order.
    :rtype: list[str]
    """
    report = ValidationReport()
    check(dbt_dir, report)
    assert all(i.check == check_name and i.severity == Severity.ERROR for i in report.issues)
    return [i.message for i in report.issues]
