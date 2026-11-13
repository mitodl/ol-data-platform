"""Fixtures every test shares."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from ol_dbt_cli.lib import fixture_generation

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def _own_table_factories(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give each test its own record of which factory fills which raw table.

    Tests declare throwaway factories for the same tables, and import packages
    that fill the demo unit's tables under more than one module name.
    """
    monkeypatch.setattr(fixture_generation, "_TABLE_FACTORIES", {})


@pytest.fixture
def dbt_dir(tmp_path: Path) -> Path:
    """Lay out an empty dbt project with ``models/`` and ``macros/`` in *tmp_path*.

    :param tmp_path: The directory to use as the project root.
    :returns: *tmp_path*.
    :rtype: Path
    """
    for name in ("models", "macros"):
        (tmp_path / name).mkdir()
    (tmp_path / "dbt_project.yml").write_text("name: p\n")
    return tmp_path
