"""Fixtures every test shares."""

from __future__ import annotations

import pytest

from ol_dbt_cli.lib import fixture_generation


@pytest.fixture(autouse=True)
def _own_table_factories(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give each test its own record of which factory fills which raw table.

    Tests declare throwaway factories for the same tables, and import packages
    that fill the demo unit's tables under more than one module name.
    """
    monkeypatch.setattr(fixture_generation, "_TABLE_FACTORIES", {})
