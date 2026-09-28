"""Tests for locating the ingestion inventory from the lakehouse package."""

from pathlib import Path

import pytest
from lakehouse.lib.inventory import INVENTORY_FALLBACK, find_inventory_dir


class TestInventoryResolution:
    """The one part every other test here monkeypatches away.

    `INVENTORY_DIR` is module-level, so getting it wrong is an import error, not
    a test failure -- which is exactly how a version that indexed a fixed parent
    reached production and crash-looped the whole lakehouse code location. These
    exercise the real resolver.
    """

    def test_resolves_the_real_inventory_from_the_source_tree(self) -> None:
        found = find_inventory_dir()
        assert found.is_dir()
        assert (found / "units").is_dir()

    def test_finds_the_inventory_at_any_depth_above_the_module(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The image lays the package out two levels below the root where the
        # source tree lays it out four, so the resolver must not assume either.
        module = tmp_path / "app" / "lakehouse" / "lib" / "inventory.py"
        module.parent.mkdir(parents=True)
        module.touch()
        expected = tmp_path / "app" / "ingestion" / "inventory"
        expected.mkdir(parents=True)
        monkeypatch.setattr("lakehouse.lib.inventory.__file__", str(module))

        assert find_inventory_dir() == expected

    def test_returns_a_path_rather_than_raising_when_absent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A missing inventory has to stay a runtime failure with a message, not
        # an unimportable module that takes the code location down with it.
        module = tmp_path / "nowhere" / "inventory.py"
        module.parent.mkdir(parents=True)
        module.touch()
        monkeypatch.setattr("lakehouse.lib.inventory.__file__", str(module))

        assert find_inventory_dir() == INVENTORY_FALLBACK
