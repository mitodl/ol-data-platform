import sys
from pathlib import Path

import pytest

from ol_dbt_cli.lib.dbt_executable import dbt_executable


def test_prefers_dbt_next_to_interpreter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "dbt").touch()
    monkeypatch.setattr(sys, "executable", str(tmp_path / "python"))
    monkeypatch.setenv("PATH", "/nonexistent")
    assert dbt_executable() == str(tmp_path / "dbt")


def test_falls_back_to_path_without_sibling(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "executable", str(tmp_path / "python"))
    assert dbt_executable() == "dbt"
