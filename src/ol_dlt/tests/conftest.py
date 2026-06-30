"""Shared pytest fixtures for ol_dlt tests.

The ``test`` profile runs every pipeline hermetically against an ephemeral
filesystem destination in a tmp dir — no AWS, Glue, or Dagster required.
"""

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest


class FakeResponse:
    """Minimal stand-in for ``requests.Response`` for mocked HTTP in tests."""

    def __init__(
        self,
        *,
        json_data: Any = None,
        content: bytes = b"",
        status_code: int = 200,
    ) -> None:
        self._json = json_data
        self.content = content
        self.status_code = status_code

    def json(self) -> Any:
        return self._json

    def raise_for_status(self) -> None:
        if self.status_code >= 400:  # noqa: PLR2004
            msg = f"HTTP {self.status_code}"
            raise RuntimeError(msg)


@pytest.fixture
def test_profile(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Path]:
    """Activate the ephemeral ``test`` profile pointing at a per-test tmp dir.

    Yields the destination root path so materialization tests can inspect the
    written parquet files directly if needed.
    """
    dest_root = tmp_path / "dest"
    dest_root.mkdir()
    monkeypatch.setenv("DLT_PROFILE", "test")
    monkeypatch.setenv("OL_DLT_BUCKET_URL", dest_root.as_uri())
    # Keep dlt's working/pipeline state inside the tmp dir too.
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    os.environ.pop("DLT_PROJECT_DIR", None)
    yield dest_root
