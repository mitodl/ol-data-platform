"""Shared pytest fixtures for ol_dlt tests.

The ``test`` profile runs every pipeline hermetically against an ephemeral
filesystem destination in a tmp dir — no AWS, Glue, or Dagster required.
"""

import json
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


@pytest.fixture
def sqlite_iceberg_lake(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point dlt's Iceberg catalog at SQLite on disk and return the lake root.

    The repo's .dlt/config.toml points dlt at the Glue catalog. dlt's in-memory
    fallback is rebuilt per client and loses the table between loads, which is
    exactly the state a test needs to survive in order to evolve an existing
    table rather than recreate one.
    """
    lake = tmp_path / "lake"
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt_data"))
    monkeypatch.setenv("ICEBERG_CATALOG__ICEBERG_CATALOG_NAME", "evolution_test")
    monkeypatch.setenv("ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE", "sql")
    monkeypatch.setenv(
        "ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG",
        json.dumps(
            {
                "type": "sql",
                "uri": f"sqlite:///{tmp_path}/catalog.db",
                "warehouse": lake.as_uri(),
            }
        ),
    )
    return lake
