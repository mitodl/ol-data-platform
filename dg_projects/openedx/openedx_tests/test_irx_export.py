"""Tests for the IRx facade export: byte parity with legacy, and asset wiring."""

import csv
import hashlib
import io
from datetime import datetime
from typing import Any

import polars as pl
from dagster import AssetKey
from openedx.assets.irx_export import (
    IRX_EXPORT_FILES,
    build_irx_export_asset,
    legacy_csv_columns,
    write_legacy_csv,
)
from upath import UPath

# Naive, as MySQL DATETIME and Iceberg timestamp both are.
ROWS: list[dict[str, Any]] = [
    {
        "id": 1,
        "flag": True,
        "ts": datetime.fromisoformat("2021-08-19 19:25:54"),
        "g": None,
    },
    {
        "id": 2,
        "flag": False,
        "ts": datetime.fromisoformat("2026-09-08 19:32:48.544749"),
        "g": 1.0,
    },
    {"id": 3, "flag": None, "ts": None, "g": 0.5},
    {
        "id": 4,
        "flag": True,
        "ts": datetime.fromisoformat("2026-09-08 19:32:48.000005"),
        "g": 0.3,
    },
]
STRINGS = ['a,"b"', "line\nbreak", "", " lead"]
COLUMNS = ["id", "flag", "ts", "g", "s"]


def _legacy_bytes() -> bytes:
    """Render the same rows through legacy_openedx's write_csv path."""
    out = io.StringIO(newline="")
    writer = csv.DictWriter(out, COLUMNS)
    writer.writeheader()
    for row, text in zip(ROWS, STRINGS, strict=True):
        flag = None if row["flag"] is None else int(row["flag"])
        writer.writerow({**row, "flag": flag, "s": text})
    return out.getvalue().encode()


def test_export_bytes_match_legacy_csv_module_output(tmp_path) -> None:
    frame = pl.LazyFrame(
        [{**row, "s": text} for row, text in zip(ROWS, STRINGS, strict=True)]
    )
    destination = UPath(tmp_path / "out.csv")

    sha256, size = write_legacy_csv(
        frame.select(legacy_csv_columns(frame.collect_schema(), COLUMNS)),
        destination,
    )

    written = destination.read_bytes()
    assert written == _legacy_bytes()
    assert sha256 == hashlib.sha256(written).hexdigest()
    assert size == len(written)


def test_role_users_projects_name_to_the_role_header() -> None:
    role_users = next(f for f in IRX_EXPORT_FILES if f.name == "role_users")

    assert role_users.renames == {"name": "role"}
    assert role_users.columns == ("id", "user_id", "org", "course_id", "role")


def test_every_file_depends_on_its_irx_model() -> None:
    asset = build_irx_export_asset("mitxonline")

    deps = {
        key.path[-1]: {dep.asset_key for dep in spec.deps}
        for key, spec in asset.specs_by_key.items()
    }

    assert deps.pop("course_ids") == set()
    assert deps == {
        export.name: {
            AssetKey(["external", f"irx__mitxonline__openedx__mysql__{export.model}"])
        }
        for export in IRX_EXPORT_FILES
    }
