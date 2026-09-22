"""Tests for the IRx facade export: byte parity with legacy, and asset wiring."""

import csv
import hashlib
import io
import json
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import polars as pl
import pytest
from dagster import AssetKey, materialize
from openedx.assets import irx_export
from openedx.assets.irx_export import (
    IRX_EXPORT_FILES,
    MANIFEST_NAME,
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

    sha256, size, row_count = write_legacy_csv(
        frame.select(legacy_csv_columns(frame.collect_schema(), COLUMNS)),
        destination,
    )

    written = destination.read_bytes()
    assert written == _legacy_bytes()
    assert sha256 == hashlib.sha256(written).hexdigest()
    assert size == len(written)
    assert row_count == len(ROWS)


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
    assert deps.pop("forum_contents") == {
        AssetKey(["external", "irx__mitxonline__openedx__mysql__forum_contents"])
    }
    assert deps.pop("manifest") == {
        AssetKey(["mitxonline", "irx_export", name])
        for name in (
            "course_ids",
            *(export.name for export in IRX_EXPORT_FILES),
            "forum_contents",
        )
    }
    assert deps == {
        export.name: {
            AssetKey(["external", f"irx__mitxonline__openedx__mysql__{export.model}"])
        }
        for export in IRX_EXPORT_FILES
    }


DROP_DATE = "2026-09-20"
COURSE_ID = "course-v1:MITx+1.00x+3T2026"


class _Snapshot(SimpleNamespace):
    snapshot_id = 42


class _Table:
    def __init__(self, name: str):
        self.name = name

    def current_snapshot(self) -> _Snapshot:
        return _Snapshot()


def _irx_frame(table: _Table) -> pl.LazyFrame:
    if table.name.endswith("forum_contents"):
        return pl.LazyFrame(schema={"_type": pl.String, "id": pl.Int64})
    export = next(f for f in IRX_EXPORT_FILES if table.name.endswith(f.model))
    model_names = {new: old for old, new in export.renames.items()}
    row = {model_names.get(c, c): "x" for c in export.columns} | {
        "course_id": COURSE_ID
    }
    return pl.LazyFrame([row, {**row, "course_id": "course-v1:not+listed+run"}])


@pytest.fixture
def drop_root(tmp_path, monkeypatch) -> UPath:
    # Both roots, so a DAGSTER_ENVIRONMENT exported in the shell can't send the
    # test's files to a real bucket.
    monkeypatch.setattr(irx_export, "IRX_EXPORT_ROOTS", {})
    monkeypatch.setattr(irx_export, "IRX_EXPORT_SANDBOX_ROOT", str(tmp_path))
    monkeypatch.setattr(
        irx_export, "load_dbt_model_table", lambda _db, name: _Table(name)
    )
    drop = UPath(tmp_path) / "mitx" / DROP_DATE.replace("-", "")
    # S3 has no directories to create; a local path does.
    (drop / "forum").mkdir(parents=True)
    return drop


def _run_export(monkeypatch, fail_on: str | None = None):
    def scan(table: _Table, _snapshot_id: int) -> pl.LazyFrame:
        if fail_on and table.name.endswith(fail_on):
            raise RuntimeError
        return _irx_frame(table)

    monkeypatch.setattr(irx_export, "scan_dbt_model_table", scan)
    openedx = SimpleNamespace(
        client=SimpleNamespace(get_edx_course_ids=lambda: [[{"id": COURSE_ID}]])
    )
    return materialize(
        [build_irx_export_asset("mitx")],
        partition_key=DROP_DATE,
        resources={"openedx": openedx},
        raise_on_error=False,
    )


def test_manifest_lists_every_delivered_file(drop_root, monkeypatch) -> None:
    result = _run_export(monkeypatch)

    assert result.success
    manifest = json.loads((drop_root / MANIFEST_NAME).read_bytes())
    assert manifest["deployment"] == "mitx"
    assert manifest["drop_date"] == "20260920"
    expected = [
        "course_ids.csv",
        *(f"{f.name}.csv" for f in IRX_EXPORT_FILES),
        "forum/contents.bson",
    ]
    assert [entry["name"] for entry in manifest["files"]] == expected
    for entry in manifest["files"]:
        data = (drop_root / entry["name"]).read_bytes()
        assert entry["sha256"] == hashlib.sha256(data).hexdigest()
        assert entry["size_bytes"] == len(data)
    # The course list filters out the unlisted run.
    assert {entry["row_count"] for entry in manifest["files"][:-1]} == {1}


def test_failed_rerun_takes_the_old_manifest_down(drop_root, monkeypatch) -> None:
    assert _run_export(monkeypatch).success
    assert (drop_root / MANIFEST_NAME).exists()

    result = _run_export(monkeypatch, fail_on="courseware_studentmodule")

    assert not result.success
    assert not (drop_root / MANIFEST_NAME).exists()


def test_rerun_fails_when_the_old_manifest_cannot_be_deleted(
    drop_root, monkeypatch
) -> None:
    assert _run_export(monkeypatch).success
    written = (drop_root / "users_query.csv").stat().st_mtime_ns
    # s3fs reports a denied delete as a success.
    monkeypatch.setattr(type(drop_root), "unlink", lambda *_args, **_kwargs: None)

    result = _run_export(monkeypatch)

    assert not result.success
    assert (drop_root / "users_query.csv").stat().st_mtime_ns == written
