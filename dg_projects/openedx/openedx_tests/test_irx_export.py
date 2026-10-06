"""Tests for the IRx facade export: byte parity with legacy, and asset wiring."""

import csv
import hashlib
import io
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import polars as pl
import pyarrow as pa
import pytest
from dagster import AssetKey, Failure, materialize
from openedx.assets import irx_export
from openedx.assets.irx_export import (
    IRX_EXPORT_FILES,
    MANIFEST_NAME,
    _DigestingWriter,
    build_irx_export_asset,
    legacy_csv_columns,
    read_data_files,
    write_legacy_csv,
    write_parquet,
)
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.table import Table
from upath import UPath

FORUM_ROWS: list[dict[str, Any]] = [
    {
        "_type": "CommentThread",
        "id": 1,
        "votes_up": ["11"],
        "votes_down": [],
        "abuse_flaggers": [],
        "historical_abuse_flaggers": [],
    },
    {
        "_type": "Comment",
        "id": 2,
        "votes_up": [],
        "votes_down": ["22"],
        "abuse_flaggers": ["33"],
        "historical_abuse_flaggers": [],
    },
]

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
    frame = pl.DataFrame(
        [{**row, "s": text} for row, text in zip(ROWS, STRINGS, strict=True)]
    )
    projected = frame.select(legacy_csv_columns(frame.schema, COLUMNS))
    destination = UPath(tmp_path / "out.csv")

    # An empty leading batch, as read_data_files yields, then the rows split
    # across batches: one header, and no batch boundary shows in the bytes.
    sha256, size, row_count = write_legacy_csv(
        [projected.clear(), projected.head(1), projected.tail(3)], destination
    )

    written = destination.read_bytes()
    assert written == _legacy_bytes()
    assert sha256 == hashlib.sha256(written).hexdigest()
    assert size == len(written)
    assert row_count == len(ROWS)


def test_row_count_ignores_crlf_inside_quoted_fields_at_any_chunk_boundary() -> None:
    frame = pl.LazyFrame(
        {"id": [1, 2, 3], "s": ['crlf\r\n"inside" quotes', "\r", 'x""\r\n']}
    )
    csv_bytes = io.BytesIO()
    frame.sink_csv(csv_bytes, line_terminator="\r\n")
    data = csv_bytes.getvalue()

    for split in range(len(data) + 1):
        writer = _DigestingWriter(io.BytesIO())
        writer.write(data[:split])
        writer.write(data[split:])
        assert writer.records - 1 == 3, split


def test_write_parquet_round_trips_array_columns_and_counts_rows(tmp_path) -> None:
    frame = pl.DataFrame(FORUM_ROWS)
    destination = UPath(tmp_path / "out.parquet")

    sha256, size, row_count = write_parquet(
        [frame.clear(), frame.head(1), frame.tail(1)], destination
    )

    written = destination.read_bytes()
    assert sha256 == hashlib.sha256(written).hexdigest()
    assert size == len(written)
    assert row_count == len(FORUM_ROWS)
    read_back = pl.read_parquet(written)
    assert read_back["votes_up"].to_list() == [row["votes_up"] for row in FORUM_ROWS]
    assert read_back["abuse_flaggers"].to_list() == [
        row["abuse_flaggers"] for row in FORUM_ROWS
    ]


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


def _irx_frame(table: _Table) -> pl.DataFrame:
    if table.name.endswith("forum_contents"):
        return pl.DataFrame(FORUM_ROWS)
    export = next(f for f in IRX_EXPORT_FILES if table.name.endswith(f.model))
    # A model column the CSV doesn't carry, which the read has to project away.
    row = dict.fromkeys(export.source_columns, "x") | {
        "course_id": COURSE_ID,
        "unexported": "x",
    }
    return pl.DataFrame([row, {**row, "course_id": "course-v1:not+listed+run"}])


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
    drop.mkdir(parents=True)
    return drop


def _run_export(monkeypatch, fail_on: str | None = None):
    def read(
        table: _Table, _snapshot_id: int, columns: tuple[str, ...]
    ) -> Iterator[pl.DataFrame]:
        if fail_on and table.name.endswith(fail_on):
            raise RuntimeError
        frame = _irx_frame(table)
        if columns != ("*",):
            frame = frame.select(columns)
        # One batch per row, after the empty schema batch, so the drop is
        # assembled across batches the way a multi-file table's would be.
        yield frame.clear()
        yield from frame.iter_slices(1)

    monkeypatch.setattr(irx_export, "read_data_files", read)
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
        "forum_contents.parquet",
    ]
    assert [entry["name"] for entry in manifest["files"]] == expected
    for entry in manifest["files"]:
        data = (drop_root / entry["name"]).read_bytes()
        assert entry["sha256"] == hashlib.sha256(data).hexdigest()
        assert entry["size_bytes"] == len(data)
    row_counts = {entry["name"]: entry["row_count"] for entry in manifest["files"]}
    # The course list filters out the unlisted run in the five course-scoped files.
    assert {row_counts[f"{f.name}.csv"] for f in IRX_EXPORT_FILES} == {1}
    assert row_counts["course_ids.csv"] == 1
    # Forum isn't cut to the course list, so both fixture rows are delivered.
    assert row_counts["forum_contents.parquet"] == len(FORUM_ROWS)


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


ICEBERG_SCHEMA = pa.schema(
    [("id", pa.int64()), ("course_id", pa.string()), ("unexported", pa.string())]
)


def _iceberg_table(tmp_path, data_files: int) -> Table:
    """Build a local Iceberg table with one data file per append."""
    catalog = SqlCatalog(
        "irx",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}",
    )
    catalog.create_namespace("irx")
    table = catalog.create_table("irx.studentmodule", schema=ICEBERG_SCHEMA)
    for n in range(data_files):
        table.append(
            pa.table(
                {
                    "id": [2 * n, 2 * n + 1],
                    "course_id": [COURSE_ID] * 2,
                    "unexported": ["x"] * 2,
                },
                schema=ICEBERG_SCHEMA,
            )
        )
    return table


def test_read_data_files_opens_one_data_file_per_pull(tmp_path, monkeypatch) -> None:
    table = _iceberg_table(tmp_path, data_files=3)
    opened: list[str] = []
    new_input = table.io.new_input

    def spy(location: str) -> Any:
        if location.endswith(".parquet"):
            opened.append(location)
        return new_input(location)

    monkeypatch.setattr(table.io, "new_input", spy)

    batches = read_data_files(
        table, table.current_snapshot().snapshot_id, ("id", "course_id")
    )
    schema_batch = next(batches)
    assert schema_batch.height == 0
    assert schema_batch.columns == ["id", "course_id"]
    assert opened == []
    first = next(batches)
    # Pulling the first rows opens the first data file and nothing past it.
    assert len(opened) == 1
    assert first.schema == schema_batch.schema
    rest = list(batches)

    assert len(opened) == 3
    assert pl.concat([first, *rest])["id"].sort().to_list() == list(range(6))


def test_read_data_files_matches_the_pyiceberg_scan(tmp_path, monkeypatch) -> None:
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("created", pa.timestamp("us")),
            ("modified", pa.timestamp("us", tz="UTC")),
            ("grade", pa.float64()),
            ("tags", pa.list_(pa.string())),
        ]
    )
    catalog = SqlCatalog(
        "irx", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}"
    )
    catalog.create_namespace("irx")
    table = catalog.create_table("irx.forum", schema=schema)
    stamp = datetime(2026, 10, 6, 6, 2, 51, 123456)  # noqa: DTZ001
    table.append(
        pa.table(
            {
                "id": [1, 2, 3],
                "created": [stamp, None, stamp],
                "modified": [stamp.replace(tzinfo=UTC)] * 3,
                "grade": [0.5, None, 1.0],
                "tags": [["a", "b"], [], None],
            },
            schema=schema,
        )
    )
    monkeypatch.setattr(irx_export, "READ_BATCH_ROWS", 2)

    read = pl.concat(
        read_data_files(table, table.current_snapshot().snapshot_id, ("*",))
    )

    scanned = pl.from_arrow(table.scan().to_arrow())
    assert read.schema == scanned.schema
    assert read.equals(scanned)


def test_read_data_files_never_decodes_more_than_a_batch_of_rows(
    tmp_path, monkeypatch
) -> None:
    # Two rows in the one data file, so a whole-file read would yield them both.
    table = _iceberg_table(tmp_path, data_files=1)
    monkeypatch.setattr(irx_export, "READ_BATCH_ROWS", 1)

    batches = list(
        read_data_files(
            table, table.current_snapshot().snapshot_id, ("id", "course_id")
        )
    )

    assert [batch.height for batch in batches] == [0, 1, 1]


def test_read_data_files_refuses_a_snapshot_with_delete_files(tmp_path) -> None:
    table = _iceberg_table(tmp_path, data_files=1)
    task = next(iter(table.scan().plan_files()))
    task.delete_files.add(task.file)
    scan = SimpleNamespace(
        projection=table.scan().projection, plan_files=lambda: [task]
    )
    stub = SimpleNamespace(scan=lambda **_: scan, io=table.io)

    batches = read_data_files(stub, 0)
    next(batches)
    with pytest.raises(Failure, match="delete files"):
        next(batches)


def test_read_data_files_yields_the_schema_for_a_snapshot_with_no_data_files(
    tmp_path,
) -> None:
    table = _iceberg_table(tmp_path, data_files=1)
    table.delete(delete_filter=AlwaysTrue())

    batches = list(
        read_data_files(
            table, table.current_snapshot().snapshot_id, ("id", "course_id")
        )
    )

    assert [batch.height for batch in batches] == [0]
    assert batches[0].columns == ["id", "course_id"]
