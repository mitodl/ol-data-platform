"""Tests for resetting an edxorg raw table for a full reload.

Run against an Iceberg filesystem destination with a persistent SQL catalog.
dlt's fallback in-memory catalog forgets table registrations between loads, so
the reset could not load the table it truncates.
"""

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import fsspec
import pytest

from ol_dlt.sources import edxorg_s3
from ol_dlt.sources.edxorg_s3 import reset

_DATASET = "raw"


@pytest.fixture
def warehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(
        "ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG",
        json.dumps(
            {
                "type": "sql",
                "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
                "warehouse": (tmp_path / "warehouse").as_uri(),
            }
        ),
    )
    monkeypatch.setenv("ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE", "sql")
    (tmp_path / "warehouse").mkdir()

    from dlt.common.libs.pyiceberg import (  # noqa: PLC0415
        get_catalog,
        is_ephemeral_catalog,
    )

    assert not is_ephemeral_catalog(get_catalog())
    return tmp_path


def _write_exports(root: Path, table_name: str, row_counts: list[int]) -> None:
    for i, rows in enumerate(row_counts):
        path = root / "land" / "db_table" / table_name / "prod" / f"c{i}" / f"e{i}.tsv"
        path.parent.mkdir(parents=True)
        path.write_text("id\tvalue\n" + "".join(f"{i}-{j}\tv\n" for j in range(rows)))
        modified = 1_700_000_000 + i
        os.utime(path, (modified, modified))


def _source(root: Path, table_name: str) -> Any:  # noqa: ANN401
    """edxorg_s3_source's wiring, reading a local landing zone."""

    @dlt.source(name="edxorg_s3")
    def source() -> Iterator[Any]:
        files = edxorg_s3.edxorg_files(
            bucket_url=(root / "land").as_uri(),
            file_glob=f"db_table/{table_name}/prod/**/*.tsv",
            credentials=fsspec.filesystem("file"),
        )
        yield (
            (files | edxorg_s3.read_edxorg_tsv(**edxorg_s3._CSV_READER_OPTIONS))  # noqa: SLF001
            .with_name(reset.resource_name_for(table_name))
            .apply_hints(write_disposition="append", table_format="iceberg")
        )

    return source()


def _pipeline(root: Path, table_name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=f"edxorg_s3__{table_name}",
        destination=dlt.destinations.filesystem(bucket_url=(root / "out").as_uri()),
        dataset_name=_DATASET,
        pipelines_dir=str(root / "pipelines"),
    )


def _load(root: Path, table_name: str) -> None:
    """One loader run, the way dagster-dlt does it: drop local state first."""
    pipeline = _pipeline(root, table_name).drop()
    pipeline.run(_source(root, table_name), loader_file_format="parquet")


def _rows(table_name: str) -> int:
    from dlt.common.libs.pyiceberg import get_catalog  # noqa: PLC0415

    table = get_catalog().load_table((_DATASET, reset.resource_name_for(table_name)))
    return table.scan().to_arrow().num_rows


def test_reset_leads_to_exactly_one_full_reload(warehouse: Path) -> None:
    _write_exports(warehouse, "a", [12000, 7000])
    _write_exports(warehouse, "b", [3000])
    _load(warehouse, "a")
    _load(warehouse, "b")  # another table's pipeline, storing the shared schema last

    plan = reset.plan_reset(_pipeline(warehouse, "a"), "a")
    reset.apply_reset(plan)

    assert plan.rows == 19000
    assert _rows("a") == 0

    _load(warehouse, "a")
    _load(warehouse, "a")  # and the cursor it saved holds

    assert _rows("a") == 19000


def test_reset_leaves_other_tables_alone(warehouse: Path) -> None:
    _write_exports(warehouse, "a", [12000])
    _write_exports(warehouse, "b", [3000])
    _load(warehouse, "a")
    _load(warehouse, "b")

    reset.apply_reset(reset.plan_reset(_pipeline(warehouse, "a"), "a"))
    _load(warehouse, "b")

    assert _rows("b") == 3000


def test_dry_run_changes_nothing(warehouse: Path) -> None:
    _write_exports(warehouse, "a", [12000])
    _load(warehouse, "a")

    assert reset.plan_reset(_pipeline(warehouse, "a"), "a").rows == 12000
    _load(warehouse, "a")

    assert _rows("a") == 12000


def test_newer_local_state_does_not_mask_the_destination(warehouse: Path) -> None:
    """A working dir left by an old local run can carry a higher state version
    than production, and sync_destination alone would keep it."""
    _write_exports(warehouse, "a", [12000])
    _load(warehouse, "a")
    stale = _pipeline(warehouse, "a")
    for _ in range(3):
        with stale.managed_state() as state:
            state["sources"] = {}

    assert reset.plan_reset(stale, "a").rows == 12000


def test_a_table_the_pipeline_never_loaded_is_refused(warehouse: Path) -> None:
    _write_exports(warehouse, "a", [10])
    _load(warehouse, "a")

    with pytest.raises(reset.NothingToResetError):
        reset.plan_reset(_pipeline(warehouse, "a"), "b")


def test_a_table_already_reset_is_refused(warehouse: Path) -> None:
    _write_exports(warehouse, "a", [10])
    _load(warehouse, "a")
    reset.apply_reset(reset.plan_reset(_pipeline(warehouse, "a"), "a"))

    with pytest.raises(reset.NothingToResetError):
        reset.plan_reset(_pipeline(warehouse, "a"), "a")


def test_a_load_after_the_plan_stops_the_reset(warehouse: Path) -> None:
    _write_exports(warehouse, "a", [12000])
    _load(warehouse, "a")
    plan = reset.plan_reset(_pipeline(warehouse, "a"), "a")
    later = warehouse / "land" / "db_table" / "a" / "prod" / "late" / "late.tsv"
    later.parent.mkdir(parents=True)
    later.write_text("id\tvalue\nlate\tv\n")
    os.utime(later, (1_800_000_000, 1_800_000_000))
    _load(warehouse, "a")

    with pytest.raises(reset.StateMovedError):
        reset.apply_reset(plan)
    assert _rows("a") == 12001


def test_a_load_on_a_later_table_stops_the_run_before_any_reset(
    warehouse: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_exports(warehouse, "a", [12000])
    _write_exports(warehouse, "b", [3000])
    _load(warehouse, "a")
    _load(warehouse, "b")
    monkeypatch.setattr(
        reset, "edxorg_s3_pipeline_for", lambda t: _pipeline(warehouse, t)
    )
    monkeypatch.setattr(reset.config, "active_table_format", lambda: "iceberg")

    real_check = reset.check_unmoved

    def load_b_then_check(plans: list[reset.ResetPlan]) -> None:
        later = warehouse / "land" / "db_table" / "b" / "prod" / "late" / "late.tsv"
        later.parent.mkdir(parents=True)
        later.write_text("id\tvalue\nlate\tv\n")
        os.utime(later, (1_800_000_000, 1_800_000_000))
        _load(warehouse, "b")
        real_check(plans)

    monkeypatch.setattr(reset, "check_unmoved", load_b_then_check)

    with pytest.raises(reset.StateMovedError):
        reset.run("a", "b", dry_run=False)
    assert _rows("a") == 12000


def test_a_repeated_table_name_is_reset_once(
    warehouse: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_exports(warehouse, "a", [12000])
    _load(warehouse, "a")
    monkeypatch.setattr(
        reset, "edxorg_s3_pipeline_for", lambda t: _pipeline(warehouse, t)
    )
    monkeypatch.setattr(reset.config, "active_table_format", lambda: "iceberg")

    reset.run("a", "a", dry_run=False)
    _load(warehouse, "a")

    assert _rows("a") == 12000
