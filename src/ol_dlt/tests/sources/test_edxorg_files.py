"""Tests for the budgeted listing resource.

These drive the resource through a real dlt pipeline rather than calling the
generator directly. An earlier version of the budget lived in the transformer
and passed a direct-call test, but dlt invokes a transformer once per page, so
in production the running total restarted at zero for every file and a 20 MB
budget read all 3,336 files of a table. Only a pipeline run shows that.
"""

import importlib
from datetime import UTC, datetime, timedelta
from typing import Any

import dlt
import pytest

from ol_dlt.sources import edxorg_s3

_START = datetime(2026, 2, 1, tzinfo=UTC)


class _FakeFs:
    """Stands in for the s3fs client the resource lists through."""

    def __init__(self, sizes: list[int]) -> None:
        self.sizes = sizes


def _file_items(
    sizes: list[int], *, newest_first: bool = False
) -> list[dict[str, Any]]:
    items = [
        {
            "file_url": f"s3://bucket/{index}.tsv",
            "file_name": f"{index}.tsv",
            "size_in_bytes": size,
            "modification_date": _START + timedelta(hours=index),
            "mime_type": "text/tab-separated-values",
        }
        for index, size in enumerate(sizes)
    ]
    return list(reversed(items)) if newest_first else items


@pytest.fixture
def listed(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Patch the glob so the resource lists a controllable set of files."""

    def _install(items: list[dict[str, Any]]) -> list[str]:
        requested: list[str] = []

        def fake_glob(fs: Any, bucket_url: str, file_glob: str) -> Any:  # noqa: ARG001
            requested.append(file_glob)
            return iter(items)

        # The package re-exports a function of the same name over the
        # module, so the dotted-path form of setattr resolves to that instead.
        fsspec_module = importlib.import_module("dlt.common.storages.fsspec_filesystem")
        monkeypatch.setattr(fsspec_module, "glob_files", fake_glob)
        return requested

    return _install


def _urls_read(pipeline: dlt.Pipeline, items: list[dict[str, Any]]) -> list[str]:
    """Run one batch and return the file URLs it yielded."""
    seen: list[str] = []

    @dlt.transformer(standalone=True)
    def collect(file_items: Any) -> Any:
        for file_item in file_items:
            seen.append(file_item["file_url"])
            yield {"file_url": file_item["file_url"]}

    files = edxorg_s3.edxorg_files(
        bucket_url="s3://bucket",
        file_glob="*.tsv",
        credentials=_FakeFs([item["size_in_bytes"] for item in items]),
        budget_bytes=25,
    )
    pipeline.run((files | collect()).with_name("collected"))
    return seen


@pytest.fixture
def pipeline(tmp_path: Any) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="edxorg_files_test",
        destination=dlt.destinations.duckdb(str(tmp_path / "test.duckdb")),
        dataset_name="probe",
        pipelines_dir=str(tmp_path / "pipelines"),
    )


def test_batch_stops_at_the_budget(listed: Any, pipeline: dlt.Pipeline) -> None:
    """The budget bounds a batch even though dlt pages the resource.

    Ten 10-byte files against a 25-byte budget is two files: the third would
    take the batch to 30, so it is left for the next one. The budget is a
    ceiling, not a target.
    """
    items = _file_items([10] * 10)
    listed(items)

    assert _urls_read(pipeline, items) == ["s3://bucket/0.tsv", "s3://bucket/1.tsv"]


def test_next_batch_resumes_where_the_last_one_stopped(
    listed: Any, pipeline: dlt.Pipeline
) -> None:
    """The cursor must sit on the last file read, not the last file listed.

    This is the property the whole design rests on: a batch that stops early
    leaves everything it did not read for the next one.
    """
    items = _file_items([10] * 10)
    listed(items)

    first = _urls_read(pipeline, items)
    second = _urls_read(pipeline, items)

    assert first == ["s3://bucket/0.tsv", "s3://bucket/1.tsv"]
    assert second == ["s3://bucket/2.tsv", "s3://bucket/3.tsv"]
    assert not set(first) & set(second), "a file must not be read by two batches"


def test_batch_is_empty_once_every_file_has_been_read(
    listed: Any, pipeline: dlt.Pipeline
) -> None:
    """An empty batch is how the caller learns the backlog is drained."""
    items = _file_items([10, 10])
    listed(items)

    assert _urls_read(pipeline, items) == ["s3://bucket/0.tsv", "s3://bucket/1.tsv"]
    assert _urls_read(pipeline, items) == []


def test_files_are_read_oldest_first(listed: Any, pipeline: dlt.Pipeline) -> None:
    """Sorting is what makes the early stop safe: everything unread sorts
    after the saved cursor. A listing in any other order would strand files
    behind it."""
    items = _file_items([10] * 5, newest_first=True)
    listed(items)

    assert _urls_read(pipeline, items) == ["s3://bucket/0.tsv", "s3://bucket/1.tsv"]


@pytest.mark.parametrize(
    ("sizes", "first", "second"),
    [
        pytest.param([100, 10], ["0.tsv"], ["1.tsv"], id="oversized_file_first"),
        pytest.param([10, 100], ["0.tsv"], ["1.tsv"], id="oversized_file_second"),
    ],
)
def test_a_file_bigger_than_the_budget_is_a_batch_of_its_own(
    listed: Any,
    pipeline: dlt.Pipeline,
    sizes: list[int],
    first: list[str],
    second: list[str],
) -> None:
    """The largest export is 14.5 GB against a 4 GiB budget. It loads as a
    batch of its own rather than being skipped, split, or stacked on top of an
    already-full batch.

    The oversized-second case is the one that matters: charging a file after
    yielding it means a batch can reach its budget plus that file's whole
    weight, which in production is 4 GiB + 14.5 GB against a limit sized for
    4 GiB + one file.
    """
    items = _file_items(sizes)
    listed(items)

    assert _urls_read(pipeline, items) == [f"s3://bucket/{name}" for name in first]
    assert _urls_read(pipeline, items) == [f"s3://bucket/{name}" for name in second]


def test_unread_files_sharing_the_boundary_second_are_still_read(
    listed: Any, pipeline: dlt.Pipeline
) -> None:
    """S3 stamps whole seconds, so a batch can stop mid-second.

    The files left in that second are not covered by the cursor even though it
    equals their mtime, so they have to come back on the next batch rather
    than being skipped as already-read.
    """
    items = _file_items([10, 10, 10])
    for item in items:
        item["modification_date"] = _START  # one second, three files
    listed(items)

    first = _urls_read(pipeline, items)
    second = _urls_read(pipeline, items)

    assert first, "the first batch reads something"
    assert sorted(first + second) == [
        "s3://bucket/0.tsv",
        "s3://bucket/1.tsv",
        "s3://bucket/2.tsv",
    ]
    assert not set(first) & set(second)


def test_every_batch_makes_progress_past_the_boundary(
    listed: Any, pipeline: dlt.Pipeline
) -> None:
    """An already-read boundary file must never consume the whole batch.

    With the 14.5 GB export on the boundary and a 4 GiB budget, charging it
    would end the batch before any new file was yielded; the caller reads an
    empty batch as a drained backlog and the backfill stalls silently.
    """
    items = _file_items([100, 10, 10])
    listed(items)

    assert _urls_read(pipeline, items) == ["s3://bucket/0.tsv"]
    # The 100-byte file is back on the boundary and deduped. The batch must
    # still move: both remaining files fit the budget behind it.
    assert _urls_read(pipeline, items) == ["s3://bucket/1.tsv", "s3://bucket/2.tsv"]
    assert _urls_read(pipeline, items) == []
