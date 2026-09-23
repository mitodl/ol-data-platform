"""Unit + materialization tests for the PostHog event export source."""

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ol_dlt import config
from ol_dlt.sources import posthog_events

_BUCKET = "test-landing-zone"
_PREFIX = "thirdparty/posthog/learn/events"

_COLUMNS = {
    "uuid": pa.string(),
    "event": pa.string(),
    "distinct_id": pa.string(),
    "person_id": pa.string(),
    "elements_chain": pa.string(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "created_at": pa.timestamp("us", tz="UTC"),
    "_inserted_at": pa.timestamp("us", tz="UTC"),
    "properties": pa.string(),
    "person_properties": pa.string(),
}


def _object_key(start: datetime, end: datetime) -> str:
    """Build an export key the way PostHog names one."""
    return (
        f"{_BUCKET}/{_PREFIX}/{end:%Y%m%d}/"
        f"{start.isoformat()}-{end.isoformat()}.parquet.zst"
    )


def _write_export_object(path: Path, rows: list[dict[str, object]]) -> None:
    """Write a Parquet file shaped like a PostHog export object."""
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([pa.field(name, kind) for name, kind in _COLUMNS.items()])
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="zstd")


def _event_row(uuid: str, event: str, moment: datetime) -> dict[str, object]:
    return {
        "uuid": uuid,
        "event": event,
        "distinct_id": "distinct-1",
        "person_id": "person-1",
        "elements_chain": "",
        "timestamp": moment,
        "created_at": moment,
        "_inserted_at": moment,
        "properties": '{"$current_url": "https://learn.mit.edu/search?q=chemistry"}',
        "person_properties": '{"$os": "Linux"}',
    }


class FakeS3FileSystem:
    """Local-directory stand-in for the subset of s3fs this source uses."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def ls(self, path: str, detail: bool = False) -> list[str]:  # noqa: ARG002, FBT001, FBT002
        directory = self.root / path
        if not directory.is_dir():
            raise FileNotFoundError(path)
        return sorted(f"{path}/{entry.name}" for entry in directory.iterdir())

    def open(self, path: str, mode: str = "rb"):  # noqa: ANN201
        return (self.root / path).open(mode)


@pytest.fixture
def export_root(tmp_path: Path) -> Path:
    """Two consecutive hours of export objects, plus one unparseable object."""
    root = tmp_path / "s3"
    hours = [
        (datetime(2026, 3, 1, 10, tzinfo=UTC), datetime(2026, 3, 1, 11, tzinfo=UTC)),
        (datetime(2026, 3, 1, 11, tzinfo=UTC), datetime(2026, 3, 1, 12, tzinfo=UTC)),
    ]
    for index, (start, end) in enumerate(hours):
        _write_export_object(
            root / _object_key(start, end),
            [
                _event_row(f"uuid-{index}-a", "search_update", start),
                _event_row(f"uuid-{index}-b", "$pageview", start),
            ],
        )
    stray = root / _BUCKET / _PREFIX / "20260301" / "_SUCCESS"
    stray.parent.mkdir(parents=True, exist_ok=True)
    stray.write_text("")
    return root


@pytest.fixture
def fake_filesystem(
    export_root: Path, monkeypatch: pytest.MonkeyPatch
) -> FakeS3FileSystem:
    """Point the source's ``s3fs.S3FileSystem()`` at the local export fixture."""
    filesystem = FakeS3FileSystem(export_root)
    monkeypatch.setattr(
        posthog_events.s3fs, "S3FileSystem", lambda *_a, **_k: filesystem
    )
    return filesystem


def test_parse_hour_window_reads_both_ends() -> None:
    start, end = posthog_events.parse_hour_window(
        "thirdparty/posthog/learn/events/20260904/"
        "2026-09-04T18:00:00+00:00-2026-09-04T19:00:00+00:00.parquet.zst"
    )
    assert start == datetime(2026, 9, 4, 18, tzinfo=UTC)
    assert end == datetime(2026, 9, 4, 19, tzinfo=UTC)


def test_parse_hour_window_rejects_an_unnamed_object() -> None:
    with pytest.raises(posthog_events.PostHogObjectNameError):
        posthog_events.parse_hour_window("thirdparty/posthog/learn/events/_SUCCESS")


def test_day_partitions_are_inclusive() -> None:
    assert list(posthog_events.day_partitions(date(2026, 2, 27), date(2026, 3, 1))) == [
        "20260227",
        "20260228",
        "20260301",
    ]


def test_list_export_objects_filters_by_window(
    fake_filesystem: FakeS3FileSystem,
) -> None:
    found = posthog_events.list_export_objects(
        fake_filesystem,
        after=datetime(2026, 3, 1, 11, tzinfo=UTC),
        until=datetime(2026, 3, 1, 23, tzinfo=UTC),
        bucket=_BUCKET,
        prefix=_PREFIX,
    )
    assert [end for end, _ in found] == [datetime(2026, 3, 1, 12, tzinfo=UTC)]


def test_list_export_objects_skips_unparseable_objects(
    fake_filesystem: FakeS3FileSystem,
) -> None:
    found = posthog_events.list_export_objects(
        fake_filesystem,
        after=datetime(2026, 3, 1, tzinfo=UTC),
        until=datetime(2026, 3, 2, tzinfo=UTC),
        bucket=_BUCKET,
        prefix=_PREFIX,
    )
    assert len(found) == 2  # noqa: PLR2004
    assert all("_SUCCESS" not in key for _, key in found)


def _source(**kwargs: object) -> object:
    return posthog_events.posthog_events_source(
        bucket=_BUCKET, prefix=_PREFIX, batch_size=1, **kwargs
    )


@pytest.mark.integration
def test_loads_the_requested_window(
    test_profile: Path,
    fake_filesystem: FakeS3FileSystem,  # noqa: ARG001
) -> None:
    pipeline = config.pipeline_for("posthog", pipeline_name="posthog_events_test")
    info = pipeline.run(
        _source(start_date=date(2026, 3, 1), end_date=date(2026, 3, 1)),
        loader_file_format="parquet",
    )
    assert not info.has_failed_jobs

    table = pipeline.dataset()[posthog_events.RESOURCE_NAME].arrow()
    assert table.num_rows == 4  # noqa: PLR2004
    assert "properties" in table.column_names
    assert "person_properties" in table.column_names
    # The JSON blobs land as strings, not as flattened per-property columns.
    assert table.schema.field("properties").type == pa.string()
    assert "s3_object_key" in table.column_names


@pytest.mark.integration
def test_second_run_rereads_only_the_lookback_window(
    test_profile: Path,
    fake_filesystem: FakeS3FileSystem,  # noqa: ARG001
) -> None:
    """A re-run repeats the lookback hours and discovers no new events.

    The re-read is the price of CURSOR_LOOKBACK and is why the staging model
    deduplicates on the event uuid. What must not change is the set of events:
    a resumed run finds nothing it has not already seen.
    """
    pipeline = config.pipeline_for("posthog", pipeline_name="posthog_events_test")
    pipeline.run(
        _source(start_date=date(2026, 3, 1), end_date=date(2026, 3, 1)),
        loader_file_format="parquet",
    )
    first = pipeline.dataset()[posthog_events.RESOURCE_NAME].arrow()

    pipeline.run(_source(end_date=date(2026, 3, 1)), loader_file_format="parquet")
    second = pipeline.dataset()[posthog_events.RESOURCE_NAME].arrow()

    # Both fixture hours sit inside the 3h lookback, so both are read again.
    assert second.num_rows == first.num_rows * 2
    assert set(second.column("uuid").to_pylist()) == set(
        first.column("uuid").to_pylist()
    )


@pytest.mark.integration
def test_an_hour_that_lands_late_is_still_picked_up(
    test_profile: Path,
    export_root: Path,
    fake_filesystem: FakeS3FileSystem,  # noqa: ARG001
) -> None:
    """A window absent when a later one is read is not skipped forever.

    The cursor is a single high-water mark, so without CURSOR_LOOKBACK reading
    12:00-13:00 while 11:00-12:00 is missing would move it past the gap and
    nothing afterwards would satisfy `after < window_end` for the missing hour.
    """
    late_start = datetime(2026, 3, 1, 11, tzinfo=UTC)
    late_end = datetime(2026, 3, 1, 12, tzinfo=UTC)
    late_object = export_root / _object_key(late_start, late_end)
    late_rows = [
        _event_row("uuid-late-a", "search_update", late_start),
        _event_row("uuid-late-b", "$pageview", late_start),
    ]
    late_object.unlink()

    _write_export_object(
        export_root / _object_key(late_end, datetime(2026, 3, 1, 13, tzinfo=UTC)),
        [_event_row("uuid-2-a", "search_update", late_end)],
    )

    pipeline = config.pipeline_for("posthog", pipeline_name="posthog_events_gap")
    pipeline.run(
        _source(start_date=date(2026, 3, 1), end_date=date(2026, 3, 1)),
        loader_file_format="parquet",
    )
    seen = set(
        pipeline.dataset()[posthog_events.RESOURCE_NAME]
        .arrow()
        .column("uuid")
        .to_pylist()
    )
    assert "uuid-2-a" in seen, "the later hour should have been read"
    assert "uuid-late-a" not in seen, "the missing hour cannot have been read yet"

    _write_export_object(late_object, late_rows)
    pipeline.run(_source(end_date=date(2026, 3, 1)), loader_file_format="parquet")

    seen = set(
        pipeline.dataset()[posthog_events.RESOURCE_NAME]
        .arrow()
        .column("uuid")
        .to_pylist()
    )
    assert "uuid-late-a" in seen, "the late hour was skipped by the cursor"


@pytest.mark.integration
def test_max_objects_bounds_a_backfill_chunk(
    test_profile: Path,
    fake_filesystem: FakeS3FileSystem,  # noqa: ARG001
) -> None:
    pipeline = config.pipeline_for("posthog", pipeline_name="posthog_events_chunked")
    pipeline.run(
        _source(start_date=date(2026, 3, 1), end_date=date(2026, 3, 1), max_objects=1),
        loader_file_format="parquet",
    )
    table = pipeline.dataset()[posthog_events.RESOURCE_NAME].arrow()
    assert table.num_rows == 2  # noqa: PLR2004

    # The next chunk picks up where the first stopped rather than starting over.
    pipeline.run(
        _source(end_date=date(2026, 3, 1), max_objects=1), loader_file_format="parquet"
    )
    table = pipeline.dataset()[posthog_events.RESOURCE_NAME].arrow()
    assert table.num_rows == 4  # noqa: PLR2004
