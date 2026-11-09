"""Tests for the tracking log source."""

import gzip
import importlib
import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import fsspec
import pyarrow.compute as pc
import pytest

from ol_dlt import config
from ol_dlt.sources import edxorg_s3, tracking_logs

_XPRO = tracking_logs.TABLES["raw__xpro__openedx__tracking_log_events"]

_EVENT: dict[str, Any] = {
    "ip": "203.0.113.7",
    "host": "courses.xpro.mit.edu",
    "name": "edx.course.enrollment.activated",
    "page": None,
    "time": "2026-10-26T03:00:01.123456+00:00",
    "agent": "Mozilla/5.0",
    "event": '{"course_id": "course-v1:xPRO+DgtlTrans+R1", "mode": "audit"}',
    "labels": ["edxapp", "production"],
    "context": '{"user_id": 42, "course_id": "course-v1:xPRO+DgtlTrans+R1"}',
    "referer": "",
    "session": "c0ffee",
    "log_file": "/opt/data/lms/logs/tracking/tracking.log",
    "username": "learner",
    "timestamp": "2026-10-26T03:00:01.200Z",
    "event_type": "edx.course.enrollment.activated",
    "environment": "production",
    "source_type": "openedx",
    "event_source": "server",
    "accept_language": "en-US",
}

# Keys older files carry that are not columns of the table.
_STRAY = {
    "minion_id": "edx-worker-0",
    "roles": "edxapp",
    "log_host": "tracking-0",
    "vector_timestamp": "2026-10-26T03:00:02Z",
    "pipelined_beta_course_id": "course-v1:MITx+1.00x+1T2018",
}


def test_row_drops_keys_outside_the_columns() -> None:
    row = tracking_logs._row(json.dumps({**_EVENT, **_STRAY}))  # noqa: SLF001

    assert list(row) == list(tracking_logs.FIELDS)


def test_row_keeps_labels_as_json_text_whether_list_or_string() -> None:
    as_list = tracking_logs._row(json.dumps(_EVENT))  # noqa: SLF001
    as_string = tracking_logs._row(json.dumps({**_EVENT, "labels": "edxapp"}))  # noqa: SLF001

    assert as_list["labels"] == '["edxapp", "production"]'
    assert as_string["labels"] == "edxapp"


def test_row_keeps_every_value_as_text() -> None:
    """event and context are JSON text already and must not be re-encoded."""
    row = tracking_logs._row(json.dumps({**_EVENT, "session": None, "ip": 7}))  # noqa: SLF001

    assert row["event"] == _EVENT["event"]
    assert row["context"] == _EVENT["context"]
    assert row["session"] is None
    assert row["ip"] == "7"


def test_row_missing_a_column_loads_null() -> None:
    """2018 files have no labels, log_file, timestamp or source_type."""
    old = {
        key: value
        for key, value in _EVENT.items()
        if key not in {"labels", "log_file", "timestamp", "source_type"}
    }
    row = tracking_logs._row(json.dumps(old))  # noqa: SLF001

    assert row["labels"] is None
    assert row["timestamp"] is None
    assert row["username"] == "learner"


@pytest.mark.parametrize("line", ['{"ip": ', '["not", "an", "object"]'])
def test_row_that_is_not_a_json_object_fails(line: str) -> None:
    with pytest.raises(tracking_logs.TrackingLogLineError):
        tracking_logs._row(line)  # noqa: SLF001


@pytest.mark.parametrize(
    ("profile", "expected"),
    [
        ("qa", "s3://xpro-qa-edxapp-tracking/valid/"),
        ("production", "s3://xpro-production-edxapp-tracking/valid/"),
        ("dev", "s3://xpro-production-edxapp-tracking/valid/"),
    ],
)
def test_openedx_reads_the_qa_bucket_only_in_qa(profile: str, expected: str) -> None:
    assert _XPRO.bucket_url_for(profile) == expected


_TODAY = date(2026, 10, 27)


@pytest.mark.parametrize(
    ("cursor", "expected"),
    [
        pytest.param(None, ["**/*.gz"], id="first_load_lists_everything"),
        pytest.param(
            datetime(2026, 10, 26, 3, tzinfo=UTC),
            ["2026-10-25/*.gz", "2026-10-26/*.gz", "2026-10-27/*.gz"],
            id="recent_cursor_lists_from_the_day_before",
        ),
        pytest.param(
            datetime(2026, 9, 1, tzinfo=UTC), ["**/*.gz"], id="old_cursor_lists_all"
        ),
    ],
)
def test_openedx_globs_follow_the_cursor(
    cursor: datetime | None, expected: list[str]
) -> None:
    assert _XPRO.file_globs(cursor, today=_TODAY) == expected


def _land(
    root: Path, relative: str, events: list[dict[str, Any]], modified: datetime
) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        gzip.compress("".join(json.dumps(event) + "\n" for event in events).encode())
    )
    os.utime(path, (modified.timestamp(), modified.timestamp()))
    return path


@pytest.fixture
def local_s3(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tracking_logs.s3fs, "S3FileSystem", lambda: fsspec.filesystem("file")
    )


@pytest.fixture
def glob_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every glob the listing runs, still listing for real."""
    fsspec_module = importlib.import_module("dlt.common.storages.fsspec_filesystem")
    real_glob = fsspec_module.glob_files
    calls: list[str] = []

    def counting_glob(fs: Any, bucket_url: str, file_glob: str) -> Any:
        calls.append(file_glob)
        return real_glob(fs, bucket_url, file_glob)

    monkeypatch.setattr(fsspec_module, "glob_files", counting_glob)
    return calls


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


@pytest.mark.integration
@pytest.mark.usefixtures("local_s3")
def test_loads_every_file_name_form_onto_the_columns(
    test_profile: Path, tmp_path: Path
) -> None:
    valid = tmp_path / "landing" / "valid"
    now = _now()
    day = f"{now:%Y-%m-%d}"
    _land(valid, f"{day}/00_1-a.log.gz", [_EVENT], now - timedelta(minutes=3))
    _land(
        valid, f"{day}/{day}_0.gz", [{**_EVENT, **_STRAY}], now - timedelta(minutes=2)
    )
    _land(valid, f"{day}/{day}_1.json.gz", [_EVENT, {}], now - timedelta(minutes=1))
    # Not a tracking log, and not read.
    (valid / day / "README.txt").write_text("not gzip")

    pipeline = tracking_logs.tracking_logs_pipeline_for(_XPRO.raw_table)
    info = pipeline.run(
        tracking_logs.tracking_logs_source(
            raw_table=_XPRO.raw_table, bucket_url=valid.as_uri()
        )
    )
    assert not info.has_failed_jobs

    table = pipeline.dataset()[_XPRO.raw_table].arrow()
    assert table.num_rows == 4
    assert set(table.column_names) == {
        *tracking_logs.FIELDS,
        "_source_file",
        "_file_modified_at",
        *config.DLT_LOAD_ID_COLUMN,
    }
    assert sorted(
        Path(url).name for url in set(table.column("_source_file").to_pylist())
    ) == sorted(["00_1-a.log.gz", f"{day}_0.gz", f"{day}_1.json.gz"])
    assert set(table.column("labels").to_pylist()) == {'["edxapp", "production"]', None}

    # A second run finds nothing past the cursor and appends nothing.
    pipeline.run(
        tracking_logs.tracking_logs_source(
            raw_table=_XPRO.raw_table, bucket_url=valid.as_uri()
        )
    )
    assert pipeline.dataset()[_XPRO.raw_table].arrow().num_rows == 4


@pytest.mark.integration
@pytest.mark.usefixtures("local_s3")
def test_a_bad_line_names_its_file_and_line(test_profile: Path, tmp_path: Path) -> None:
    valid = tmp_path / "landing" / "valid"
    path = valid / f"{_now():%Y-%m-%d}" / "bad.log.gz"
    path.parent.mkdir(parents=True)
    path.write_bytes(gzip.compress(f"{json.dumps(_EVENT)}\n\n{{truncated\n".encode()))

    pipeline = tracking_logs.tracking_logs_pipeline_for(_XPRO.raw_table)
    with pytest.raises(Exception, match=r"bad\.log\.gz line 3"):
        pipeline.run(
            tracking_logs.tracking_logs_source(
                raw_table=_XPRO.raw_table, bucket_url=valid.as_uri()
            )
        )


@pytest.mark.integration
@pytest.mark.usefixtures("local_s3")
@pytest.mark.parametrize(
    "content",
    [
        pytest.param(b'{"ip": "1"}\n', id="not_gzip"),
        pytest.param(gzip.compress(b'{"ip": "1"}\n')[:-12], id="truncated"),
        # A valid gzip header over a deflate block of the reserved type, which
        # zlib rejects with zlib.error rather than gzip.BadGzipFile.
        pytest.param(gzip.compress(b'{"ip": "1"}\n')[:10] + b"\xff" * 16, id="corrupt"),
        pytest.param(gzip.compress(b'{"ip": "1"}\n{"ip": "\xff"}\n'), id="not_utf8"),
    ],
)
def test_an_unreadable_file_names_itself(
    test_profile: Path, tmp_path: Path, content: bytes
) -> None:
    valid = tmp_path / "landing" / "valid"
    path = valid / f"{_now():%Y-%m-%d}" / "broken.log.gz"
    path.parent.mkdir(parents=True)
    path.write_bytes(content)

    pipeline = tracking_logs.tracking_logs_pipeline_for(_XPRO.raw_table)
    with pytest.raises(Exception, match=r"broken\.log\.gz after line"):
        pipeline.run(
            tracking_logs.tracking_logs_source(
                raw_table=_XPRO.raw_table, bucket_url=valid.as_uri()
            )
        )


def _compressible_events(count: int) -> list[dict[str, Any]]:
    """Events whose JSON is ~100x their gzip, like a real file's 13-22x."""
    return [{**_EVENT, "event": "x" * 20_000} for _ in range(count)]


def _sources_read(pipeline: Any, valid: Path, **source_kwargs: Any) -> list[str]:
    pipeline.run(
        tracking_logs.tracking_logs_source(
            raw_table=_XPRO.raw_table, bucket_url=valid.as_uri(), **source_kwargs
        )
    )
    trace = pipeline.last_trace
    assert trace is not None
    if not trace.last_normalize_info.row_counts.get(_XPRO.raw_table):
        return []
    table = pipeline.dataset()[_XPRO.raw_table].arrow()
    load_id = trace.last_normalize_info.loads_ids[-1]
    rows = table.filter(pc.equal(table.column("_dlt_load_id"), load_id))
    return sorted({Path(url).name for url in rows.column("_source_file").to_pylist()})


@pytest.mark.integration
@pytest.mark.usefixtures("local_s3")
def test_the_budget_counts_gzip_bytes(test_profile: Path, tmp_path: Path) -> None:
    """Three files of ~1 KB gzip and ~100 KB of JSON against a 2.5 KB budget.

    Counted in gzip bytes the first batch holds two files. Counted in JSON
    bytes every file would be over the budget and form a batch of its own.
    """
    valid = tmp_path / "landing" / "valid"
    start = _now() - timedelta(minutes=10)
    paths = [
        _land(
            valid,
            f"{start:%Y-%m-%d}/{index}.log.gz",
            _compressible_events(5),
            start + timedelta(minutes=index),
        )
        for index in range(3)
    ]
    sizes = [path.stat().st_size for path in paths]
    assert max(sizes) < 1_250
    budget = sizes[0] + sizes[1]

    pipeline = tracking_logs.tracking_logs_pipeline_for(_XPRO.raw_table)

    assert _sources_read(pipeline, valid, budget_bytes=budget) == [
        "0.log.gz",
        "1.log.gz",
    ]
    assert _sources_read(pipeline, valid, budget_bytes=budget) == ["2.log.gz"]
    assert _sources_read(pipeline, valid, budget_bytes=budget) == []


@pytest.mark.integration
@pytest.mark.usefixtures("local_s3")
def test_batches_sharing_a_listing_list_once(
    test_profile: Path, tmp_path: Path, glob_calls: list[str]
) -> None:
    """A run lists once, and every batch carves its files from that listing.

    A file landing mid-run is left for the next run's listing, which starts
    from the cursor the run saved and lists only the daily prefixes from the day
    before it.
    """
    valid = tmp_path / "landing" / "valid"
    start = _now() - timedelta(minutes=10)
    day = f"{start:%Y-%m-%d}"
    for index in range(4):
        _land(
            valid, f"{day}/{index}.log.gz", [_EVENT], start + timedelta(minutes=index)
        )
    size = (valid / day / "0.log.gz").stat().st_size
    pipeline = tracking_logs.tracking_logs_pipeline_for(_XPRO.raw_table)

    run = edxorg_s3.FileListing()
    first = _sources_read(pipeline, valid, budget_bytes=2 * size, listing=run)
    _land(valid, f"{day}/late.log.gz", [_EVENT], start + timedelta(minutes=4))
    second = _sources_read(pipeline, valid, budget_bytes=2 * size, listing=run)
    third = _sources_read(pipeline, valid, budget_bytes=2 * size, listing=run)

    assert (first, second, third) == (
        ["0.log.gz", "1.log.gz"],
        ["2.log.gz", "3.log.gz"],
        [],
    )
    assert glob_calls == ["**/*.gz"], "the run's three batches listed once"

    glob_calls.clear()
    assert _sources_read(
        pipeline, valid, budget_bytes=2 * size, listing=edxorg_s3.FileListing()
    ) == ["late.log.gz"]
    cursor = start + timedelta(minutes=3)  # 3.log.gz, the last file read
    yesterday = cursor.date() - timedelta(days=1)
    assert glob_calls[0] == f"{yesterday:%Y-%m-%d}/*.gz"
    assert "**/*.gz" not in glob_calls


@pytest.mark.integration
@pytest.mark.usefixtures("local_s3")
def test_batches_without_a_listing_list_every_batch(
    test_profile: Path, tmp_path: Path, glob_calls: list[str]
) -> None:
    valid = tmp_path / "landing" / "valid"
    start = _now() - timedelta(minutes=10)
    for index in range(2):
        _land(
            valid,
            f"{start:%Y-%m-%d}/{index}.log.gz",
            [_EVENT],
            start + timedelta(minutes=index),
        )
    pipeline = tracking_logs.tracking_logs_pipeline_for(_XPRO.raw_table)

    _sources_read(pipeline, valid, budget_bytes=1)
    _sources_read(pipeline, valid, budget_bytes=1)

    assert glob_calls[0] == "**/*.gz"
    assert len(glob_calls) > 1


@pytest.mark.integration
@pytest.mark.usefixtures("local_s3")
def test_a_listing_leaves_files_past_its_horizon_for_the_next_run(
    test_profile: Path, tmp_path: Path
) -> None:
    """A file modified within ``settle`` of the listing waits, though listed.

    Reading it could carry the cursor past a file that landed in a prefix the
    listing had already passed, so it is read by a later run's listing instead.
    """
    valid = tmp_path / "landing" / "valid"
    now = _now()
    day = f"{now:%Y-%m-%d}"
    _land(valid, f"{day}/settled.log.gz", [_EVENT], now - timedelta(minutes=10))
    recent = _land(valid, f"{day}/recent.log.gz", [_EVENT], now - timedelta(minutes=1))
    pipeline = tracking_logs.tracking_logs_pipeline_for(_XPRO.raw_table)

    run = edxorg_s3.FileListing()
    assert _sources_read(pipeline, valid, listing=run) == ["settled.log.gz"]
    assert _sources_read(pipeline, valid, listing=run) == []

    # A later run, once the file has settled.
    settled_at = (now - timedelta(minutes=6)).timestamp()
    os.utime(recent, (settled_at, settled_at))
    assert _sources_read(pipeline, valid, listing=edxorg_s3.FileListing()) == [
        "recent.log.gz"
    ]
