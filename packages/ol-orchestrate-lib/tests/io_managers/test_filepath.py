"""Tests for ol_orchestrate.io_managers.filepath."""

from pathlib import Path

import pytest
from dagster import AssetKey, Failure, MetadataValue
from ol_orchestrate.io_managers.filepath import (
    MATERIALIZATION_LOOKBACK,
    LocalFileObjectIOManager,
)


class _Materialization:
    def __init__(self, metadata: dict[str, MetadataValue]) -> None:
        self.metadata = metadata


class _Record:
    def __init__(self, metadata: dict[str, MetadataValue]) -> None:
        self.asset_materialization = _Materialization(metadata)


class _Instance:
    """Stands in for DagsterInstance, returning canned materialization records."""

    def __init__(self, records: list[_Record]) -> None:
        self._records = records
        self.requested_limit: int | None = None

    def get_event_records(self, event_records_filter, limit):  # noqa: ARG002
        self.requested_limit = limit
        return self._records[:limit]


class _InputContext:
    def __init__(self, records: list[_Record]) -> None:
        self.asset_key = AssetKey(("edxorg", "raw_data", "course_xml"))
        self.partition_key = "MITx-3.012Sx-3T2021|prod"
        self.instance = _Instance(records)


def _path_record(path: Path | str) -> _Record:
    """Build a record the way LocalFileObjectIOManager.handle_output does.

    The ``file://`` scheme matters: ``configure_path_fs`` dispatches on the
    URL protocol, and a bare filesystem path carries an empty one.
    """
    return _Record({"path": MetadataValue.path(f"file://{path}")})


def test_load_input_returns_recorded_path(tmp_path: Path) -> None:
    """The happy path resolves to the object the upstream asset wrote."""
    target = tmp_path / "course.xml.tar.gz"
    target.write_bytes(b"archive")

    resolved = LocalFileObjectIOManager().load_input(
        _InputContext([_path_record(target)])
    )

    assert resolved.exists()
    assert str(resolved).endswith(str(target))


def test_load_input_skips_materializations_without_a_path(tmp_path: Path) -> None:
    """A newer event lacking 'path' must not mask the one that has it.

    Regression test for the KeyError that took down every downstream step
    whenever an upstream materialization was emitted without a location.
    """
    target = tmp_path / "course.xml.tar.gz"
    target.write_bytes(b"archive")
    records = [
        _Record({"object_key": MetadataValue.text("no/path/here")}),
        _path_record(target),
    ]

    resolved = LocalFileObjectIOManager().load_input(_InputContext(records))

    assert resolved.exists()
    assert str(resolved).endswith(str(target))


def test_load_input_fails_without_retries_when_object_is_gone(tmp_path: Path) -> None:
    """A path recorded for an object that no longer exists is terminal.

    Retrying cannot conjure the key back, so the Failure is marked
    non-retryable and names the path that is missing. Note that
    ``allow_retries`` only governs an op/asset ``RetryPolicy`` -- the
    run-level auto-reexecution daemon ignores it -- so this asserts the
    property, not that the run is spared a re-run.
    """
    missing = tmp_path / "expired.xml.tar.gz"

    with pytest.raises(Failure) as exc_info:
        LocalFileObjectIOManager().load_input(_InputContext([_path_record(missing)]))

    assert exc_info.value.allow_retries is False
    assert str(missing) in str(exc_info.value.description)


def test_load_input_fails_when_partition_never_materialized() -> None:
    """No materialization at all gets a message naming the partition."""
    with pytest.raises(Failure) as exc_info:
        LocalFileObjectIOManager().load_input(_InputContext([]))

    assert exc_info.value.allow_retries is False
    assert "MITx-3.012Sx-3T2021|prod" in str(exc_info.value.description)


def test_load_input_fails_when_no_event_records_a_path() -> None:
    """Every event lacking a location is a real problem, not one to skip past."""
    records = [_Record({"object_key": MetadataValue.text("no/path/here")})] * 3

    with pytest.raises(Failure) as exc_info:
        LocalFileObjectIOManager().load_input(_InputContext(records))

    assert exc_info.value.allow_retries is False
    assert "recorded a 'path'" in str(exc_info.value.description)


def test_load_input_bounds_the_history_scan() -> None:
    """The event-log query stays bounded rather than walking all of history."""
    context = _InputContext([])

    with pytest.raises(Failure):
        LocalFileObjectIOManager().load_input(context)

    assert context.instance.requested_limit == MATERIALIZATION_LOOKBACK
