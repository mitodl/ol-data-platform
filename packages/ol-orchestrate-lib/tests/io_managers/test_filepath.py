"""Tests for FileObjectIOManager.load_input.

The manager's job is to turn a materialization record into a path a downstream
step can open. The record is a claim about the object store, and these pin what
happens when the claim is wrong -- which is the whole of Group A: one S3 key
that was recorded but never present, re-run ~368,000 times in fourteen days
because every attempt failed somewhere downstream of here.
"""

from pathlib import Path

import pytest
from dagster import AssetKey, MetadataValue
from ol_orchestrate.io_managers.filepath import (
    FileObjectIOManager,
    UpstreamObjectUnavailable,
)

ASSET_KEY = AssetKey(["edxorg", "raw_data", "course_xml"])
PARTITION = "course-v1:MITx+6.002x+2T2024"


class FakeMaterialization:
    def __init__(self, metadata) -> None:
        self.metadata = metadata


class FakeRecord:
    def __init__(self, metadata) -> None:
        self.asset_materialization = FakeMaterialization(metadata)


class FakeInstance:
    def __init__(self, records) -> None:
        self._records = records

    def get_event_records(self, event_records_filter, limit):  # noqa: ARG002
        return self._records


class FakeInputContext:
    """The three attributes load_input reads, and nothing else."""

    def __init__(self, records) -> None:
        self.instance = FakeInstance(records)
        self.asset_key = ASSET_KEY
        self.partition_key = PARTITION


def context_for(metadata) -> FakeInputContext:
    return FakeInputContext([FakeRecord(metadata)])


def test_an_existing_object_resolves_to_its_path(tmp_path: Path) -> None:
    """The happy path: a recorded path whose object is there comes back."""
    archive = tmp_path / "course.tar.gz"
    archive.write_bytes(b"course")
    context = context_for({"path": MetadataValue.path(f"file://{archive}")})

    resolved = FileObjectIOManager().load_input(context)

    assert str(resolved) == f"file://{archive}"
    assert resolved.read_bytes() == b"course"


def test_a_missing_object_fails_permanently_and_names_the_key(
    tmp_path: Path,
) -> None:
    """The Group A failure: a recorded path pointing at nothing.

    Handing this path back left the NoSuchKey to surface deep inside whichever
    library opened it, with nothing in the error saying which object was gone.

    The path is named in metadata, not in the description: Sentry titles an
    event from the exception message, and an S3 key with a content hash in it
    gave DAGSTER-2H a different title for every occurrence.
    """
    missing = tmp_path / "never-written.tar.gz"
    context = context_for({"path": MetadataValue.path(f"file://{missing}")})

    with pytest.raises(UpstreamObjectUnavailable) as raised:
        FileObjectIOManager().load_input(context)

    assert str(missing) in raised.value.metadata["missing_path"].value
    assert raised.value.metadata["partition"].value == PARTITION
    assert str(missing) not in raised.value.description
    assert raised.value.allow_retries is False


def test_a_materialization_without_path_metadata_fails_permanently() -> None:
    """DAGSTER-1 and DAGSTER-2: KeyError: 'path' out of a bare dict lookup."""
    context = context_for({"size": MetadataValue.int(0)})

    with pytest.raises(UpstreamObjectUnavailable) as raised:
        FileObjectIOManager().load_input(context)

    assert "path" in raised.value.description
    assert raised.value.allow_retries is False


def test_no_materialization_at_all_fails_permanently() -> None:
    """An IndexError off ``[0]`` said nothing about the missing upstream."""
    context = FakeInputContext([])

    with pytest.raises(UpstreamObjectUnavailable) as raised:
        FileObjectIOManager().load_input(context)

    assert raised.value.metadata["asset_key"].value == ASSET_KEY.to_user_string()
    assert ASSET_KEY.to_user_string() not in raised.value.description
    assert raised.value.allow_retries is False
