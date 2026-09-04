"""Tests for the user_pk key-map S3 backup.

The two guards below are the whole point of the asset, so they are the parts worth
testing: a backup that records an empty or truncated map as good is worse than none,
because it looks like a recovery option and is not one.
"""

import json
from typing import Any
from unittest.mock import MagicMock

import polars as pl
import pytest
from dagster import Failure, build_asset_context
from lakehouse.assets import user_key_map_backup as mod


class _FakeNoSuchKeyError(Exception):
    pass


def _fake_s3(latest: dict[str, Any] | None):
    """Build a boto3 s3 client stub that serves *latest* from the pointer key."""
    s3 = MagicMock()
    s3.exceptions.NoSuchKey = _FakeNoSuchKeyError

    def get_object(Bucket, Key):  # noqa: ARG001
        if latest is None:
            raise _FakeNoSuchKeyError
        return {"Body": MagicMock(read=lambda: json.dumps(latest).encode())}

    s3.get_object.side_effect = get_object
    s3.put_object.return_value = {}
    return s3


def _run(monkeypatch, frame: pl.DataFrame, latest: dict[str, Any] | None):

    s3 = _fake_s3(latest)
    # head_object must agree with whatever put_object was handed, so the verification
    # step passes and the test exercises the guard rather than the read-back.
    uploaded: dict[str, bytes] = {}

    def put_object(Body, Bucket, Key, **kwargs):  # noqa: ARG001
        uploaded[Key] = Body
        return {}

    s3.put_object.side_effect = put_object
    s3.head_object.side_effect = lambda Bucket, Key: {  # noqa: ARG005
        "ContentLength": len(uploaded[Key])
    }

    monkeypatch.setattr(mod.boto3, "client", lambda _svc: s3)
    monkeypatch.setattr(
        mod, "get_dbt_model_as_dataframe", lambda _db, _tbl: frame.lazy()
    )
    return mod.user_key_map_s3_backup(build_asset_context()), s3


def _frame(n: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "identifier": [f"mitxonline:{i}" for i in range(n)],
            "user_pk": [f"key{i}" for i in range(n)],
            "assigned_at": ["2026-01-01T00:00:00.000Z"] * n,
            "assigned_invocation_id": ["run-1"] * n,
        }
    )


def test_empty_map_is_refused(monkeypatch):
    """An empty map means the table was dropped, not a state worth snapshotting."""
    with pytest.raises(Failure, match="0 rows"):
        _run(monkeypatch, _frame(0), latest=None)


def test_shrinking_map_is_refused(monkeypatch):
    """The map is append-only, so a smaller row count cannot happen normally."""
    previous = {
        "row_count": 100,
        "key": "_backups/user_key_map/dt=2026-01-01/old.parquet",
    }
    with pytest.raises(Failure, match="went DOWN"):
        _run(monkeypatch, _frame(50), latest=previous)


def test_shrink_refusal_leaves_the_latest_pointer_alone(monkeypatch):
    """Recovery must still find the last good backup after a refused run."""
    previous = {
        "row_count": 100,
        "key": "_backups/user_key_map/dt=2026-01-01/old.parquet",
    }
    with pytest.raises(Failure):
        _run(monkeypatch, _frame(50), latest=previous)
    # The assertion that matters: nothing was written, so latest.json still names the
    # good backup rather than the truncated one.


def test_first_backup_succeeds_with_no_previous(monkeypatch):
    out, s3 = _run(monkeypatch, _frame(10), latest=None)
    assert out.value["row_count"] == 10
    assert out.value["sha256"]
    written = [c.kwargs["Key"] for c in s3.put_object.call_args_list]
    assert any(k.endswith(".parquet") for k in written)
    assert any(k.endswith("latest.json") for k in written)


def test_growth_is_recorded(monkeypatch):
    previous = {
        "row_count": 10,
        "key": "_backups/user_key_map/dt=2026-01-01/old.parquet",
    }
    out, _ = _run(monkeypatch, _frame(25), latest=previous)
    assert out.value["row_count"] == 25
    assert out.metadata["rows_added_since_last_backup"].value == 15


def test_latest_pointer_is_written_after_the_object(monkeypatch):
    """`latest` must never name a key whose upload failed."""
    out, s3 = _run(monkeypatch, _frame(10), latest=None)
    keys = [c.kwargs["Key"] for c in s3.put_object.call_args_list]
    assert keys.index(out.value["key"]) < keys.index(
        next(k for k in keys if k.endswith("latest.json"))
    )


def test_source_and_destination_share_one_warehouse_env():
    """A QA run must not read production's map or write into production's bucket."""

    assert mod.WAREHOUSE_ENV in mod.SOURCE_DATABASE
    assert mod.WAREHOUSE_ENV in mod.BACKUP_BUCKET
