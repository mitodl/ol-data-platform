"""A load that writes no rows must not move the raw table's data version.

The lakehouse dbt models rebuild when a parent's data version changes, and a
dlt asset with no code version gets a new one from every run id. Without the
carry-over in ``drain``, an OCW republish with identical bytes rebuilds every
model below raw__ocw__s3__course_content.
"""

from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DagsterInstance,
    MaterializeResult,
    asset,
    materialize,
)
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from data_loading.defs.ingestion import assets

_RESOURCE = "raw__ocw__s3__course_content"
_KEY = AssetKey(["ol_warehouse_raw_data", _RESOURCE])


class _FakeSource:
    resources = {_RESOURCE: None}  # noqa: RUF012


class _FakeNormalizeInfo:
    def __init__(self, rows: int) -> None:
        self.row_counts = {_RESOURCE: rows}


class _FakeTrace:
    def __init__(self, rows: int) -> None:
        self.last_normalize_info = _FakeNormalizeInfo(rows)


class _FakePipeline:
    def __init__(self, rows_per_batch: list[int]) -> None:
        self._rows = list(rows_per_batch)
        self.last_trace: _FakeTrace | None = None


class _FakeDlt:
    def __init__(self, pipeline: _FakePipeline) -> None:
        self.pipeline = pipeline

    def run(self, **_: Any) -> Any:
        rows = self.pipeline._rows.pop(0) if self.pipeline._rows else 0
        self.pipeline.last_trace = _FakeTrace(rows)
        return iter([MaterializeResult(asset_key=_KEY, metadata={})])


def _load(instance: DagsterInstance, rows_per_batch: list[int]) -> str:
    """Materialize the raw asset once and return the data version it recorded."""
    pipeline = _FakePipeline(rows_per_batch)

    @asset(key=_KEY)
    def raw(context: AssetExecutionContext) -> Any:
        yield from assets.drain(
            context=context,
            dlt=_FakeDlt(pipeline),
            build_source=_FakeSource,
            pipeline=pipeline,
        )

    assert materialize([raw], instance=instance).success
    event = instance.get_latest_materialization_event(_KEY)
    assert event is not None
    assert event.asset_materialization is not None
    return event.asset_materialization.tags[DATA_VERSION_TAG]


def test_an_empty_load_keeps_the_previous_data_version() -> None:
    instance = DagsterInstance.ephemeral()
    loaded = _load(instance, [5, 0])

    assert _load(instance, [0]) == loaded


def test_a_load_with_rows_moves_the_data_version() -> None:
    instance = DagsterInstance.ephemeral()
    first = _load(instance, [5, 0])
    empty = _load(instance, [0])

    assert _load(instance, [3, 0]) not in {first, empty}


def test_the_first_load_records_a_data_version_even_when_empty() -> None:
    """With nothing to carry over, Dagster derives one as it did before."""
    instance = DagsterInstance.ephemeral()

    assert _load(instance, [0])
