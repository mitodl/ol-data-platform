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
    RetryPolicy,
    asset,
    materialize,
)
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from data_loading.defs.ingestion import assets

_RESOURCE = "raw__ocw__s3__course_content"
_KEY = AssetKey(["ol_warehouse_raw_data", _RESOURCE])
_COURSE = "a-course"


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


def test_an_empty_load_after_a_failed_run_moves_the_data_version() -> None:
    """The failed run committed a batch before it raised, so the table changed
    even though no materialization says so.
    """
    instance = DagsterInstance.ephemeral()
    loaded = _load(instance, [5, 0])
    pipeline = _FakePipeline([7])

    class _FailsOnSecondBatch(_FakeDlt):
        def run(self, **kwargs: Any) -> Any:
            if pipeline.last_trace is not None:
                msg = "Tika outage after batch 1 committed"
                raise RuntimeError(msg)
            return super().run(**kwargs)

    @asset(key=_KEY)
    def raw(context: AssetExecutionContext) -> Any:
        yield from assets.drain(
            context=context,
            dlt=_FailsOnSecondBatch(pipeline),
            build_source=_FakeSource,
            pipeline=pipeline,
        )

    assert not materialize([raw], instance=instance, raise_on_error=False).success

    assert _load(instance, [0]) != loaded


def test_an_empty_retry_after_a_committed_batch_moves_the_data_version() -> None:
    """A step retry, like a daemon resume of a run whose pod died, re-runs the
    step under the same run id, so the planned-run check cannot see it.
    """
    instance = DagsterInstance.ephemeral()
    loaded = _load(instance, [5, 0])
    pipeline = _FakePipeline([7, 0])

    class _FailsOnSecondBatchOnce(_FakeDlt):
        failed = False

        def run(self, **kwargs: Any) -> Any:
            if pipeline.last_trace is not None and not self.failed:
                _FailsOnSecondBatchOnce.failed = True
                msg = "Pod evicted after batch 1 committed"
                raise RuntimeError(msg)
            return super().run(**kwargs)

    @asset(key=_KEY, retry_policy=RetryPolicy(max_retries=1))
    def raw(context: AssetExecutionContext) -> Any:
        yield from assets.drain(
            context=context,
            dlt=_FailsOnSecondBatchOnce(pipeline),
            build_source=_FakeSource,
            pipeline=pipeline,
        )

    assert materialize([raw], instance=instance).success
    event = instance.get_latest_materialization_event(_KEY)
    assert event is not None
    assert event.asset_materialization is not None
    assert event.asset_materialization.metadata["rows_loaded"].value == 0
    assert event.asset_materialization.tags[DATA_VERSION_TAG] != loaded


def test_the_first_load_records_a_data_version_even_when_empty() -> None:
    """With nothing to carry over, Dagster derives one as it did before."""
    instance = DagsterInstance.ephemeral()

    assert _load(instance, [0])


def _course_load(
    instance: DagsterInstance, rows_per_batch: list[int], *, course: str = _COURSE
) -> None:
    """Run the OCW course load's reporting over a fake load of one partition."""
    instance.add_dynamic_partitions(assets.OCW_COURSE_SLUGS_NAME, [course])
    pipeline = _FakePipeline(rows_per_batch)

    class _CourseDlt(_FakeDlt):
        def run(self, **kwargs: Any) -> Any:
            super().run(**kwargs)
            return iter([MaterializeResult(asset_key=assets.OCW_COURSE_LOAD_KEY)])

    @asset(key=assets.OCW_COURSE_LOAD_KEY, partitions_def=assets.ocw_course_slugs)
    def course_load(context: AssetExecutionContext) -> Any:
        yield from assets.report_raw_table(
            context,
            assets.drain(
                context=context,
                dlt=_CourseDlt(pipeline),
                build_source=_FakeSource,
                pipeline=pipeline,
            ),
            [context.partition_key],
        )

    assert materialize([course_load], instance=instance, partition_key=course).success


def _fail(instance: DagsterInstance, key: AssetKey, partition: str | None) -> None:
    """Fail a run that planned the asset, as one that committed rows and died."""
    if partition:
        instance.add_dynamic_partitions(assets.OCW_COURSE_SLUGS_NAME, [partition])

    @asset(key=key, partitions_def=assets.ocw_course_slugs if partition else None)
    def failed() -> None:
        msg = "The pod died after a batch committed"
        raise RuntimeError(msg)

    assert not materialize(
        [failed], instance=instance, partition_key=partition, raise_on_error=False
    ).success


def _raw_reports(instance: DagsterInstance) -> int:
    return len(instance.fetch_materializations(_KEY, limit=100).records)


def test_a_course_load_with_rows_reports_the_raw_table() -> None:
    instance = DagsterInstance.ephemeral()
    _load(instance, [5, 0])
    before = _raw_reports(instance)

    _course_load(instance, [3, 0])

    assert _raw_reports(instance) == before + 1


def test_an_empty_course_load_does_not_report_the_raw_table() -> None:
    instance = DagsterInstance.ephemeral()
    _load(instance, [5, 0])
    _course_load(instance, [3, 0])
    before = _raw_reports(instance)

    _course_load(instance, [0])

    assert _raw_reports(instance) == before


def test_an_empty_course_load_reports_rows_a_failed_sweep_committed() -> None:
    instance = DagsterInstance.ephemeral()
    _load(instance, [5, 0])
    _fail(instance, _KEY, None)
    before = _raw_reports(instance)

    _course_load(instance, [0])
    assert _raw_reports(instance) == before + 1

    # Reported now, so the next empty load has nothing to add.
    _course_load(instance, [0])
    assert _raw_reports(instance) == before + 1


def test_an_empty_course_load_reports_rows_its_own_failed_run_committed() -> None:
    instance = DagsterInstance.ephemeral()
    _load(instance, [5, 0])
    _fail(instance, assets.OCW_COURSE_LOAD_KEY, _COURSE)
    before = _raw_reports(instance)

    _course_load(instance, [0])

    assert _raw_reports(instance) == before + 1


def test_another_partitions_run_is_not_an_unreported_attempt() -> None:
    """The sensor queues one run per course, each planning its own partition."""
    instance = DagsterInstance.ephemeral()
    _load(instance, [5, 0])
    _course_load(instance, [3, 0])
    _fail(instance, assets.OCW_COURSE_LOAD_KEY, "b-course")
    before = _raw_reports(instance)

    _course_load(instance, [0])

    assert _raw_reports(instance) == before


def test_the_raw_table_is_reported_before_the_course_result() -> None:
    """A worker that dies after the result must not leave the rows unreported."""
    instance = DagsterInstance.ephemeral()
    _load(instance, [5, 0])

    _course_load(instance, [3, 0])

    raw = instance.fetch_materializations(_KEY, limit=1).records[0]
    course = instance.fetch_materializations(
        assets.OCW_COURSE_LOAD_KEY, limit=1
    ).records[0]
    assert raw.storage_id < course.storage_id


def test_an_empty_sweep_keeps_the_version_a_course_load_reported() -> None:
    instance = DagsterInstance.ephemeral()
    _load(instance, [5, 0])
    _course_load(instance, [3, 0])
    event = instance.get_latest_materialization_event(_KEY)
    assert event is not None
    assert event.asset_materialization is not None
    reported = event.asset_materialization.tags[DATA_VERSION_TAG]

    assert _load(instance, [0]) == reported
