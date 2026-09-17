"""Tests for the per-batch edxorg load loop.

The loop is what makes an 11.1 TB table loadable: each batch is one dlt load
covering at most the source's byte budget, and each commits its own cursor. It
is driven here with fakes rather than a real pipeline, which is what lets the
stop condition and the batch cap be asserted at all.
"""

from typing import Any

import pytest
from dagster import AssetKey, MaterializeResult
from data_loading.defs.ingestion import assets


class _FakeLog:
    def __init__(self) -> None:
        self.info_messages: list[str] = []
        self.warning_messages: list[str] = []

    def info(self, message: str, *args: Any) -> None:
        self.info_messages.append(message % args)

    def warning(self, message: str, *args: Any) -> None:
        self.warning_messages.append(message % args)


class _FakeContext:
    def __init__(self) -> None:
        self.log = _FakeLog()


class _FakeNormalizeInfo:
    def __init__(self, row_counts: dict[str, int]) -> None:
        self.row_counts = row_counts


class _FakeTrace:
    def __init__(self, row_counts: dict[str, int]) -> None:
        self.last_normalize_info = _FakeNormalizeInfo(row_counts)


class _FakePipeline:
    """Reports the row counts of each successive load, then zero forever."""

    def __init__(self, row_counts_per_batch: list[int], table_name: str) -> None:
        self._counts = list(row_counts_per_batch)
        self._table_name = table_name
        self.last_trace: _FakeTrace | None = None

    def record_batch(self) -> None:
        rows = self._counts.pop(0) if self._counts else 0
        self.last_trace = _FakeTrace({self._table_name: rows})


class _FakeDlt:
    def __init__(self, pipeline: _FakePipeline) -> None:
        self.pipeline = pipeline
        self.sources_seen: list[Any] = []

    def run(
        self,
        *,
        context: Any,  # noqa: ARG002
        dlt_source: Any,
        loader_file_format: str,  # noqa: ARG002
    ) -> Any:
        self.sources_seen.append(dlt_source)
        self.pipeline.record_batch()
        return iter(
            [MaterializeResult(asset_key=AssetKey(["raw", "auth_user"]), metadata={})]
        )


_RESOURCE = "raw__edxorg__s3__tables__auth_user"


def _run(row_counts_per_batch: list[int]) -> tuple[Any, Any, tuple[Any, int, int]]:
    pipeline = _FakePipeline(row_counts_per_batch, _RESOURCE)
    dlt = _FakeDlt(pipeline)
    context = _FakeContext()
    outcome = assets.load_in_batches(
        context=context,
        dlt=dlt,
        table_name="auth_user",
        pipeline=pipeline,
        resource_name=_RESOURCE,
    )
    return context, dlt, outcome


def test_keeps_loading_until_a_batch_finds_nothing() -> None:
    _, dlt, (_, batches, rows_loaded) = _run([500, 300, 0])

    assert batches == 3
    assert rows_loaded == 800
    assert len(dlt.sources_seen) == 3


def test_builds_a_fresh_source_for_every_batch() -> None:
    """A DltSource's resources are generators, spent by the batch that ran
    them. Reusing one would make every batch after the first load nothing,
    which looks exactly like a drained backlog.
    """
    _, dlt, _ = _run([10, 10, 0])

    assert len({id(source) for source in dlt.sources_seen}) == 3


def test_stops_at_the_batch_cap_and_says_so() -> None:
    """A source that never drains must not spin forever. The next run resumes
    from the cursor this one saved, so stopping is not data loss.
    """
    context, dlt, (_, batches, _) = _run([1] * (assets._MAX_BATCHES_PER_RUN + 5))

    assert batches == assets._MAX_BATCHES_PER_RUN
    assert len(dlt.sources_seen) == assets._MAX_BATCHES_PER_RUN
    assert context.log.warning_messages, "hitting the cap must be visible in the logs"


def test_one_empty_batch_is_a_complete_run() -> None:
    """Nothing new in the landing zone is a normal outcome, not an error."""
    _, _, (results, batches, rows_loaded) = _run([0])

    assert (batches, rows_loaded) == (1, 0)
    assert results, "the asset still needs a materialization to emit"


def test_the_loop_reads_the_pipeline_dagster_dlt_actually_runs() -> None:
    """The stop condition depends on these being one object.

    `load_in_batches` reads `last_trace` off the pipeline captured in the
    asset's closure, while `dlt.run()` is called without `dlt_pipeline=` and
    resolves it from the asset's metadata. If a refactor ever made those two
    different objects, the trace would be written to one and read from the
    other, every batch would look like zero rows, and the walk would stop
    after the first batch having loaded whatever that batch held.
    """
    from dagster_dlt.constants import META_KEY_PIPELINE  # noqa: PLC0415

    asset_def = next(
        a for a in assets.edxorg_s3_table_assets if "auth_user" in a.op.name
    )
    from_metadata = next(iter(asset_def.metadata_by_key.values()))[META_KEY_PIPELINE]

    compute_fn = asset_def.op.compute_fn.decorated_fn
    closure = dict(
        zip(
            compute_fn.__code__.co_freevars,
            (cell.cell_contents for cell in compute_fn.__closure__ or ()),
            strict=True,
        )
    )

    assert closure["pipeline"] is from_metadata


@pytest.mark.parametrize(
    ("trace", "expected"),
    [
        pytest.param(None, 0, id="no_trace_yet"),
        pytest.param(_FakeTrace({}), 0, id="table_absent_from_counts"),
        pytest.param(_FakeTrace({_RESOURCE: 42}), 42, id="counted"),
    ],
)
def test_row_count_reads_the_last_normalize_step(trace: Any, expected: int) -> None:
    pipeline = _FakePipeline([], _RESOURCE)
    pipeline.last_trace = trace

    assert assets._normalized_row_count(pipeline, _RESOURCE) == expected
