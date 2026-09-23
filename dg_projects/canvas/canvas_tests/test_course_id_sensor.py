"""Tests for the Canvas course-ID sensor's dynamic partition management.

The sensor diffs a Google Sheet against the existing dynamic partitions and
deletes the difference. Both halves of that had a way to delete a partition
something was still using, and deleting a dynamic partition does not cancel the
run using it -- the run completes its export and then dies storing its output
(DAGSTER-2B).

``partitions_with_a_run_in_flight`` is exercised against a real ephemeral
instance with real run records rather than a stub, because the parts most likely
to be wrong are which statuses count as "still going" and whether the partition
tag is where we think it is.
"""

from collections.abc import Iterator
from types import SimpleNamespace

import dagster as dg
import pytest
from canvas.lib.canvas import fetch_canvas_course_ids_from_google_sheet
from canvas.sensors.canvas import (
    PARTITION_NAME_TAG,
    PENDING_RUN_STATUSES,
    partition_changes,
    partitions_with_a_run_in_flight,
)
from dagster import DagsterRunStatus
from dagster._core.test_utils import create_run_for_test

EXISTING = {"1001", "1002", "1003"}


@pytest.fixture
def instance() -> Iterator[dg.DagsterInstance]:
    with dg.DagsterInstance.ephemeral() as ephemeral:
        yield ephemeral


def start_run_for(
    instance: dg.DagsterInstance,
    partition: str,
    status: DagsterRunStatus = DagsterRunStatus.STARTED,
) -> None:
    """Put a real run record in the instance, tagged with its partition."""
    create_run_for_test(
        instance,
        job_name="canvas_course_export_job",
        status=status,
        tags={PARTITION_NAME_TAG: partition},
    )


# ── partition_changes ─────────────────────────────────────────────────────────


def test_a_partition_with_a_run_in_flight_is_not_deleted() -> None:
    """DAGSTER-2B.

    Deleting the partition does not stop the run. It completes the export, then
    fails in get_output_context resolving the key range against a partitions
    definition that no longer contains it -- "Partition range 33842 to 33842 is
    not a valid range" -- and the finished work is discarded.
    """
    _, to_delete = partition_changes(
        sheet_course_ids={"1001"},
        existing_partitions=EXISTING,
        in_flight={"1002"},
    )

    assert "1002" not in to_delete, "a run is still using this partition"
    assert "1003" in to_delete, "the idle one is still cleaned up"


def test_a_deferred_deletion_happens_once_the_run_ends() -> None:
    """Deferral must not become permanent.

    The diff is recomputed from the live partition set every tick, so the same
    inputs minus the in-flight run delete it -- no bookkeeping required.
    """
    _, while_running = partition_changes({"1001"}, EXISTING, in_flight={"1002"})
    assert "1002" not in while_running

    _, once_finished = partition_changes({"1001"}, EXISTING, in_flight=set())

    assert "1002" in once_finished


def test_an_empty_sheet_deletes_everything_idle() -> None:
    """A genuinely empty sheet must still clean up.

    This is why a failed read has to be distinguishable from an empty one at the
    source rather than guarded here -- see the sheet-read tests below.
    """
    _, to_delete = partition_changes(set(), EXISTING, in_flight=set())

    assert to_delete == EXISTING


def test_new_course_ids_are_added() -> None:
    to_add, to_delete = partition_changes(
        EXISTING | {"2001"}, EXISTING, in_flight=set()
    )

    assert to_add == {"2001"}
    assert to_delete == set()


def test_an_in_flight_run_does_not_block_additions() -> None:
    """The in-flight check guards deletion only; adding is always safe."""
    to_add, _ = partition_changes(
        EXISTING | {"2001"}, EXISTING, in_flight={"2001", "1002"}
    )

    assert to_add == {"2001"}


def test_no_changes_yields_nothing() -> None:
    to_add, to_delete = partition_changes(EXISTING, EXISTING, in_flight=set())

    assert (to_add, to_delete) == (set(), set())


# ── partitions_with_a_run_in_flight ───────────────────────────────────────────


@pytest.mark.parametrize(
    "status",
    [
        DagsterRunStatus.NOT_STARTED,
        DagsterRunStatus.STARTING,
        DagsterRunStatus.STARTED,
        DagsterRunStatus.CANCELING,
    ],
)
def test_every_pending_status_counts_as_in_flight(instance, status) -> None:
    """A run that has not started yet still intends to.

    dagster's own IN_PROGRESS_RUN_STATUSES omits NOT_STARTED; a run whose
    partition is deleted before it starts fails exactly the same way as one
    already executing.
    """
    start_run_for(instance, "1002", status=status)
    context = dg.build_sensor_context(instance=instance)

    assert partitions_with_a_run_in_flight(context) == {"1002"}


def test_queued_is_covered_by_the_filter() -> None:
    """QUEUED is the status this most needs to cover, and the one the test
    harness cannot construct.

    ``create_run_for_test`` requires a RemoteJobOrigin for a queued run, which
    needs a loaded code location. Asserting on the filter is weaker than
    exercising it, but a queued run sitting behind a concurrency limit is
    exactly the window in which the sheet sensor ticks and deletes its
    partition, so leaving it uncovered entirely would be worse.
    """
    assert DagsterRunStatus.QUEUED in PENDING_RUN_STATUSES


@pytest.mark.parametrize(
    "status",
    [DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED],
)
def test_a_terminal_run_is_not_in_flight(instance, status) -> None:
    """Nothing is using the partition any more, so cleanup must proceed."""
    start_run_for(instance, "1002", status=status)
    context = dg.build_sensor_context(instance=instance)

    assert partitions_with_a_run_in_flight(context) == set()


def test_an_untagged_run_contributes_nothing(instance) -> None:
    """An unpartitioned run has no partition tag to read."""
    create_run_for_test(
        instance, job_name="some_other_job", status=DagsterRunStatus.STARTED
    )
    context = dg.build_sensor_context(instance=instance)

    assert partitions_with_a_run_in_flight(context) == set()


def test_no_runs_at_all(instance) -> None:
    context = dg.build_sensor_context(instance=instance)

    assert partitions_with_a_run_in_flight(context) == set()


# ── fetch_canvas_course_ids_from_google_sheet ─────────────────────────────────


def _sheet_context(service_account_json):
    return SimpleNamespace(
        resources=SimpleNamespace(
            google_sheet_config=SimpleNamespace(
                service_account_json=service_account_json,
                sheet_id="sheet-1",
                worksheet_id=0,
            )
        ),
        log=SimpleNamespace(error=lambda *_args: None),
    )


def test_missing_credentials_reads_as_failure_not_as_an_empty_sheet() -> None:
    """The sharpest form of the bug.

    This used to return set(). The sensor could not tell that from a genuinely
    empty sheet, so a transient Vault problem diffed every partition into the
    delete set and wiped the entire Canvas partition definition -- taking any
    in-flight export down with it.
    """
    assert fetch_canvas_course_ids_from_google_sheet(_sheet_context(None)) is None
