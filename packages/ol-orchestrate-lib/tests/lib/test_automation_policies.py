"""Tests for ol_orchestrate.lib.automation_policies.

These drive real runs against an ephemeral instance rather than asserting on
the shape of the condition tree, because the behaviour that matters -- whether
an upstream update survives a failed run -- only shows up in the interaction
between the condition, the event log, and actual run records.
"""

import time
from collections.abc import Iterator

import dagster as dg
import pytest
from dagster import AssetKey, DynamicPartitionsDefinition, Output
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from dagster._core.events import (
    AssetMaterializationPlannedData,
    DagsterEvent,
    DagsterEventType,
)
from dagster._core.events.log import EventLogEntry
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.storage.tags import PARTITION_NAME_TAG
from dagster._core.test_utils import create_run_for_test
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes

PARTITIONS = DynamicPartitionsDefinition(name="test_course_run")
UPSTREAM_KEY = AssetKey(["test", "upstream"])
DOWNSTREAM_KEY = AssetKey(["test", "downstream"])
PARTITION = "run-a"
IN_FLIGHT_JOB = "__ASSET_JOB"


class DownstreamBehaviour:
    """Controls what the downstream asset does when it executes."""

    SUCCEED = "succeed"
    FAIL = "fail"
    NO_OUTPUT = "no_output"

    def __init__(self) -> None:
        self.mode = self.SUCCEED


@pytest.fixture
def behaviour() -> DownstreamBehaviour:
    return DownstreamBehaviour()


@pytest.fixture
def instance() -> Iterator[dg.DagsterInstance]:
    with dg.DagsterInstance.ephemeral() as ephemeral_instance:
        ephemeral_instance.add_dynamic_partitions(PARTITIONS.name, [PARTITION])
        yield ephemeral_instance


@pytest.fixture
def graph(behaviour: DownstreamBehaviour):
    """Build an observable upstream feeding a downstream on the policy."""

    @dg.observable_source_asset(key=UPSTREAM_KEY, partitions_def=PARTITIONS)
    def upstream(context): ...

    @dg.asset(
        key=DOWNSTREAM_KEY,
        partitions_def=PARTITIONS,
        deps=[UPSTREAM_KEY],
        automation_condition=upstream_or_code_changes(),
        # Mirrors course_xml, which emits nothing when the course is gone.
        output_required=False,
    )
    def downstream(context):  # noqa: ARG001
        if behaviour.mode == DownstreamBehaviour.FAIL:
            msg = "downstream blew up"
            raise RuntimeError(msg)
        if behaviour.mode == DownstreamBehaviour.NO_OUTPUT:
            return
        yield Output(None)

    return dg.Definitions(assets=[upstream, downstream]), downstream


class Harness:
    """Drives automation ticks, observations, and runs against one instance."""

    def __init__(self, defs, downstream_asset, instance) -> None:
        self.defs = defs
        self.downstream_asset = downstream_asset
        self.instance = instance
        self.cursor = None

    def tick(self) -> list[str]:
        result = dg.evaluate_automation_conditions(
            defs=self.defs, instance=self.instance, cursor=self.cursor
        )
        self.cursor = result.cursor
        return sorted(result.get_requested_partitions(DOWNSTREAM_KEY))

    def observe(self, version: str) -> None:
        self.instance.report_runless_asset_event(
            dg.AssetObservation(
                asset_key=UPSTREAM_KEY,
                partition=PARTITION,
                tags={DATA_VERSION_TAG: version},
            )
        )

    def execute_downstream(self) -> None:
        dg.materialize(
            [self.downstream_asset],
            instance=self.instance,
            partition_key=PARTITION,
            raise_on_error=False,
        )

    def _log(self, run_id: str, dagster_event: DagsterEvent) -> None:
        self.instance.handle_new_event(
            EventLogEntry(
                error_info=None,
                level="debug",
                user_message="",
                run_id=run_id,
                timestamp=time.time(),
                dagster_event=dagster_event,
            )
        )

    def start_downstream_run(self):
        """Put a downstream run in flight without finishing it.

        materialize() is synchronous, so an in-progress run has to be staged by
        hand. The MATERIALIZATION_PLANNED event is the part that matters: that
        is what in_progress() reads for a partitioned asset, not the run record.
        """
        run = create_run_for_test(
            self.instance,
            job_name=IN_FLIGHT_JOB,
            status=DagsterRunStatus.STARTED,
            asset_selection={DOWNSTREAM_KEY},
            tags={PARTITION_NAME_TAG: PARTITION},
        )
        self._log(
            run.run_id,
            DagsterEvent(
                event_type_value=DagsterEventType.ASSET_MATERIALIZATION_PLANNED.value,
                job_name=IN_FLIGHT_JOB,
                event_specific_data=AssetMaterializationPlannedData(
                    asset_key=DOWNSTREAM_KEY, partition=PARTITION
                ),
            ),
        )
        return run

    def finish_downstream_run(self, run) -> None:
        """Complete the staged run successfully, materialization and all."""
        self.instance.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=DOWNSTREAM_KEY, partition=PARTITION)
        )
        self._log(
            run.run_id,
            DagsterEvent(
                event_type_value=DagsterEventType.RUN_SUCCESS.value,
                job_name=IN_FLIGHT_JOB,
            ),
        )

    def reach_steady_state(self, behaviour: DownstreamBehaviour) -> None:
        """Observe v1 and export it successfully, so nothing is outstanding."""
        behaviour.mode = DownstreamBehaviour.SUCCEED
        self.observe("v1")
        self.tick()
        self.execute_downstream()
        assert self.tick() == []


@pytest.fixture
def harness(graph, instance) -> Harness:
    defs, downstream_asset = graph
    return Harness(defs, downstream_asset, instance)


def test_upstream_version_change_requests_the_downstream(harness, behaviour) -> None:
    """The baseline: a new upstream data version asks for a materialization."""
    harness.reach_steady_state(behaviour)

    harness.observe("v2")

    assert harness.tick() == [PARTITION]


def test_unchanged_upstream_version_requests_nothing(harness, behaviour) -> None:
    """Re-observing the same version must not churn the downstream."""
    harness.reach_steady_state(behaviour)

    harness.observe("v1")

    assert harness.tick() == []


def test_update_is_retried_after_a_failed_run(harness, behaviour) -> None:
    """An upstream update must survive the run it launches failing.

    data_version_changed is edge-triggered, so without execution_failed the
    signal is consumed by the first tick and a failed run drops the update
    permanently -- the asset stays stale while every later tick reports
    nothing to do. This is the regression that left Open edX course archives
    un-exported for months (mitodl/hq#12739).
    """
    harness.reach_steady_state(behaviour)
    harness.observe("v2")
    assert harness.tick() == [PARTITION]

    behaviour.mode = DownstreamBehaviour.FAIL
    harness.execute_downstream()

    assert harness.tick() == [PARTITION]
    assert harness.tick() == [PARTITION]


def test_retrying_stops_once_a_run_succeeds(harness, behaviour) -> None:
    """The retry is level-triggered, so a success clears it."""
    harness.reach_steady_state(behaviour)
    harness.observe("v2")
    harness.tick()
    behaviour.mode = DownstreamBehaviour.FAIL
    harness.execute_downstream()
    assert harness.tick() == [PARTITION]

    behaviour.mode = DownstreamBehaviour.SUCCEED
    harness.execute_downstream()

    assert harness.tick() == []


def test_update_survives_a_run_that_was_already_in_flight(harness, behaviour) -> None:
    """An upstream update during an in-flight run must not be swallowed.

    ~in_progress() gates the whole condition, so the one tick where
    data_version_changed is true for v2 is suppressed because the v1 run is
    still going. That run then succeeds -- it exported v1, but it succeeded --
    so execution_failed never fires and, unlatched, the v2 edge is gone for
    good: the asset sits on v1 until the upstream happens to change again.

    The window is not narrow in practice. Open edX exports poll Studio for
    minutes, and the LMS is swept on a cron, so a republish landing inside
    someone else's export is routine (mitodl/hq#12755).
    """
    harness.reach_steady_state(behaviour)
    in_flight = harness.start_downstream_run()
    assert harness.tick() == [], "a run is already going"

    harness.observe("v2")
    assert harness.tick() == [], "still suppressed by in_progress"

    harness.finish_downstream_run(in_flight)

    assert harness.tick() == [PARTITION], "v2 must still be asked for"


def test_the_latch_clears_once_the_update_is_requested(harness, behaviour) -> None:
    """The re-request is one run, not a loop.

    The latch resets on newly_requested, so it clears whether or not that run
    ends up emitting anything -- which is what keeps this from reintroducing
    the forever-spinning behaviour the no-output test below pins.
    """
    harness.reach_steady_state(behaviour)
    in_flight = harness.start_downstream_run()
    harness.observe("v2")
    harness.tick()
    harness.finish_downstream_run(in_flight)
    assert harness.tick() == [PARTITION]

    harness.execute_downstream()

    assert harness.tick() == []
    assert harness.tick() == []


def test_run_that_succeeds_without_output_is_not_retried(harness, behaviour) -> None:
    """An output_required=False asset that emits nothing must not spin.

    This is why the retry keys off execution_failed rather than latching the
    upstream signal until the next materialization: a latch never resets for an
    asset that legitimately produces nothing, so it would re-request forever.
    """
    harness.reach_steady_state(behaviour)
    harness.observe("v2")
    assert harness.tick() == [PARTITION]

    behaviour.mode = DownstreamBehaviour.NO_OUTPUT
    harness.execute_downstream()

    assert harness.tick() == []
    assert harness.tick() == []
