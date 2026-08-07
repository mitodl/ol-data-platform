"""Tests for ol_orchestrate.lib.automation_policies.

These drive real runs against an ephemeral instance rather than asserting on
the shape of the condition tree, because the behaviour that matters -- whether
an upstream update survives a failed run -- only shows up in the interaction
between the condition, the event log, and actual run records.
"""

from collections.abc import Iterator

import dagster as dg
import pytest
from dagster import AssetKey, DynamicPartitionsDefinition, Output
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes

PARTITIONS = DynamicPartitionsDefinition(name="test_course_run")
UPSTREAM_KEY = AssetKey(["test", "upstream"])
DOWNSTREAM_KEY = AssetKey(["test", "downstream"])
PARTITION = "run-a"


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
