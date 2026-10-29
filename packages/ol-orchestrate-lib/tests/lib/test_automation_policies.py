"""Tests for ol_orchestrate.lib.automation_policies.

These drive real runs against an ephemeral instance rather than asserting on
the shape of the condition tree, because the behaviour that matters -- whether
an upstream update survives a failed run -- only shows up in the interaction
between the condition, the event log, and actual run records.
"""

import time
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta

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
from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus
from dagster._core.storage.tags import PARTITION_NAME_TAG
from dagster._core.test_utils import create_run_for_test
from ol_orchestrate.lib.automation_policies import (
    dbt_upstream_or_code_changes,
    upstream_or_code_changes,
)

PARTITIONS = DynamicPartitionsDefinition(name="test_course_run")
UPSTREAM_KEY = AssetKey(["test", "upstream"])
DOWNSTREAM_KEY = AssetKey(["test", "downstream"])
PARTITION = "run-a"
IN_FLIGHT_JOB = "__ASSET_JOB"


def _log_run_event(
    instance: dg.DagsterInstance, run_id: str, dagster_event: DagsterEvent
) -> None:
    instance.handle_new_event(
        EventLogEntry(
            error_info=None,
            level="debug",
            user_message="",
            run_id=run_id,
            timestamp=time.time(),
            dagster_event=dagster_event,
        )
    )


def _start_run(
    instance: dg.DagsterInstance, key: AssetKey, partition: str | None = None
) -> DagsterRun:
    """Put a run of ``key`` in flight without finishing it.

    materialize() is synchronous, so an in-progress run has to be staged by
    hand. The MATERIALIZATION_PLANNED event is the part that matters: that is
    what in_progress() reads, not the run record.
    """
    run = create_run_for_test(
        instance,
        job_name=IN_FLIGHT_JOB,
        status=DagsterRunStatus.STARTED,
        asset_selection={key},
        tags={PARTITION_NAME_TAG: partition} if partition else None,
    )
    _log_run_event(
        instance,
        run.run_id,
        DagsterEvent(
            event_type_value=DagsterEventType.ASSET_MATERIALIZATION_PLANNED.value,
            job_name=IN_FLIGHT_JOB,
            event_specific_data=AssetMaterializationPlannedData(
                asset_key=key, partition=partition
            ),
        ),
    )
    return run


def _finish_run(
    instance: dg.DagsterInstance,
    run: DagsterRun,
    key: AssetKey,
    partition: str | None = None,
) -> None:
    """Complete a staged run successfully, materialization and all."""
    instance.report_runless_asset_event(
        dg.AssetMaterialization(asset_key=key, partition=partition)
    )
    _log_run_event(
        instance,
        run.run_id,
        DagsterEvent(
            event_type_value=DagsterEventType.RUN_SUCCESS.value,
            job_name=IN_FLIGHT_JOB,
        ),
    )


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

    def start_downstream_run(self):
        """Put a downstream run in flight without finishing it."""
        return _start_run(self.instance, DOWNSTREAM_KEY, PARTITION)

    def finish_downstream_run(self, run) -> None:
        """Complete the staged run successfully, materialization and all."""
        _finish_run(self.instance, run, DOWNSTREAM_KEY, PARTITION)

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


def test_the_failure_retry_is_one_run_not_a_treadmill(harness, behaviour) -> None:
    """A failure that keeps failing must stop asking.

    execution_failed is level-triggered, so used bare it stays true for as long
    as the latest execution is a failure -- a partition that can never succeed
    is re-requested on every tick forever, and run_retries multiplies each
    request. That is the mechanism behind ~368,000 failed runs from one asset in
    fourteen days. .newly_true() spends the failure on a single re-request.
    """
    harness.reach_steady_state(behaviour)
    harness.observe("v2")
    harness.tick()

    behaviour.mode = DownstreamBehaviour.FAIL
    harness.execute_downstream()
    assert harness.tick() == [PARTITION], "one retry for the failure"

    assert harness.tick() == [], "and not a second one for the same failure"

    harness.execute_downstream()
    assert harness.tick() == [], "nor for the retry that failed the same way"
    assert harness.tick() == []


def test_retrying_stops_once_a_run_succeeds(harness, behaviour) -> None:
    """A success clears the failure, so nothing is outstanding."""
    harness.reach_steady_state(behaviour)
    harness.observe("v2")
    harness.tick()
    behaviour.mode = DownstreamBehaviour.FAIL
    harness.execute_downstream()
    assert harness.tick() == [PARTITION]

    behaviour.mode = DownstreamBehaviour.SUCCEED
    harness.execute_downstream()

    assert harness.tick() == []


def test_a_later_failure_gets_its_own_retry(harness, behaviour) -> None:
    """Bounding the retry must not spend it permanently.

    execution_failed falls back to false when a run succeeds, so the next
    genuine failure is a fresh edge and earns its own re-request. Without that
    the bound would silently downgrade to "one retry per asset, ever".
    """
    harness.reach_steady_state(behaviour)
    harness.observe("v2")
    harness.tick()
    behaviour.mode = DownstreamBehaviour.FAIL
    harness.execute_downstream()
    assert harness.tick() == [PARTITION]
    behaviour.mode = DownstreamBehaviour.SUCCEED
    harness.execute_downstream()
    assert harness.tick() == []

    harness.observe("v3")
    assert harness.tick() == [PARTITION]
    behaviour.mode = DownstreamBehaviour.FAIL
    harness.execute_downstream()

    assert harness.tick() == [PARTITION]


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


# -- dbt_upstream_or_code_changes ---------------------------------------------
#
# These model the dbt graph the way dbt_automation_sensor sees it: unpartitioned
# assets whose code version is their SQL, staging models outside the sensor's
# target, and a deploy as a new Definitions with new code versions evaluated
# against the same instance and cursor.

STAGING_GROUP = "staging"
SENSOR_TARGET = dg.AssetSelection.all() - dg.AssetSelection.groups(STAGING_GROUP)
SENSOR_INTERVAL = timedelta(hours=4)
# name -> (code version, parent names)
Models = Mapping[str, tuple[str, Sequence[str]]]


def _dbt_like_defs(models: Models, condition: dg.AutomationCondition) -> dg.Definitions:
    def _model(name: str, code_version: str, parents: Sequence[str]):
        @dg.asset(
            name=name,
            deps=parents,
            code_version=code_version,
            group_name=STAGING_GROUP if name.startswith("stg") else "intermediate",
            automation_condition=condition,
        )
        def _asset() -> None: ...

        return _asset

    return dg.Definitions(assets=[_model(name, *spec) for name, spec in models.items()])


class DbtSensor:
    """Ticks the dbt sensor's target on a fake clock, one sensor interval apart.

    ``deploy_tick`` is the first tick after ``settle()``, the one that sees a
    deploy made right after settling. Keep it off the hour: a tick exactly on
    the cron time counts the same cron tick as passed on the next one too.
    """

    def __init__(
        self,
        instance: dg.DagsterInstance,
        condition: dg.AutomationCondition,
        deploy_tick: datetime,
    ) -> None:
        self.instance = instance
        self.condition = condition
        self.now = deploy_tick - 3 * SENSOR_INTERVAL
        self.cursor = None
        self.defs = dg.Definitions()

    def deploy(self, models: Models) -> None:
        self.defs = _dbt_like_defs(models, self.condition)

    def materialize(self, *names: str) -> None:
        for name in names:
            self.instance.report_runless_asset_event(
                dg.AssetMaterialization(asset_key=name)
            )

    def start_run(self, name: str) -> DagsterRun:
        return _start_run(self.instance, AssetKey(name))

    def finish_run(self, run: DagsterRun, name: str) -> None:
        _finish_run(self.instance, run, AssetKey(name))

    def tick(self, at: datetime | None = None) -> set[str]:
        self.now = at or self.now + SENSOR_INTERVAL
        result = dg.evaluate_automation_conditions(
            defs=self.defs,
            instance=self.instance,
            asset_selection=SENSOR_TARGET,
            evaluation_time=self.now,
            cursor=self.cursor,
        )
        self.cursor = result.cursor
        return {
            key.to_user_string()
            for key in self.defs.resolve_asset_graph().get_all_asset_keys()
            if result.get_num_requested(key)
        }

    def settle(self, models: Models) -> None:
        """Deploy and build everything, so nothing is outstanding."""
        self.deploy(models)
        self.materialize(*models)
        self.tick()
        assert self.tick() == set()


@pytest.fixture
def dbt_instance() -> Iterator[dg.DagsterInstance]:
    with dg.DagsterInstance.ephemeral() as ephemeral_instance:
        yield ephemeral_instance


# A morning, well clear of the midnight expiry tick.
DEPLOY_MORNING = datetime(2026, 10, 20, 8, 10, tzinfo=UTC)

CATALOG_V1: Models = {
    "stg_cms_page": ("a", []),
    "int_catalog_pages": ("v1", ["stg_cms_page"]),
    "integrations_courses": ("v1", ["int_catalog_pages"]),
    "integrations_programs": ("v1", ["int_catalog_pages"]),
}
# #3050: catalog_pages gains a column and a staging parent that has never been
# built, and both children start selecting the column.
CATALOG_V2: Models = {
    "stg_cms_page": ("a", []),
    "stg_cms_faq": ("a", []),
    "int_catalog_pages": ("v2", ["stg_cms_page", "stg_cms_faq"]),
    "integrations_courses": ("v2", ["int_catalog_pages"]),
    "integrations_programs": ("v2", ["int_catalog_pages"]),
}
CATALOG_MODELS = {"int_catalog_pages", "integrations_courses", "integrations_programs"}

DOCUMENTS_V1: Models = {
    "stg_document_text": ("v1", []),
    "int_extraction_sources": ("v1", ["stg_document_text"]),
}
# #3032: a staging model only the daily job builds gains a column, and its
# child starts selecting it in the same release.
DOCUMENTS_V2: Models = {
    "stg_document_text": ("v2", []),
    "int_extraction_sources": ("v2", ["stg_document_text"]),
}


def test_shared_condition_runs_children_before_a_blocked_parent(dbt_instance) -> None:
    """The #3050 failure, pinned on the condition every non-dbt asset still uses.

    catalog_pages is blocked by its missing new parent on the deploy tick, so
    its edge-triggered code change is spent, while its changed children are
    free to run against the old table. This stays as it was for canvas,
    edxorg, delivery and the rest, which have no column contract with their
    parents.
    """
    sensor = DbtSensor(dbt_instance, upstream_or_code_changes(), DEPLOY_MORNING)
    sensor.settle(CATALOG_V1)

    sensor.deploy(CATALOG_V2)

    assert sensor.tick() == {"integrations_courses", "integrations_programs"}


def test_shared_condition_runs_a_child_before_its_staging_parent(dbt_instance) -> None:
    """The #3032 failure under the shared condition, for contrast."""
    sensor = DbtSensor(dbt_instance, upstream_or_code_changes(), DEPLOY_MORNING)
    sensor.settle(DOCUMENTS_V1)

    sensor.deploy(DOCUMENTS_V2)

    assert sensor.tick() == {"int_extraction_sources"}


def test_shared_condition_tree_is_unchanged() -> None:
    """Node ids key the cursors, so the shared tree must hash as it did.

    This is the id upstream_or_code_changes() had before the dbt variant was
    split out of it. A different one means a reshaped tree, and cursors reset
    in every code location that uses it.
    """
    tree_id = "817d461e68594a7c0362864d45c4f4f9"  # pragma: allowlist secret
    assert upstream_or_code_changes().get_unique_id() == tree_id


def test_a_change_shipped_with_the_dbt_condition_is_still_requested(
    dbt_instance,
) -> None:
    """Switching an existing cursor to the dbt tree keeps the deploy-tick request.

    The cursor comes from the tree dbt ran on before (the shared one). A model
    whose SQL changes in the same release must still be requested on the
    deploy tick, which only holds if the bare code_version_changed keeps the
    node id its previous code version is stored under.
    """
    sensor = DbtSensor(dbt_instance, upstream_or_code_changes(), DEPLOY_MORNING)
    sensor.settle(DOCUMENTS_V1)

    sensor.condition = dbt_upstream_or_code_changes()
    sensor.deploy(
        {
            "stg_document_text": ("v1", []),
            "int_extraction_sources": ("v2", ["stg_document_text"]),
        }
    )

    assert sensor.tick() == {"int_extraction_sources"}


def test_children_wait_for_a_parent_blocked_by_a_new_staging_model(
    dbt_instance,
) -> None:
    """#3050: nothing runs until the new staging parent exists, then all three do.

    catalog_pages keeps its code change through the ticks it spends blocked,
    and its children are held because it has new code it has not built. Once
    staging builds the new parent, catalog_pages is requested, and the children
    are requested on the same tick so dbt builds them after it in one run.
    """
    sensor = DbtSensor(dbt_instance, dbt_upstream_or_code_changes(), DEPLOY_MORNING)
    sensor.settle(CATALOG_V1)

    sensor.deploy(CATALOG_V2)

    assert sensor.tick() == set(), "deploy tick"
    assert sensor.tick() == set()
    sensor.materialize("stg_cms_faq")
    assert sensor.tick() == CATALOG_MODELS


def test_child_waits_for_a_staging_parent_outside_the_target(dbt_instance) -> None:
    """#3032: the child runs only after the staging job rebuilds its parent.

    This sensor never requests the staging model, so will_be_requested cannot
    release the child; only the staging model's materialization does.
    """
    sensor = DbtSensor(dbt_instance, dbt_upstream_or_code_changes(), DEPLOY_MORNING)
    sensor.settle(DOCUMENTS_V1)

    sensor.deploy(DOCUMENTS_V2)

    assert sensor.tick() == set(), "deploy tick"
    assert sensor.tick() == set()
    sensor.materialize("stg_document_text")
    assert sensor.tick() == {"int_extraction_sources"}
    sensor.materialize("int_extraction_sources")
    assert sensor.tick() == set()


@pytest.mark.parametrize(
    "deploy_tick",
    [
        pytest.param(DEPLOY_MORNING, id="morning"),
        pytest.param(datetime(2026, 10, 20, 23, 30, tzinfo=UTC), id="before-midnight"),
    ],
)
def test_a_parent_that_is_never_built_releases_its_children(
    dbt_instance, deploy_tick: datetime
) -> None:
    """The hold expires at the second midnight after the change, not the first.

    Without the expiry a parent that never succeeds would hold its children
    forever. Expiring at the first midnight would release a change seen at
    23:30 an hour later, before the 06:00 staging build it is waiting for.
    """
    sensor = DbtSensor(dbt_instance, dbt_upstream_or_code_changes(), deploy_tick)
    sensor.settle(DOCUMENTS_V1)
    sensor.deploy(DOCUMENTS_V2)
    second_midnight = deploy_tick.replace(hour=0, minute=0) + timedelta(days=2)

    assert sensor.tick() == set(), "deploy tick"
    while sensor.now + SENSOR_INTERVAL < second_midnight:
        assert sensor.tick() == set(), sensor.now

    assert sensor.tick(second_midnight + timedelta(minutes=30)) == {
        "int_extraction_sources"
    }


def test_the_expiry_restarts_with_each_change(dbt_instance) -> None:
    """A later change gets a full hold, not what is left of an earlier one's."""
    sensor = DbtSensor(dbt_instance, dbt_upstream_or_code_changes(), DEPLOY_MORNING)
    sensor.settle(DOCUMENTS_V1)
    sensor.deploy(DOCUMENTS_V2)
    sensor.tick()
    sensor.materialize("stg_document_text")
    assert sensor.tick() == {"int_extraction_sources"}
    sensor.materialize("int_extraction_sources")
    # Past the first midnight, so the expiry armed once for the v2 change.
    assert sensor.tick(DEPLOY_MORNING + timedelta(days=1, hours=1)) == set()

    sensor.deploy(
        {
            "stg_document_text": ("v3", []),
            "int_extraction_sources": ("v3", ["stg_document_text"]),
        }
    )

    assert sensor.tick() == set(), "deploy tick"
    assert sensor.tick(DEPLOY_MORNING + timedelta(days=2, minutes=30)) == set(), (
        "the first midnight for v3"
    )


def test_a_change_during_a_requested_run_is_requested_after_it(dbt_instance) -> None:
    """A request made before a code change must not consume that change.

    The model is requested on one tick and its run is still going when the
    next tick sees new SQL. newly_requested is true on that tick too, and a
    since() tie goes to the reset, so a bare newly_requested reset would clear
    the latch on the same tick it is set. in_progress blocks the edge-triggered
    code_version_changed as well, so the new SQL would wait for an upstream
    change.
    """
    sensor = DbtSensor(dbt_instance, dbt_upstream_or_code_changes(), DEPLOY_MORNING)
    sensor.settle(DOCUMENTS_V1)
    sensor.deploy(
        {
            "stg_document_text": ("v1", []),
            "int_extraction_sources": ("v2", ["stg_document_text"]),
        }
    )
    assert sensor.tick() == {"int_extraction_sources"}
    in_flight = sensor.start_run("int_extraction_sources")

    sensor.deploy(
        {
            "stg_document_text": ("v1", []),
            "int_extraction_sources": ("v3", ["stg_document_text"]),
        }
    )
    assert sensor.tick() == set(), "the v2 run is still going"
    assert sensor.tick() == set()

    sensor.finish_run(in_flight, "int_extraction_sources")

    assert sensor.tick() == {"int_extraction_sources"}, "v3 must still be built"


def test_an_expired_hold_stays_released_while_the_child_is_blocked(
    dbt_instance,
) -> None:
    """Once expired, the hold does not re-arm on a later tick.

    since() only turns true again on its trigger, a new code change, so a
    child blocked by something unrelated on the expiry tick is requested on
    the first tick after that blocker clears, not another day later.
    """
    v1: Models = {
        "stg_document_text": ("v1", []),
        "stg_other": ("v1", []),
        "int_extraction_sources": ("v1", ["stg_document_text", "stg_other"]),
    }
    v2: Models = {
        "stg_document_text": ("v2", []),
        "stg_other": ("v1", []),
        "int_extraction_sources": ("v2", ["stg_document_text", "stg_other"]),
    }
    sensor = DbtSensor(dbt_instance, dbt_upstream_or_code_changes(), DEPLOY_MORNING)
    sensor.settle(v1)
    sensor.deploy(v2)
    second_midnight = DEPLOY_MORNING.replace(hour=0, minute=0) + timedelta(days=2)
    assert sensor.tick() == set(), "deploy tick"
    while sensor.now + SENSOR_INTERVAL < second_midnight:
        assert sensor.tick() == set(), sensor.now

    unrelated = sensor.start_run("stg_other")
    assert sensor.tick(second_midnight + timedelta(minutes=30)) == set(), (
        "expired, but another parent is in progress"
    )
    sensor.finish_run(unrelated, "stg_other")

    assert sensor.tick() == {"int_extraction_sources"}
