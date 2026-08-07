"""Tests for the openedx/courseware observable source asset and what it drives."""

import time
from collections.abc import Iterator
from datetime import UTC, datetime

import httpx2 as httpx
import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetSelection,
    DagsterInstance,
    Definitions,
    DynamicPartitionsDefinition,
    IOManager,
    ResourceDefinition,
    evaluate_automation_conditions,
)
from dagster._core.definitions.asset_daemon_cursor import AssetDaemonCursor
from dagster._core.definitions.observe import observe
from dagster._core.events import (
    AssetMaterializationPlannedData,
    DagsterEvent,
    DagsterEventType,
)
from dagster._core.events.log import EventLogEntry
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.storage.tags import PARTITION_NAME_TAG
from dagster._core.test_utils import create_run_for_test
from openedx.assets.openedx import (
    HTTP_NOT_FOUND,
    build_courseware_source_asset,
    course_xml,
)
from openedx.lib.assets_helper import (
    add_prefix_to_asset_keys,
    late_bind_partition_to_asset,
)

DEPLOYMENT = "mitxonline"
COURSEWARE_KEY = AssetKey([DEPLOYMENT, "openedx", "courseware"])
COURSE_XML_KEY = AssetKey([DEPLOYMENT, "openedx", "raw_data", "course_xml"])
EXPORT_JOB = "__ASSET_JOB"


@pytest.fixture
def partitions() -> DynamicPartitionsDefinition:
    """Return a dynamic partitions definition isolated to a single test."""
    return DynamicPartitionsDefinition(name="test_openedx_course_run")


@pytest.fixture
def instance() -> Iterator[DagsterInstance]:
    """Return a throwaway Dagster instance for event-log assertions."""
    with DagsterInstance.ephemeral() as ephemeral_instance:
        yield ephemeral_instance


class _OutlineClient:
    """Serves canned course outlines, and can be made to fail for some of them."""

    def __init__(
        self,
        versions: dict[str, str],
        missing: set[str] | None = None,
        raises: set[str] | None = None,
    ) -> None:
        self.versions = versions
        self.missing = missing or set()
        self.raises = raises or set()

    def get_course_outline(self, course_id: str) -> dict[str, str]:
        if course_id in self.missing:
            msg = f"no outline for {course_id}"
            raise httpx.HTTPStatusError(
                msg,
                request=httpx.Request("GET", "https://lms.example/outline"),
                response=httpx.Response(HTTP_NOT_FOUND),
            )
        if course_id in self.raises:
            msg = f"boom for {course_id}"
            raise ValueError(msg)
        return {"published_version": self.versions[course_id]}


class _FakeOpenEdx:
    """Stand-in for OpenEdxApiClientFactory holding a fixed outline client."""

    def __init__(self, client: _OutlineClient) -> None:
        self.client = client
        self.deployment = DEPLOYMENT


class _NoopIOManager(IOManager):
    def handle_output(self, context, obj) -> None:
        """Never called: these tests evaluate conditions, they do not execute."""

    def load_input(self, context):
        """Never called: these tests evaluate conditions, they do not execute."""


def _definitions(
    partitions: DynamicPartitionsDefinition, client: _OutlineClient
) -> Definitions:
    """Build courseware plus the course_xml that reacts to it, as production does."""
    return Definitions(
        assets=[
            build_courseware_source_asset(DEPLOYMENT, partitions),
            late_bind_partition_to_asset(
                add_prefix_to_asset_keys(course_xml, DEPLOYMENT), partitions
            ),
        ],
        resources={
            "openedx": ResourceDefinition.hardcoded_resource(_FakeOpenEdx(client)),
            "s3": ResourceDefinition.hardcoded_resource(None),
            "s3file_io_manager": _NoopIOManager(),
        },
    )


def _observe(defs: Definitions, instance: DagsterInstance) -> None:
    """Run the courseware observation the way the automation sensor would."""
    source_asset = defs.get_repository_def().source_assets_by_key[COURSEWARE_KEY]
    assert observe([source_asset], instance=instance).success


def _observed_versions(instance: DagsterInstance) -> dict[str, str | None]:
    """Map partition key to the data version its latest observation reported."""
    records = instance.fetch_observations(COURSEWARE_KEY, limit=100).records
    versions: dict[str, str | None] = {}
    for record in reversed(records):
        observation = record.asset_observation
        if observation is None or observation.partition is None:
            continue
        versions[observation.partition] = observation.tags.get("dagster/data_version")
    return versions


def test_every_registered_partition_is_observed(
    instance: DagsterInstance, partitions: DynamicPartitionsDefinition
) -> None:
    """One observation run reports a version for the whole deployment.

    Observation is per asset, not per partition, so this is the property that
    makes a few thousand course runs affordable: the concurrent sweep happens
    inside a single run rather than one run per course.
    """
    instance.add_dynamic_partitions(partitions.name, ["course-a", "course-b"])
    defs = _definitions(
        partitions, _OutlineClient({"course-a": "v1", "course-b": "v2"})
    )

    _observe(defs, instance)

    assert _observed_versions(instance) == {"course-a": "v1", "course-b": "v2"}


def test_a_course_missing_from_the_lms_is_not_observed(
    instance: DagsterInstance, partitions: DynamicPartitionsDefinition
) -> None:
    """A 404 leaves the partition out entirely so its last version stands.

    Emitting anything for it - even a null version - would read as a change and
    ask for an export of a course that is no longer there to export.
    """
    instance.add_dynamic_partitions(partitions.name, ["course-a", "course-gone"])
    defs = _definitions(
        partitions,
        _OutlineClient({"course-a": "v1"}, missing={"course-gone"}),
    )

    _observe(defs, instance)

    assert _observed_versions(instance) == {"course-a": "v1"}


def test_one_failing_lookup_does_not_stop_the_sweep(
    instance: DagsterInstance, partitions: DynamicPartitionsDefinition
) -> None:
    """A single broken course is skipped; the rest of the deployment reports."""
    instance.add_dynamic_partitions(partitions.name, ["course-a", "course-bad"])
    defs = _definitions(
        partitions,
        _OutlineClient({"course-a": "v1", "course-bad": "v9"}, raises={"course-bad"}),
    )

    _observe(defs, instance)

    assert _observed_versions(instance) == {"course-a": "v1"}


def test_every_lookup_failing_fails_the_observation(
    instance: DagsterInstance, partitions: DynamicPartitionsDefinition
) -> None:
    """A bad token or a 500-ing LMS must not look like a clean sweep.

    Succeeding with an empty mapping would leave every downstream quiet,
    hourly, forever, with a green run to say so.
    """
    keys = ["course-a", "course-b"]
    instance.add_dynamic_partitions(partitions.name, keys)
    defs = _definitions(
        partitions,
        _OutlineClient(dict.fromkeys(keys, "v1"), raises=set(keys)),
    )
    source_asset = defs.get_repository_def().source_assets_by_key[COURSEWARE_KEY]

    result = observe([source_asset], instance=instance, raise_on_error=False)

    assert not result.success


def _requested(
    defs: Definitions, instance: DagsterInstance, cursor: AssetDaemonCursor | None
):
    """Evaluate course_xml's automation condition and return what it asks for."""
    result = evaluate_automation_conditions(
        defs=defs,
        instance=instance,
        asset_selection=AssetSelection.assets(COURSE_XML_KEY),
        cursor=cursor,
    )
    return set(result.get_requested_partitions(COURSE_XML_KEY)), result.cursor


def _record_export(instance: DagsterInstance, partition_key: str) -> None:
    """Stand in for a successful course_xml run for one partition."""
    instance.report_runless_asset_event(
        AssetMaterialization(asset_key=COURSE_XML_KEY, partition=partition_key)
    )


def _start_export_run(instance: DagsterInstance, partition_key: str):
    """Stage an export run that Dagster reports as in flight for course_xml.

    The MATERIALIZATION_PLANNED event is the load-bearing part -- that is what
    in_progress() reads for a partitioned asset, not the run record.
    """
    run = create_run_for_test(
        instance,
        job_name=EXPORT_JOB,
        status=DagsterRunStatus.STARTED,
        asset_selection={COURSE_XML_KEY},
        tags={PARTITION_NAME_TAG: partition_key},
    )
    instance.handle_new_event(
        EventLogEntry(
            error_info=None,
            level="debug",
            user_message="",
            run_id=run.run_id,
            timestamp=time.time(),
            dagster_event=DagsterEvent(
                event_type_value=DagsterEventType.ASSET_MATERIALIZATION_PLANNED.value,
                job_name=EXPORT_JOB,
                event_specific_data=AssetMaterializationPlannedData(
                    asset_key=COURSE_XML_KEY, partition=partition_key
                ),
            ),
        )
    )
    return run


def _finish_export_run(instance: DagsterInstance, run, partition_key: str) -> None:
    """Complete a staged export run successfully, archive and all."""
    _record_export(instance, partition_key)
    instance.handle_new_event(
        EventLogEntry(
            error_info=None,
            level="debug",
            user_message="",
            run_id=run.run_id,
            timestamp=time.time(),
            dagster_event=DagsterEvent(
                event_type_value=DagsterEventType.RUN_SUCCESS.value,
                job_name=EXPORT_JOB,
            ),
        )
    )


def test_a_republish_during_an_export_is_not_lost(
    instance: DagsterInstance, partitions: DynamicPartitionsDefinition
) -> None:
    """A course republished mid-export still gets exported afterwards.

    Exports poll Studio for minutes and the sweep runs on a cron, so a
    republish landing inside a running export is routine rather than exotic.
    Without the latch in upstream_or_code_changes() the only tick where
    data_version_changed is true is the one ~in_progress() suppresses, and the
    running export then succeeds -- so nothing fails, nothing re-fires, and the
    archive silently stays a version behind until the course changes again.
    """
    instance.add_dynamic_partitions(partitions.name, ["course-a"])
    client = _OutlineClient({"course-a": "v1"})
    defs = _definitions(partitions, client)

    _observe(defs, instance)
    requested, cursor = _requested(defs, instance, None)
    assert requested == {"course-a"}
    _record_export(instance, "course-a")
    requested, cursor = _requested(defs, instance, cursor)
    assert requested == set(), "v1 is exported"

    in_flight = _start_export_run(instance, "course-a")
    client.versions["course-a"] = "v2"
    _observe(defs, instance)
    requested, cursor = _requested(defs, instance, cursor)
    assert requested == set(), "suppressed while the earlier export runs"

    _finish_export_run(instance, in_flight, "course-a")
    requested, cursor = _requested(defs, instance, cursor)
    assert requested == {"course-a"}, "v2 must still be exported"


def test_the_observation_drives_the_full_export_cycle(
    instance: DagsterInstance, partitions: DynamicPartitionsDefinition
) -> None:
    """The asset graph alone covers everything course_version_sensor hand-rolled.

    Never exported, already exported, re-observed unchanged, republished: each
    step is a plain consequence of ``upstream_or_code_changes()`` reading the
    data versions the observation reports, with no reconciliation of our own.
    """
    instance.add_dynamic_partitions(partitions.name, ["course-a", "course-b"])
    client = _OutlineClient({"course-a": "v1", "course-b": "v1"})
    defs = _definitions(partitions, client)

    _observe(defs, instance)
    requested, cursor = _requested(defs, instance, None)
    assert requested == {"course-a", "course-b"}, "never exported"

    _record_export(instance, "course-a")
    _record_export(instance, "course-b")
    requested, cursor = _requested(defs, instance, cursor)
    assert requested == set(), "exported at the observed version"

    _observe(defs, instance)
    requested, cursor = _requested(defs, instance, cursor)
    assert requested == set(), "re-observed unchanged"

    client.versions["course-b"] = "v2"
    _observe(defs, instance)
    requested, cursor = _requested(defs, instance, cursor)
    assert requested == {"course-b"}, "republished"


def test_the_observation_is_requested_once_an_hour(
    instance: DagsterInstance, partitions: DynamicPartitionsDefinition
) -> None:
    """The cron tick is what schedules the sweep now that the sensor is gone.

    Once per hour, not once per evaluation: the automation sensor ticks far
    more often than that, and every extra tick would be another full outline
    sweep of the deployment.
    """
    instance.add_dynamic_partitions(partitions.name, ["course-a"])
    defs = _definitions(partitions, _OutlineClient({"course-a": "v1"}))
    selection = AssetSelection.assets(COURSEWARE_KEY)

    def _evaluate(at: datetime, cursor: AssetDaemonCursor | None):
        result = evaluate_automation_conditions(
            defs=defs,
            instance=instance,
            asset_selection=selection,
            evaluation_time=at,
            cursor=cursor,
        )
        return result.total_requested, result.cursor

    requested, cursor = _evaluate(datetime(2026, 8, 7, 13, 0, 1, tzinfo=UTC), None)
    assert requested == 0, "no cron boundary crossed yet"

    requested, cursor = _evaluate(datetime(2026, 8, 7, 13, 5, 0, tzinfo=UTC), cursor)
    assert requested == 0, "still the same hour"

    requested, cursor = _evaluate(datetime(2026, 8, 7, 14, 0, 1, tzinfo=UTC), cursor)
    assert requested == 1, "the hour turned over"


def test_a_partition_registered_later_is_exported(
    instance: DagsterInstance, partitions: DynamicPartitionsDefinition
) -> None:
    """A course run discovered after the first sweep still gets exported.

    This is the whole job the discovery sensor hands off: it registers the
    partition and stops, and the next observation is what makes the graph ask
    for the export.
    """
    instance.add_dynamic_partitions(partitions.name, ["course-a"])
    client = _OutlineClient({"course-a": "v1", "course-new": "v1"})
    defs = _definitions(partitions, client)

    _observe(defs, instance)
    _requested(defs, instance, None)
    _record_export(instance, "course-a")
    _, cursor = _requested(defs, instance, None)

    instance.add_dynamic_partitions(partitions.name, ["course-new"])
    _observe(defs, instance)
    requested, _ = _requested(defs, instance, cursor)

    assert requested == {"course-new"}
