"""Tests for the failed-partition inventory checks.

These drive real runs against an ephemeral instance rather than stubbing the
partition status cache, because the thing being tested is whether Dagster
considers a partition failed -- which is a property of the event log and the
status cache, not of our arithmetic.
"""

from collections.abc import Iterator

import dagster as dg
import pytest
from dagster import AssetCheckSeverity, StaticPartitionsDefinition, asset
from ol_orchestrate.lib.failed_partitions import (
    FAILED_PARTITION_CHECK_NAME,
    MAX_REPORTED_PARTITION_KEYS,
    build_failed_partition_checks,
    failed_partition_check_schedule,
    failed_partition_subset,
)

PARTITIONS = StaticPartitionsDefinition(["a", "b", "c"])


class Behaviour:
    """Controls whether the asset succeeds for a given partition."""

    def __init__(self) -> None:
        self.failing: set[str] = set()


@pytest.fixture
def behaviour() -> Behaviour:
    return Behaviour()


@pytest.fixture
def instance() -> Iterator[dg.DagsterInstance]:
    with dg.DagsterInstance.ephemeral() as ephemeral:
        yield ephemeral


@pytest.fixture
def partitioned_asset(behaviour: Behaviour):
    @asset(name="partitioned", partitions_def=PARTITIONS)
    def partitioned(context) -> None:
        if context.partition_key in behaviour.failing:
            msg = f"{context.partition_key} is broken"
            raise RuntimeError(msg)

    return partitioned


def materialize(asset_def, instance, partition_key: str) -> None:
    dg.materialize(
        [asset_def],
        instance=instance,
        partition_key=partition_key,
        raise_on_error=False,
    )


def run_check(check_def, asset_def, instance):
    """Execute the inventory check on its own and return its single evaluation."""
    result = dg.materialize(
        [asset_def, check_def],
        instance=instance,
        selection=dg.AssetSelection.checks(check_def),
        raise_on_error=False,
    )
    (evaluation,) = result.get_asset_check_evaluations()
    return evaluation


def test_a_clean_asset_passes(partitioned_asset, instance) -> None:
    for partition in ("a", "b", "c"):
        materialize(partitioned_asset, instance, partition)
    (check_def,) = build_failed_partition_checks([partitioned_asset])

    evaluation = run_check(check_def, partitioned_asset, instance)

    assert evaluation.passed is True
    assert evaluation.metadata["failed_partitions"].value == 0


def test_a_failed_partition_fails_the_check(
    partitioned_asset, instance, behaviour
) -> None:
    """The signal the bounded retry removed.

    After execution_failed().newly_true() spends its one retry, nothing
    re-requests this partition and the Sentry issue falls silent -- which in a
    list of issues is indistinguishable from being fixed.
    """
    behaviour.failing = {"b"}
    for partition in ("a", "b", "c"):
        materialize(partitioned_asset, instance, partition)
    (check_def,) = build_failed_partition_checks([partitioned_asset])

    evaluation = run_check(check_def, partitioned_asset, instance)

    assert evaluation.passed is False
    assert evaluation.severity == AssetCheckSeverity.ERROR
    assert evaluation.metadata["failed_partitions"].value == 1
    assert evaluation.metadata["sample"].value == ["b"]


def test_the_check_names_which_partitions(
    partitioned_asset, instance, behaviour
) -> None:
    """A count alone does not tell anyone where to start."""
    behaviour.failing = {"a", "c"}
    for partition in ("a", "b", "c"):
        materialize(partitioned_asset, instance, partition)
    (check_def,) = build_failed_partition_checks([partitioned_asset])

    evaluation = run_check(check_def, partitioned_asset, instance)

    assert evaluation.metadata["sample"].value == ["a", "c"]
    assert evaluation.metadata["sample_truncated"].value is False


def test_a_partition_that_recovers_stops_being_reported(
    partitioned_asset, instance, behaviour
) -> None:
    """The inventory reports current state, not history.

    Otherwise it becomes a permanent red mark that people learn to ignore --
    the same fate as the alert stream this replaces.
    """
    behaviour.failing = {"b"}
    materialize(partitioned_asset, instance, "b")
    (check_def,) = build_failed_partition_checks([partitioned_asset])
    assert run_check(check_def, partitioned_asset, instance).passed is False

    behaviour.failing = set()
    materialize(partitioned_asset, instance, "b")

    assert run_check(check_def, partitioned_asset, instance).passed is True


def test_an_asset_never_materialized_passes(instance) -> None:
    """Nothing has had the chance to fail yet, so there is nothing to report."""

    @asset(name="untouched", partitions_def=PARTITIONS)
    def untouched() -> None: ...

    assert failed_partition_subset(instance, untouched.key, PARTITIONS) is None


def test_unpartitioned_assets_are_skipped() -> None:
    """A run-level failure there is already visible as a failed run, and the
    check would have nothing to count.
    """

    @asset(name="plain")
    def plain() -> None: ...

    assert build_failed_partition_checks([plain]) == []


def test_every_partitioned_asset_key_gets_a_check(partitioned_asset) -> None:
    (check_def,) = build_failed_partition_checks([partitioned_asset])

    (check_key,) = check_def.check_keys
    assert check_key.asset_key == partitioned_asset.key
    assert check_key.name == FAILED_PARTITION_CHECK_NAME


def test_the_schedule_is_stopped_by_default(partitioned_asset) -> None:
    """Turning it on is a deliberate act, like every other sensor here."""
    checks = build_failed_partition_checks([partitioned_asset])

    schedule = failed_partition_check_schedule(checks)

    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.cron_schedule == "0 13 * * *"


def test_the_sample_is_capped_but_the_count_is_not(instance) -> None:
    """A Slack block and a Sentry issue both have limits; the count does not."""
    many = StaticPartitionsDefinition([f"p{index:03d}" for index in range(40)])

    @asset(name="many_partitions", partitions_def=many)
    def many_partitions() -> None:
        msg = "always broken"
        raise RuntimeError(msg)

    for partition in many.get_partition_keys():
        materialize(many_partitions, instance, partition)
    (check_def,) = build_failed_partition_checks([many_partitions])

    evaluation = run_check(check_def, many_partitions, instance)

    assert evaluation.metadata["failed_partitions"].value == 40
    assert len(evaluation.metadata["sample"].value) == MAX_REPORTED_PARTITION_KEYS
    assert evaluation.metadata["sample_truncated"].value is True
