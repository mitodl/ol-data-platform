"""Tests for openedx.sensors.openedx."""

from collections.abc import Iterator

import pytest
from dagster import AssetKey, AssetMaterialization, DagsterInstance
from openedx.sensors.openedx import last_exported_version

COURSE_XML_KEY = AssetKey(("mitxonline", "openedx", "raw_data", "course_xml"))


@pytest.fixture
def instance() -> Iterator[DagsterInstance]:
    """Return a throwaway Dagster instance for event-log assertions."""
    with DagsterInstance.ephemeral() as ephemeral_instance:
        yield ephemeral_instance


def _record_export(
    instance: DagsterInstance, partition_key: str, version: str | None
) -> None:
    """Record a course_xml materialization, optionally without the version key."""
    metadata = {"courseware_published_version": version} if version else {}
    instance.report_runless_asset_event(
        AssetMaterialization(
            asset_key=COURSE_XML_KEY, partition=partition_key, metadata=metadata
        )
    )


def test_last_exported_version_reads_the_latest_materialization(
    instance: DagsterInstance,
) -> None:
    """The most recent materialization's recorded version wins."""
    _record_export(instance, "course-v1:org+num+run", "version-one")
    _record_export(instance, "course-v1:org+num+run", "version-two")

    result = last_exported_version(instance, COURSE_XML_KEY, "course-v1:org+num+run")

    assert result == "version-two"


def test_last_exported_version_is_none_when_never_exported(
    instance: DagsterInstance,
) -> None:
    """A partition with no materialization has no exported version."""
    result = last_exported_version(instance, COURSE_XML_KEY, "course-v1:org+num+run")

    assert result is None


def test_last_exported_version_is_none_for_pre_existing_archives(
    instance: DagsterInstance,
) -> None:
    """Archives materialized before the metadata key existed count as unknown."""
    _record_export(instance, "course-v1:org+num+run", None)

    result = last_exported_version(instance, COURSE_XML_KEY, "course-v1:org+num+run")

    assert result is None
