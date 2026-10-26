"""Tests for the empty full refresh check on the Airbyte connection assets.

Global Alumni's api_enrollments is a full refresh stream that read no rows every
night from 2026-05-04. Airbyte reported each sync as a success, Dagster recorded
a materialization, and the raw table stayed frozen. The check fails the first
such sync; these tests hold it to that without failing the syncs that read
nothing legitimately.
"""

from datetime import UTC, datetime
from typing import Any

import pytest
from dagster import AssetCheckSeverity, AssetKey, materialize
from dagster_airbyte import DagsterAirbyteTranslator, build_airbyte_assets_definitions
from dagster_airbyte.translator import AirbyteConnectionTableProps
from dagster_airbyte.types import AirbyteOutput
from dagster_airbyte.utils import DAGSTER_AIRBYTE_TRANSLATOR_METADATA_KEY
from lakehouse.lib import airbyte_assets
from lakehouse.lib.airbyte_assets import (
    EMPTY_FULL_REFRESH_CHECK,
    build_airbyte_assets,
    empty_full_refresh_check,
    raw_table_record_count,
)
from lakehouse.resources.airbyte import (
    AirbyteOSSClient,
    AirbyteOSSWorkspace,
    StreamSync,
)
from pyiceberg.exceptions import NoSuchTableError

ENROLLMENTS = AssetKey("raw__global_alumni__bigquery__api_enrollments")
CONNECTION_NAME = "Global Alumni BigQuery → S3 Data Lake"
PREFIX = "raw__global_alumni__bigquery__"
JOB_STARTED_AT = datetime(2026, 10, 19, 0, 0, 12, tzinfo=UTC)


def _stream(
    sync_mode: str, records_emitted: int, key: AssetKey = ENROLLMENTS
) -> StreamSync:
    return StreamSync(
        asset_key=key,
        spec_metadata={},
        description="",
        job_id=60322,
        job_started_at=JOB_STARTED_AT,
        sync_mode=sync_mode,
        records_emitted=records_emitted,
        records_committed=records_emitted,
    )


class _Counter:
    """A destination where every table held *rows*, recording each lookup."""

    def __init__(self, rows: int) -> None:
        self.rows = rows
        self.looked_up: list[tuple[AssetKey, datetime]] = []

    def __call__(self, asset_key: AssetKey, as_of: datetime) -> int:
        self.looked_up.append((asset_key, as_of))
        return self.rows


class TestRawTableRecordCount:
    """The count is of the table as the sync found it, not as it left it.

    An overwrite stream's empty sync may leave an empty table behind; counting
    now would then pass the sync that emptied it.
    """

    @pytest.fixture
    def catalog(self, monkeypatch):
        class Snapshot:
            summary = {"total-records": "143740"}  # noqa: RUF012

        class Table:
            def snapshot_as_of_timestamp(_self, timestamp_ms: int):  # noqa: N805
                self.asked_for = timestamp_ms
                return Snapshot() if self.snapshot else None

        class Catalog:
            def load_table(_self, identifier: str):  # noqa: N805
                self.loaded = identifier
                if not self.exists:
                    raise NoSuchTableError(identifier)
                return Table()

        self.exists = True
        self.snapshot = True
        self.loaded = ""
        self.asked_for = 0
        monkeypatch.setattr(airbyte_assets, "get_glue_catalog", Catalog)

    @pytest.mark.usefixtures("catalog")
    def test_the_snapshot_current_when_the_sync_started_is_counted(self) -> None:
        key = AssetKey(["ol_warehouse_raw_data", ENROLLMENTS.path[-1]])
        assert raw_table_record_count("raw_db", key, JOB_STARTED_AT) == 143740
        assert self.loaded == f"raw_db.{ENROLLMENTS.path[-1]}"
        assert self.asked_for == int(JOB_STARTED_AT.timestamp() * 1000)

    @pytest.mark.usefixtures("catalog")
    def test_a_table_with_no_snapshot_then_held_nothing(self) -> None:
        self.snapshot = False
        assert raw_table_record_count("raw_db", ENROLLMENTS, JOB_STARTED_AT) == 0

    @pytest.mark.usefixtures("catalog")
    def test_a_table_that_does_not_exist_holds_nothing(self) -> None:
        self.exists = False
        assert raw_table_record_count("raw_db", ENROLLMENTS, JOB_STARTED_AT) == 0


class TestEmptyFullRefreshCheck:
    def test_an_empty_sync_into_a_table_with_rows_fails(self) -> None:
        result = empty_full_refresh_check(_stream("full_refresh", 0), _Counter(143740))
        assert result is not None
        assert not result.passed
        assert result.severity == AssetCheckSeverity.ERROR
        assert "60322" in (result.description or "")

    def test_an_empty_sync_into_an_empty_table_passes(self) -> None:
        """A source with nothing in it, e.g. MITx's assessment_aiclassifier."""
        result = empty_full_refresh_check(_stream("full_refresh", 0), _Counter(0))
        assert result is not None
        assert result.passed

    def test_a_sync_that_read_rows_passes_without_reading_the_table(self) -> None:
        counter = _Counter(143740)
        result = empty_full_refresh_check(_stream("full_refresh", 71870), counter)
        assert result is not None
        assert result.passed
        assert counter.looked_up == []

    def test_an_incremental_stream_is_not_evaluated(self) -> None:
        """No new rows since the last cursor is the ordinary incremental sync."""
        counter = _Counter(143740)
        assert empty_full_refresh_check(_stream("incremental", 0), counter) is None
        assert counter.looked_up == []


def _catalog_stream(name: str, sync_mode: str) -> dict[str, Any]:
    return {
        "stream": {"name": name, "jsonSchema": {"properties": {}}},
        "config": {"selected": True, "syncMode": sync_mode},
    }


CATALOG_STREAMS = [
    _catalog_stream("api_enrollments", "full_refresh"),
    _catalog_stream("api_batches", "full_refresh"),
    _catalog_stream("api_events", "incremental"),
    {
        "stream": {"name": "api_unused", "jsonSchema": {}},
        "config": {"selected": False, "syncMode": "full_refresh"},
    },
]
CONNECTION_DETAILS = {
    "connectionId": "conn-1",
    "name": CONNECTION_NAME,
    "prefix": PREFIX,
    "destinationId": "dest-1",
    "syncCatalog": {"streams": CATALOG_STREAMS},
}

BATCHES = AssetKey(f"{PREFIX}api_batches")
EVENTS = AssetKey(f"{PREFIX}api_events")


@pytest.fixture
def workspace(monkeypatch) -> AirbyteOSSWorkspace:
    """Build a workspace whose one connection syncs without HTTP.

    The sync reads 5 rows from api_batches and nothing from the other two
    streams, the way job 60322 read nothing from api_enrollments.
    """
    translator = DagsterAirbyteTranslator()
    specs = [
        translator.get_asset_spec(
            AirbyteConnectionTableProps(
                table_name=f"{PREFIX}{stream['stream']['name']}",
                stream_prefix=PREFIX,
                stream_name=stream["stream"]["name"],
                json_schema=stream["stream"]["jsonSchema"],
                connection_id="conn-1",
                connection_name=CONNECTION_NAME,
                destination_type=None,
                database=None,
                schema=None,
            )
        ).merge_attributes(
            metadata={DAGSTER_AIRBYTE_TRANSLATOR_METADATA_KEY: translator}
        )
        for stream in CATALOG_STREAMS
        if stream["config"]["selected"]
    ]
    monkeypatch.setattr(AirbyteOSSWorkspace, "load_asset_specs", lambda *_, **__: specs)
    monkeypatch.setattr(
        AirbyteOSSClient,
        "sync_and_poll",
        lambda *_, **__: AirbyteOutput(
            job_details={
                "jobId": 60322,
                "status": "succeeded",
                "startTime": "2026-10-19T00:00:12Z",
            },
            connection_details=CONNECTION_DETAILS,
        ),
    )
    monkeypatch.setattr(
        AirbyteOSSClient,
        "get_stream_stats",
        lambda *_, **__: {
            "api_batches": {
                "streamName": "api_batches",
                "recordsEmitted": 5,
                "recordsCommitted": 5,
            }
        },
    )
    return AirbyteOSSWorkspace(
        api_server="https://airbyte.example.invalid",
        username="dagster",
        password="not-a-real-password",  # pragma: allowlist secret
        workspace_id="workspace-1",
    )


def _build(workspace: AirbyteOSSWorkspace, counter: _Counter):
    return build_airbyte_assets(
        workspace=workspace,
        dagster_airbyte_translator=DagsterAirbyteTranslator(),
        connection_selector_fn=lambda _: True,
        destination_record_count=counter,
    )


class TestConnectionAssets:
    def test_one_multi_asset_per_connection_named_as_the_library_names_it(
        self, workspace
    ) -> None:
        """Renaming the op would orphan its run history and job selections."""
        (assets_def,) = _build(workspace, _Counter(0))
        (library_def,) = build_airbyte_assets_definitions(
            workspace=workspace,
            dagster_airbyte_translator=DagsterAirbyteTranslator(),
        )
        assert assets_def.op.name == library_def.op.name
        assert assets_def.keys == library_def.keys == {ENROLLMENTS, BATCHES, EVENTS}

    def test_every_stream_carries_the_check(self, workspace) -> None:
        """The sync mode is only known once the sync has run."""
        (assets_def,) = _build(workspace, _Counter(0))
        assert {key.asset_key for key in assets_def.check_keys} == {
            ENROLLMENTS,
            BATCHES,
            EVENTS,
        }
        assert {key.name for key in assets_def.check_keys} == {EMPTY_FULL_REFRESH_CHECK}

    def test_the_sync_fails_the_empty_full_refresh_stream_only(self, workspace) -> None:
        counter = _Counter(143740)
        result = materialize(
            _build(workspace, counter), resources={"airbyte": workspace}
        )

        assert result.success
        evaluations = {
            evaluation.asset_key: evaluation
            for evaluation in result.get_asset_check_evaluations()
        }
        assert set(evaluations) == {ENROLLMENTS, BATCHES}
        assert not evaluations[ENROLLMENTS].passed
        assert evaluations[ENROLLMENTS].severity == AssetCheckSeverity.ERROR
        assert evaluations[BATCHES].passed
        assert counter.looked_up == [(ENROLLMENTS, JOB_STARTED_AT)]

    def test_each_materialization_records_what_the_stream_read(self, workspace) -> None:
        result = materialize(
            _build(workspace, _Counter(0)), resources={"airbyte": workspace}
        )

        metadata = {
            event.asset_key: event.step_materialization_data.materialization.metadata
            for event in result.get_asset_materialization_events()
        }
        assert set(metadata) == {ENROLLMENTS, BATCHES, EVENTS}
        assert metadata[BATCHES]["airbyte_records_emitted"].value == 5
        assert metadata[ENROLLMENTS]["airbyte_records_emitted"].value == 0
        assert metadata[EVENTS]["airbyte_sync_mode"].value == "incremental"
        assert metadata[ENROLLMENTS]["airbyte_job_id"].value == 60322
