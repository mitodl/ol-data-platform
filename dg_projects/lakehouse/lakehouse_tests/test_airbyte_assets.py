"""Tests for the empty full refresh check on the Airbyte connection assets.

Global Alumni's api_enrollments is a full refresh stream that read no rows every
night from 2026-05-04. Airbyte reported each sync as a success, Dagster recorded
a materialization, and the raw table stayed frozen. The check fails the first
such sync; these tests hold it to that without failing the syncs that read
nothing legitimately, and without failing a sync that succeeded because a lookup
after it did not.
"""

import logging
from datetime import UTC, datetime
from typing import Any

import pytest
from dagster import (
    AssetCheckSeverity,
    AssetKey,
    AssetSelection,
    Failure,
    materialize,
)
from dagster_airbyte import DagsterAirbyteTranslator, build_airbyte_assets_definitions
from dagster_airbyte.translator import AirbyteConnectionTableProps
from dagster_airbyte.types import AirbyteOutput
from dagster_airbyte.utils import DAGSTER_AIRBYTE_TRANSLATOR_METADATA_KEY
from lakehouse.lib import airbyte_assets
from lakehouse.lib.airbyte_assets import (
    EMPTY_FULL_REFRESH_CHECK,
    RecordCountUnknownError,
    build_airbyte_assets,
    empty_full_refresh_check,
    raw_table_record_count,
)
from lakehouse.resources.airbyte import (
    AirbyteOSSClient,
    AirbyteOSSWorkspace,
    JobStats,
    StreamSync,
)
from pyiceberg.exceptions import NoSuchTableError

ENROLLMENTS = AssetKey("raw__global_alumni__bigquery__api_enrollments")
CONNECTION_NAME = "Global Alumni BigQuery → S3 Data Lake"
PREFIX = "raw__global_alumni__bigquery__"
JOB_STARTED_AT = datetime(2026, 10, 19, 0, 0, 12, tzinfo=UTC)
LOG = logging.getLogger(__name__)


def _stream(
    sync_mode: str,
    records_emitted: int | None,
    job_started_at: datetime | None = JOB_STARTED_AT,
) -> StreamSync:
    return StreamSync(
        asset_key=ENROLLMENTS,
        spec_metadata={},
        description="",
        job_id=60322,
        job_type="sync",
        job_started_at=job_started_at,
        sync_mode=sync_mode,
        records_emitted=records_emitted,
        records_committed=records_emitted,
    )


class _Counter:
    """A destination where every table held *rows*, recording each lookup.

    Given an exception instead, every lookup raises it.
    """

    def __init__(self, rows: int | Exception) -> None:
        self.rows = rows
        self.looked_up: list[tuple[AssetKey, datetime]] = []

    def __call__(self, asset_key: AssetKey, as_of: datetime) -> int:
        self.looked_up.append((asset_key, as_of))
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows


class TestRawTableRecordCount:
    """The count is of the table as the sync found it, not as it left it.

    An overwrite stream's empty sync may leave an empty table behind; counting
    now would then pass the sync that emptied it.
    """

    @pytest.fixture
    def catalog(self, monkeypatch):
        class Snapshot:
            snapshot_id = 42

            def __init__(_self, summary):  # noqa: N805
                _self.summary = summary

        class Table:
            def current_snapshot(_self):  # noqa: N805
                return Snapshot({}) if self.any_snapshot else None

            def snapshot_as_of_timestamp(_self, timestamp_ms: int):  # noqa: N805
                self.asked_for = timestamp_ms
                return Snapshot(self.summary) if self.snapshot else None

        class Catalog:
            def load_table(_self, identifier: str):  # noqa: N805
                self.loaded = identifier
                if not self.exists:
                    raise NoSuchTableError(identifier)
                return Table()

        self.exists = True
        self.snapshot = True
        self.any_snapshot = True
        self.summary: dict[str, Any] | None = {"total-records": "143740"}
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
    def test_a_table_with_no_snapshot_at_all_held_nothing(self) -> None:
        self.snapshot = False
        self.any_snapshot = False
        assert raw_table_record_count("raw_db", ENROLLMENTS, JOB_STARTED_AT) == 0

    @pytest.mark.usefixtures("catalog")
    def test_a_table_whose_snapshot_at_the_start_is_gone_is_unknown(self) -> None:
        """Maintenance can expire it once an overwrite replaces it."""
        self.snapshot = False
        with pytest.raises(RecordCountUnknownError, match="is retained"):
            raw_table_record_count("raw_db", ENROLLMENTS, JOB_STARTED_AT)

    @pytest.mark.usefixtures("catalog")
    def test_a_table_that_does_not_exist_is_unknown(self) -> None:
        """Counted as empty, a wrong database or table name passed every sync."""
        self.exists = False
        with pytest.raises(RecordCountUnknownError) as raised:
            raw_table_record_count("raw_db", ENROLLMENTS, JOB_STARTED_AT)
        assert raised.value.table == f"raw_db.{ENROLLMENTS.path[-1]}"

    @pytest.mark.usefixtures("catalog")
    @pytest.mark.parametrize("summary", [None, {}, {"total-records": None}])
    def test_a_snapshot_without_a_total_is_unknown(self, summary) -> None:
        self.summary = summary
        with pytest.raises(RecordCountUnknownError, match="no total-records"):
            raw_table_record_count("raw_db", ENROLLMENTS, JOB_STARTED_AT)


def _assert_unknown(result) -> None:
    assert result is not None
    assert not result.passed
    assert result.severity == AssetCheckSeverity.WARN
    assert result.metadata["outcome"].value == "unknown"


class TestEmptyFullRefreshCheck:
    def test_an_empty_sync_into_a_table_with_rows_fails(self) -> None:
        result = empty_full_refresh_check(
            _stream("full_refresh", 0), _Counter(143740), LOG
        )
        assert result is not None
        assert not result.passed
        assert result.severity == AssetCheckSeverity.ERROR
        assert "60322" in (result.description or "")

    def test_an_empty_sync_into_an_empty_table_passes(self) -> None:
        """A source with nothing in it, e.g. MITx's assessment_aiclassifier."""
        result = empty_full_refresh_check(_stream("full_refresh", 0), _Counter(0), LOG)
        assert result is not None
        assert result.passed

    def test_a_sync_that_read_rows_passes_without_reading_the_table(self) -> None:
        counter = _Counter(143740)
        result = empty_full_refresh_check(_stream("full_refresh", 71870), counter, LOG)
        assert result is not None
        assert result.passed
        assert counter.looked_up == []

    def test_an_incremental_stream_is_not_evaluated(self) -> None:
        """No new rows since the last cursor is the ordinary incremental sync."""
        counter = _Counter(143740)
        assert empty_full_refresh_check(_stream("incremental", 0), counter, LOG) is None
        assert counter.looked_up == []

    def test_an_unknown_record_count_is_a_warning(self) -> None:
        """Missing stats must not read as an empty sync of every stream."""
        counter = _Counter(143740)
        _assert_unknown(
            empty_full_refresh_check(_stream("full_refresh", None), counter, LOG)
        )
        assert counter.looked_up == []

    def test_a_job_without_a_start_time_is_a_warning(self) -> None:
        _assert_unknown(
            empty_full_refresh_check(
                _stream("full_refresh", 0, job_started_at=None), _Counter(143740), LOG
            )
        )

    def test_a_missing_table_is_a_warning_naming_it(self) -> None:
        error = RecordCountUnknownError("raw_db.raw__x", "not found in Glue")
        result = empty_full_refresh_check(
            _stream("full_refresh", 0), _Counter(error), LOG
        )
        _assert_unknown(result)
        assert result is not None
        assert result.metadata["table"].value == "raw_db.raw__x"

    def test_a_failed_table_lookup_is_a_warning(self) -> None:
        result = empty_full_refresh_check(
            _stream("full_refresh", 0), _Counter(OSError("S3 timed out")), LOG
        )
        _assert_unknown(result)


def _catalog_stream(
    name: str, sync_mode: str, namespace: str = "mit_xpro_api"
) -> dict[str, Any]:
    return {
        "stream": {
            "name": name,
            "namespace": namespace,
            "jsonSchema": {"properties": {}},
        },
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

BATCHES = AssetKey(f"{PREFIX}api_batches")
EVENTS = AssetKey(f"{PREFIX}api_events")
ALL_KEYS = {ENROLLMENTS, BATCHES, EVENTS}


def _attempt(
    attempt_id: int,
    status: str = "succeeded",
    total: int | None = None,
    streams: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one ``attempts[]`` entry of a ``jobs/get_without_logs`` response."""
    attempt: dict[str, Any] = {"id": attempt_id, "status": status}
    if total is not None:
        attempt["totalStats"] = {"recordsEmitted": total}
    if streams is not None:
        attempt["streamStats"] = [
            {"streamName": name, "stats": {"recordsEmitted": emitted}}
            for name, emitted in streams.items()
        ]
    return {"attempt": attempt}


@pytest.fixture
def airbyte() -> dict[str, Any]:
    """Set what the stubbed Airbyte answers; a test changes it before syncing.

    By default the sync reads 5 rows from api_batches and nothing from the
    other two streams, the way job 60322 read nothing from api_enrollments.
    """
    return {
        "streams": list(CATALOG_STREAMS),
        "job_type": "sync",
        "rows_synced": 5,
        "attempts": [
            _attempt(
                0,
                total=5,
                streams={"api_enrollments": 0, "api_batches": 5, "api_events": 0},
            )
        ],
        "stats": [
            {"streamName": "api_enrollments", "recordsEmitted": 0},
            {"streamName": "api_batches", "recordsEmitted": 5},
            {"streamName": "api_events", "recordsEmitted": 0},
        ],
    }


@pytest.fixture
def workspace(monkeypatch, airbyte) -> AirbyteOSSWorkspace:
    """Build a workspace whose one connection syncs without HTTP."""
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

    def sync_and_poll(*_, **__) -> AirbyteOutput:
        return AirbyteOutput(
            job_details={
                "jobId": 60322,
                "status": "succeeded",
                "startTime": "2026-10-19T00:00:12Z",
                "jobType": airbyte["job_type"],
                "rowsSynced": airbyte["rows_synced"],
            },
            connection_details={
                "connectionId": "conn-1",
                "name": CONNECTION_NAME,
                "prefix": PREFIX,
                "destinationId": "dest-1",
                "syncCatalog": {"streams": airbyte["streams"]},
            },
        )

    def get_job_stats(*_, **__) -> JobStats:
        if isinstance(airbyte["stats"], Exception):
            raise airbyte["stats"]
        return JobStats.from_job_info(
            {
                "job": {"id": 60322, "streamAggregatedStats": airbyte["stats"]},
                "attempts": airbyte["attempts"],
            }
        )

    monkeypatch.setattr(AirbyteOSSWorkspace, "load_asset_specs", lambda *_, **__: specs)
    monkeypatch.setattr(AirbyteOSSClient, "sync_and_poll", sync_and_poll)
    monkeypatch.setattr(AirbyteOSSClient, "get_job_stats", get_job_stats)
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


def _sync(workspace: AirbyteOSSWorkspace, counter: _Counter, **kwargs):
    result = materialize(
        _build(workspace, counter), resources={"airbyte": workspace}, **kwargs
    )
    evaluations = {
        evaluation.asset_key: evaluation
        for evaluation in result.get_asset_check_evaluations()
    }
    materialized = {
        event.asset_key for event in result.get_asset_materialization_events()
    }
    return result, evaluations, materialized


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
        assert assets_def.keys == library_def.keys == ALL_KEYS

    def test_every_stream_carries_the_check(self, workspace) -> None:
        """The sync mode is only known once the sync has run."""
        (assets_def,) = _build(workspace, _Counter(0))
        assert {key.asset_key for key in assets_def.check_keys} == ALL_KEYS
        assert {key.name for key in assets_def.check_keys} == {EMPTY_FULL_REFRESH_CHECK}

    def test_the_sync_fails_the_empty_full_refresh_stream_only(self, workspace) -> None:
        counter = _Counter(143740)
        result, evaluations, _ = _sync(workspace, counter)

        assert result.success
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
        assert set(metadata) == ALL_KEYS
        assert metadata[BATCHES]["airbyte_records_emitted"].value == 5
        assert metadata[ENROLLMENTS]["airbyte_records_emitted"].value == 0
        assert metadata[EVENTS]["airbyte_sync_mode"].value == "incremental"
        assert metadata[ENROLLMENTS]["airbyte_job_id"].value == 60322

    def test_a_failed_stats_lookup_still_materializes_every_stream(
        self, workspace, airbyte
    ) -> None:
        """The sync succeeded, so staging must still run after it."""
        airbyte["stats"] = Failure("Max retries (4) exceeded")
        result, evaluations, materialized = _sync(workspace, _Counter(143740))

        assert result.success
        assert materialized == ALL_KEYS
        assert {key: e.severity for key, e in evaluations.items()} == {
            ENROLLMENTS: AssetCheckSeverity.WARN,
            BATCHES: AssetCheckSeverity.WARN,
        }

    def test_a_failed_table_lookup_still_materializes_every_stream(
        self, workspace
    ) -> None:
        result, evaluations, materialized = _sync(
            workspace, _Counter(OSError("Glue throttled"))
        )

        assert result.success
        assert materialized == ALL_KEYS
        assert evaluations[ENROLLMENTS].severity == AssetCheckSeverity.WARN
        assert evaluations[BATCHES].passed

    @pytest.mark.parametrize("stats", [None, []])
    def test_stats_missing_with_rows_synced_raise_no_error(
        self, workspace, airbyte, stats
    ) -> None:
        """Read as zeros, a reshaped response failed every stream with rows."""
        airbyte["stats"] = stats
        airbyte["attempts"] = [_attempt(0, total=5)]
        result, evaluations, _ = _sync(workspace, _Counter(143740))

        assert result.success
        assert all(
            e.severity == AssetCheckSeverity.WARN and not e.passed
            for e in evaluations.values()
        )

    def test_a_null_record_count_is_unknown(self, workspace, airbyte) -> None:
        airbyte["stats"][0]["recordsEmitted"] = None
        airbyte["attempts"] = [
            _attempt(0, total=5, streams={"api_enrollments": None, "api_batches": 5})
        ]
        _, evaluations, _ = _sync(workspace, _Counter(143740))
        assert evaluations[ENROLLMENTS].severity == AssetCheckSeverity.WARN

    def test_rows_synced_zero_with_no_stats_is_not_an_empty_sync(
        self, workspace, airbyte
    ) -> None:
        """Missing attempt stats hydrate as [] and rowsSynced 0, not as null."""
        airbyte["rows_synced"] = 0
        airbyte["stats"] = []
        airbyte["attempts"] = [{"attempt": {"id": 0, "status": "succeeded"}}]
        _, evaluations, _ = _sync(workspace, _Counter(143740))
        assert {e.severity for e in evaluations.values()} == {AssetCheckSeverity.WARN}

    def test_a_single_attempt_that_emitted_nothing_fails_without_stream_stats(
        self, workspace, airbyte
    ) -> None:
        """Global Alumni's shape: one attempt whose own summary totals 0."""
        airbyte["rows_synced"] = 0
        airbyte["stats"] = []
        airbyte["attempts"] = [_attempt(0, total=0)]
        _, evaluations, _ = _sync(workspace, _Counter(143740))
        assert not evaluations[ENROLLMENTS].passed
        assert evaluations[ENROLLMENTS].severity == AssetCheckSeverity.ERROR

    def test_an_earlier_attempt_that_read_rows_leaves_it_unknown(
        self, workspace, airbyte
    ) -> None:
        """A resumed refresh's last attempt can read 0 after another read rows."""
        airbyte["rows_synced"] = 0
        airbyte["stats"] = []
        airbyte["attempts"] = [_attempt(0, "failed", total=7), _attempt(1, total=0)]
        _, evaluations, _ = _sync(workspace, _Counter(143740))
        assert evaluations[ENROLLMENTS].severity == AssetCheckSeverity.WARN

    def test_an_aggregate_zero_without_attempt_evidence_is_unknown(
        self, workspace, airbyte
    ) -> None:
        """Airbyte adds a null attempt count into the aggregate as 0."""
        airbyte["attempts"] = [_attempt(0, total=5)]
        _, evaluations, _ = _sync(workspace, _Counter(143740))
        assert evaluations[ENROLLMENTS].severity == AssetCheckSeverity.WARN
        assert evaluations[BATCHES].passed

    @pytest.mark.parametrize("job_type", ["reset", "clear", "refresh", None])
    def test_a_job_that_is_not_a_sync_is_never_an_error(
        self, workspace, airbyte, job_type
    ) -> None:
        """poll_previous_running_sync attaches to resets and clears too."""
        airbyte["job_type"] = job_type
        airbyte["attempts"] = [
            {
                "attempt": {
                    "id": 0,
                    "status": "succeeded",
                    "totalStats": {"recordsEmitted": 0},
                }
            }
        ]
        result, evaluations, materialized = _sync(workspace, _Counter(143740))
        assert result.success
        assert materialized == ALL_KEYS
        assert {e.severity for e in evaluations.values()} == {AssetCheckSeverity.WARN}

    def test_one_name_in_two_namespaces_is_one_asset(self, workspace, airbyte) -> None:
        """Yielding the name twice raised "output returned multiple times"."""
        airbyte["streams"] = [
            *CATALOG_STREAMS,
            _catalog_stream("api_enrollments", "full_refresh", namespace="other"),
        ]
        result, _, materialized = _sync(workspace, _Counter(0))
        assert result.success
        assert materialized == ALL_KEYS

    def test_selecting_only_the_checks_evaluates_them(self, workspace) -> None:
        (assets_def,) = _build(workspace, _Counter(143740))
        result = materialize(
            [assets_def],
            resources={"airbyte": workspace},
            selection=AssetSelection.checks(*assets_def.check_keys),
        )
        evaluations = {e.asset_key for e in result.get_asset_check_evaluations()}
        assert result.success
        assert evaluations == {ENROLLMENTS, BATCHES}
