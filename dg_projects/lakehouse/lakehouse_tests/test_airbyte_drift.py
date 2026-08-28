"""Tests for the Airbyte inventory drift asset.

The comparison itself is tested in `src/ol_dbt_cli/tests/test_inventory_drift.py`.
What is worth testing here is the part that only exists in Dagster: that the
raw workspace read is adapted into the shape the check expects, that a failed
read is refused rather than reported as mass drift, and that an ERROR fails the
run while a warning does not.
"""

from pathlib import Path
from typing import Any

import pytest
from dagster import Failure, build_asset_context
from lakehouse.assets.airbyte_drift import _fetch_workspace, airbyte_inventory_drift

PREFIX = "raw__mitxonline__openedx__mysql__"
CONNECTION_NAME = "MITx Online Open edX DB → S3 Data Lake"

UNIT = f"""
schema_version: 1
deployment: mitxonline
layer: mysql
scope: scoped
strategies:
  qa: omit
  local: fixture
loader: airbyte
table_prefix: {PREFIX}
airbyte:
  source_kind: source-mysql
  replication_method: cursor
  connections:
  - name: {CONNECTION_NAME}
    status: active
    sync_interval_hours: 12
    streams:
    - auth_user
tables:
- name: auth_user
  raw_table: {PREFIX}auth_user
  sync_mode: incremental_append
  cursor_field:
  - id
  modeled: true
"""


class FakeClient:
    """Stands in for AirbyteOSSWorkspace.get_client()."""

    rest_api_base_url = "https://airbyte.example.invalid/api/public/v1"

    def __init__(
        self,
        connections: list[dict[str, Any]],
        sources: list[dict[str, Any]],
        detail: dict[str, Any] | None = None,
    ) -> None:
        self._connections = connections
        self._sources = sources
        self._detail = detail
        self.detail_calls = 0

    # Both are called with keyword arguments, so the ones this fake ignores are
    # absorbed rather than named and silenced.
    def _paginated_request(self, url: str, **_: Any) -> list[dict[str, Any]]:
        return self._sources if url.endswith("/sources") else self._connections

    def _single_request(self, **_: Any) -> dict[str, Any]:
        self.detail_calls += 1
        return self._detail or {}


class FakeWorkspace:
    workspace_id = "workspace-1"
    request_page_size = 15

    def __init__(self, client: FakeClient) -> None:
        self._client = client

    def get_client(self) -> FakeClient:
        return self._client


def connection(**overrides: Any) -> dict[str, Any]:
    base = {
        "connectionId": "conn-1",
        "name": CONNECTION_NAME,
        "status": "active",
        "prefix": PREFIX,
        "sourceId": "src-1",
        "schedule": {"scheduleType": "manual"},
        "configurations": {
            "streams": [
                {
                    "name": "auth_user",
                    "syncMode": "incremental_append",
                    "cursorField": ["id"],
                    "primaryKey": [],
                }
            ]
        },
    }
    base.update(overrides)
    return base


SOURCE = {
    "sourceId": "src-1",
    "sourceType": "mysql",
    "configuration": {"replication_method": {"method": "STANDARD"}},
}


@pytest.fixture
def inventory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "inventory"
    (root / "units").mkdir(parents=True)
    (root / "units" / "mitxonline__mysql.yml").write_text(UNIT)
    monkeypatch.setattr("lakehouse.assets.airbyte_drift.INVENTORY_DIR", root)
    return root


def _materialise(client: FakeClient) -> Any:
    return airbyte_inventory_drift(build_asset_context(), FakeWorkspace(client))


class TestFetchAdaptation:
    def test_streams_missing_from_the_list_response_are_re_fetched(self) -> None:
        # Mirrors bin/airbyte-inventory.py: some server versions omit stream
        # configs from the list response.
        bare = connection(configurations={})
        detail = {
            "configurations": {
                "streams": [{"name": "auth_user", "syncMode": "incremental_append"}]
            }
        }
        client = FakeClient([bare], [SOURCE], detail=detail)
        fetched = _fetch_workspace(FakeWorkspace(client))

        assert client.detail_calls == 1
        assert (
            fetched["connections"][0]["configurations"]["streams"][0]["name"]
            == "auth_user"
        )

    def test_a_complete_list_response_is_not_re_fetched(self) -> None:
        client = FakeClient([connection()], [SOURCE])
        _fetch_workspace(FakeWorkspace(client))
        assert client.detail_calls == 0


class TestRefusals:
    @pytest.mark.usefixtures("inventory")
    def test_an_empty_read_is_refused_rather_than_reported_as_drift(self) -> None:
        # Every declared connection would otherwise be reported as deleted,
        # which is a page-worthy alarm produced by a failed read.
        with pytest.raises(
            Failure, match="Refusing to report drift against an empty read"
        ):
            _materialise(FakeClient([], [SOURCE]))

    def test_an_empty_inventory_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        empty = tmp_path / "inventory"
        (empty / "units").mkdir(parents=True)
        monkeypatch.setattr("lakehouse.assets.airbyte_drift.INVENTORY_DIR", empty)
        with pytest.raises(Failure, match="No inventory units"):
            _materialise(FakeClient([connection()], [SOURCE]))


class TestOutcome:
    @pytest.mark.usefixtures("inventory")
    def test_agreement_returns_counts_and_does_not_fail(self) -> None:
        result = _materialise(FakeClient([connection()], [SOURCE]))
        assert result.value == {
            "live_connections": 1,
            "units": 1,
            "errors": 0,
            "warnings": 0,
        }

    @pytest.mark.usefixtures("inventory")
    def test_an_error_fails_the_run(self) -> None:
        # A paused connection the inventory declares active: the run failing is
        # the report, since the existing run-failure sensor routes it onward.
        with pytest.raises(Failure, match="no longer describes"):
            _materialise(FakeClient([connection(status="inactive")], [SOURCE]))

    @pytest.mark.usefixtures("inventory")
    def test_a_warning_alone_does_not_fail(self) -> None:
        # An undeclared connection is usually config nobody deleted. Paging on
        # it would train people to ignore this.
        extra = connection(
            connectionId="conn-2",
            name="Something From The UI",
            configurations={"streams": []},
        )
        result = _materialise(FakeClient([connection(), extra], [SOURCE]))
        assert result.value["errors"] == 0
        assert result.value["warnings"] == 1
