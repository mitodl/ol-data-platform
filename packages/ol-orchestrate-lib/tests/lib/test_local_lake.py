"""Tests for reaching the local lake from a host process."""

import os
from pathlib import Path

import pytest
from ol_orchestrate.lib import local_lake
from ol_orchestrate.lib.local_lake import (
    HostEndpointRestCatalog,
    get_local_lake_catalog,
    local_lake_database,
)
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io import S3_ENDPOINT

IN_CLUSTER_ENDPOINT = "http://rustfs.local-infra.svc.cluster.local:9000"


@pytest.fixture(autouse=True)
def no_local_pyiceberg_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Hide the developer's own catalog entry and overrides from pyiceberg."""
    for name in list(os.environ):
        if name.startswith("PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("PYICEBERG_HOME", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def offline_rest_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep RestCatalog from fetching /v1/config when it is constructed."""

    def _init(self: RestCatalog, name: str, **properties: object) -> None:
        self.name = name
        self.properties = dict(properties)

    monkeypatch.setattr(RestCatalog, "__init__", _init)


@pytest.mark.parametrize("env", ["qa", "production"])
def test_a_deployed_database_maps_to_the_local_schema_of_its_layer(env: str) -> None:
    assert (
        local_lake_database(f"ol_warehouse_{env}_integrations")
        == "ol_warehouse_local_integrations"
    )


def test_a_database_with_no_layer_is_refused() -> None:
    with pytest.raises(ValueError, match="no counterpart"):
        local_lake_database("b2b_analytics")


@pytest.mark.usefixtures("offline_rest_catalog")
def test_table_io_keeps_the_host_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """The endpoint a table response names does not replace the client's."""
    seen: dict[str, object] = {}

    def _load_file_io(
        _self: RestCatalog,
        properties: dict[str, object],
        _location: str | None = None,
    ) -> None:
        seen.update(properties)

    monkeypatch.setattr(RestCatalog, "_load_file_io", _load_file_io)
    get_local_lake_catalog()._load_file_io(
        {S3_ENDPOINT: IN_CLUSTER_ENDPOINT, "s3.region": "us-east-1"}
    )

    assert seen[S3_ENDPOINT] == "http://localhost:9000"
    assert seen["s3.region"] == "us-east-1"


@pytest.mark.usefixtures("offline_rest_catalog")
def test_an_environment_override_moves_the_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A moved port-forward is followed through pyiceberg's own variables."""
    monkeypatch.setenv(
        "PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__S3__ENDPOINT", "http://localhost:9100"
    )

    catalog = get_local_lake_catalog()

    assert isinstance(catalog, HostEndpointRestCatalog)
    assert catalog.properties[S3_ENDPOINT] == "http://localhost:9100"
    assert catalog.properties["uri"] == local_lake._DEFAULT_PROPERTIES["uri"]
