"""Tests for the local lake catalog a host-run ``dev`` pipeline uses."""

from typing import Any

import pytest
from dlt.common.libs import pyiceberg as dlt_pyiceberg
from pyiceberg.catalog.rest import RestCatalog

from ol_dlt import config, local_lake

HOST_ENDPOINT = "http://localhost:9000"
CLUSTER_ENDPOINT = "http://rustfs.local-infra.svc.cluster.local:9000"


def _serve_cluster_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``/v1/config`` answer as Gravitino does, with no network."""

    def fetch_config(self: RestCatalog) -> None:
        self.properties = {**self.properties, "s3.endpoint": CLUSTER_ENDPOINT}

    monkeypatch.setattr(RestCatalog, "_fetch_config", fetch_config)


def test_dlt_loads_the_host_endpoint_catalog_for_the_local_lake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _serve_cluster_endpoint(monkeypatch)
    catalog = dlt_pyiceberg.get_catalog(config.LOCAL_LAKE_CATALOG)
    assert isinstance(catalog, local_lake.HostEndpointRestCatalog)
    assert catalog.uri == "http://localhost:9001/iceberg"


def test_other_catalogs_go_to_dlts_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        local_lake, "_dlt_load_catalog_from_pyiceberg", lambda name: f"dlt:{name}"
    )
    assert local_lake.load_catalog_from_pyiceberg("aws_glue") == "dlt:aws_glue"


@pytest.mark.parametrize(
    "table_config",
    [{}, {"s3.endpoint": CLUSTER_ENDPOINT}],
    ids=["server-override-in-config", "endpoint-in-table-response"],
)
def test_table_io_keeps_the_host_endpoint(
    monkeypatch: pytest.MonkeyPatch, table_config: dict[str, Any]
) -> None:
    """Neither place the server names its endpoint reaches the table's IO."""
    _serve_cluster_endpoint(monkeypatch)
    catalog = local_lake.load_catalog_from_pyiceberg(config.LOCAL_LAKE_CATALOG)
    assert catalog.properties["s3.endpoint"] == CLUSTER_ENDPOINT
    io = catalog._load_file_io(table_config, "s3://ol-data-lake-local/raw/t")  # noqa: SLF001
    assert io.properties["s3.endpoint"] == HOST_ENDPOINT


def test_a_plain_rest_catalog_takes_the_server_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The behaviour the subclass exists for; when this fails, delete it."""
    _serve_cluster_endpoint(monkeypatch)
    catalog = RestCatalog(
        "plain", uri="http://localhost:9001/iceberg", **{"s3.endpoint": HOST_ENDPOINT}
    )
    io = catalog._load_file_io({}, "s3://ol-data-lake-local/raw/t")  # noqa: SLF001
    assert io.properties["s3.endpoint"] == CLUSTER_ENDPOINT


def test_object_store_credentials_follow_the_catalog_entry() -> None:
    credentials = local_lake.object_store_credentials()
    assert credentials.endpoint_url == HOST_ENDPOINT
    assert credentials.region_name == "us-east-1"
    assert credentials.aws_secret_access_key == "localdevsecret123"  # noqa: S105  # pragma: allowlist secret


def test_a_missing_catalog_entry_names_the_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "LOCAL_LAKE_CATALOG", "not_configured")
    with pytest.raises(ValueError, match="PYICEBERG_HOME"):
        local_lake.catalog_properties()
