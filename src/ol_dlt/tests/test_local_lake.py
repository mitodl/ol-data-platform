"""Tests for the local lake catalog a host-run ``dev`` pipeline uses."""

from typing import Any

import dlt
import pytest
from dlt.common.libs import pyiceberg as dlt_pyiceberg
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError

from ol_dlt import config, local_lake

HOST_ENDPOINT = "http://localhost:9000"
CLUSTER_ENDPOINT = "http://rustfs.local-infra.svc.cluster.local:9000"
# What Gravitino puts in the ``config`` of a table response.
TABLE_CONFIG = {"s3.endpoint": CLUSTER_ENDPOINT}
LOCATION = "s3://ol-data-lake-local/raw/t"


@pytest.fixture(autouse=True)
def _committed_catalog_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Read the committed entry, whatever overrides the developer has exported."""
    for name in (
        "PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__URI",
        "PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__S3__ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def offline_catalog(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Let a REST catalog be built with no server; record namespaces it creates."""
    created: list[str] = []
    monkeypatch.setattr(RestCatalog, "_fetch_config", lambda _self: None)
    monkeypatch.setattr(
        RestCatalog,
        "create_namespace",
        lambda _self, namespace, properties=None: created.append(namespace),  # noqa: ARG005
    )
    return created


def _open_table_catalog(profile: str, tmp_path: Any) -> Any:  # noqa: ANN401
    pipeline = dlt.pipeline(
        pipeline_name=f"catalog_{profile}",
        pipelines_dir=str(tmp_path),
        destination=config.destination_for("oll", profile),
        dataset_name=config.dataset_name(profile),
    )
    with pipeline.destination_client() as client:
        return client.get_open_table_catalog("iceberg")


def test_dlt_opens_the_local_lake_catalog_for_the_dev_destination(
    offline_catalog: list[str], tmp_path: Any
) -> None:
    """Through dlt's own filesystem client, which is what a load calls."""
    catalog = _open_table_catalog("dev", tmp_path)
    assert isinstance(catalog, local_lake.HostEndpointRestCatalog)
    assert catalog.uri == "http://localhost:9001/iceberg"
    assert offline_catalog == ["ol_warehouse_local_raw"]


def test_a_dev_destination_does_not_change_the_catalog_of_another(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """A deployed destination built in the same process still gets dlt's catalog."""
    config.destination_for("oll", "dev")
    calls: list[dict[str, Any]] = []

    class _Catalog:
        name = "aws_glue"

        def create_namespace(self, *_args: Any, **_kwargs: Any) -> None:
            pass

    def dlt_get_catalog(*_args: Any, **kwargs: Any) -> _Catalog:
        calls.append(kwargs)
        return _Catalog()

    monkeypatch.setattr(local_lake, "_dlt_get_catalog", dlt_get_catalog)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")  # pragma: allowlist secret
    catalog = _open_table_catalog("production", tmp_path)
    assert isinstance(catalog, _Catalog)
    assert len(calls) == 1


def test_the_wrapper_is_what_dlt_calls() -> None:
    assert dlt_pyiceberg.get_catalog is local_lake.get_catalog


def test_table_io_keeps_the_host_endpoint(offline_catalog: list[str]) -> None:
    del offline_catalog
    catalog = local_lake.get_catalog(credentials=local_lake.object_store_credentials())
    io = catalog._load_file_io(TABLE_CONFIG, LOCATION)  # noqa: SLF001
    assert io.properties["s3.endpoint"] == HOST_ENDPOINT


def test_a_plain_rest_catalog_takes_the_table_response_endpoint(
    offline_catalog: list[str],
) -> None:
    """The behaviour the subclass exists for; when this fails, delete it."""
    del offline_catalog
    catalog = RestCatalog(
        "plain", uri="http://localhost:9001/iceberg", **{"s3.endpoint": HOST_ENDPOINT}
    )
    io = catalog._load_file_io(TABLE_CONFIG, LOCATION)  # noqa: SLF001
    assert io.properties["s3.endpoint"] == CLUSTER_ENDPOINT


class _Table:
    def __init__(self, *columns: str) -> None:
        self._columns = columns

    def schema(self) -> Any:  # noqa: ANN401
        return type("Schema", (), {"column_names": list(self._columns)})


@pytest.fixture
def lake_tables(
    offline_catalog: list[str], monkeypatch: pytest.MonkeyPatch
) -> dict[str, Any]:
    """A local lake catalog holding these tables, and the calls made on it."""
    del offline_catalog
    tables: dict[str, _Table] = {}
    calls: dict[str, Any] = {"tables": tables, "dropped": [], "evolved": []}

    def load_table(_self: Any, table_id: str) -> _Table:  # noqa: ANN401
        if table_id not in tables:
            raise NoSuchTableError(table_id)
        return tables[table_id]

    monkeypatch.setattr(local_lake.HostEndpointRestCatalog, "load_table", load_table)
    monkeypatch.setattr(
        local_lake.HostEndpointRestCatalog,
        "drop_table",
        lambda _self, table_id: calls["dropped"].append(table_id),
    )
    monkeypatch.setattr(
        local_lake,
        "_dlt_evolve_table",
        lambda **kwargs: calls["evolved"].append(kwargs["table_id"]),
    )
    calls["catalog"] = local_lake.get_catalog(
        credentials=local_lake.object_store_credentials()
    )
    return calls


def test_dlt_calls_the_evolve_wrapper() -> None:
    assert dlt_pyiceberg.evolve_table is local_lake.evolve_table


@pytest.mark.parametrize(
    ("columns", "schema", "dropped"),
    [
        pytest.param(("id", "title"), object(), ["raw.t"], id="fixture-table-loaded"),
        pytest.param(("id", "_dlt_load_id"), object(), [], id="dlt-table-loaded"),
        pytest.param(
            ("value", "_dlt_parent_id", "_dlt_list_idx", "_dlt_id"),
            object(),
            [],
            id="dlt-nested-table-loaded",
        ),
        pytest.param(("id", "title"), None, [], id="fixture-table-read"),
        pytest.param(None, object(), [], id="no-table"),
    ],
)
def test_a_load_replaces_only_a_table_no_dlt_load_made(
    lake_tables: dict[str, Any],
    columns: tuple[str, ...] | None,
    schema: object,
    dropped: list[str],
) -> None:
    if columns is not None:
        lake_tables["tables"]["raw.t"] = _Table(*columns)
    local_lake.evolve_table(
        catalog=lake_tables["catalog"], table_id="raw.t", schema=schema
    )
    assert lake_tables["dropped"] == dropped
    assert lake_tables["evolved"] == ["raw.t"]


def test_a_load_through_another_catalog_drops_nothing(
    lake_tables: dict[str, Any],
) -> None:
    lake_tables["tables"]["raw.t"] = _Table("id")
    local_lake.evolve_table(catalog=object(), table_id="raw.t", schema=object())
    assert lake_tables["dropped"] == []
    assert lake_tables["evolved"] == ["raw.t"]


def test_a_positional_call_goes_to_dlt_untouched(
    lake_tables: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """dlt calls with keywords today; a release that does not must still load."""
    seen: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        local_lake, "_dlt_evolve_table", lambda *args, **_kwargs: seen.append(args)
    )
    local_lake.evolve_table(lake_tables["catalog"], None, None, "raw.t", "s3://x")
    assert len(seen) == 1
    assert lake_tables["dropped"] == []


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
