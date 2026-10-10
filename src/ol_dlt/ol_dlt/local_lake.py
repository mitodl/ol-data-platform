"""The local lake of ol-infrastructure/local-dev, as a host process reaches it.

The lake is RustFS behind Gravitino's Iceberg REST catalog, and the ``dev``
profile writes raw tables into it so a source can be run against a local app
database with no Vault, VPN or AWS credentials.

Gravitino names its own object store endpoint,
``http://rustfs.local-infra.svc.cluster.local:9000``, in the ``config`` of every
table response, and pyiceberg builds a table's IO from that ``config`` over
whatever the client configured. (``/v1/config`` sends the endpoint too, but as
a default, which the client's value does replace.) The name resolves only
inside the cluster, so a pipeline on the host would create a table and then
fail to write its first file. ``HostEndpointRestCatalog`` keeps the endpoint
from this process's own catalog entry, which is the same object store through
a port-forward. Table locations are ``s3://`` URIs with no endpoint in them, so
what the host writes is what StarRocks reads.

pyiceberg can load a catalog class named by ``py-catalog-impl``, but
``load_catalog`` looks for that key only in its keyword arguments and never in
the ``.pyiceberg.yaml`` entry, and dlt calls ``load_catalog(name)`` with none.
So this module wraps dlt's loader and builds the local lake's catalog itself.
Every other catalog name goes to dlt's loader untouched.
"""

from typing import Any

from dlt.common.libs import pyiceberg as dlt_pyiceberg
from dlt.sources.credentials import AwsCredentials
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io import S3_ENDPOINT, FileIO
from pyiceberg.typedef import EMPTY_DICT, Properties
from pyiceberg.utils.config import Config

from ol_dlt import config

_dlt_load_catalog_from_pyiceberg = dlt_pyiceberg._load_catalog_from_pyiceberg  # noqa: SLF001


class HostEndpointRestCatalog(RestCatalog):
    """REST catalog whose table IO uses the endpoint this client configured."""

    def __init__(self, name: str, **properties: str) -> None:
        # Read before the parent constructor runs: it fetches /v1/config and
        # merges any override the server sends into self.properties.
        self._host_s3_endpoint = properties[S3_ENDPOINT]
        super().__init__(name, **properties)

    def _load_file_io(
        self, properties: Properties = EMPTY_DICT, location: str | None = None
    ) -> FileIO:
        return super()._load_file_io(
            {**properties, S3_ENDPOINT: self._host_s3_endpoint}, location
        )


def catalog_properties() -> dict[str, Any]:
    """Return the local lake's catalog entry, environment overrides applied.

    :returns: The ``ol_data_lake_local`` entry of ``.pyiceberg.yaml`` merged
        with any ``PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__*`` variables.
    :rtype: dict[str, Any]
    :raises ValueError: When pyiceberg finds no such entry, which means
        ``PYICEBERG_HOME`` points somewhere other than this project's ``.dlt``.
    """
    properties = Config().get_catalog_config(config.LOCAL_LAKE_CATALOG)
    if properties is None:
        msg = (
            f"No {config.LOCAL_LAKE_CATALOG} catalog in pyiceberg's configuration. "
            "PYICEBERG_HOME has to point at the directory holding ol_dlt's "
            ".pyiceberg.yaml."
        )
        raise ValueError(msg)
    return dict(properties)


def object_store_credentials() -> AwsCredentials:
    """Return the object store credentials for dlt's own files.

    dlt writes its state and load records beside the tables with fsspec, not
    through the catalog, so it needs the endpoint and keys as well. They are
    read from the catalog entry so the two cannot disagree.

    :returns: Credentials pointing at the local object store.
    :rtype: AwsCredentials
    """
    return AwsCredentials.from_pyiceberg_fileio_config(catalog_properties())


def load_catalog_from_pyiceberg(catalog_name: str) -> Catalog:
    """Load ``catalog_name`` as dlt does, except for the local lake's catalog."""
    if catalog_name == config.LOCAL_LAKE_CATALOG:
        return HostEndpointRestCatalog(catalog_name, **catalog_properties())
    return _dlt_load_catalog_from_pyiceberg(catalog_name)


# dlt's get_catalog resolves this name in its module globals at call time.
dlt_pyiceberg._load_catalog_from_pyiceberg = load_catalog_from_pyiceberg  # noqa: SLF001
