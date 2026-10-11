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

dlt has no way to give one destination its own catalog: the catalog name is
process-wide configuration, which ``.dlt/config.toml`` sets to the Glue one,
and pyiceberg's ``py-catalog-impl`` is read only from ``load_catalog``'s keyword
arguments, never from a ``.pyiceberg.yaml`` entry. dlt does pass the
destination's credentials to ``get_catalog``, so this module wraps that
function and answers with the local lake's catalog for the one destination
whose credentials name the local lake's object store. Every other call goes to dlt
untouched, whatever profile the process started under.

``ol-dbt fixtures load`` writes the same raw schema, with the column types the
deployed tables have today. Where those are the types Airbyte landed
(``timestamp`` and ``double`` for a source's ``timestamptz`` and ``numeric``),
pyiceberg refuses to evolve the fixture's table into the source's and the load
fails. ``evolve_table`` wraps dlt's function to drop a table of this lake that
no dlt load made, so the source creates its own.
"""

import logging
from typing import Any

from dlt.common.libs import pyiceberg as dlt_pyiceberg
from dlt.sources.credentials import AwsCredentials
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import S3_ENDPOINT, FileIO
from pyiceberg.typedef import EMPTY_DICT, Properties
from pyiceberg.utils.config import Config

from ol_dlt import config

logger = logging.getLogger(__name__)

_dlt_get_catalog = dlt_pyiceberg.get_catalog
_dlt_evolve_table = dlt_pyiceberg.evolve_table


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

    :returns: The ``ol-data-lake-local`` entry of ``.pyiceberg.yaml`` merged
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
    """Return the object store credentials for the local lake's destination.

    dlt writes its state and load records beside the tables with fsspec, not
    through the catalog, so it needs the endpoint and keys as well. They are
    read from the catalog entry so the two cannot disagree.

    :returns: Credentials pointing at the local object store.
    :rtype: AwsCredentials
    """
    return AwsCredentials.from_pyiceberg_fileio_config(catalog_properties())


def get_catalog(*args: Any, **kwargs: Any) -> Catalog:  # noqa: ANN401
    """Return the local lake's catalog for its destination, else dlt's own.

    The destination is recognised by its object store endpoint. dlt resolves
    the credentials it was given into a new object, so the endpoint is the one
    thing ``object_store_credentials`` set that still tells them apart: no
    other profile's destination sets one.
    """
    endpoint = getattr(kwargs.get("credentials"), "endpoint_url", None)
    if endpoint is not None:
        properties = catalog_properties()
        if endpoint == properties[S3_ENDPOINT]:
            return HostEndpointRestCatalog(config.LOCAL_LAKE_CATALOG, **properties)
    return _dlt_get_catalog(*args, **kwargs)


def evolve_table(**kwargs: Any) -> Any:  # noqa: ANN401
    """Open a table for a load, replacing one in the local lake no dlt load made.

    Every table a source here writes carries ``_dlt_load_id``, an empty one
    included, so a table without it came from ``ol-dbt fixtures load``. Only a
    load passes ``schema``, so reading a fixture's table leaves it alone.
    """
    catalog, table_id = kwargs["catalog"], kwargs["table_id"]
    if (
        isinstance(catalog, HostEndpointRestCatalog)
        and kwargs.get("schema") is not None
    ):
        try:
            columns = catalog.load_table(table_id).schema().column_names
        except NoSuchTableError:
            columns = None
        if columns is not None and not set(columns) & set(config.DLT_LOAD_ID_COLUMN):
            logger.warning(
                "Dropping %s from the local lake: it was not written by a dlt "
                "load (`ol-dbt fixtures load` makes such tables), and this load "
                "creates its own.",
                table_id,
            )
            catalog.drop_table(table_id)
    return _dlt_evolve_table(**kwargs)


# dlt's filesystem client imports both from this module at call time.
dlt_pyiceberg.get_catalog = get_catalog
dlt_pyiceberg.evolve_table = evolve_table
