"""Reading dbt models from the local lake of ol-infrastructure/local-dev.

The local lake is RustFS behind Gravitino's Iceberg REST catalog. ``ol-dbt
starrocks build --env dev`` writes its models there as
``ol_warehouse_local_<layer>``, where a deployed environment has
``ol_warehouse_<env>_<layer>`` in Glue.

Gravitino names its in-cluster object store endpoint in the ``config`` of every
table response, and pyiceberg builds a table's IO from that over whatever the
client configured. The name resolves only inside the cluster, so a process on
the host would load a table and then fail to read its first file.
``HostEndpointRestCatalog`` keeps this client's endpoint, which is the same
object store through a port-forward. ``ol_dlt.local_lake`` does the same for
the writing side; the two packages share no dependency to hold one copy.
"""

import re
import types
from typing import Any

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io import S3_ENDPOINT, FileIO
from pyiceberg.typedef import EMPTY_DICT, Properties
from pyiceberg.utils.config import Config

# The same name as the entry in src/ol_dlt/.dlt/.pyiceberg.yaml, so one set of
# PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__* variables moves both.
LOCAL_LAKE_CATALOG = "ol-data-lake-local"

# The addresses are the Tilt port-forwards of local-dev and the keys are the
# fixed pair from local-dev/infra/modules/objectstore.py.
_DEFAULT_PROPERTIES = types.MappingProxyType(
    {
        "uri": "http://localhost:9001/iceberg",
        # Local Gravitino runs the `simple` authenticator, which answers 401 to
        # the Bearer header pyiceberg sends by default.
        "auth": {"type": "noop"},
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        S3_ENDPOINT: "http://localhost:9000",
        "s3.access-key-id": "localdevaccess",
        "s3.secret-access-key": "localdevsecret123",  # pragma: allowlist secret
        "s3.region": "us-east-1",
    }
)

_DEPLOYED_DATABASE = re.compile(r"ol_warehouse_(?:qa|production)_(?P<layer>\w+)")


class HostEndpointRestCatalog(RestCatalog):
    """REST catalog whose table IO uses the endpoint this client configured."""

    def __init__(self, name: str, **properties: Any) -> None:
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


def get_local_lake_catalog() -> HostEndpointRestCatalog:
    """Return the local lake's catalog as a host process reaches it.

    :returns: The catalog, with any ``PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__*``
        variable or ``.pyiceberg.yaml`` entry of that name applied over the
        local-dev defaults.
    :rtype: HostEndpointRestCatalog
    """
    overrides = Config().get_catalog_config(LOCAL_LAKE_CATALOG) or {}
    return HostEndpointRestCatalog(
        LOCAL_LAKE_CATALOG, **{**_DEFAULT_PROPERTIES, **overrides}
    )


def local_lake_database(database_name: str) -> str:
    """Return the local lake's schema for a deployed warehouse database.

    :param database_name: A Glue database, ``ol_warehouse_<env>_<layer>``.
    :returns: ``ol_warehouse_local_<layer>``. Everything after the environment
        is kept, so a database with a ``DBT_SCHEMA_SUFFIX`` in its name maps to
        a schema no local build writes and the table load fails on it.
    :rtype: str
    :raises ValueError: When the name is not a deployed warehouse database, so
        there is no layer to carry over.
    """
    match = _DEPLOYED_DATABASE.fullmatch(database_name)
    if match is None:
        msg = (
            f"{database_name} is not an ol_warehouse_<qa|production>_<layer> "
            "database, so it has no counterpart in the local lake."
        )
        raise ValueError(msg)
    return f"ol_warehouse_local_{match['layer']}"
