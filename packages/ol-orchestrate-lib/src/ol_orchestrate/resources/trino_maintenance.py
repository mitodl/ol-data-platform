"""Trino maintenance resource for Iceberg OPTIMIZE and ANALYZE operations.

OPTIMIZE and ANALYZE are run via Trino (Starburst Galaxy) rather than pyiceberg
because:

- OPTIMIZE distributes file rewriting across Galaxy workers.  Running it inside
  the Dagster worker pod would consume orchestration-tier CPU/memory for work
  that is inherently a compute job.
- ANALYZE is not available in pyiceberg at all; it is a Trino-only operation
  that collects statistics for the query optimizer.

pyiceberg is used instead for EXPIRE SNAPSHOTS and REMOVE ORPHAN FILES because
those are metadata bookkeeping operations (Glue API calls + targeted S3 deletes)
that do not need a distributed engine.

Credentials are read from Vault using BasicAuthentication (service account),
not OAuth2 (which is for interactive developer use only).  The vault secret at
``vault_path`` must contain ``username`` and ``password`` keys matching the
service account credentials used for dbt's DBT_TRINO_USERNAME / DBT_TRINO_PASSWORD
environment variables.  If ``vault_path`` is empty, the resource falls back to
the ``DBT_TRINO_USERNAME`` / ``DBT_TRINO_PASSWORD`` environment variables so that
local development works without a Vault connection.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import trino
from dagster import ConfigurableResource, ResourceDependency
from pydantic import PrivateAttr
from trino.auth import BasicAuthentication

from ol_orchestrate.resources.secrets.vault import Vault

log = logging.getLogger(__name__)


class TrinoMaintenanceResource(ConfigurableResource):
    """Dagster resource for running Trino maintenance queries on Iceberg tables.

    Configure one instance per environment by keying off ``DAGSTER_ENV`` in
    ``definitions.py``, analogous to ``airbyte_host_map``.

    Example resource configuration::

        trino_maintenance = TrinoMaintenanceResource(
            host="mitol-ol-data-lake-production.trino.galaxy.starburst.io",
            catalog="ol_data_lake_production",
            vault=vault,
            vault_path="pipelines/trino-service-account",
        )

    The underlying Trino connection is created lazily on first use and reused
    for the lifetime of the resource.
    """

    host: str
    port: int = 443
    http_scheme: str = "https"
    catalog: str = "awsdatacatalog"
    vault: ResourceDependency[Vault]
    # Vault KV-v1 path whose secret contains "username" and "password" keys.
    # Leave empty to fall back to DBT_TRINO_USERNAME / DBT_TRINO_PASSWORD env vars.
    vault_path: str = ""
    vault_mount_point: str = "secret-data"

    _connection: trino.dbapi.Connection | None = PrivateAttr(default=None)

    def _get_credentials(self) -> tuple[str, str]:
        """Return (username, password) from Vault or environment variables."""
        if self.vault_path:
            secret = self.vault.client.secrets.kv.v1.read_secret(
                path=self.vault_path,
                mount_point=self.vault_mount_point,
            )["data"]
            return secret["username"], secret["password"]
        # Fall back to env vars used by dbt for local development
        username = os.environ.get("DBT_TRINO_USERNAME", "")
        password = os.environ.get("DBT_TRINO_PASSWORD", "")
        missing = [
            name
            for name, val in (
                ("DBT_TRINO_USERNAME", username),
                ("DBT_TRINO_PASSWORD", password),
            )
            if not val
        ]
        if missing:
            noun = "is" if len(missing) == 1 else "are"
            msg = (
                "TrinoMaintenanceResource: no vault_path configured and "
                f"{', '.join(missing)} {noun} not set"
            )
            raise ValueError(msg)
        return username, password

    def _get_connection(self) -> trino.dbapi.Connection:
        if self._connection is None:
            username, password = self._get_credentials()
            self._connection = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                http_scheme=self.http_scheme,
                catalog=self.catalog,
                auth=BasicAuthentication(username, password),
            )
        return self._connection

    def execute(self, sql: str) -> list[Any]:
        """Execute a SQL statement and return all rows."""
        conn = self._get_connection()
        cursor = conn.cursor()
        log.debug("Trino maintenance SQL: %.200s", sql)
        cursor.execute(sql)
        return cursor.fetchall()

    def optimize(
        self,
        schema: str,
        table: str,
        file_size_mb: int = 512,
    ) -> None:
        """Compact small Parquet files for the given Iceberg table.

        Issues::

            ALTER TABLE <schema>.<table>
            EXECUTE optimize(file_size_threshold => '512MB')

        This is particularly important for ``incremental`` dbt models using the
        ``delete+insert`` strategy, which produce one small file per run.  With
        6-12 hour sync schedules those tables accumulate small files rapidly.
        """
        sql = (
            f"ALTER TABLE {schema}.{table} "
            f"EXECUTE optimize(file_size_threshold => '{file_size_mb}MB')"
        )
        self.execute(sql)
        log.info("OPTIMIZE complete: %s.%s", schema, table)

    def analyze(self, schema: str, table: str) -> None:
        """Collect Trino query optimizer statistics for the given table.

        Issues ``ANALYZE <schema>.<table>``.  Stale statistics degrade Trino's
        query planner for mart and dimensional models.  ANALYZE is not available
        in pyiceberg, so Trino is the only engine that can perform this operation.
        """
        sql = f"ANALYZE {schema}.{table}"
        self.execute(sql)
        log.info("ANALYZE complete: %s.%s", schema, table)
