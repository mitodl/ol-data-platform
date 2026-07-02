"""StarRocks database resource with Vault dynamic credentials.

Modeled on VaultMySQLClientFactory (legacy_openedx/resources/mysql_db.py):
credentials come from Vault's database secrets engine, generated fresh on each
connection -- no passwords are held in Dagster config.
"""

import logging
import time

from dagster import ConfigurableResource, ResourceDependency
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic import Field as PydanticField
from pymysql import connect
from pymysql.cursors import DictCursor
from pymysql.err import OperationalError

log = logging.getLogger(__name__)

DEFAULT_STARROCKS_PORT = 9030

# MySQL-wire-protocol error codes that warrant a fresh Vault credential + retry.
# 1044 ER_DBACCESS_DENIED_ERROR / 1045 ER_ACCESS_DENIED_ERROR - the dynamic user
#   Vault just created hasn't propagated across StarRocks FE nodes yet.
# 2006 CR_SERVER_GONE_ERROR / 2013 CR_SERVER_LOST - dropped connection.
_RETRIABLE_ERRORS: frozenset[int] = frozenset({1044, 1045, 2006, 2013})
_MAX_ATTEMPTS = 3
_RETRY_BASE_DELAY = 1  # seconds; doubles each attempt


class StarRocksResource(ConfigurableResource["StarRocksResource"]):
    """Executes statements against StarRocks over its MySQL wire protocol port."""

    vault: ResourceDependency[Vault]
    vault_mount_point: str = PydanticField(
        description=(
            "Vault database secrets engine mount point for StarRocks, "
            "e.g. 'mysql-starrocks-production'."
        )
    )
    vault_role: str = PydanticField(
        default="dagster",
        description="Vault database role to generate credentials for.",
    )
    host: str = PydanticField(description="StarRocks FE host name.")
    port: int = PydanticField(
        default=DEFAULT_STARROCKS_PORT,
        description="StarRocks MySQL-protocol query port.",
    )
    database: str = PydanticField(
        description="StarRocks database/schema to connect to, e.g. 'b2b_analytics'."
    )

    def _generate_credentials(self) -> tuple[str, str]:
        creds = self.vault.client.secrets.database.generate_credentials(
            mount_point=self.vault_mount_point,
            name=self.vault_role,
        )["data"]
        return creds["username"], creds["password"]

    def execute(self, sql: str) -> None:
        """Run *sql*, retrying with fresh Vault credentials on a transient error.

        A fresh set of dynamic credentials is generated on every attempt (not just
        the first) since a 1044/1045 is most often caused by the previous attempt's
        just-created user not yet being visible on the FE node we connect to --
        generating a new one and retrying gives replication another round to catch
        up rather than reusing credentials known to be affected.
        """
        last_exc: OperationalError | None = None
        for attempt in range(_MAX_ATTEMPTS):
            if attempt:
                delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
                log.warning(
                    "StarRocks error %s (attempt %d/%d) -- retrying in %ds with "
                    "fresh Vault credentials",
                    last_exc,
                    attempt,
                    _MAX_ATTEMPTS,
                    delay,
                )
                time.sleep(delay)

            username, password = self._generate_credentials()
            try:
                conn = connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=username,
                    password=password,
                    cursorclass=DictCursor,
                )
            except OperationalError as exc:
                if exc.args[0] not in _RETRIABLE_ERRORS:
                    raise
                last_exc = exc
                continue

            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                conn.commit()
            except OperationalError as exc:
                if exc.args[0] not in _RETRIABLE_ERRORS:
                    raise
                last_exc = exc
                continue
            else:
                return
            finally:
                conn.close()

        raise last_exc  # type: ignore[misc]  # loop always sets last_exc before falling through
