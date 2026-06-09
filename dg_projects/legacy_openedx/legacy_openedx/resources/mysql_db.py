"""MySQL / MariaDB database resources for legacy Open edX pipelines."""

import contextlib
import logging
import ssl
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Self

import pymysql
import pymysql.err
from dagster import (
    ConfigurableResource,
    Field,
    InitResourceContext,
    Int,
    ResourceDependency,
    String,
    resource,
)
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic import Field as PydanticField
from pydantic import PrivateAttr
from pymysql.cursors import DictCursor

log = logging.getLogger(__name__)

# MySQL / MariaDB error codes that warrant a credential refresh and reconnect.
# 1044 ER_DBACCESS_DENIED_ERROR - Vault dynamic credential revoked / expired
# 1045 ER_ACCESS_DENIED_ERROR   - bad username or password
# 2006 CR_SERVER_GONE_ERROR     - server gone away (restart / idle timeout)
# 2013 CR_SERVER_LOST           - connection lost mid-query
_RETRIABLE_MYSQL_ERRORS: frozenset[int] = frozenset({1044, 1045, 2006, 2013})

DEFAULT_MYSQL_PORT = 3306


def _make_ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class MySQLClient:
    def __init__(
        self,
        hostname: str,
        username: str,
        password: str,
        db_name: str | None = None,
        port: int = 3306,
    ):
        """Instantiate a connection to a MySQL database.

        :param hostname: DNS or IP address of MySQL database
        :type hostname: str

        :param username: Username for MySQL database with readonly access to database
        :type username: str

        :param password: Password for specified username
        :type password: str

        :param db_name: Database name to run queries in
        :type db_name: str

        :param port: Port number for MySQL database
        :type port: Int
        """
        self.connection = pymysql.connect(
            host=hostname,
            user=username,
            password=password,
            port=port,
            database=db_name,
            cursorclass=DictCursor,
            ssl=_make_ssl_context(),
        )

    def run_query(self, query: str) -> tuple[list[str], list[dict[Any, Any]]]:
        """Execute the passed query against the MySQL database connection.

        Execute a query on the configured MySQL database and return the row data as a
        dictionary.

        :param query: SQL query string to execute
        :type query: str

        :returns: Query results as a list of dictionaries

        :rtype: List[Dict]
        """
        with self.connection.cursor() as db_cursor:
            db_cursor.execute(query)
            query_fields = [field[0] for field in db_cursor.description]
            return query_fields, list(db_cursor.fetchall())


@resource(
    config_schema={
        "mysql_hostname": Field(
            String, is_required=True, description="Host string for MySQL/MariaDB server"
        ),
        "mysql_port": Field(
            Int,
            is_required=False,
            default_value=DEFAULT_MYSQL_PORT,
            description="TCP Port number for MySQL/MariaDB server",
        ),
        "mysql_username": Field(
            String,
            is_required=True,
            description="Username for authenticating to MySQL/MariaDB server",
        ),
        "mysql_password": Field(
            String,
            is_required=False,
            description="Password for authenticating to MySQL/MariaDB server",
        ),
        "mysql_db_name": Field(
            String,
            is_required=False,
            description="Database name to connect to for executing queries",
        ),
    }
)
def mysql_db_resource(resource_context: InitResourceContext):
    client = MySQLClient(
        hostname=resource_context.resource_config["mysql_hostname"],
        username=resource_context.resource_config["mysql_username"],
        password=resource_context.resource_config["mysql_password"],
        port=resource_context.resource_config["mysql_port"],
        db_name=resource_context.resource_config["mysql_db_name"],
    )
    yield client
    client.connection.close()


class VaultMySQLClientFactory(ConfigurableResource["VaultMySQLClientFactory"]):
    """MySQL resource that fetches credentials from Vault at runtime.

    Unlike the low-level :func:`mysql_db_resource`, this resource holds **no**
    passwords in its Dagster config.  Credentials are generated via Vault's
    database secrets engine when the resource is first used and are automatically
    re-generated whenever a query fails due to an expired or revoked dynamic
    credential (error codes 1044/1045) or a dropped connection (2006/2013).

    Configure the non-secret fields in the run ``resources`` section::

        resources:
          sqldb:
            config:
              mysql_hostname: "edxapp-db-mitx-production.example.rds.amazonaws.com"
              mysql_db_name: "edxapp"
              vault_mount_point: "mariadb-mitx"
              vault_role: "readonly"
    """

    vault: ResourceDependency[Vault]
    vault_mount_point: str = PydanticField(
        description="Vault database secrets engine mount point, e.g. 'mariadb-mitx'."
    )
    vault_role: str = PydanticField(
        default="readonly",
        description="Vault database role to generate credentials for.",
    )
    mysql_hostname: str = PydanticField(
        description="MySQL / MariaDB host name or IP address."
    )
    mysql_db_name: str = PydanticField(
        description="Database name to connect to for executing queries."
    )
    mysql_port: int = PydanticField(
        default=DEFAULT_MYSQL_PORT,
        description="TCP port number for MySQL / MariaDB server.",
    )

    _client: MySQLClient | None = PrivateAttr(default=None)

    def _generate_credentials(self) -> tuple[str, str]:
        """Fetch a fresh set of dynamic credentials from Vault."""
        creds = self.vault.client.secrets.database.generate_credentials(
            mount_point=self.vault_mount_point,
            name=self.vault_role,
        )["data"]
        return creds["username"], creds["password"]

    def _connect(self) -> None:
        """Open a new database connection using freshly generated Vault credentials."""
        with contextlib.suppress(Exception):
            if self._client is not None:
                self._client.connection.close()
        username, password = self._generate_credentials()
        self._client = MySQLClient(
            hostname=self.mysql_hostname,
            username=username,
            password=password,
            db_name=self.mysql_db_name,
            port=self.mysql_port,
        )

    def run_query(self, query: str) -> tuple[list[str], list[dict[Any, Any]]]:
        """Execute *query*, regenerating Vault credentials on auth/connection failure.

        On the first :class:`pymysql.err.OperationalError` with a retriable error
        code (expired credential, connection dropped), fresh credentials are fetched
        from Vault and the query is retried exactly once.

        :param query: SQL query string to execute
        :returns: ``(column_names, rows)``
        :raises pymysql.err.OperationalError: if the retry also fails or the error
            code is not retriable.
        """
        if self._client is None:
            self._connect()
        try:
            return self._client.run_query(query)  # type: ignore[union-attr]
        except pymysql.err.OperationalError as exc:
            if exc.args[0] in _RETRIABLE_MYSQL_ERRORS:
                log.warning(
                    "MySQL OperationalError %s (%s) - regenerating Vault credentials "
                    "and retrying.",
                    exc.args[0],
                    exc.args[1] if len(exc.args) > 1 else "",
                )
                self._connect()
                return self._client.run_query(query)  # type: ignore[union-attr]
            raise

    @contextmanager
    def yield_for_execution(
        self,
        context: InitResourceContext,  # noqa: ARG002
    ) -> Generator[Self, None, None]:
        """Yield *self* and close the database connection on teardown."""
        try:
            yield self
        finally:
            if self._client is not None:
                with contextlib.suppress(Exception):
                    self._client.connection.close()
                self._client = None
