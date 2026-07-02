"""StarRocks database resource with Vault dynamic credentials.

Modeled on VaultMySQLClientFactory (legacy_openedx/resources/mysql_db.py):
credentials come from Vault's database secrets engine, generated fresh on each
connection -- no passwords are held in Dagster config.
"""

from dagster import ConfigurableResource, ResourceDependency
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic import Field as PydanticField
from pymysql import connect
from pymysql.cursors import DictCursor

DEFAULT_STARROCKS_PORT = 9030


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
        username, password = self._generate_credentials()
        conn = connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=username,
            password=password,
            cursorclass=DictCursor,
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()
        finally:
            conn.close()
