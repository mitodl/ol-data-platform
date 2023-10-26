from typing import Any, Optional

import pymysql
from dagster import Field, InitResourceContext, Int, String, resource
from pymysql.cursors import DictCursor
from pypika import Query

DEFAULT_MYSQL_PORT = 3306


class MySQLClient:
    def __init__(  # noqa: PLR0913
        self,
        hostname: str,
        username: str,
        password: str,
        db_name: Optional[str] = None,
        port: Int = 3306,
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
            passwd=password,
            port=port,
            db=db_name,
            cursorclass=DictCursor,
        )

    def run_query(self, query: Query) -> tuple[list[str], list[dict[Any, Any]]]:
        """Execute the passed query against the MySQL database connection.

        Execute a query on the configured MySQL database and return the row data as a
        dictionary.

        :param query: PyPika query object that specifies the desired query
        :type query: Query

        :returns: Query results as a list of dictionaries

        :rtype: List[Dict]
        """
        with self.connection.cursor() as db_cursor:
            db_cursor.execute(str(query))
            query_fields = [field[0] for field in db_cursor.description]
            return query_fields, db_cursor.fetchall()  # type: ignore  # noqa: PGH003


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
