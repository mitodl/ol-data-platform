import sqlite3
from typing import Any

from dagster import Field, InitResourceContext, String, resource

DEFAULT_MYSQL_PORT = 3306


class SQLiteClient:
    def __init__(self, db_name: str):
        """Instantiate a connection to a SQLite database.

        :param db_name: Database name to run queries in
        :type db_name: str
        """
        self.connection = sqlite3.connect(db_name)
        self.connection.row_factory = sqlite3.Row

    def run_query(self, query: str) -> tuple[list[str], list[dict[Any, Any]]]:
        """Execute the passed query against the SQLite database connection.

        Executes a query on the configured SQLite database and returns the row data as a
        dictionary.

        :param query: SQL query string to execute
        :type query: str

        :returns: Query results as a list of dictionaries

        :rtype: List[Dict]
        """
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute(query)
            query_fields = [field[0] for field in cursor.description]
            return query_fields, cursor.fetchall()


@resource(
    config_schema={
        "sqlite_db_name": Field(
            String,
            is_required=False,
            description="Database name to connect to for executing queries",
        ),
    }
)
def sqlite_db_resource(resource_context: InitResourceContext):
    """Dagster resource for using SQLite as a local database for development work.

    :param resource_context: The resource context object provided by Dagster
    :type resource_context: InitResourceContext

    :yield: Yields the SQLite client for use within solids that are relying on the
            resource.

    :rtype: SQLiteClient
    """
    client = SQLiteClient(
        db_name=resource_context.resource_config["sqlite_db_name"],
    )
    yield client
    client.connection.close()
