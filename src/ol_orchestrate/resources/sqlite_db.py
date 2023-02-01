import sqlite3

from dagster import Field, InitResourceContext, String, resource
from pypika import Query  # noqa: TCH002

DEFAULT_MYSQL_PORT = 3306


class SQLiteClient:
    def __init__(self, db_name: str):
        """Instantiate a connection to a SQLite database.

        :param db_name: Database name to run queries in
        :type db_name: str
        """
        self.connection = sqlite3.connect(db_name)
        self.connection.row_factory = sqlite3.Row

    def run_query(self, query: Query) -> tuple[list[str], list[dict]]:
        """Execute the passed query against the SQLite database connection.

        Executes a query on the configured SQLite database and returns the row data as a
        dictionary.

        :param query: PyPika query object that specifies the desired query
        :type query: Query

        :returns: Query results as a list of dictionaries

        :rtype: List[Dict]
        """
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute(str(query))
            query_fields = [field[0] for field in cursor.description]
            return query_fields, cursor.fetchall()  # type: ignore


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
