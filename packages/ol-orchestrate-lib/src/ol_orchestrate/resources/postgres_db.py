"""Resource for connection to a postgres db."""

import pandas  # noqa: ICN001
import psycopg2
import pyarrow  # noqa: ICN001
from dagster import Field, InitResourceContext, Int, String, resource

DEFAULT_POSTGRES_PORT = 5432
DEFAULT_POSTGRES_QUERY_CHUNKSIZE = 5000


class PostgresClient:
    def __init__(
        self,
        hostname: str,
        username: str,
        password: str,
        db_name: str,
        port: Int = DEFAULT_POSTGRES_PORT,
    ):
        """Instantiate a connection to a Postgres database.

        :param hostname: DNS or IP address of Postgres database
        :type hostname: str

        :param username: Username for Postgres database
        :type username: str

        :param password: Password for specified username
        :type password: str

        :param db_name: Database name to run queries in
        :type db_name: str

        :param port: Port number for Postgres database
        :type port: Int
        """
        self.connection = psycopg2.connect(
            dbname=db_name,
            user=username,
            password=password,
            host=hostname,
            port=port,
        )

    def run_chunked_query(self, query: str, chunksize: Int) -> pyarrow.Table:
        """Execute the passed query against the Postgres connection.

        Executes a query against the target Postgres database and yields the row data as
        an Arrow Table.

        :param query: SQL query string to execute
        :type query: str

        :param chunksize: Number of rows to fetch per chunk
        :type chunksize: Int

        :yields: chunked Query results as arrow table

        :rtype: Table
        """
        for chunk in pandas.read_sql_query(
            query,
            self.connection,
            chunksize=chunksize,
        ):
            arrow_table = pyarrow.Table.from_pandas(chunk)
            yield arrow_table

    def run_write_query(self, query: str):
        """Execute the passed write query.

        :param query: SQL query string to execute
        :type query: str
        """
        with self.connection.cursor() as db_cursor:
            db_cursor.execute(query)
            self.connection.commit()


@resource(
    config_schema={
        "postgres_hostname": Field(
            String, is_required=True, description="Host string for Postgres server"
        ),
        "postgres_port": Field(
            Int,
            is_required=False,
            default_value=DEFAULT_POSTGRES_PORT,
            description="TCP Port number for Postgres server",
        ),
        "postgres_username": Field(
            String,
            is_required=True,
            description="Username for authenticating to Postgres server",
        ),
        "postgres_password": Field(
            String,
            is_required=True,
            description="Password for authenticating to Postgres server",
        ),
        "postgres_db_name": Field(
            String,
            is_required=True,
            description="Database name to connect to for executing queries",
        ),
    }
)
def postgres_db_resource(resource_context: InitResourceContext):
    """
     Create a connection to a postgres database.

    :param resource_context: Dagster execution context for configuration data
    :type resource_context: InitResourceContext

    :yields: A postgres client instance for use during pipeline execution.
    """
    client = PostgresClient(
        hostname=resource_context.resource_config["postgres_hostname"],
        username=resource_context.resource_config["postgres_username"],
        password=resource_context.resource_config["postgres_password"],
        port=resource_context.resource_config["postgres_port"],
        db_name=resource_context.resource_config["postgres_db_name"],
    )

    yield client

    client.connection.close()
