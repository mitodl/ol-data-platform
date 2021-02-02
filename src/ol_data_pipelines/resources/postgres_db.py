"""Resource for connection to a postgres db."""
from typing import Text

import pandas
import psycopg2
import pyarrow
from dagster import Field, InitResourceContext, Int, String, resource
from pypika import Query

DEFAULT_POSTGRES_PORT = 5432


class PostgresClient:
    def __init__(  # noqa: WPS211
        self,
        hostname: Text,
        username: Text,
        password: Text,
        db_name: Text,
        port: Int = DEFAULT_POSTGRES_PORT,
    ):
        """Instantiate a connection to a Postgres database.

        :param hostname: DNS or IP address of Postgres database
        :type hostname: Text

        :param username: Username for Postgres database
        :type username: Text

        :param password: Password for specified username
        :type password: Text

        :param db_name: Database name to run queries in
        :type db_name: Text

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

    def run_chunked_query(self, query: Query, chunksize: Int) -> pyarrow.Table:
        """Execute the passed query against the Postgres connection and yeild the row data as Arrow Table.

        :param query: PyPika query object that specifies the desired query
        :type query: Query

        :param chunksize: PyPika query object that specifies the desired query
        :type chunksize: Int

        :yields: chunked Query results as arrow table

        :rtype: Table
        """
        for chunk in pandas.read_sql_query(  # noqa: WPS352
            str(query),
            self.connection,
            chunksize=chunksize,
        ):
            arrow_table = pyarrow.Table.from_pandas(chunk)
            yield arrow_table


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
