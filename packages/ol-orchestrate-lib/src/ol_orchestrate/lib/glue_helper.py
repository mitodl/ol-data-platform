"""Helper functions for AWS Glue operations and dbt model data retrieval."""

import types

import boto3
import polars as pl
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table

TYPE_RENAME = types.MappingProxyType(
    {
        "bool": "boolean",
        "date32[day]": "timestamp",
        "double": "double",
        "int64": "bigint",
        "string": "string",
        "timestamp[ns, tz=UTC]": "timestamp",
        "timestamp[us, tz=UTC]": "timestamp",
    }
)


def convert_schema(schema):
    """Convert arrow schema to glue schema.

    :schema: oyarrow schema
    :type schema: Schema

    :returns: List of column names and types in glue's input format
    :rtype: list of dict
    """
    schema_list = []

    for name, datatype in zip(schema.names, schema.types):
        schema_list.append({"Name": name, "Type": TYPE_RENAME[str(datatype)]})

    return schema_list


def create_or_update_table(
    database_name, table_name, formatted_schema, location, partition_keys_list=None
):
    """Create or update glue table from s3 data.

    :database_name: Athena database name
    :type database_name: string

    :table_name: table name
    :type table_name: string

    :formatted_schema: schema in glue format
    :type formatted_schema: list of dict

    :location: s3 path of table data
    :type location: string

    :partition_keys_list: list of columns that the data is partitioned by
    :type partition_keys_list: list of string
    """
    if partition_keys_list:
        partition_keys = [
            column_schema
            for column_schema in formatted_schema
            if column_schema["Name"] in partition_keys_list
        ]
    else:
        partition_keys = []

    table_input = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns": formatted_schema,
            "Location": location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",  # noqa: E501
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",  # noqa: E501
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"  # noqa: E501
            },
            "Parameters": {"parquetTimestampInMillisecond": "true"},
        },
        "PartitionKeys": partition_keys,
    }

    session = boto3.session.Session()
    glue_client = session.client("glue")

    try:
        glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
    except glue_client.exceptions.AlreadyExistsException:
        glue_client.update_table(DatabaseName=database_name, TableInput=table_input)


def get_dbt_model_as_dataframe(database_name: str, table_name: str) -> pl.LazyFrame:
    """Retrieve a dbt model from AWS Glue as a Polars DataFrame.

    This function fetches table metadata from AWS Glue and loads the Iceberg
    table data into a Polars DataFrame.

    Args:
        database_name: The Glue database name containing the table
        table_name: The name of the table to retrieve

    Returns:
        A Polars DataFrame containing the table data

    Raises:
        KeyError: If the table metadata doesn't contain the expected fields
        boto3 exceptions: If the AWS Glue API call fails
    """
    glue = GlueCatalog("default", client=boto3.client("glue", region_name="us-east-1"))
    table = glue.load_table(f"{database_name}.{table_name}")

    return table.to_polars()


def get_or_create_iceberg_table(
    database_name: str,
    table_name: str,
    schema: Schema,
) -> Table:
    """
    Create an Iceberg table in AWS Glue if it does not exist

    Args:
        database_name: Glue database name
        table_name: Iceberg table name
        schema: Iceberg schema

    Returns:
        pyiceberg.table.Table
    """
    glue = GlueCatalog("default", client=boto3.client("glue", region_name="us-east-1"))
    table_identifier = f"{database_name}.{table_name}"

    try:
        table = glue.load_table(table_identifier)
    except NoSuchTableError:
        table = glue.create_table(
            identifier=table_identifier,
            schema=schema,
        )

    return table
