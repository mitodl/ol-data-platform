"""Resource for connection to athena."""

import pyathena
from dagster import Field, InitResourceContext, Noneable, String, resource
from pyathena.cursor import DictCursor


class AthenaClient:
    def __init__(
        self,
        work_group: str,
        schema_name: str,
        region_name: str = "us-east-1",
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ):
        """Instantiate a connection to a Athena database.

        :param work_group: Athena workgroup
        :type work_group: str

        :param region_name: AWS region name
        :type region_name: str

        :param schema_name: Athena schema
        :type region_name: str

        :param aws_access_key_id: AWS access key id
        :type aws_access_key_id: str

        :param aws_secret_access_key: AWS secret access key
        :type aws_secret_access_key: str

        """
        self.cursor = pyathena.connect(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            work_group=work_group,
            cursor_class=DictCursor,
            region_name=region_name,
            schema_name=schema_name,
        ).cursor()

    def run_query(self, query: str) -> DictCursor:
        """Execute the passed query against the Athena connection.

        Execute a query on the target Athena database and yield the row data as
        DictCursor.

        :param query: SQL query string to execute
        :type query: str

        :returns: DictCursor
        """
        return self.cursor.execute(query)


@resource(
    config_schema={
        "aws_access_key_id": Field(
            Noneable(str),
            default_value=None,
            is_required=False,
            description="AWS key for Athena connection",
        ),
        "aws_secret_access_key": Field(
            Noneable(str),
            default_value=None,
            is_required=False,
            description="Aws key for Athena connection",
        ),
        "work_group": Field(
            String,
            is_required=True,
            description="Work group for Athena connection",
        ),
        "region_name": Field(
            String,
            default_value="us-east-1",
            is_required=False,
            description="Region for Athena connection",
        ),
        "schema_name": Field(
            String,
            is_required=True,
            description="Schema for Athena connection",
        ),
    }
)
def athena_db_resource(resource_context: InitResourceContext):
    """Create a connection to athena.

    :param resource_context: Dagster execution context for configuration data
    :type resource_context: InitResourceContext

    :yields: A Athena client instance for use during pipeline execution.
    """
    client = AthenaClient(
        work_group=resource_context.resource_config["work_group"],
        region_name=resource_context.resource_config["region_name"],
        schema_name=resource_context.resource_config["schema_name"],
        aws_access_key_id=resource_context.resource_config["aws_access_key_id"],
        aws_secret_access_key=resource_context.resource_config["aws_secret_access_key"],
    )
    yield client
