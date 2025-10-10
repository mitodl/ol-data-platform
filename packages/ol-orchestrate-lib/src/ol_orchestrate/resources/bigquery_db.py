"""Resource for connection to a bigquery database."""

from dagster import Field, InitResourceContext, String, resource
from google.cloud import bigquery
from google.oauth2 import service_account


@resource(
    config_schema={
        "project_id": Field(
            String,
            is_required=True,
            description="service account project_id field",
        ),
        "client_x509_cert_url": Field(
            String,
            is_required=True,
            description="service account client_x509_cert_url field",
        ),
        "private_key_id": Field(
            String,
            is_required=True,
            description="service account private_key_id field",
        ),
        "auth_uri": Field(
            String,
            is_required=True,
            description="service account auth_uri field",
        ),
        "token_uri": Field(
            String,
            is_required=True,
            description="service account token_uri field",
        ),
        "client_id": Field(
            String,
            is_required=True,
            description="service account client_id field",
        ),
        "private_key": Field(
            String,
            is_required=True,
            description="service account private_key field",
        ),
        "client_email": Field(
            String,
            is_required=True,
            description="service account client_email field",
        ),
    }
)
def bigquery_db_resource(resource_context: InitResourceContext):
    """Create a connection to bigquery database.

    :param resource_context: Dagster execution context for configuration data
    :type resource_context: InitResourceContext

    :yields: A BigQuery client instance for use during pipeline execution.
    """
    access_json = {
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "project_id": resource_context.resource_config["project_id"],
        "client_x509_cert_url": resource_context.resource_config[
            "client_x509_cert_url"
        ],
        "private_key_id": resource_context.resource_config["private_key_id"],
        "auth_uri": resource_context.resource_config["auth_uri"],
        "token_uri": resource_context.resource_config["token_uri"],
        "client_id": resource_context.resource_config["client_id"],
        "private_key": resource_context.resource_config["private_key"],
        "type": "service_account",
        "client_email": resource_context.resource_config["client_email"],
    }

    credentials = service_account.Credentials.from_service_account_info(access_json)
    yield bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
