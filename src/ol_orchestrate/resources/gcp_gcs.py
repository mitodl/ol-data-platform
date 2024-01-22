"""Resource for connecting to a gcs."""

from contextlib import contextmanager

from dagster import ConfigurableResource, InitResourceContext
from google.cloud import storage
from google.oauth2 import service_account
from pydantic import Field, PrivateAttr


class GCSConnection(ConfigurableResource):
    project_id: str = Field(
        description="service account project_id field",
    )
    client_x509_cert_url: str = Field(
        description="service account client_x509_cert_url field",
    )
    private_key_id: str = Field(
        description="service account private_key_id field",
    )
    auth_uri: str = Field(
        description="service account auth_uri field",
    )
    token_uri: str = Field(
        description="service account token_uri field",
    )
    client_id: str = Field(
        description="service account client_id field",
    )
    private_key: str = Field(
        description="service account private_key field",
    )
    client_email: str = Field(
        description="service account client_email field",
    )
    auth_provider_x509_cert_url: str = Field(
        default="https://www.googleapis.com/oauth2/v1/certs",
        description="URL for the x509 cert used by the auth provider",
    )
    type: str = Field(
        default="service_account",
        description="Credential type",
    )

    _gcs_client: storage.Client = PrivateAttr()

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext):  # noqa: ARG002
        access_json = self.dict()
        credentials = service_account.Credentials.from_service_account_info(access_json)
        self._gcs_client = storage.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
        yield self

    @property
    def client(self) -> storage.Client:
        return self._gcs_client
