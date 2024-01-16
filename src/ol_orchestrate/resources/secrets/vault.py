import os
from collections.abc import Generator
from typing import Optional

import boto3
import hvac
from dagster import ConfigurableResource, InitResourceContext
from dagster._config.pythonic_config.conversion_utils import TResValue
from pydantic import PrivateAttr


class Vault(ConfigurableResource):
    vault_addr: str
    vault_token: Optional[str]
    vault_role: Optional[str]
    vault_auth_type: str = "aws-iam"  # can be one of ["github", "aws-iam", "token"]
    auth_mount: Optional[str] = None
    _client: hvac.Client = PrivateAttr()

    def _auth_aws_iam(self):
        session = boto3.Session()
        credentials: boto3.iam.Credentials = session.get_credentials()

        self._client = hvac.Client()
        self._client.auth.aws.iam_login(
            credentials.access_key,
            credentials.secret_key,
            credentials.token,
            use_token=True,
            role=self.vault_role,
            mount_point=self.auth_mount or "aws",
        )

    def _auth_github(self):
        gh_token = os.environ.get("GITHUB_TOKEN")
        self._client = hvac.Client()
        self._client.auth.github.login(
            token=gh_token,
            use_token=True,
            mount_point=self.auth_mount or "github",
        )

    def yield_for_execution(
        self,
        context: InitResourceContext,  # noqa: ARG002
    ) -> Generator[TResValue, None, None]:
        return self._client

    @property
    def client(self) -> hvac.Client:
        return self._client
