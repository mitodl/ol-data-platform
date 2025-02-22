import os
from typing import Optional

import boto3
import hvac
from dagster import ConfigurableResource
from pydantic import PrivateAttr


class Vault(ConfigurableResource):
    vault_addr: str
    vault_token: Optional[str]
    vault_role: Optional[str]
    vault_auth_type: str = "aws-iam"  # can be one of ["github", "aws-iam", "token"]
    auth_mount: Optional[str] = None
    verify_tls: bool = True
    _client: hvac.Client = PrivateAttr(default=None)

    def _auth_aws_iam(self):
        session = boto3.Session()
        credentials: boto3.iam.Credentials = session.get_credentials()

        self.client.auth.aws.iam_login(
            credentials.access_key,
            credentials.secret_key,
            credentials.token,
            use_token=True,
            role=self.vault_role,
            mount_point=self.auth_mount or "aws",
        )

    def _auth_github(self):
        gh_token = os.environ.get("GITHUB_TOKEN")
        self.client.auth.github.login(
            token=gh_token,
            use_token=True,
            mount_point=self.auth_mount or "github",
        )

    @property
    def client(self) -> hvac.Client:
        if self._client is None:
            self._client = hvac.Client(url=self.vault_addr, verify=self.verify_tls)
        return self._client
