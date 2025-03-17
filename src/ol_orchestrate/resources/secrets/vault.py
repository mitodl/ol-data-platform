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
        self._initialize_client()
        session = boto3.Session()
        credentials: boto3.iam.Credentials = session.get_credentials()

        self._client.auth.aws.iam_login(
            credentials.access_key,
            credentials.secret_key,
            credentials.token,
            use_token=True,
            role=self.vault_role,
            mount_point=self.auth_mount or "aws",
        )

    def _auth_github(self):
        self._initialize_client()
        gh_token = os.environ.get("GITHUB_TOKEN")
        self._client.auth.github.login(
            token=gh_token,
            use_token=True,
            mount_point=self.auth_mount or "github",
        )

    def authenticate(self):
        if self.vault_auth_type == "aws-iam":
            self._auth_aws_iam()
        elif self.vault_auth_type == "github":
            self._auth_github()
        elif self.vault_auth_type == "token":
            if not self.vault_token:
                err_msg = "Vault token is required for token authentication"
                raise ValueError(err_msg)
            self.client.token = self.vault_token
        else:
            err_msg = f"Invalid auth type: {self.vault_auth_type}"
            raise ValueError(err_msg)

    @property
    def client(self) -> hvac.Client:
        self._initialize_client()
        if not self._client.is_authenticated():
            self.authenticate()
        return self._client

    def _initialize_client(self):
        if self._client is None:
            self._client = hvac.Client(url=self.vault_addr, verify=self.verify_tls)
