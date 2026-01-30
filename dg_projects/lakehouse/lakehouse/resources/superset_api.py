from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Self

from dagster import ConfigurableResource, InitResourceContext, ResourceDependency
from ol_orchestrate.resources.oauth import OAuthApiClient
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic import Field, PrivateAttr


class SupersetApiClient(OAuthApiClient):
    token_type: str = Field(
        default="Bearer",
        description="Token type to generate for use with authenticated requests",
    )
    username: str = Field(
        description="service account username for Superset API access. "
    )
    password: str = Field(
        description="service account password for Superset API access. "
    )
    scope: str = Field(
        default="openid profile email roles",
        description="scope to request from the token endpoint",
    )
    _access_token: str | None = PrivateAttr(default=None)
    _access_token_expires: datetime | None = PrivateAttr(default=None)

    @property
    def _csrf_token_url(self) -> str:
        return f"{self.base_url}/api/v1/security/csrf_token/"

    def _fetch_access_token(self) -> str | None:
        now = datetime.now(tz=UTC)
        if self._access_token is None or (self._access_token_expires or now) <= now:
            payload = {
                "grant_type": "password",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "token_type": self.token_type,
                "username": self.username,
                "password": self.password,
                "scope": self.scope,
            }
            response = self.http_client.post(self.token_url, data=payload)
            response.raise_for_status()
            self._access_token = response.json()["access_token"]
            self._access_token_expires = now + timedelta(
                seconds=response.json()["expires_in"]
            )
        return self._access_token

    def _get_csrf_token(self) -> str:
        response = self.http_client.get(
            f"{self.base_url}/api/v1/security/csrf_token/",
            headers={
                "Authorization": f"{self.token_type} {self._fetch_access_token()}"
            },
        )
        response.raise_for_status()
        return response.json().get("result")

    def get_dataset_list(
        self, page_size: int = 100
    ) -> Generator[list[dict[str, str]], None, None]:
        """Retrieve all items from the Superset REST API including pagination.

        :param page_size: The number of datasets to retrieve per page via the API.
        :type page_size: int

        :yield: A generator for walking the paginated list of datasets returned from
            the API
        """
        request_url = f"{self.base_url}/api/v1/dataset/"
        page = 0
        total_fetched = 0
        while True:
            query_string = (
                f"(order_column:changed_on_delta_humanized,order_direction:desc,"
                f"page:{page},page_size:{page_size})"
            )
            response_data = self.fetch_with_auth(
                request_url, extra_params={"q": query_string}
            )
            dataset_result = response_data["result"]  # type: ignore[call-overload]
            total_fetched += len(dataset_result)  # type: ignore[arg-type]

            yield dataset_result  # type: ignore[misc]

            count = response_data.get("count", 0)  # type: ignore[union-attr]
            if total_fetched >= count:
                break

            page += 1

    def get_or_create_dataset(self, schema_suffix: str, table_name: str) -> str:
        """Retrieve a dataset by name, or create it if it doesn't exist

        Args:
            schema_suffix (str): The schema suffix. e.g. mart, reporting
            table_name (str): The name of the table to create a dataset for.
        Returns:
            Dict[str, Any]: The response from the Superset API.
        """
        request_url = f"{self.base_url}/api/v1/dataset/get_or_create/"
        payload = {
            "database_id": 1,  # Trino database ID
            "schema": f"ol_warehouse_production_{schema_suffix}",
            "table_name": table_name,
        }
        response = self.http_client.post(
            request_url,
            json=payload,
            headers={
                "Authorization": f"{self.token_type} {self._fetch_access_token()}",
                "X-CSRFToken": self._get_csrf_token(),
                "Referer": self._csrf_token_url,
            },
            timeout=300,
        )

        response.raise_for_status()
        response_data = response.json()

        return response_data.get("result", {}).get("table_id")

    def refresh_dataset(self, dataset_id: int) -> dict[str, Any]:
        """Refresh and update columns for a dataset in Superset."""
        request_url = f"{self.base_url}/api/v1/dataset/{dataset_id}/refresh"
        response = self.http_client.put(
            request_url,
            headers={
                "Authorization": f"{self.token_type} {self._fetch_access_token()}",
                "X-CSRFToken": self._get_csrf_token(),
                "Referer": self._csrf_token_url,
                "Content-Type": "application/json",
            },
            timeout=300,
        )
        response.raise_for_status()
        return response.json()

    def update_dataset(
        self, dataset_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Update dataset metadata in Superset
        """
        request_url = f"{self.base_url}/api/v1/dataset/{dataset_id}"
        response = self.http_client.put(
            request_url,
            headers={
                "Authorization": f"{self.token_type} {self._fetch_access_token()}",
                "X-CSRFToken": self._get_csrf_token(),
                "Referer": self._csrf_token_url,
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=300,
        )
        response.raise_for_status()
        return response.json()


class SupersetApiClientFactory(ConfigurableResource):
    deployment: str = Field(description="The name of the deployment")
    _client: SupersetApiClient | None = PrivateAttr(default=None)
    vault: ResourceDependency[Vault]

    def _initialize_client(self) -> SupersetApiClient:
        client_secrets = self.vault.client.secrets.kv.v1.read_secret(
            mount_point="secret-data",
            path="superset_service_account",
        )["data"]

        return SupersetApiClient(
            client_id=client_secrets["client_id"],
            client_secret=client_secrets["client_secret"],
            base_url=client_secrets["superset_url"],
            token_url=client_secrets["token_url"],
            username=client_secrets["service_account_username"],
            password=client_secrets["service_account_password"],
            scope=client_secrets.get("scope", "openid profile email roles"),
        )

    @property
    def client(self) -> SupersetApiClient:
        if not self._client:
            self._client = self._initialize_client()
        return self._client

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self]:  # noqa: ARG002
        yield self
