from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Self, cast

from dagster import ConfigurableResource, InitResourceContext, ResourceDependency
from ol_orchestrate.resources.oauth import OAuthApiClient
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic import Field, PrivateAttr

UNPROCESSABLE_ENTITY = 422


class SupersetApiClient(OAuthApiClient):
    token_type: str = Field(
        default="Bearer",
        description="Token type to generate for use with authenticated requests",
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
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": self.scope,
            }
            response = self.http_client.post(self.token_url, data=payload)
            if not response.is_success:
                msg = (
                    f"Failed to fetch access token from {self.token_url}: "
                    f"HTTP {response.status_code} — {response.text}"
                )
                raise RuntimeError(msg)
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
            response_data = cast(
                dict[str, Any],
                self.fetch_with_auth(request_url, extra_params={"q": query_string}),
            )
            dataset_result = response_data["result"]
            total_fetched += len(dataset_result)

            yield dataset_result

            count = response_data.get("count", 0)
            if total_fetched >= count:
                break

            page += 1

    def find_dataset(
        self, database_id: int, schema: str, table_name: str
    ) -> int | None:
        """Look up a dataset by its full (database, schema, table_name) identity.

        Superset's own ``/api/v1/dataset/get_or_create/`` matches on
        ``(database_id, table_name)`` alone via ``DatasetDAO.get_table_by_name``,
        so it raises ``MultipleResultsFound`` -> HTTP 500 as soon as one
        table_name repeats across two schemas in the same database. Filtering on
        the schema too is both correct and immune to that. (Fixed upstream in
        apache/superset#40494, which is unreleased as of Superset 6.1.0.)

        Args:
            database_id (int): The Superset database ID to search within.
            schema (str): The fully qualified schema name.
            table_name (str): The name of the table.

        Returns:
            int | None: The Superset dataset ID, or None if no dataset matches.
        """
        query_string = (
            "(filters:!("
            f"(col:database,opr:rel_o_m,value:{database_id}),"
            f"(col:schema,opr:eq,value:'{schema}'),"
            f"(col:table_name,opr:eq,value:'{table_name}')"
            "))"
        )
        response_data = cast(
            dict[str, Any],
            self.fetch_with_auth(
                f"{self.base_url}/api/v1/dataset/",
                extra_params={"q": query_string},
            ),
        )
        # min() rather than [0] so a pre-existing duplicate pair resolves to the
        # same dataset on every run regardless of the API's result ordering.
        return min(response_data.get("ids") or [], default=None)

    def create_dataset(
        self, database_id: int, schema: str, table_name: str
    ) -> int | None:
        """Create a physical dataset.

        Args:
            database_id (int): The Superset database ID to create the dataset in.
            schema (str): The fully qualified schema name.
            table_name (str): The name of the table.

        Returns:
            int | None: The new dataset's ID, or None if Superset rejected the
                dataset as invalid -- which is the expected answer for a model
                that has no table in this particular database (e.g. a Trino-only
                dbt model whose StarRocks twin was never built).
        """
        payload = {
            "database": database_id,
            "schema": schema,
            "table_name": table_name,
        }
        response = self.http_client.post(
            f"{self.base_url}/api/v1/dataset/",
            json=payload,
            headers={
                "Authorization": f"{self.token_type} {self._fetch_access_token()}",
                "X-CSRFToken": self._get_csrf_token(),
                "Referer": self._csrf_token_url,
                "Content-Type": "application/json",
            },
            timeout=300,
        )

        if response.is_success:
            return response.json()["id"]

        if response.status_code == UNPROCESSABLE_ENTITY:
            # Either the table genuinely isn't in this database, or another
            # process won a create race between find_dataset() and here. A
            # re-read distinguishes the two without parsing error strings.
            return self.find_dataset(database_id, schema, table_name)

        msg = (
            f"Failed to create dataset {payload!r}: "
            f"HTTP {response.status_code} — {response.text}"
        )
        raise RuntimeError(msg)

    def get_or_create_dataset(
        self,
        schema_suffix: str,
        table_name: str,
        database_id: int = 1,
        schema_base: str = "ol_warehouse_production",
    ) -> int | None:
        """Retrieve a dataset by name, or create it if it doesn't exist

        Args:
            schema_suffix (str): The schema suffix. e.g. mart, reporting
            table_name (str): The name of the table to create a dataset for.
            database_id (int): The Superset database ID to use.
            schema_base (str): The schema base prefix (without trailing underscore),
                e.g. "ol_warehouse_production" or "ol_warehouse_qa".
        Returns:
            int | None: The Superset dataset ID, or None if not found.
        """
        schema = f"{schema_base}_{schema_suffix}"
        dataset_id = self.find_dataset(database_id, schema, table_name)
        if dataset_id is not None:
            return dataset_id
        return self.create_dataset(database_id, schema, table_name)

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
