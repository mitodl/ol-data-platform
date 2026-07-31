"""Tests for the Superset dataset lookup/create client.

These cover the failure that motivated replacing Superset's
``/api/v1/dataset/get_or_create/`` endpoint with an explicit
find-then-create: that endpoint matches on ``(database_id, table_name)``
only, so two datasets sharing a table_name across schemas made it return
HTTP 500 ``MultipleResultsFound`` on every run (production run
06561830, 2026-07-29 -- Enrollment_Activity_Counts_Dataset and
combined_enrollments_with_gender_and_date failed 11 runs in a row).
"""

from typing import Any

import pytest
from lakehouse.resources.superset_api import SupersetApiClient

BASE_URL = "https://bi.example.edu"
DATASET_LIST_URL = f"{BASE_URL}/api/v1/dataset/"


class FakeResponse:
    def __init__(self, status_code: int, json_body=None, text: str = ""):
        self.status_code = status_code
        self._json_body = json_body
        self.text = text

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self):
        return self._json_body


class FakeHttpClient:
    """Records POSTs and replays a queued response for each."""

    def __init__(self, responses: list[FakeResponse]):
        self.responses = list(responses)
        self.posts: list[tuple[str, dict[str, Any]]] = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return self.responses.pop(0)


@pytest.fixture
def client(monkeypatch):
    """Build a SupersetApiClient with its auth seams stubbed out."""
    monkeypatch.setattr(
        SupersetApiClient, "_fetch_access_token", lambda _self: "access-token"
    )
    monkeypatch.setattr(
        SupersetApiClient, "_get_csrf_token", lambda _self: "csrf-token"
    )
    return SupersetApiClient(
        client_id="test-client",
        client_secret="test-secret",  # pragma: allowlist secret
        base_url=BASE_URL,
        token_url="https://sso.example.edu/token",
    )


def stub_list_responses(monkeypatch, *payloads):
    """Replay one dataset-list payload per fetch_with_auth call, recording queries."""
    queue = list(payloads)
    queries: list[tuple[str, str | None]] = []

    def fake_fetch_with_auth(_self, request_url, page_size=100, extra_params=None):  # noqa: ARG001
        queries.append((request_url, (extra_params or {}).get("q")))
        return queue.pop(0)

    monkeypatch.setattr(SupersetApiClient, "fetch_with_auth", fake_fetch_with_auth)
    return queries


class TestFindDataset:
    def test_filters_on_database_schema_and_table(self, client, monkeypatch):
        """The schema filter is the whole point -- without it Superset 500s."""
        queries = stub_list_responses(monkeypatch, {"count": 1, "ids": [42]})

        dataset_id = client.find_dataset(
            3, "ol_warehouse_production_dimensional", "tfact"
        )

        assert dataset_id == 42
        request_url, query = queries[0]
        assert request_url == DATASET_LIST_URL
        assert "(col:database,opr:rel_o_m,value:3)" in query
        assert (
            "(col:schema,opr:eq,value:'ol_warehouse_production_dimensional')" in query
        )
        assert "(col:table_name,opr:eq,value:'tfact')" in query

    def test_returns_none_when_no_dataset_matches(self, client, monkeypatch):
        stub_list_responses(monkeypatch, {"count": 0, "ids": []})

        assert client.find_dataset(1, "ol_warehouse_production_mart", "absent") is None

    def test_missing_ids_key_is_not_an_error(self, client, monkeypatch):
        stub_list_responses(monkeypatch, {"count": 0})

        assert client.find_dataset(1, "ol_warehouse_production_mart", "absent") is None

    def test_duplicate_matches_resolve_to_the_lowest_id(self, client, monkeypatch):
        """Duplicates within one schema must still pin to one dataset per run,
        whatever order the API returns them in.
        """
        stub_list_responses(monkeypatch, {"count": 2, "ids": [88, 42]})

        assert client.find_dataset(1, "ol_warehouse_production_reporting", "dupe") == 42

    def test_string_ids_still_resolve_to_the_numeric_minimum(self, client, monkeypatch):
        """Superset's list endpoint can return ids as strings; a naive min()
        over strings would pick "88" over "42" lexicographically.
        """
        stub_list_responses(monkeypatch, {"count": 2, "ids": ["88", "42"]})

        dataset_id = client.find_dataset(1, "ol_warehouse_production_reporting", "dupe")

        assert dataset_id == 42
        assert isinstance(dataset_id, int)


class TestCreateDataset:
    def test_posts_physical_dataset_and_returns_new_id(self, client, monkeypatch):
        http_client = FakeHttpClient([FakeResponse(201, {"id": 512})])
        monkeypatch.setattr(SupersetApiClient, "http_client", http_client)

        result = client.create_dataset(3, "ol_warehouse_production_reporting", "report")

        assert result == 512
        url, kwargs = http_client.posts[0]
        assert url == DATASET_LIST_URL
        assert kwargs["json"] == {
            "database": 3,
            "schema": "ol_warehouse_production_reporting",
            "table_name": "report",
        }
        assert kwargs["headers"]["X-CSRFToken"] == "csrf-token"

    def test_unprocessable_table_is_reported_as_absent(self, client, monkeypatch):
        """422 for a model with no table in this database (e.g. a Trino-only
        model whose StarRocks twin was never built) is a skip, not a failure.
        """
        http_client = FakeHttpClient(
            [FakeResponse(422, {"message": "Table does not exist"})]
        )
        monkeypatch.setattr(SupersetApiClient, "http_client", http_client)
        stub_list_responses(monkeypatch, {"count": 0, "ids": []})

        result = client.create_dataset(3, "ol_warehouse_production_mart", "trino_only")

        assert result is None

    def test_lost_create_race_resolves_to_the_winners_dataset(
        self, client, monkeypatch
    ):
        http_client = FakeHttpClient(
            [FakeResponse(422, {"message": "Dataset already exists"})]
        )
        monkeypatch.setattr(SupersetApiClient, "http_client", http_client)
        stub_list_responses(monkeypatch, {"count": 1, "ids": [77]})

        assert client.create_dataset(1, "ol_warehouse_production_mart", "raced") == 77

    def test_server_error_raises(self, client, monkeypatch):
        http_client = FakeHttpClient(
            [FakeResponse(500, text='{"message":"Fatal error"}')]
        )
        monkeypatch.setattr(SupersetApiClient, "http_client", http_client)

        with pytest.raises(RuntimeError, match="Failed to create dataset"):
            client.create_dataset(1, "ol_warehouse_production_mart", "broken")


class TestGetOrCreateDataset:
    def test_existing_dataset_short_circuits_before_creating(self, client, monkeypatch):
        http_client = FakeHttpClient([])  # a POST would IndexError
        monkeypatch.setattr(SupersetApiClient, "http_client", http_client)
        queries = stub_list_responses(monkeypatch, {"count": 1, "ids": [9]})

        result = client.get_or_create_dataset(
            schema_suffix="dimensional",
            table_name="tfact_order",
            database_id=3,
        )

        assert result == 9
        assert http_client.posts == []
        assert "value:'ol_warehouse_production_dimensional'" in queries[0][1]

    def test_absent_dataset_is_created(self, client, monkeypatch):
        http_client = FakeHttpClient([FakeResponse(201, {"id": 1024})])
        monkeypatch.setattr(SupersetApiClient, "http_client", http_client)
        stub_list_responses(monkeypatch, {"count": 0, "ids": []})

        result = client.get_or_create_dataset(
            schema_suffix="reporting",
            table_name="organization_administration_report",
            database_id=3,
        )

        assert result == 1024
        _url, kwargs = http_client.posts[0]
        assert kwargs["json"]["schema"] == "ol_warehouse_production_reporting"

    def test_schema_base_override_is_honoured(self, client, monkeypatch):
        queries = stub_list_responses(monkeypatch, {"count": 1, "ids": [5]})

        client.get_or_create_dataset(
            schema_suffix="mart",
            table_name="some_model",
            database_id=1,
            schema_base="ol_warehouse_qa",
        )

        assert "value:'ol_warehouse_qa_mart'" in queries[0][1]
