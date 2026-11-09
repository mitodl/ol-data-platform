"""Tests for the mass-unpublish override and for what a refused batch reports."""

import json
from types import SimpleNamespace
from typing import Any

import dagster as dg
import httpx2 as httpx
import polars as pl
import pytest
from delivery.assets import mitpe
from delivery.assets.mit_climate import mit_climate_webhook
from delivery.assets.mitpe import mitpe_webhook
from delivery.assets.open_learning_library import oll_webhook
from delivery.lib.learn_webhook import webhook_failure
from ol_orchestrate.lib.http_errors import PermanentHTTPFailure, TransientHTTPFailure
from ol_orchestrate.resources.learn_api import MITLearnApiClient

REFUSAL = "Refusing to sync: 44 of 58 published oll course resources would unpublish"
MITPE_OP = "mit_learn_delivery__mitpe_webhook"

_MITPE_COURSE: dict[str, Any] = {
    "readable_id": "course-1",
    "etl_source": "mitpe",
    "platform": "mitpe",
    "resource_type": "course",
    "title": "A course",
    "url": "https://professional.mit.edu/a-course",
    "image_url": None,
    "image_alt": None,
    "description": "About",
    "delivery": "online",
    "price": None,
    "instructors": [],
    "location": "",
    "duration": "",
    "min_weeks": None,
    "max_weeks": None,
    "topics": [],
}
_EMPTY_RUNS: dict[str, list[Any]] = {
    "readable_id": [],
    "run_id": [],
    "run_position": [],
}


def _learn_client(
    response: httpx.Response,
) -> tuple[MITLearnApiClient, list[dict[str, Any]]]:
    """Build a real client whose requests get ``response``, recording each body."""
    sent: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        return response

    client = MITLearnApiClient(base_url="https://learn.example.com", token="t")
    client._http_client = httpx.Client(transport=httpx.MockTransport(handler))
    return client, sent


def _mitpe_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve one course and no programs or runs in place of the Glue tables."""
    tables = {
        "integrations__learn__mitpe_courses": pl.DataFrame([_MITPE_COURSE]),
        "integrations__learn__mitpe_programs": pl.DataFrame(),
        "integrations__learn__mitpe_runs": pl.DataFrame(_EMPTY_RUNS),
    }
    monkeypatch.setattr(
        mitpe,
        "get_dbt_model_as_dataframe",
        lambda database_name, table_name: tables[table_name].lazy(),  # noqa: ARG005
    )


def _materialize_mitpe(
    client: MITLearnApiClient, run_config: dict[str, Any] | None = None
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [mitpe_webhook],
        resources={"learn_api": SimpleNamespace(client=client)},
        run_config=run_config or {},
    )


@pytest.mark.parametrize(
    "webhook",
    [mit_climate_webhook, mitpe_webhook, oll_webhook],
)
def test_every_delivery_asset_defaults_the_override_off(
    webhook: dg.AssetsDefinition,
) -> None:
    """A sensor run passes no run config, so the default is what it sends."""
    field = webhook.op.config_schema.config_type.fields["allow_mass_unpublish"]
    assert field.default_value is False


def test_a_run_with_no_config_does_not_send_the_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The run a delivery sensor launches, which carries no run config."""
    _mitpe_tables(monkeypatch)
    client, sent = _learn_client(httpx.Response(200, json={"status": "success"}))

    assert _materialize_mitpe(client).success
    assert "allow_mass_unpublish" not in sent[0]


def test_a_hand_materialization_can_send_the_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The run config in CONTRIBUTING.md reaches the request body."""
    _mitpe_tables(monkeypatch)
    client, sent = _learn_client(httpx.Response(200, json={"status": "success"}))

    result = _materialize_mitpe(
        client, {"ops": {MITPE_OP: {"config": {"allow_mass_unpublish": True}}}}
    )

    assert result.success
    assert sent[0]["allow_mass_unpublish"] is True


def test_a_refused_batch_fails_once_with_learns_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Materialized with the asset's RetryPolicy: one POST, and the reason is shown."""
    _mitpe_tables(monkeypatch)
    client, sent = _learn_client(
        httpx.Response(422, json={"status": "error", "message": REFUSAL})
    )

    result = dg.materialize(
        [mitpe_webhook],
        resources={"learn_api": SimpleNamespace(client=client)},
        raise_on_error=False,
    )

    assert not result.success
    assert len(sent) == 1
    [failure] = result.get_step_failure_events()
    message = failure.step_failure_data.error.message
    assert REFUSAL in message
    assert "HTTP 422" in message


def _status_error(status_code: int, body: str) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://learn.example.com/api/v1/webhooks/")
    response = httpx.Response(status_code, text=body, request=request)
    return httpx.HTTPStatusError("failed", request=request, response=response)


def test_a_rejection_is_permanent_and_says_why() -> None:
    """The same batch gets the same answer, so a retry only repeats the request."""
    failure = webhook_failure("OLL", _status_error(409, "The webhook does not own"))
    assert isinstance(failure, PermanentHTTPFailure)
    assert "The webhook does not own" in failure.description


def test_a_server_error_retries_and_keeps_its_page_out_of_the_description() -> None:
    """A 5xx's body is an error page, which belongs in metadata and not in an alert."""
    failure = webhook_failure("OLL", _status_error(502, "<html>Bad Gateway</html>"))
    assert isinstance(failure, TransientHTTPFailure)
    assert "<html>" not in failure.description
    assert failure.metadata["response_body"].text == "<html>Bad Gateway</html>"
