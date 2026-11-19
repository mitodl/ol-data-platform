"""Tests for the notice that tells MIT Learn its warehouse views were rebuilt."""

import json
from types import SimpleNamespace
from typing import Any

import dagster as dg
import httpx2 as httpx
from delivery.assets.learn_warehouse_views import (
    LEARN_WAREHOUSE_VIEWS,
    VIEW_KEYS,
    on_view_rebuilt,
    warehouse_views_rebuilt,
)
from ol_orchestrate.lib.http_errors import PermanentHTTPFailure
from ol_orchestrate.resources.learn_api import MITLearnApiClient

OCW = "integrations__learn__ocw_courses"
XPRO = "integrations__learn__xpro_courses"
QUEUED = {"status": "success", "message": "Queued 1 sync(s): SyncOCWCoursesTask"}


def _learn_client(
    response: httpx.Response,
) -> tuple[MITLearnApiClient, list[dict[str, Any]]]:
    sent: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        return response

    client = MITLearnApiClient(base_url="https://learn.example.com", token="t")
    client._http_client = httpx.Client(transport=httpx.MockTransport(handler))
    return client, sent


def _rebuild(instance: dg.DagsterInstance, *views: str) -> None:
    for view in views:
        instance.report_runless_asset_event(
            dg.AssetMaterialization(dg.AssetKey(["integrations", view]))
        )


def _materialize(
    instance: dg.DagsterInstance, client: MITLearnApiClient
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [warehouse_views_rebuilt],
        instance=instance,
        resources={"learn_api": SimpleNamespace(client=client)},
        raise_on_error=False,
    )


def test_a_run_sends_only_the_views_rebuilt_since_the_last_one() -> None:
    instance = dg.DagsterInstance.ephemeral()
    client, sent = _learn_client(httpx.Response(200, json=QUEUED))
    _rebuild(instance, OCW, XPRO)
    assert _materialize(instance, client).success
    _rebuild(instance, OCW)

    result = _materialize(instance, client)

    assert result.success
    assert sent == [{"views": [OCW, XPRO]}, {"views": [OCW]}]
    (materialization,) = result.asset_materializations_for_node(
        "mit_learn_delivery__warehouse_views_rebuilt"
    )
    assert materialization.metadata["learn_message"].value == QUEUED["message"]


def test_a_run_with_nothing_rebuilt_sends_every_view() -> None:
    """Only a hand run gets here, and it asks Learn to resync everything."""
    instance = dg.DagsterInstance.ephemeral()
    client, sent = _learn_client(httpx.Response(200, json=QUEUED))

    assert _materialize(instance, client).success
    assert sent == [{"views": list(LEARN_WAREHOUSE_VIEWS)}]


def test_a_rejected_notice_fails_once_with_learns_reason() -> None:
    instance = dg.DagsterInstance.ephemeral()
    client, sent = _learn_client(
        httpx.Response(400, json={"views": ["This list may not be empty."]})
    )
    _rebuild(instance, OCW)

    result = _materialize(instance, client)

    assert not result.success
    assert len(sent) == 1
    (failure,) = result.get_step_failure_events()
    error = failure.step_failure_data.error
    assert error is not None
    assert "This list may not be empty." in (error.message or "")
    assert PermanentHTTPFailure.__name__ in (error.cls_name or "")


def _requests(instance: dg.DagsterInstance, cursor: Any = None) -> tuple[int, Any]:
    conditioned = warehouse_views_rebuilt.map_asset_specs(
        lambda spec: spec.replace_attributes(automation_condition=on_view_rebuilt())
    )
    result = dg.evaluate_automation_conditions(
        defs=dg.Definitions(
            assets=[conditioned, *(dg.AssetSpec(key) for key in VIEW_KEYS)],
            resources={"learn_api": dg.ResourceDefinition.none_resource()},
        ),
        instance=instance,
        asset_selection=dg.AssetSelection.assets(conditioned),
        cursor=cursor,
    )
    return result.get_num_requested(conditioned.key), result.cursor


def test_enabling_the_sensor_sends_nothing_until_a_view_is_rebuilt() -> None:
    instance = dg.DagsterInstance.ephemeral()
    _rebuild(instance, OCW)

    requested, cursor = _requests(instance)
    assert requested == 0
    requested, cursor = _requests(instance, cursor)
    assert requested == 0

    _rebuild(instance, OCW, XPRO)
    requested, cursor = _requests(instance, cursor)
    assert requested == 1
    requested, _ = _requests(instance, cursor)
    assert requested == 0
