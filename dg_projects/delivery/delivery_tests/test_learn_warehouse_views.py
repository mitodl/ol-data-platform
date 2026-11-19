"""Tests for the notice that tells MIT Learn its warehouse views were rebuilt."""

from types import SimpleNamespace
from typing import Any

import dagster as dg
import httpx2 as httpx
import pytest
from delivery.assets import learn_warehouse_views
from delivery.assets.learn_warehouse_views import (
    LEARN_WAREHOUSE_VIEWS,
    VIEW_KEYS,
    on_view_rebuilt,
    warehouse_views_rebuilt,
)
from ol_orchestrate.lib.failures import RETRY_ON_ASSET_OR_OP_FAILURE_TAG
from ol_orchestrate.lib.http_errors import PermanentHTTPFailure
from ol_orchestrate.resources.learn_api import MITLearnApiClient

from delivery_tests.learn_client import learn_client

OCW = "integrations__learn__ocw_courses"
XPRO = "integrations__learn__xpro_courses"
LAST = LEARN_WAREHOUSE_VIEWS[-1]
OP = "mit_learn_delivery__warehouse_views_rebuilt"
QUEUED = {"status": "success", "message": "Queued 1 sync(s): SyncOCWCoursesTask"}


def _rebuild(instance: dg.DagsterInstance, *views: str) -> None:
    for view in views:
        instance.report_runless_asset_event(
            dg.AssetMaterialization(dg.AssetKey(["integrations", view]))
        )


def _materialize(
    instance: dg.DagsterInstance,
    client: MITLearnApiClient,
    run_config: dict[str, Any] | None = None,
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [warehouse_views_rebuilt],
        instance=instance,
        resources={"learn_api": SimpleNamespace(client=client)},
        run_config=run_config or {},
        raise_on_error=False,
    )


def test_a_run_sends_only_the_views_rebuilt_since_the_last_one() -> None:
    instance = dg.DagsterInstance.ephemeral()
    client, sent = learn_client(httpx.Response(200, json=QUEUED))
    _rebuild(instance, OCW, XPRO)
    assert _materialize(instance, client).success
    _rebuild(instance, OCW)

    result = _materialize(instance, client)

    assert result.success
    assert sent == [{"views": [OCW, XPRO]}, {"views": [OCW]}]
    (materialization,) = result.asset_materializations_for_node(OP)
    assert materialization.metadata["learn_message"].value == QUEUED["message"]


def test_a_run_with_nothing_rebuilt_sends_nothing() -> None:
    instance = dg.DagsterInstance.ephemeral()
    client, sent = learn_client(httpx.Response(200, json=QUEUED))
    _rebuild(instance, OCW)
    assert _materialize(instance, client).success

    assert _materialize(instance, client).success
    assert sent == [{"views": [OCW]}]


def test_a_hand_run_can_name_every_view() -> None:
    instance = dg.DagsterInstance.ephemeral()
    client, sent = learn_client(httpx.Response(200, json=QUEUED))
    config = {"ops": {OP: {"config": {"all_views": True}}}}

    assert _materialize(instance, client, config).success
    assert sent == [{"views": list(LEARN_WAREHOUSE_VIEWS)}]


def test_a_view_rebuilt_while_a_run_posts_goes_in_the_next_run() -> None:
    instance = dg.DagsterInstance.ephemeral()

    def rebuild_during_first_post(posts: int) -> None:
        if posts == 1:
            _rebuild(instance, XPRO)

    client, sent = learn_client(
        httpx.Response(200, json=QUEUED), on_post=rebuild_during_first_post
    )
    _rebuild(instance, OCW)

    assert _materialize(instance, client).success
    assert _materialize(instance, client).success
    assert sent == [{"views": [OCW]}, {"views": [XPRO]}]


def test_a_failed_run_leaves_its_views_for_the_next() -> None:
    instance = dg.DagsterInstance.ephemeral()
    _rebuild(instance, OCW)
    refusing, _ = learn_client(httpx.Response(400, json={"views": ["bad"]}))
    assert not _materialize(instance, refusing).success

    client, sent = learn_client(httpx.Response(200, json=QUEUED))
    assert _materialize(instance, client).success
    assert sent == [{"views": [OCW]}]


def test_a_rejected_notice_fails_once_with_learns_reason() -> None:
    instance = dg.DagsterInstance.ephemeral()
    client, sent = learn_client(
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


def test_a_view_rebuilt_just_after_a_request_is_requested_again() -> None:
    """The reset and a rebuild on the same tick must not drop the rebuild."""
    instance = dg.DagsterInstance.ephemeral()
    counts = []
    requested, cursor = _requests(instance)
    counts.append(requested)
    _rebuild(instance, OCW)
    requested, cursor = _requests(instance, cursor)
    counts.append(requested)
    _rebuild(instance, XPRO)
    for _ in range(3):
        requested, cursor = _requests(instance, cursor)
        counts.append(requested)

    assert counts == [0, 1, 1, 0, 0]


def test_a_view_rebuilt_between_two_reads_goes_in_the_next_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later view's newer id must not cover an earlier view read before it."""
    instance = dg.DagsterInstance.ephemeral()
    read = learn_warehouse_views._latest_storage_id
    reads: list[dg.AssetKey] = []

    def read_then_rebuild(instance: dg.DagsterInstance, key: dg.AssetKey) -> int:
        storage_id = read(instance, key)
        reads.append(key)
        if len(reads) == 1:
            _rebuild(instance, OCW, LAST)
        return storage_id

    monkeypatch.setattr(learn_warehouse_views, "_latest_storage_id", read_then_rebuild)
    client, sent = learn_client(httpx.Response(200, json=QUEUED))

    assert _materialize(instance, client).success
    assert _materialize(instance, client).success
    # The first read was OCW's, before its rebuild, so the first run sends the
    # later view only, and the second run has to send OCW.
    assert next(iter(learn_warehouse_views.VIEW_KEYS.values())) == OCW
    assert sent[0] == {"views": [LAST]}
    assert OCW in sent[1]["views"]


def test_a_hand_reported_materialization_does_not_stop_the_asset() -> None:
    instance = dg.DagsterInstance.ephemeral()
    _rebuild(instance, OCW)
    instance.report_runless_asset_event(
        dg.AssetMaterialization(warehouse_views_rebuilt.key)
    )
    client, sent = learn_client(httpx.Response(200, json=QUEUED))

    assert _materialize(instance, client).success
    assert sent == [{"views": [OCW]}]


def test_a_failed_run_is_not_rerun_by_run_retries() -> None:
    """Step retries already cover a 5xx, so run retries would only multiply them.

    The tag goes on before the POST whatever it returns; a 400 keeps the test
    from sleeping through the step retries a 5xx gets.
    """
    instance = dg.DagsterInstance.ephemeral()
    _rebuild(instance, OCW)
    client, _ = learn_client(httpx.Response(400, json={"views": ["bad"]}))

    result = _materialize(instance, client)

    assert not result.success
    run = instance.get_run_by_id(result.run_id)
    assert run is not None
    assert run.tags[RETRY_ON_ASSET_OR_OP_FAILURE_TAG] == "false"
