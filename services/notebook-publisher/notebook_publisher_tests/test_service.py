from typing import Any

import pytest
from kubernetes import client
from notebook_publisher import render
from notebook_publisher.service import (
    ConcurrentChangeError,
    NameTakenError,
    NotebookNotFoundError,
    NotOwnerError,
    PodMismatchError,
    Publisher,
    RolloutFailedError,
)
from notebook_publisher.settings import Settings
from notebook_publisher.validation import (
    NotebookPublish,
    NotebookRejectedError,
    NotebookUpdate,
)

from notebook_publisher_tests.conftest import NOTEBOOK, FakeCluster

ALICE = "alice@mit.edu"
BOB = "bob@mit.edu"


def _revision(cluster: FakeCluster, name: str) -> str:
    return cluster.notebooks[name]["metadata"]["annotations"][
        render.REVISION_ANNOTATION
    ]


def _crash(pod: dict[str, Any]) -> dict[str, Any]:
    pod["status"]["containerStatuses"][0]["state"] = {
        "waiting": {"reason": "CrashLoopBackOff", "message": "back-off"}
    }
    return pod


def test_publish_serves_the_notebook_behind_a_route(
    publisher: Publisher, cluster: FakeCluster
) -> None:
    result = publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))

    assert result.url == "https://apps.nb-ci.data.ol.mit.edu/q3/"
    assert result.ready
    revision = _revision(cluster, "q3")
    assert set(cluster.secrets) == {f"q3-token-{revision}", f"q3-route-auth-{revision}"}
    route_plugins = cluster.routes["q3"]["spec"]["http"][0]["plugins"]
    assert route_plugins[1]["secretRef"] == f"q3-route-auth-{revision}"
    assert "<redacted>" in result.logs
    assert "s3cret-token" not in result.logs


def test_publish_refuses_a_taken_name(publisher: Publisher) -> None:
    publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))
    with pytest.raises(NameTakenError):
        publisher.publish(BOB, NotebookPublish(name="q3", content=NOTEBOOK))


def test_failed_publish_leaves_nothing_behind(
    publisher: Publisher, cluster: FakeCluster
) -> None:
    cluster.pod_hook = _crash
    with pytest.raises(RolloutFailedError) as raised:
        publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))

    assert "CrashLoopBackOff" in str(raised.value)
    assert "no solution found" in raised.value.logs
    assert not cluster.notebooks
    assert not cluster.secrets
    assert not cluster.routes


def test_publish_times_out_while_marimo_is_unhealthy(
    publisher: Publisher, cluster: FakeCluster
) -> None:
    cluster.healthy = False
    with pytest.raises(RolloutFailedError, match="didn't start serving"):
        publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))
    assert not cluster.notebooks


def test_publish_refuses_a_pod_the_overrides_missed(
    publisher: Publisher, cluster: FakeCluster
) -> None:
    def unpatched(pod: dict[str, Any]) -> dict[str, Any]:
        pod["spec"]["serviceAccountName"] = "default"
        pod["spec"]["volumes"].append({"name": "extra", "secret": {"secretName": "x"}})
        return pod

    cluster.pod_hook = unpatched
    with pytest.raises(PodMismatchError, match=r"serviceAccountName.*volumes"):
        publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))
    assert not cluster.notebooks


def test_update_rotates_the_token_and_drops_the_old_one(
    publisher: Publisher, cluster: FakeCluster
) -> None:
    publisher.publish(
        ALICE, NotebookPublish(name="q3", content=NOTEBOOK, git_commit="abc1234")
    )
    first = _revision(cluster, "q3")
    new_content = NOTEBOOK.replace(
        "app = marimo.App()", 'app = marimo.App(width="full")'
    )

    publisher.update(ALICE, "q3", NotebookUpdate(content=new_content))

    second = _revision(cluster, "q3")
    assert second != first
    assert set(cluster.secrets) == {f"q3-token-{second}", f"q3-route-auth-{second}"}
    notebook = cluster.notebooks["q3"]
    assert notebook["spec"]["content"] == new_content
    # Provenance belongs to one revision.
    assert render.GIT_COMMIT_ANNOTATION not in notebook["metadata"]["annotations"]
    route_plugins = cluster.routes["q3"]["spec"]["http"][0]["plugins"]
    assert route_plugins[1]["secretRef"] == f"q3-route-auth-{second}"


def test_failed_update_restores_the_previous_version(
    publisher: Publisher, cluster: FakeCluster
) -> None:
    publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))
    first = _revision(cluster, "q3")
    route_before = cluster.routes["q3"]["spec"]
    cluster.pod_hook = _crash

    with pytest.raises(RolloutFailedError):
        publisher.update(ALICE, "q3", NotebookUpdate(content=NOTEBOOK + "\n"))

    assert _revision(cluster, "q3") == first
    assert cluster.notebooks["q3"]["spec"]["content"] == NOTEBOOK
    assert set(cluster.secrets) == {f"q3-token-{first}", f"q3-route-auth-{first}"}
    assert cluster.routes["q3"]["spec"] == route_before


def test_only_the_owner_can_change_or_read_a_notebook(publisher: Publisher) -> None:
    publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))
    with pytest.raises(NotOwnerError):
        publisher.update(BOB, "q3", NotebookUpdate(content=NOTEBOOK))
    with pytest.raises(NotOwnerError):
        publisher.unpublish(BOB, "q3")
    with pytest.raises(NotOwnerError):
        publisher.status(BOB, "q3")
    with pytest.raises(NotebookNotFoundError):
        publisher.status(ALICE, "nope")


def test_list_shows_only_the_callers_notebooks(publisher: Publisher) -> None:
    publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))
    publisher.publish(BOB, NotebookPublish(name="q4", content=NOTEBOOK))
    assert [n.name for n in publisher.list_owned(ALICE)] == ["q3"]
    assert [n.name for n in publisher.list_owned(BOB)] == ["q4"]


def test_unpublish_removes_everything(
    publisher: Publisher, cluster: FakeCluster
) -> None:
    publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))
    publisher.unpublish(ALICE, "q3")
    assert not cluster.notebooks
    assert not cluster.secrets
    assert not cluster.routes


def test_a_superseded_update_does_not_restore_over_the_newer_one(
    publisher: Publisher, cluster: FakeCluster
) -> None:
    publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))
    newer: dict[str, Any] = {}

    def superseded_by_another_update(pod: dict[str, Any]) -> dict[str, Any]:
        # While this update waits, a second one moves the app to revision r9.
        notebook = cluster.notebooks["q3"]
        if not newer:
            newer["content"] = "second update"
            notebook["spec"]["content"] = "second update"
            notebook["metadata"]["annotations"][render.REVISION_ANNOTATION] = "r9"
        # The operator's pod now carries the newer revision's token.
        for volume in pod["spec"]["volumes"]:
            if volume["name"] == render.AUTH_VOLUME:
                volume["secret"]["secretName"] = (
                    "q3-token-r9"  # pragma: allowlist secret
                )
        return pod

    cluster.pod_hook = superseded_by_another_update
    with pytest.raises(ConcurrentChangeError):
        publisher.update(ALICE, "q3", NotebookUpdate(content=NOTEBOOK + "\n"))

    assert _revision(cluster, "q3") == "r9"
    assert cluster.notebooks["q3"]["spec"]["content"] == "second update"


def test_failed_publish_leaves_a_recreated_app_alone(
    publisher: Publisher, cluster: FakeCluster
) -> None:
    def recreated_by_someone_else(pod: dict[str, Any]) -> dict[str, Any]:
        # The name is unpublished and published again while this publish waits.
        cluster.notebooks["q3"]["metadata"]["uid"] = "someone-elses"
        return _crash(pod)

    cluster.pod_hook = recreated_by_someone_else
    with pytest.raises(RolloutFailedError):
        publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))
    assert cluster.notebooks["q3"]["metadata"]["uid"] == "someone-elses"


def test_cleanup_failures_keep_the_original_error(
    publisher: Publisher, cluster: FakeCluster, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_delete(*_: str) -> None:
        raise RuntimeError

    cluster.pod_hook = _crash
    monkeypatch.setattr(cluster, "delete_notebook", broken_delete)
    with pytest.raises(RolloutFailedError):
        publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))


def test_publish_caps_notebooks_per_user(
    publisher: Publisher, settings: Settings
) -> None:
    for index in range(settings.max_notebooks_per_user):
        publisher.publish(ALICE, NotebookPublish(name=f"n{index}", content=NOTEBOOK))
    with pytest.raises(NotebookRejectedError, match="most allowed"):
        publisher.publish(ALICE, NotebookPublish(name="one-more", content=NOTEBOOK))
    publisher.publish(BOB, NotebookPublish(name="bobs", content=NOTEBOOK))


def test_restore_retries_a_conflict_from_a_status_write(
    publisher: Publisher, cluster: FakeCluster, monkeypatch: pytest.MonkeyPatch
) -> None:
    publisher.publish(ALICE, NotebookPublish(name="q3", content=NOTEBOOK))
    first = _revision(cluster, "q3")
    replace = cluster.replace_notebook
    conflicts = iter([False, True])  # the update's replace, then the restore's

    def conflict_once(body: dict[str, Any]) -> dict[str, Any]:
        if next(conflicts, False):
            raise client.ApiException(status=409)
        return replace(body)

    monkeypatch.setattr(cluster, "replace_notebook", conflict_once)
    cluster.pod_hook = _crash
    with pytest.raises(RolloutFailedError):
        publisher.update(ALICE, "q3", NotebookUpdate(content=NOTEBOOK + "\n"))
    assert _revision(cluster, "q3") == first
