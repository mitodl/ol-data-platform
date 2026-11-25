"""Publish, update, inspect and unpublish notebooks for a hub user."""

import copy
import re
import secrets
import time
from datetime import UTC, datetime
from typing import Any

from kubernetes import client
from pydantic import BaseModel

from notebook_publisher import render
from notebook_publisher.cluster import Cluster
from notebook_publisher.settings import Settings
from notebook_publisher.validation import (
    NotebookPublish,
    NotebookUpdate,
    check_notebook,
)

HTTP_CONFLICT = 409
LOG_TAIL_LINES = 100
# Container states that won't recover by waiting.
FAILED_WAITING_REASONS = frozenset(
    {
        "CrashLoopBackOff",
        "CreateContainerConfigError",
        "CreateContainerError",
        "ErrImagePull",
        "ImagePullBackOff",
        "InvalidImageName",
        "RunContainerError",
    }
)
# marimo prints its startup URL with the token password in the query string.
ACCESS_TOKEN_IN_URL = re.compile(r"(access_token=)[^&\s]+")


class NotebookNotFoundError(Exception):
    """No published notebook has that name."""


class NotOwnerError(Exception):
    """The caller didn't publish this notebook."""


class NameTakenError(Exception):
    """Another notebook already has that name."""


class ConcurrentChangeError(Exception):
    """The notebook changed while this request was updating it."""


class RolloutFailedError(Exception):
    """The new revision didn't start serving."""

    def __init__(self, message: str, logs: str) -> None:
        super().__init__(message)
        self.logs = logs


class PodMismatchError(Exception):
    """The operator's pod differs from what the publisher rendered."""


class NotebookSummary(BaseModel):
    """A published notebook, as listed for its owner."""

    name: str
    url: str
    access: str
    revision: str
    published_at: str
    phase: str | None
    git_remote: str | None
    git_commit: str | None


class NotebookStatus(NotebookSummary):
    """A published notebook with its pod's state, for its owner."""

    pod_phase: str | None
    ready: bool
    restarts: int
    events: list[str]
    logs: str


def redact(logs: str) -> str:
    """Remove the app's marimo token from its log output."""
    return ACCESS_TOKEN_IN_URL.sub(r"\1<redacted>", logs)


def _auth_secret(pod: dict[str, Any]) -> str | None:
    for volume in pod["spec"].get("volumes", []):
        if volume["name"] == render.AUTH_VOLUME:
            return str(volume["secret"]["secretName"])
    return None


def _container_statuses(pod: dict[str, Any]) -> list[dict[str, Any]]:
    status = pod.get("status", {})
    return [
        *status.get("initContainerStatuses", []),
        *status.get("containerStatuses", []),
    ]


def _failed_reason(pod: dict[str, Any]) -> str | None:
    if pod.get("status", {}).get("phase") == "Failed":
        return "pod failed"
    for container in _container_statuses(pod):
        waiting = container.get("state", {}).get("waiting") or {}
        if waiting.get("reason") in FAILED_WAITING_REASONS:
            message = waiting.get("message", "")
            return f"{container['name']}: {waiting['reason']} {message}".strip()
    return None


def _ready(pod: dict[str, Any]) -> bool:
    return any(
        condition["type"] == "Ready" and condition["status"] == "True"
        for condition in pod.get("status", {}).get("conditions", [])
    )


def pod_problems(
    pod: dict[str, Any], name: str, revision: str, settings: Settings
) -> list[str]:
    """Compare the operator's pod with what the publisher asked for.

    The operator applies podOverrides with a strategic merge and silently falls
    back to its unpatched pod if the merge fails, so the result has to be read
    back rather than assumed.

    :returns: One message per difference; empty when the pod is as rendered.
    :rtype: list[str]
    """
    spec = pod["spec"]
    problems = []
    if spec.get("serviceAccountName") != settings.service_account:
        problems.append(f"serviceAccountName is {spec.get('serviceAccountName')}")
    if spec.get("automountServiceAccountToken") is not False:
        problems.append("the ServiceAccount token is mounted")
    containers = spec["containers"]
    if [container["name"] for container in containers] != [render.MARIMO_CONTAINER]:
        problems.append(f"containers are {[c['name'] for c in containers]}")
    marimo = containers[0]
    if marimo.get("args") != render.marimo_args(name, settings):
        problems.append(f"marimo args are {marimo.get('args')}")
    if marimo.get("image") != settings.runtime_image:
        problems.append(f"marimo image is {marimo.get('image')}")
    volumes = {volume["name"] for volume in spec.get("volumes", [])}
    if volumes != render.EXPECTED_VOLUMES:
        problems.append(f"volumes are {sorted(volumes)}")
    if _auth_secret(pod) != render.token_secret_name(name, revision):
        problems.append("the token volume is from another revision")
    for container in [*spec.get("initContainers", []), *containers]:
        context = container.get("securityContext") or {}
        if context.get("allowPrivilegeEscalation") is not False or "ALL" not in (
            context.get("capabilities", {}).get("drop", [])
        ):
            problems.append(f"{container['name']} isn't restricted")
    return problems


def _summary(notebook: dict[str, Any], settings: Settings) -> NotebookSummary:
    metadata = notebook["metadata"]
    annotations = metadata["annotations"]
    return NotebookSummary(
        name=metadata["name"],
        url=render.app_url(metadata["name"], settings),
        access=metadata["labels"][render.ACCESS_LABEL],
        revision=annotations[render.REVISION_ANNOTATION],
        published_at=annotations[render.PUBLISHED_AT_ANNOTATION],
        phase=notebook.get("status", {}).get("phase"),
        git_remote=annotations.get(render.GIT_REMOTE_ANNOTATION),
        git_commit=annotations.get(render.GIT_COMMIT_ANNOTATION),
    )


class Publisher:
    """Carries out each request against the cluster."""

    def __init__(self, cluster: Cluster, settings: Settings) -> None:
        self.cluster = cluster
        self.settings = settings

    def _owned(self, name: str, user: str) -> dict[str, Any]:
        notebook = self.cluster.get_notebook(name)
        if notebook is None:
            raise NotebookNotFoundError(name)
        if notebook["metadata"]["annotations"][render.OWNER_ANNOTATION] != user:
            raise NotOwnerError(name)
        return notebook

    def _create_secrets(self, name: str, revision: str, owner: dict[str, Any]) -> None:
        token = secrets.token_urlsafe(32)
        for secret in render.render_secrets(
            name, revision, token, owner, self.settings
        ):
            self.cluster.create_secret(secret)

    def _delete_secrets(self, name: str, revision: str) -> None:
        self.cluster.delete_secret(render.token_secret_name(name, revision))
        self.cluster.delete_secret(render.route_auth_secret_name(name, revision))

    def _wait_for_rollout(self, name: str, revision: str) -> None:
        """Wait until the revision's pod is as rendered and marimo answers.

        :raises RolloutFailedError: If it fails or doesn't serve in time.
        :raises PodMismatchError: If the pod isn't what was rendered.
        """
        token_secret = render.token_secret_name(name, revision)
        deadline = time.monotonic() + self.settings.rollout_timeout_seconds
        while True:
            pod = self.cluster.get_pod(name)
            if pod is not None and _auth_secret(pod) == token_secret:
                if problems := pod_problems(pod, name, revision, self.settings):
                    raise PodMismatchError("; ".join(problems))
                if reason := _failed_reason(pod):
                    raise RolloutFailedError(reason, self._logs(name))
                if _ready(pod) and self.cluster.app_healthy(
                    name, self.settings.marimo_port
                ):
                    return
            if time.monotonic() > deadline:
                message = (
                    "The notebook didn't start serving within "
                    f"{self.settings.rollout_timeout_seconds}s."
                )
                raise RolloutFailedError(message, self._logs(name))
            time.sleep(self.settings.rollout_poll_seconds)

    def _logs(self, name: str) -> str:
        return redact(self.cluster.pod_logs(name, LOG_TAIL_LINES))

    def _apply_route(self, name: str, revision: str, owner: dict[str, Any]) -> None:
        route = render.render_route(name, revision, owner, self.settings)
        existing = self.cluster.get_route(name)
        if existing is None:
            self.cluster.create_route(route)
        else:
            route["metadata"]["resourceVersion"] = existing["metadata"][
                "resourceVersion"
            ]
            self.cluster.replace_route(route)

    def publish(self, user: str, request: NotebookPublish) -> NotebookStatus:
        """Publish a notebook under a new name.

        The route is created only once the app serves, and a failed publish
        leaves nothing behind.
        """
        check_notebook(request.content, self.settings)
        revision = secrets.token_hex(4)
        body = render.render_notebook(
            request.name,
            user,
            request,
            revision,
            datetime.now(tz=UTC).isoformat(),
            self.settings,
        )
        try:
            notebook = self.cluster.create_notebook(body)
        except client.ApiException as error:
            if error.status == HTTP_CONFLICT:
                raise NameTakenError(request.name) from error
            raise
        owner = render.owner_reference(notebook)
        try:
            self._create_secrets(request.name, revision, owner)
            self._wait_for_rollout(request.name, revision)
            self._apply_route(request.name, revision, owner)
        except Exception:
            # ownerReferences take the Secrets and the route with it.
            self.cluster.delete_notebook(request.name)
            raise
        return self.status(user, request.name)

    def update(self, user: str, name: str, request: NotebookUpdate) -> NotebookStatus:
        """Publish a new version of the caller's notebook.

        The route moves to the new token only once the new pod serves. If it
        doesn't, the previous MarimoNotebook is put back and its token, which
        the route still uses, is left in place.
        """
        check_notebook(request.content, self.settings)
        current = self._owned(name, user)
        previous_revision = current["metadata"]["annotations"][
            render.REVISION_ANNOTATION
        ]
        revision = secrets.token_hex(4)
        owner = render.owner_reference(current)
        rendered = render.render_notebook(
            name,
            user,
            request,
            revision,
            datetime.now(tz=UTC).isoformat(),
            self.settings,
        )
        updated = copy.deepcopy(current)
        updated["spec"] = rendered["spec"]
        updated["metadata"]["labels"].update(rendered["metadata"]["labels"])
        updated["metadata"]["annotations"] = {
            **{
                key: value
                for key, value in current["metadata"]["annotations"].items()
                if key not in render.PUBLISHER_ANNOTATIONS
            },
            **rendered["metadata"]["annotations"],
        }

        self._create_secrets(name, revision, owner)
        try:
            self.cluster.replace_notebook(updated)
        except client.ApiException as error:
            self._delete_secrets(name, revision)
            if error.status == HTTP_CONFLICT:
                raise ConcurrentChangeError(name) from error
            raise
        try:
            self._wait_for_rollout(name, revision)
            self._apply_route(name, revision, owner)
        except Exception:
            self._restore(current)
            self._delete_secrets(name, revision)
            raise
        self._delete_secrets(name, previous_revision)
        return self.status(user, name)

    def _restore(self, previous: dict[str, Any]) -> None:
        latest = self.cluster.get_notebook(previous["metadata"]["name"])
        if latest is None:
            return
        restored = copy.deepcopy(previous)
        restored["metadata"]["resourceVersion"] = latest["metadata"]["resourceVersion"]
        self.cluster.replace_notebook(restored)

    def list_owned(self, user: str) -> list[NotebookSummary]:
        """Return the caller's published notebooks."""
        selector = f"{render.OWNER_HASH_LABEL}={render.owner_hash(user)}"
        return [
            _summary(notebook, self.settings)
            for notebook in self.cluster.list_notebooks(selector)
            if notebook["metadata"]["annotations"][render.OWNER_ANNOTATION] == user
        ]

    def status(self, user: str, name: str) -> NotebookStatus:
        """Return the caller's notebook with its pod's state, events and log."""
        notebook = self._owned(name, user)
        pod = self.cluster.get_pod(name)
        pod_status = (pod or {}).get("status", {})
        return NotebookStatus(
            **_summary(notebook, self.settings).model_dump(),
            pod_phase=pod_status.get("phase"),
            ready=pod is not None and _ready(pod),
            restarts=sum(
                container.get("restartCount", 0)
                for container in pod_status.get("containerStatuses", [])
            ),
            events=self.cluster.pod_events(name) if pod else [],
            logs=self._logs(name) if pod else "",
        )

    def unpublish(self, user: str, name: str) -> None:
        """Delete the caller's notebook and everything rendered for it."""
        self._owned(name, user)
        self.cluster.delete_notebook(name)
