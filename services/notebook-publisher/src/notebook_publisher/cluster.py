"""The Kubernetes calls the publisher makes, in its one namespace.

Its Role allows create and delete on Secrets but never get or list, so the
publisher can write each app's token and then can't read any token back.
"""

import urllib.error
import urllib.request
from typing import Any

from kubernetes import client

from notebook_publisher.render import (
    APISIX_GROUP,
    APISIX_ROUTE_PLURAL,
    APISIX_VERSION,
    MARIMO_GROUP,
    MARIMO_PLURAL,
    MARIMO_VERSION,
)

HTTP_NOT_FOUND = 404
HTTP_OK = 200
HEALTH_TIMEOUT_SECONDS = 5


class Cluster:
    """Reads and writes one namespace's published-notebook objects."""

    def __init__(self, api_client: client.ApiClient, namespace: str) -> None:
        # The client's generated types say custom objects are `object` and
        # bodies are models; it returns and accepts plain dicts.
        self.api_client: Any = api_client
        self.namespace = namespace
        self.core: Any = client.CoreV1Api(api_client)
        self.custom: Any = client.CustomObjectsApi(api_client)

    def _get_custom(self, group: str, version: str, plural: str, name: str) -> Any:
        try:
            return self.custom.get_namespaced_custom_object(
                group, version, self.namespace, plural, name
            )
        except client.ApiException as error:
            if error.status == HTTP_NOT_FOUND:
                return None
            raise

    def get_notebook(self, name: str) -> dict[str, Any] | None:
        """Return a MarimoNotebook, or None if there is none by that name."""
        return self._get_custom(MARIMO_GROUP, MARIMO_VERSION, MARIMO_PLURAL, name)

    def list_notebooks(self, label_selector: str) -> list[dict[str, Any]]:
        """Return the MarimoNotebooks matching a label selector."""
        result = self.custom.list_namespaced_custom_object(
            MARIMO_GROUP,
            MARIMO_VERSION,
            self.namespace,
            MARIMO_PLURAL,
            label_selector=label_selector,
        )
        return list(result["items"])

    def create_notebook(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create a MarimoNotebook; raises ApiException 409 if the name exists."""
        return self.custom.create_namespaced_custom_object(
            MARIMO_GROUP, MARIMO_VERSION, self.namespace, MARIMO_PLURAL, body
        )

    def replace_notebook(self, body: dict[str, Any]) -> dict[str, Any]:
        """Replace a MarimoNotebook; 409 if its resourceVersion is stale."""
        return self.custom.replace_namespaced_custom_object(
            MARIMO_GROUP,
            MARIMO_VERSION,
            self.namespace,
            MARIMO_PLURAL,
            body["metadata"]["name"],
            body,
        )

    def delete_notebook(self, name: str) -> None:
        """Delete a MarimoNotebook; ownerReferences collect everything else."""
        try:
            self.custom.delete_namespaced_custom_object(
                MARIMO_GROUP,
                MARIMO_VERSION,
                self.namespace,
                MARIMO_PLURAL,
                name,
                propagation_policy="Background",
            )
        except client.ApiException as error:
            if error.status != HTTP_NOT_FOUND:
                raise

    def get_route(self, name: str) -> dict[str, Any] | None:
        """Return an ApisixRoute, or None if there is none by that name."""
        return self._get_custom(APISIX_GROUP, APISIX_VERSION, APISIX_ROUTE_PLURAL, name)

    def create_route(self, body: dict[str, Any]) -> None:
        """Create an ApisixRoute."""
        self.custom.create_namespaced_custom_object(
            APISIX_GROUP, APISIX_VERSION, self.namespace, APISIX_ROUTE_PLURAL, body
        )

    def replace_route(self, body: dict[str, Any]) -> None:
        """Replace an ApisixRoute; body carries the current resourceVersion."""
        self.custom.replace_namespaced_custom_object(
            APISIX_GROUP,
            APISIX_VERSION,
            self.namespace,
            APISIX_ROUTE_PLURAL,
            body["metadata"]["name"],
            body,
        )

    def create_secret(self, body: dict[str, Any]) -> None:
        """Create a Secret."""
        self.core.create_namespaced_secret(self.namespace, body)

    def delete_secret(self, name: str) -> None:
        """Delete a Secret if it exists."""
        try:
            self.core.delete_namespaced_secret(name, self.namespace)
        except client.ApiException as error:
            if error.status != HTTP_NOT_FOUND:
                raise

    def get_pod(self, name: str) -> dict[str, Any] | None:
        """Return a Pod as its API JSON, or None if there is none by that name."""
        try:
            pod = self.core.read_namespaced_pod(name, self.namespace)
        except client.ApiException as error:
            if error.status == HTTP_NOT_FOUND:
                return None
            raise
        return self.api_client.sanitize_for_serialization(pod)

    def pod_logs(self, name: str, tail_lines: int) -> str:
        """Return the end of the marimo container's log, or "" if there is none."""
        try:
            return self.core.read_namespaced_pod_log(
                name, self.namespace, container="marimo", tail_lines=tail_lines
            )
        except client.ApiException as error:
            # No pod yet, or the container hasn't started.
            if error.status in {HTTP_NOT_FOUND, 400}:
                return ""
            raise

    def pod_events(self, name: str) -> list[str]:
        """Return the events recorded against a Pod, oldest first."""
        events = self.core.list_namespaced_event(
            self.namespace,
            field_selector=f"involvedObject.kind=Pod,involvedObject.name={name}",
        )
        return [
            f"{event.type} {event.reason}: {event.message}"
            for event in sorted(
                events.items,
                key=lambda event: event.last_timestamp or event.event_time or 0,
            )
        ]

    def app_healthy(self, name: str, port: int) -> bool:
        """Return whether marimo answers its unauthenticated health check."""
        url = f"http://{name}.{self.namespace}.svc:{port}/{name}/health"
        try:
            with urllib.request.urlopen(
                url, timeout=HEALTH_TIMEOUT_SECONDS
            ) as response:
                return bool(response.status == HTTP_OK)
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            return False
