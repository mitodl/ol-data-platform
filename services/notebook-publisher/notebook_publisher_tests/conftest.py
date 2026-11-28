"""A fake cluster that renders pods the way marimo-operator v0.3.0 does."""

import copy
import json
import uuid
from pathlib import Path
from typing import Any, cast

import pytest
from kubernetes import client
from notebook_publisher import render, validation
from notebook_publisher.cluster import Cluster
from notebook_publisher.service import Publisher
from notebook_publisher.settings import Settings

NOTEBOOK = """# /// script
# dependencies = ["marimo", "ol-notebook"]
# ///
import marimo

__generated_with = "0.25.2"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
"""

OIDC_PLUGIN = {
    "name": "openid-connect",
    "enable": True,
    "secretRef": "ol-apisix-marimo-data-oidc-secrets",  # pragma: allowlist secret
    "config": {
        "scope": "openid profile email",
        "logout_path": "/logout/oidc",
        "set_userinfo_header": True,
        "set_id_token_header": True,
        "set_access_token_header": False,
        "unauth_action": "auth",
    },
}


def operator_pod(notebook: dict[str, Any]) -> dict[str, Any]:
    """Build the pod marimo-operator would create, podOverrides merged by name."""
    spec = notebook["spec"]
    overrides = copy.deepcopy(spec.get("podOverrides", {}))
    name = notebook["metadata"]["name"]
    secret = spec["auth"]["password"]["secretKeyRef"]["name"]
    containers = {
        "marimo": {
            "name": "marimo",
            "image": spec["image"],
            "args": ["run", "--headless", "--sandbox", "operator-default"],
        }
    }
    init_containers = {
        init: {"name": init, "image": "busybox"} for init in render.INIT_CONTAINERS
    }
    for override in overrides.pop("containers", []):
        containers.setdefault(override["name"], {}).update(override)
    for override in overrides.pop("initContainers", []):
        init_containers.setdefault(override["name"], {}).update(override)
    return {
        "metadata": {
            "name": name,
            # A new pod for every revision, like the operator's recreate.
            "uid": f"pod-{notebook['metadata']['uid']}-{secret}",
            "ownerReferences": [{"uid": notebook["metadata"]["uid"]}],
        },
        "spec": {
            "containers": list(containers.values()),
            "initContainers": list(init_containers.values()),
            "volumes": [
                {"name": "notebook-data", "emptyDir": {}},
                {"name": "notebook-content", "configMap": {"name": f"{name}-content"}},
                {"name": "venv", "emptyDir": {}},
                {"name": "auth-secret", "secret": {"secretName": secret}},
            ],
            "serviceAccountName": "default",
            **overrides,
        },
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
            "containerStatuses": [{"name": "marimo", "restartCount": 0, "state": {}}],
        },
    }


class FakeCluster:
    """In-memory stand-in for the publisher's namespace."""

    def __init__(self) -> None:
        self.notebooks: dict[str, dict[str, Any]] = {}
        self.routes: dict[str, dict[str, Any]] = {}
        self.secrets: dict[str, dict[str, Any]] = {}
        self.healthy = True
        # Applied to each pod the "operator" builds, to simulate failures.
        self.pod_hook = lambda pod: pod
        self._version = 0

    def _stamp(self, obj: dict[str, Any]) -> dict[str, Any]:
        self._version += 1
        obj["metadata"]["resourceVersion"] = str(self._version)
        return copy.deepcopy(obj)

    def get_notebook(self, name: str) -> dict[str, Any] | None:
        notebook = self.notebooks.get(name)
        return copy.deepcopy(notebook) if notebook else None

    def list_notebooks(self, label_selector: str) -> list[dict[str, Any]]:
        key, value = label_selector.split("=")
        return [
            copy.deepcopy(notebook)
            for notebook in self.notebooks.values()
            if notebook["metadata"]["labels"].get(key) == value
        ]

    def create_notebook(self, body: dict[str, Any]) -> dict[str, Any]:
        name = body["metadata"]["name"]
        if name in self.notebooks:
            raise client.ApiException(status=409)
        notebook = copy.deepcopy(body)
        notebook["metadata"]["uid"] = str(uuid.uuid4())
        self.notebooks[name] = notebook
        return self._stamp(notebook)

    def replace_notebook(self, body: dict[str, Any]) -> dict[str, Any]:
        name = body["metadata"]["name"]
        current = self.notebooks[name]
        if (
            body["metadata"]["resourceVersion"]
            != current["metadata"]["resourceVersion"]
        ):
            raise client.ApiException(status=409)
        self.notebooks[name] = copy.deepcopy(body)
        return self._stamp(self.notebooks[name])

    def delete_notebook(self, name: str, uid: str) -> None:
        notebook = self.notebooks.get(name)
        if notebook is None:
            return
        if notebook["metadata"]["uid"] != uid:
            raise client.ApiException(status=409)
        del self.notebooks[name]
        for store in (self.secrets, self.routes):
            for key in [
                key
                for key, obj in store.items()
                if obj["metadata"]["ownerReferences"][0]["uid"] == uid
            ]:
                del store[key]

    def get_route(self, name: str) -> dict[str, Any] | None:
        route = self.routes.get(name)
        return copy.deepcopy(route) if route else None

    def create_route(self, body: dict[str, Any]) -> None:
        self.routes[body["metadata"]["name"]] = copy.deepcopy(body)
        self._stamp(self.routes[body["metadata"]["name"]])

    def replace_route(self, body: dict[str, Any]) -> None:
        name = body["metadata"]["name"]
        assert (
            body["metadata"]["resourceVersion"]
            == self.routes[name]["metadata"]["resourceVersion"]
        )
        self.routes[name] = copy.deepcopy(body)
        self._stamp(self.routes[name])

    def create_secret(self, body: dict[str, Any]) -> None:
        assert body["metadata"]["name"] not in self.secrets
        self.secrets[body["metadata"]["name"]] = copy.deepcopy(body)

    def delete_secret(self, name: str) -> None:
        self.secrets.pop(name, None)

    def get_pod(self, name: str) -> dict[str, Any] | None:
        notebook = self.notebooks.get(name)
        return self.pod_hook(operator_pod(notebook)) if notebook else None

    def pod_logs(self, name: str, tail_lines: int) -> str:  # noqa: ARG002
        return (
            f"URL: http://0.0.0.0:2718/{name}?access_token=s3cret-token\n"
            "error: no solution found"
        )

    def pod_events(self, uid: str) -> list[str]:  # noqa: ARG002
        return ["Normal Started: Started container marimo"]

    def app_healthy(self, name: str, port: int) -> bool:  # noqa: ARG002
        return self.healthy


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    oidc_file = tmp_path / "oidc.json"
    oidc_file.write_text(json.dumps(OIDC_PLUGIN))
    return Settings(
        apps_host="apps.nb-ci.data.ol.mit.edu",
        runtime_image="ghcr.io/mitodl/marimo-runtime:2026.11.24.5.1",
        oidc_plugin_file=oidc_file,
        shared_plugin_config_name="marimo-data-ol-shared-plugins",
        rollout_timeout_seconds=1,
        rollout_poll_seconds=0.01,
    )


@pytest.fixture
def cluster() -> FakeCluster:
    return FakeCluster()


@pytest.fixture
def publisher(
    cluster: FakeCluster, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> Publisher:
    # test_validation runs the real `marimo check`; it takes seconds a call.
    monkeypatch.setattr(validation, "run_marimo_check", lambda *_: None)
    return Publisher(cast("Cluster", cluster), settings)
