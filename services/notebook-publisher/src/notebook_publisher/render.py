"""Render the Kubernetes objects behind one published notebook.

The shape is the one proven by hand on data-qa (a MarimoNotebook plus a gated
ApisixRoute); see docs/plans/marimo-first-data-notebooks-spec.md in
ol-infrastructure for why each field is there. Pulumi doesn't own these
objects: they are user content, and the CRs are the publisher's only state.
"""

import base64
import copy
import hashlib
from typing import Any

from notebook_publisher.settings import Settings
from notebook_publisher.validation import NotebookUpdate

MARIMO_API_VERSION = "marimo.io/v1alpha1"
MARIMO_GROUP, MARIMO_VERSION = MARIMO_API_VERSION.split("/")
MARIMO_PLURAL = "marimos"
APISIX_API_VERSION = "apisix.apache.org/v2"
APISIX_GROUP, APISIX_VERSION = APISIX_API_VERSION.split("/")
APISIX_ROUTE_PLURAL = "apisixroutes"

MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY = "notebook-publisher"
NOTEBOOK_LABEL = "ol.mit.edu/notebook"
# Label values can't hold an email address, so listing selects on a hash and
# the owner annotation is what ownership checks compare.
OWNER_HASH_LABEL = "ol.mit.edu/owner-hash"
ACCESS_LABEL = "ol.mit.edu/access"
OWNER_ANNOTATION = "ol.mit.edu/owner"
REVISION_ANNOTATION = "ol.mit.edu/revision"
PUBLISHED_AT_ANNOTATION = "ol.mit.edu/published-at"
# Provenance the caller reports and nobody verifies.
GIT_REMOTE_ANNOTATION = "ol.mit.edu/git-remote"
GIT_COMMIT_ANNOTATION = "ol.mit.edu/git-commit"
PUBLISHER_ANNOTATIONS = (
    OWNER_ANNOTATION,
    REVISION_ANNOTATION,
    PUBLISHED_AT_ANNOTATION,
    GIT_REMOTE_ANNOTATION,
    GIT_COMMIT_ANNOTATION,
)

# marimo-operator v0.3.0 (pkg/resources/pod.go) names these.
MARIMO_CONTAINER = "marimo"
INIT_CONTAINERS = ("copy-content", "setup-venv")
AUTH_VOLUME = "auth-secret"
EXPECTED_VOLUMES = frozenset({"notebook-data", "notebook-content", "venv", AUTH_VOLUME})
NOTEBOOK_PATH = "/home/marimo/notebooks/notebook.py"
TOKEN_KEY = "password"  # noqa: S105 (the key name in the Secret)
RUNTIME_UID = 1000

# What the restricted Pod Security Standard requires of every container.
CONTAINER_SECURITY_CONTEXT = {
    "allowPrivilegeEscalation": False,
    "capabilities": {"drop": ["ALL"]},
}

# Request headers the viewer could send, or the gateway could add, that notebook
# code must not see. The openid-connect plugin overwrites X-Access-Token itself,
# but not its underscore spelling.
STRIPPED_HEADERS = ["Cookie", "X-ID-Token", "X-Userinfo", "X_Access_Token"]


def owner_hash(username: str) -> str:
    """Return the label value that stands for a hub user."""
    return hashlib.sha256(username.encode()).hexdigest()[:32]


def app_url(name: str, settings: Settings) -> str:
    """Return where a published notebook is served."""
    return f"https://{settings.apps_host}/{name}/"


def token_secret_name(name: str, revision: str) -> str:
    """Return the Secret holding one revision's marimo token."""
    return f"{name}-token-{revision}"


def route_auth_secret_name(name: str, revision: str) -> str:
    """Return the Secret APISIX reads one revision's Authorization header from."""
    return f"{name}-route-auth-{revision}"


def marimo_args(name: str, settings: Settings) -> list[str]:
    """Return the marimo command line, with the base URL the operator can't set.

    Stopgap until marimo-operator ships spec.baseUrl (marimo-operator#21).
    """
    return [
        "run",
        "--headless",
        "--host=0.0.0.0",
        f"--port={settings.marimo_port}",
        "--token-password-file",
        "/etc/marimo/password",
        f"--base-url=/{name}",
        "--sandbox",
        NOTEBOOK_PATH,
    ]


def owner_reference(notebook: dict[str, Any]) -> dict[str, Any]:
    """Return an ownerReference that garbage-collects an object with its app."""
    return {
        "apiVersion": MARIMO_API_VERSION,
        "kind": "MarimoNotebook",
        "name": notebook["metadata"]["name"],
        "uid": notebook["metadata"]["uid"],
    }


def render_notebook(  # noqa: PLR0913
    name: str,
    owner: str,
    request: NotebookUpdate,
    revision: str,
    published_at: str,
    settings: Settings,
) -> dict[str, Any]:
    """Render the MarimoNotebook for one revision of a published notebook.

    :param name: The app name.
    :param owner: The hub username of the publisher.
    :param request: The notebook and its provenance.
    :param revision: Identifies this publish; names its token Secrets.
    :param published_at: ISO 8601 time of this publish.
    :param settings: Deployment configuration.
    :returns: The MarimoNotebook object.
    :rtype: dict[str, Any]
    """
    annotations = {
        OWNER_ANNOTATION: owner,
        REVISION_ANNOTATION: revision,
        PUBLISHED_AT_ANNOTATION: published_at,
    }
    if request.git_remote:
        annotations[GIT_REMOTE_ANNOTATION] = request.git_remote
    if request.git_commit:
        annotations[GIT_COMMIT_ANNOTATION] = request.git_commit
    return {
        "apiVersion": MARIMO_API_VERSION,
        "kind": "MarimoNotebook",
        "metadata": {
            "name": name,
            "namespace": settings.namespace,
            "labels": {
                MANAGED_BY_LABEL: MANAGED_BY,
                NOTEBOOK_LABEL: name,
                OWNER_HASH_LABEL: owner_hash(owner),
                ACCESS_LABEL: request.access,
            },
            "annotations": annotations,
        },
        "spec": {
            "mode": "run",
            "image": settings.runtime_image,
            "port": settings.marimo_port,
            "content": request.content,
            # Each revision gets a new token Secret. Its name is part of the
            # pod spec, so a new revision always replaces the pod, which is
            # also how a content-only change rolls out (the operator's spec
            # hash doesn't cover content, marimo-operator#22).
            "auth": {
                "password": {
                    "secretKeyRef": {
                        "name": token_secret_name(name, revision),
                        "key": TOKEN_KEY,
                    }
                }
            },
            "resources": {
                "requests": {
                    "cpu": settings.cpu_request,
                    "memory": settings.memory_request,
                    "ephemeral-storage": settings.ephemeral_storage_request,
                },
                "limits": {
                    "memory": settings.memory_limit,
                    "ephemeral-storage": settings.ephemeral_storage_limit,
                },
            },
            "podOverrides": {
                "serviceAccountName": settings.service_account,
                "automountServiceAccountToken": False,
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": RUNTIME_UID,
                    "runAsGroup": RUNTIME_UID,
                    "fsGroup": RUNTIME_UID,
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
                "initContainers": [
                    {"name": init, "securityContext": CONTAINER_SECURITY_CONTEXT}
                    for init in INIT_CONTAINERS
                ],
                "containers": [
                    {
                        "name": MARIMO_CONTAINER,
                        "args": marimo_args(name, settings),
                        "securityContext": CONTAINER_SECURITY_CONTEXT,
                    }
                ],
            },
        },
    }


def render_secrets(
    name: str,
    revision: str,
    token: str,
    owner: dict[str, Any],
    settings: Settings,
) -> list[dict[str, Any]]:
    """Render one revision's marimo token and the header APISIX injects with it.

    :param name: The app name.
    :param revision: The revision the Secrets belong to.
    :param token: The marimo token password.
    :param owner: ownerReference to the app's MarimoNotebook.
    :param settings: Deployment configuration.
    :returns: The token Secret and the route's Authorization Secret.
    :rtype: list[dict[str, Any]]
    """
    basic = base64.b64encode(f"marimo:{token}".encode()).decode()
    metadata = {
        "namespace": settings.namespace,
        "labels": {MANAGED_BY_LABEL: MANAGED_BY, NOTEBOOK_LABEL: name},
        "ownerReferences": [owner],
    }
    return [
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": token_secret_name(name, revision), **metadata},
            "type": "Opaque",
            "stringData": {TOKEN_KEY: token},
        },
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": route_auth_secret_name(name, revision), **metadata},
            "type": "Opaque",
            # The ingress controller merges secretRef keys into the plugin
            # config, splitting them on dots, so the token stays out of the
            # ApisixRoute.
            "stringData": {"headers.set.Authorization": f"Basic {basic}"},
        },
    ]


def render_route(
    name: str,
    revision: str,
    owner: dict[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    """Render the Keycloak-gated route to a published notebook.

    :param name: The app name.
    :param revision: The revision whose token APISIX injects.
    :param owner: ownerReference to the app's MarimoNotebook.
    :param settings: Deployment configuration.
    :returns: The ApisixRoute object.
    :rtype: dict[str, Any]
    """
    oidc = copy.deepcopy(settings.oidc_plugin)
    oidc["enable"] = True
    oidc["config"].update(
        {
            "unauth_action": "auth",
            # The logout path is matched before any rewrite, so it carries the
            # app's prefix rather than taking over a path on the shared host.
            "logout_path": f"/{name}/logout/oidc",
            "post_logout_redirect_uri": f"/{name}/",
            # Published apps query the warehouse as the viewer, with the access
            # token, and see nothing else of the viewer's.
            "set_access_token_header": True,
            "set_id_token_header": False,
            "set_userinfo_header": False,
            "set_raw_id_token_header": False,
            "set_refresh_token_header": False,
        }
    )
    rewrite = {
        "name": "proxy-rewrite",
        "enable": True,
        "secretRef": route_auth_secret_name(name, revision),
        "config": {"headers": {"remove": STRIPPED_HEADERS}},
    }
    return {
        "apiVersion": APISIX_API_VERSION,
        "kind": "ApisixRoute",
        "metadata": {
            "name": name,
            "namespace": settings.namespace,
            "labels": {MANAGED_BY_LABEL: MANAGED_BY, NOTEBOOK_LABEL: name},
            "ownerReferences": [owner],
        },
        "spec": {
            # ingress-controller 2.x silently ignores a route with no class.
            "ingressClassName": settings.ingress_class,
            "http": [
                {
                    "name": name,
                    "priority": 10,
                    "match": {
                        "hosts": [settings.apps_host],
                        "paths": [f"/{name}", f"/{name}/*"],
                    },
                    "websocket": True,
                    "timeout": {"connect": "60s", "read": "3600s", "send": "3600s"},
                    "plugin_config_name": settings.shared_plugin_config_name,
                    "plugins": [oidc, rewrite],
                    "backends": [
                        {"serviceName": name, "servicePort": settings.marimo_port}
                    ],
                }
            ],
        },
    }
