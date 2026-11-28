import base64

from notebook_publisher import render
from notebook_publisher.settings import Settings
from notebook_publisher.validation import NotebookUpdate

from notebook_publisher_tests.conftest import NOTEBOOK, OIDC_PLUGIN

OWNER = {
    "apiVersion": "marimo.io/v1alpha1",
    "kind": "MarimoNotebook",
    "name": "q3",
    "uid": "u-1",
}


def test_notebook_runs_as_the_published_service_account(settings: Settings) -> None:
    notebook = render.render_notebook(
        "q3",
        "someone@mit.edu",
        NotebookUpdate(content=NOTEBOOK, git_commit="abc1234"),
        "r1",
        "2026-11-25T00:00:00+00:00",
        settings,
    )
    spec = notebook["spec"]
    overrides = spec["podOverrides"]
    assert spec["mode"] == "run"
    assert spec["image"] == settings.runtime_image
    assert spec["auth"]["password"]["secretKeyRef"]["name"] == "q3-token-r1"
    assert overrides["serviceAccountName"] == "marimo-published"
    assert overrides["automountServiceAccountToken"] is False
    assert overrides["securityContext"]["runAsNonRoot"] is True
    assert "--base-url=/q3" in overrides["containers"][0]["args"]
    assert {c["name"] for c in overrides["initContainers"]} == set(
        render.INIT_CONTAINERS
    )
    assert "sidecars" not in spec
    assert "mounts" not in spec
    metadata = notebook["metadata"]
    assert metadata["annotations"][render.OWNER_ANNOTATION] == "someone@mit.edu"
    assert metadata["annotations"][render.GIT_COMMIT_ANNOTATION] == "abc1234"
    assert render.GIT_REMOTE_ANNOTATION not in metadata["annotations"]
    assert metadata["labels"][render.ACCESS_LABEL] == "keycloak"
    assert all(len(value) <= 63 for value in metadata["labels"].values())


def test_secrets_carry_the_token_and_its_basic_header(settings: Settings) -> None:
    token_secret, auth_secret = render.render_secrets(
        "q3", "r1", "tok", OWNER, settings
    )
    assert token_secret["metadata"]["name"] == "q3-token-r1"
    assert token_secret["stringData"] == {"password": "tok"}  # pragma: allowlist secret
    assert auth_secret["metadata"]["name"] == "q3-route-auth-r1"
    header = auth_secret["stringData"]["headers.set.Authorization"]
    assert base64.b64decode(header.removeprefix("Basic ")) == b"marimo:tok"
    assert token_secret["metadata"]["ownerReferences"] == [OWNER]


def test_route_forwards_only_the_access_token(settings: Settings) -> None:
    route = render.render_route("q3", "r1", OWNER, settings)
    assert route["spec"]["ingressClassName"] == "apache-apisix"
    assert route["metadata"]["ownerReferences"] == [OWNER]
    (http,) = route["spec"]["http"]
    assert http["match"] == {
        "hosts": ["apps.nb-ci.data.ol.mit.edu"],
        "paths": ["/q3", "/q3/*"],
    }
    assert http["websocket"] is True
    assert http["backends"] == [{"serviceName": "q3", "servicePort": 2718}]
    oidc, rewrite = http["plugins"]
    assert oidc["secretRef"] == OIDC_PLUGIN["secretRef"]
    config = oidc["config"]
    assert config["scope"] == "openid profile email"
    assert config["unauth_action"] == "auth"
    assert config["logout_path"] == "/q3/logout/oidc"
    assert config["set_access_token_header"] is True
    assert config["set_id_token_header"] is False
    assert config["set_userinfo_header"] is False
    assert rewrite["secretRef"] == "q3-route-auth-r1"  # pragma: allowlist secret
    assert set(rewrite["config"]["headers"]["remove"]) == {
        "Cookie",
        "X-ID-Token",
        "X-Userinfo",
        "X_Access_Token",
    }


def test_route_leaves_the_shared_oidc_config_alone(settings: Settings) -> None:
    render.render_route("q3", "r1", OWNER, settings)
    assert settings.oidc_plugin["config"]["logout_path"] == "/logout/oidc"
