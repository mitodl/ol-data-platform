"""Vault OIDC authentication and dynamic credential helpers.

Shared by all ol-dbt commands that need Vault-issued credentials.
Tokens are cached at ~/.cache/starrocks-auth/vault-token-{env}.json so that
``bin/starrocks-auth --mode vault`` and ``ol-dbt starrocks`` share the same
authenticated session.
"""

from __future__ import annotations

import http.server
import json
import sys
import threading
import urllib.parse
import webbrowser
from pathlib import Path
from typing import Any

import hvac

_VAULT_CALLBACK_PORT = 8250
_VAULT_REDIRECT_URI = f"http://localhost:{_VAULT_CALLBACK_PORT}/oidc/callback"
_VAULT_OIDC_ROLE = "developer"
_CACHE_DIR = Path(__import__("os").environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache"))) / "starrocks-auth"


def _vault_client(vault_addr: str, token: str | None = None) -> hvac.Client:
    return hvac.Client(url=vault_addr, token=token)


def _oidc_callback() -> str:
    """Start a one-shot HTTP server on port 8250 and return the auth code."""
    result: dict[str, str] = {}

    class _Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_: Any) -> None:  # noqa: ANN002
            pass

        def do_GET(self) -> None:
            params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(self.path).query))
            error = params.get("error")
            if error:
                result["error"] = error
                self.send_response(400)
                self.end_headers()
                self.wfile.write(f"<h2>Vault authentication failed: {error}. You can close this tab.</h2>".encode())
            else:
                result["code"] = params.get("code", "")
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"<h2>Vault authentication successful. You can close this tab.</h2>")
            threading.Thread(target=self.server.shutdown, daemon=True).start()

    server = http.server.HTTPServer(("localhost", _VAULT_CALLBACK_PORT), _Handler)
    server.serve_forever()
    if "error" in result:
        msg = f"Vault OIDC authentication denied: {result['error']}"
        raise RuntimeError(msg)
    code = result.get("code", "")
    if not code:
        msg = "Vault OIDC callback received no authorization code"
        raise RuntimeError(msg)
    return code


def load_vault_token(vault_addr: str, env_name: str, oidc_role: str = _VAULT_OIDC_ROLE) -> str:
    """Return a valid Vault token, triggering OIDC browser login if needed."""
    cache_path = _CACHE_DIR / f"vault-token-{env_name}-{oidc_role}.json"
    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text())
            token = str(cached.get("token", ""))
            if token:
                client = _vault_client(vault_addr, token)
                if client.is_authenticated():
                    return token
        except Exception:  # noqa: BLE001, S110
            pass

    client = _vault_client(vault_addr)
    auth_resp = client.auth.oidc.oidc_authorization_url_request(
        role=oidc_role,
        redirect_uri=_VAULT_REDIRECT_URI,
    )
    auth_url: str = auth_resp["data"]["auth_url"]
    if not auth_url:
        msg = f"Vault at {vault_addr} did not return an auth URL"
        raise RuntimeError(msg)

    qs = urllib.parse.parse_qs(urllib.parse.urlparse(auth_url).query)
    nonce_list = qs.get("nonce")
    state_list = qs.get("state")
    if not nonce_list or not state_list:
        msg = f"Vault OIDC auth URL missing nonce/state parameters: {auth_url}"
        raise RuntimeError(msg)
    nonce = nonce_list[0]
    state = state_list[0]

    print(  # noqa: T201
        f"Opening browser for Vault login ({vault_addr})…\nIf it does not open automatically, visit:\n{auth_url}",
        file=sys.stderr,
    )
    webbrowser.open(auth_url)
    code = _oidc_callback()

    login_resp = client.auth.oidc.oidc_callback(code=code, nonce=nonce, state=state)
    token = str(login_resp["auth"]["client_token"])

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps({"token": token}))
    return token


def fetch_vault_db_credentials(
    vault_addr: str,
    vault_mount: str,
    env_name: str,
    role: str,
    oidc_role: str = _VAULT_OIDC_ROLE,
) -> tuple[str, str]:
    """Fetch dynamic database credentials from Vault's database secrets engine."""
    token = load_vault_token(vault_addr, env_name, oidc_role)
    client = _vault_client(vault_addr, token)
    vault_path = f"{vault_mount}/creds/{role}"
    try:
        response: dict[str, Any] | None = client.read(vault_path)
    except hvac.exceptions.Forbidden as exc:
        msg = f"Vault permission denied: {vault_path!r} — check token policy"
        raise RuntimeError(msg) from exc
    if response is None:
        msg = f"Vault path not found: {vault_path!r} — check vault_mount and role name"
        raise RuntimeError(msg)
    data: dict[str, str] = response["data"]
    return data["username"], data["password"]
