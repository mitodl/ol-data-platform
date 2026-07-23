"""Vault credential resolution for ol_dlt sources.

Pure ``hvac`` — this module must not import Dagster (see the ruff banned-api
rule in ``pyproject.toml``). It mirrors the authentication contract that
``ol_orchestrate.lib.utils.authenticate_vault`` uses in the Dagster code
locations, so a dlt pipeline resolves credentials the same way whether it runs
inside a Dagster pod or standalone:

  - In Kubernetes (the service-account token file exists): Vault Kubernetes
    auth with the role/mount from ``DAGSTER_VAULT_ROLE`` /
    ``DAGSTER_VAULT_MOUNT``.
  - Anywhere else: an existing token from ``VAULT_TOKEN`` (e.g. after
    ``vault login`` or ``bin/starrocks-auth``).

Credentials are fetched at pipeline *run* time (never at import), and cached
for a fraction of the Vault lease so every table in a single run shares one
lease instead of opening a new one per table.
"""

import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

import hvac

logger = logging.getLogger(__name__)

K8S_SERVICE_ACCOUNT_TOKEN = Path(
    "/var/run/secrets/kubernetes.io/serviceaccount/token"  # noqa: S108
)

# Re-fetch once this fraction of the lease has elapsed. Long-lived code-location
# pods outlive any single lease, so a process-lifetime cache would eventually
# hand out revoked credentials.
_LEASE_USE_FRACTION = 0.5

# (mount, role) -> (expires_at_monotonic, username, password)
_CREDENTIAL_CACHE: dict[tuple[str, str], tuple[float, str, str]] = {}

# Guards the check-then-fetch above. Extraction is single-threaded today
# (``workers = 1`` in .dlt/config.toml), so nothing races for this lock — but
# that setting lives in another file for an unrelated reason (a dlt injectable-
# context bug), and raising it would otherwise silently turn "one lease per
# run" into "one lease per worker".
_CREDENTIAL_LOCK = threading.Lock()


def vault_address() -> str:
    """Return the Vault address for the current environment.

    Mirrors ``ol_orchestrate.lib.constants.VAULT_ADDRESS``: ``VAULT_ADDR`` wins,
    otherwise it is derived from ``DAGSTER_ENVIRONMENT`` (``dev`` points at QA
    so local development never touches production Vault).
    """
    explicit = os.getenv("VAULT_ADDR")
    if explicit:
        return explicit
    environment = os.getenv("DAGSTER_ENVIRONMENT", "dev")
    if environment == "dev":
        environment = "qa"
    return f"https://vault-{environment}.odl.mit.edu"


def _authenticated_client() -> hvac.Client:
    """Return an authenticated Vault client (Kubernetes auth, else token)."""
    client = hvac.Client(url=vault_address())
    if K8S_SERVICE_ACCOUNT_TOKEN.exists():
        client.auth.kubernetes.login(
            role=os.getenv("DAGSTER_VAULT_ROLE", "dagster"),
            jwt=K8S_SERVICE_ACCOUNT_TOKEN.read_text(),
            use_token=True,
            mount_point=os.getenv("DAGSTER_VAULT_MOUNT", "k8s-data"),
        )
        return client
    token = os.getenv("VAULT_TOKEN")
    if not token:
        msg = (
            "No Vault Kubernetes service-account token and no VAULT_TOKEN in the "
            "environment. Authenticate first (`vault login`) or run against the "
            "dev profile, which uses the local-dev database instead of Vault."
        )
        raise RuntimeError(msg)
    client.token = token
    return client


def read_database_credentials(mount: str, role: str = "readonly") -> tuple[str, str]:
    """Return ``(username, password)`` from Vault's database secrets engine.

    Args:
        mount: Database secrets-engine mount point, e.g. ``postgres-keycloak``.
        role: Role to issue credentials for. ``readonly`` is the only role an
            ingestion pipeline should ever ask for.
    """
    cache_key = (mount, role)
    with _CREDENTIAL_LOCK:
        return _read_database_credentials_locked(cache_key, mount, role)


def _read_database_credentials_locked(
    cache_key: tuple[str, str], mount: str, role: str
) -> tuple[str, str]:
    """Return cached credentials, or issue and cache a fresh lease."""
    cached = _CREDENTIAL_CACHE.get(cache_key)
    if cached and time.monotonic() < cached[0]:
        return cached[1], cached[2]

    vault_path = f"{mount}/creds/{role}"
    client = _authenticated_client()
    try:
        response: dict[str, Any] | None = client.read(vault_path)
    except hvac.exceptions.Forbidden as exc:
        msg = (
            f"Vault denied {vault_path!r} — the token's policy does not grant it. "
            "Source database mounts must be added to the dagster policy in "
            "ol-infrastructure (dagster_server_policy.hcl)."
        )
        raise RuntimeError(msg) from exc
    if response is None:
        msg = f"Vault path not found: {vault_path!r} — check the mount and role name"
        raise RuntimeError(msg)

    lease_duration = int(response.get("lease_duration") or 0)
    issued = response.get("data") or {}
    username, password = issued.get("username"), issued.get("password")
    if not (username and password):
        # The realistic cause is a mount that exists but isn't a database
        # secrets engine (a KV-v2 mount, say, nests its payload under
        # data.data), which otherwise surfaces as a bare KeyError.
        msg = (
            f"Vault response from {vault_path!r} carries no username/password — "
            f"is {mount!r} a database secrets-engine mount?"
        )
        raise RuntimeError(msg)
    logger.info(
        "Issued Vault database credentials from %s (lease %ss)",
        vault_path,
        lease_duration,
    )
    _CREDENTIAL_CACHE[cache_key] = (
        time.monotonic() + lease_duration * _LEASE_USE_FRACTION,
        username,
        password,
    )
    return username, password
