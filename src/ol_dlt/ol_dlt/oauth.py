"""OAuth client-credentials helpers for ol_dlt API sources.

Pure dlt/``requests`` -- this module must not import Dagster (see the ruff
banned-api rule).
"""

from typing import Any

from dlt.sources.helpers import requests

from ol_dlt import config, vault

KV_MOUNT = "secret-data"


def resolve_client_credentials(
    *,
    vault_path: str,
    env_prefix: str,
    client_id: str | None,
    client_secret: str | None,
    access_token_url: str | None,
) -> dict[str, str]:
    """Return the OAuth client for the active profile.

    Deployed profiles take the client from the KV secret at ``vault_path`` (keys
    ``id``, ``secret`` and ``token_url``), the same secrets the Dagster
    OAuthApiClientFactory reads. Explicit arguments and the
    ``<env_prefix>_CLIENT_ID`` / ``_CLIENT_SECRET`` / ``_ACCESS_TOKEN_URL``
    environment variables apply only to the other profiles.
    """
    if config.active_profile() in config.ICEBERG_PROFILES:
        oauth_client = vault.read_kv_secret(KV_MOUNT, vault_path)
        return {
            "client_id": oauth_client["id"],
            "client_secret": oauth_client["secret"],
            "access_token_url": oauth_client["token_url"],
        }
    return config.require_secrets(
        client_id=config.resolve_secret(client_id, f"{env_prefix}_CLIENT_ID"),
        client_secret=config.resolve_secret(
            client_secret, f"{env_prefix}_CLIENT_SECRET"
        ),
        access_token_url=config.resolve_secret(
            access_token_url, f"{env_prefix}_ACCESS_TOKEN_URL"
        ),
    )


def jwt_auth_headers(creds: dict[str, str]) -> dict[str, Any]:
    """Fetch a client-credentials token and return the JWT auth header."""
    token_resp = requests.post(
        creds["access_token_url"],
        data={
            "grant_type": "client_credentials",
            "client_id": creds["client_id"],
            "client_secret": creds["client_secret"],
            "token_type": "jwt",
        },
        timeout=30,
    )
    token_resp.raise_for_status()
    return {"Authorization": f"JWT {token_resp.json()['access_token']}"}
