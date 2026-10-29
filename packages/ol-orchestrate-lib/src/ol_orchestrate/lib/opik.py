"""Where a deployment's Opik Keycloak client is in Vault."""

import os

OPIK_KEYCLOAK_VAULT_MOUNT_ENV = "OPIK_KEYCLOAK_VAULT_MOUNT"
OPIK_KEYCLOAK_VAULT_PATH_ENV = "OPIK_KEYCLOAK_VAULT_PATH"


def opik_keycloak_vault_location() -> tuple[str, str] | None:
    """Return the KV v1 mount and path of the Opik Keycloak client, if set.

    ol-infrastructure sets both variables together with OPIK_URL_OVERRIDE, so a
    deployment moves to an Opik instance and the client that instance accepts in
    the same pod start. Without them the deployment doesn't trace.

    :returns: the mount and path, or None when either variable is unset
    :rtype: tuple[str, str] | None
    """
    mount = os.environ.get(OPIK_KEYCLOAK_VAULT_MOUNT_ENV)
    path = os.environ.get(OPIK_KEYCLOAK_VAULT_PATH_ENV)
    if mount and path:
        return mount, path
    return None
