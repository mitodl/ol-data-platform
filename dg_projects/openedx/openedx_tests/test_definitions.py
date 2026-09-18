"""Load the code location's module the way the gRPC server does."""

import importlib

import pytest
from dagster import RepositoryDefinition

EXPECTED_REPOSITORIES = {
    "mitx_openedx",
    "mitxonline_openedx",
    "xpro_openedx",
    "openedx_shared_jobs",
}


def test_every_repository_in_the_code_location_loads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Load every repository `openedx.definitions` exposes, not only components.

    `load_all_definitions` is where op/job name collisions raise, and it only
    runs at server startup. The component test covers the per-deployment
    repositories; this one also covers `openedx_shared_jobs` and anything else
    added to the module later.

    Importing the module authenticates to Vault. Without a cached token the
    OIDC flow blocks on a browser callback, so this makes a cache miss raise and
    the module fall back to its unauthenticated Vault.
    """
    monkeypatch.setenv("VAULT_OIDC_NONINTERACTIVE", "1")
    definitions = importlib.import_module("openedx.definitions")

    repositories = {
        value.name: value
        for value in vars(definitions).values()
        if isinstance(value, RepositoryDefinition)
    }

    assert set(repositories) == EXPECTED_REPOSITORIES
    for repository in repositories.values():
        repository.load_all_definitions()
