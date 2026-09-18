"""Load the code location's module the way the gRPC server does."""

import importlib
from pathlib import Path

import pytest
from dagster._core.definitions.reconstruct import repository_def_from_target_def
from dagster._core.workspace.autodiscovery import loadable_targets_from_loaded_module

EXPECTED_REPOSITORIES = {
    "mitx_openedx",
    "mitxonline_openedx",
    "xpro_openedx",
    "openedx_shared_jobs",
}


def test_every_repository_in_the_code_location_loads(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Load every repository the code server would load from `openedx.definitions`.

    `load_all_definitions` is where op/job name collisions raise, and it only
    runs at server startup. Targets are resolved with the server's own
    autodiscovery, so if the module later gains a `Definitions` object the
    server loads that instead of the repositories, and the name check below
    fails rather than passing on repositories nothing loads.

    Importing the module authenticates to Vault. The empty token cache and
    VAULT_OIDC_NONINTERACTIVE make that fail fast, the same as in CI, and the
    module falls back to its unauthenticated Vault. Without them a developer's
    cached token logs in for real, and a cache miss blocks on the OIDC browser
    callback. This only works while nothing imports the module earlier, e.g. at
    test collection.
    """
    monkeypatch.setenv("VAULT_TOKEN_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("VAULT_OIDC_NONINTERACTIVE", "1")
    definitions = importlib.import_module("openedx.definitions")

    repositories = [
        repository_def_from_target_def(target.target_definition)
        for target in loadable_targets_from_loaded_module(definitions)
    ]

    assert {repository.name for repository in repositories if repository} == (
        EXPECTED_REPOSITORIES
    )
    for repository in repositories:
        assert repository is not None
        repository.load_all_definitions()
