"""Tests for the legacy_openedx code location definitions."""

import sys
import warnings

import pytest

MODULE_UNDER_TEST = "legacy_openedx.definitions"

# ol_orchestrate.lib.constants snapshots DAGSTER_ENVIRONMENT and VAULT_ADDR
# into module-level DAGSTER_ENV/VAULT_ADDRESS at import, and the module under
# test imports those *values*. Evicting only the leaf leaves the cached
# constants in place, so `definitions` would rebind to a VAULT_ADDRESS
# resolved from whatever environment imported constants first -- in practice
# the real vault-qa host, which turns a fast connection refusal into a network
# timeout and quietly stops testing the degraded path.
MODULES_TO_EVICT = (MODULE_UNDER_TEST, "ol_orchestrate.lib.constants")


def _import_with_vault_unreachable(monkeypatch: pytest.MonkeyPatch):
    """Import the code location fresh, with Vault unreachable.

    Points Vault at a closed port so ``authenticate_vault`` fails fast rather
    than blocking on a network timeout. This is the degraded path the module's
    resilient loading is written for, and the one that used to take the whole
    code location down.

    Everything being set up here happens at *import* time, so a cached entry
    in ``sys.modules`` would hand back a module built under whatever
    environment imported it first and none of the patching would apply.
    Evicting forces re-execution against this environment; monkeypatch
    restores the previous entries on teardown so the eviction cannot leak.
    The constants module has to go too -- see MODULES_TO_EVICT.
    """
    monkeypatch.setenv("VAULT_ADDR", "http://127.0.0.1:1")
    monkeypatch.setenv("VAULT_ADDRESS", "http://127.0.0.1:1")
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", "qa")
    for module_name in MODULES_TO_EVICT:
        monkeypatch.delitem(sys.modules, module_name, raising=False)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # Deferred on purpose: the module does its Vault authentication at
        # import time, so it has to be imported after the environment above
        # is in place.
        import legacy_openedx.definitions as definitions_module  # noqa: PLC0415
    return definitions_module


@pytest.fixture
def definitions(monkeypatch: pytest.MonkeyPatch):
    """Build a fresh legacy_openedx.definitions with Vault unavailable."""
    return _import_with_vault_unreachable(monkeypatch)


def test_import_is_not_served_from_the_module_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each import must re-execute the module rather than reuse a cached one.

    Without the sys.modules eviction, the second call here returns the object
    built by the first, so any test after the first one in a session would
    silently assert against a module created under a different environment --
    passing or failing on import order rather than on behaviour.
    """
    first = _import_with_vault_unreachable(monkeypatch)
    second = _import_with_vault_unreachable(monkeypatch)

    assert first is not second


def test_vault_address_comes_from_this_test_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The module must see the closed port, not a stale real Vault host.

    ol_orchestrate.lib.constants resolves VAULT_ADDRESS at import time, and
    legacy_openedx.definitions imports the resolved value. Evicting only
    definitions left the cached constants in place, so an earlier import
    anywhere in the session would leave this pointed at vault-qa.odl.mit.edu
    -- a network timeout instead of an immediate connection refusal, and no
    longer a test of the degraded path at all.

    Primes the cache with constants resolved against a *different* address
    first, which is what any earlier import in the session does. Without the
    constants eviction the assertion below sees that stale host instead.
    """
    monkeypatch.delitem(sys.modules, "ol_orchestrate.lib.constants", raising=False)
    monkeypatch.setenv("VAULT_ADDR", "https://vault-qa.odl.mit.edu")
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", "qa")
    import ol_orchestrate.lib.constants as stale_constants  # noqa: PLC0415

    assert stale_constants.VAULT_ADDRESS == "https://vault-qa.odl.mit.edu"

    definitions = _import_with_vault_unreachable(monkeypatch)

    assert definitions.VAULT_ADDRESS == "http://127.0.0.1:1"
    assert definitions.vault_authenticated is False


def test_repository_builds_without_vault(definitions) -> None:
    """The code location must stay loadable when Vault is unavailable.

    Regression test for DAGSTER-F. ``_job_default_config`` returned ``{}`` on
    the no-Vault path, and ``to_job(config={})`` does not mean "no default" --
    Dagster validates the empty mapping against the job's config schema and
    raises

        Missing required config entries ['ops', 'resources'] at the root

    That happens while the repository is being constructed, so it killed the
    gRPC server for the entire code location -- all three jobs and all three
    schedules -- rather than merely leaving the launchpad unpopulated.
    """
    repository = definitions.defs.get_repository_def()

    job_names = {job.name for job in repository.get_all_jobs()}
    assert {
        "residential_edx_course_pipeline",
        "xpro_edx_course_pipeline",
        "mitxonline_edx_course_pipeline",
    } <= job_names


def test_job_default_config_is_none_without_vault(definitions) -> None:
    """The degraded default must be None, not an empty mapping."""
    assert definitions._job_default_config("mitx") is None
