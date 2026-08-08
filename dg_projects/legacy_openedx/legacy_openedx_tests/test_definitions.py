"""Tests for the legacy_openedx code location definitions."""

import warnings

import pytest


@pytest.fixture
def definitions(monkeypatch: pytest.MonkeyPatch):
    """Import the code location with Vault unreachable.

    Points Vault at a closed port so ``authenticate_vault`` fails fast rather
    than blocking on a network timeout. This is the degraded path the module's
    resilient loading is written for, and the one that used to take the whole
    code location down.
    """
    monkeypatch.setenv("VAULT_ADDR", "http://127.0.0.1:1")
    monkeypatch.setenv("VAULT_ADDRESS", "http://127.0.0.1:1")
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", "qa")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # Deferred on purpose: the module does its Vault authentication at
        # import time, so it has to be imported after the environment above
        # is in place.
        import legacy_openedx.definitions as definitions_module  # noqa: PLC0415
    return definitions_module


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
