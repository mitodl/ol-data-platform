"""Unit tests for ol_orchestrate.lib.constants."""

from __future__ import annotations

import importlib
from types import ModuleType

import pytest
from ol_orchestrate.lib import constants


def _reload_with_env(monkeypatch: pytest.MonkeyPatch, value: str | None) -> ModuleType:
    if value is None:
        monkeypatch.delenv("DAGSTER_ENVIRONMENT", raising=False)
    else:
        monkeypatch.setenv("DAGSTER_ENVIRONMENT", value)
    return importlib.reload(constants)


@pytest.fixture(autouse=True)
def _restore_module(monkeypatch: pytest.MonkeyPatch):
    """Leave the imported module matching the real environment again."""
    yield
    monkeypatch.undo()
    importlib.reload(constants)


@pytest.mark.parametrize("env", ["dev", "ci", "qa", "production"])
def test_recognized_environments_are_accepted(
    monkeypatch: pytest.MonkeyPatch, env: str
) -> None:
    module = _reload_with_env(monkeypatch, env)

    assert env == module.DAGSTER_ENV


def test_unset_environment_defaults_to_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _reload_with_env(monkeypatch, None)

    assert module.DAGSTER_ENV == "dev"


def test_unrecognized_environment_fails_with_a_readable_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A typo used to surface as a bare KeyError from an unrelated dict lookup."""
    with pytest.raises(ValueError, match="productoin") as excinfo:
        _reload_with_env(monkeypatch, "productoin")

    message = str(excinfo.value)
    assert "DAGSTER_ENVIRONMENT" in message
    # The message has to name the valid options, otherwise it is no more
    # actionable than the KeyError it replaces.
    for env in ("dev", "ci", "qa", "production"):
        assert env in message
