"""Tests for commands/starrocks.py — StarRocks target/env resolution and credential injection."""

from __future__ import annotations

import os
import socket
from unittest.mock import patch

import pytest

from ol_dbt_cli.commands.starrocks import _ENVS, _port_is_free, run

# ---------------------------------------------------------------------------
# _ENVS config consistency
# ---------------------------------------------------------------------------

_REQUIRED_ENV_KEYS = {
    "host",
    "eks_context",
    "k8s_namespace",
    "fe_service",
    "vault_addr",
    "vault_mount",
    "dbt_target",
    # Which lake to READ, independent of the cluster dbt_target connects to.
    # Required so adding an environment cannot leave it to be inferred from a
    # target name, which is the RFC 12711 step 1 defect.
    "data_lake_env",
}


@pytest.mark.parametrize("env_name", list(_ENVS))
def test_env_config_has_required_keys(env_name: str) -> None:
    assert _REQUIRED_ENV_KEYS <= _ENVS[env_name].keys()


@pytest.mark.parametrize("env_name", list(_ENVS))
def test_data_lake_env_is_a_real_catalog(env_name: str) -> None:
    """Interpolated into `ol_data_lake_<env>`, so a typo fails only at query time."""
    assert _ENVS[env_name]["data_lake_env"] in {"qa", "production"}


def test_dev_is_qa_cluster_reading_production() -> None:
    """The combination local b2b development needs, and the one that was missing.

    `'qa' in target.name` could not express it: dev's target is
    starrocks_qa_vault, so the substring test forced the (empty) QA lake and
    the b2b models could not be developed locally at all. Mirrors
    STARROCKS_DBT_TARGET_MAP["dev"] / DATA_LAKE_ENV_MAP["dev"] in
    lakehouse.lib.dbt_environment, which the Dagster side resolves from.
    """
    assert _ENVS["dev"]["dbt_target"] == _ENVS["qa"]["dbt_target"]
    assert _ENVS["dev"]["host"] == _ENVS["qa"]["host"]
    assert _ENVS["dev"]["data_lake_env"] == "production"
    assert _ENVS["qa"]["data_lake_env"] == "qa"


def test_ci_connects_directly_like_production() -> None:
    """Ci has no port-forward and connects directly, like production.

    Both connect directly to their FE service, unlike qa's port-forwarded
    dev workflow.
    """
    assert _ENVS["ci"]["port_forward"] is False
    assert _ENVS["ci"]["dbt_target"] == _ENVS["production"]["dbt_target"]


# ---------------------------------------------------------------------------
# _port_is_free
# ---------------------------------------------------------------------------


def test_port_is_free_when_unbound() -> None:
    # Bind to an ephemeral port, then release it, and confirm _port_is_free
    # observes it free again.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    assert _port_is_free(port) is True


def test_port_is_free_false_when_bound() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as holder:
        holder.bind(("127.0.0.1", 0))
        holder.listen(1)
        port = holder.getsockname()[1]
        assert _port_is_free(port) is False


# ---------------------------------------------------------------------------
# run() target/env resolution and credential injection
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "DBT_STARROCKS_USERNAME",
        "DBT_STARROCKS_PASSWORD",
        "DBT_STARROCKS_HOST",
        "DBT_DATA_LAKE_ENV",
    ):
        monkeypatch.delenv(key, raising=False)


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_default_env_uses_qa_target_and_port_forward(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    mock_fetch.return_value = ("v-token-user", "v-token-pass")
    run()

    mock_port_forward.assert_called_once()
    assert os.environ["DBT_STARROCKS_USERNAME"] == "v-token-user"
    assert os.environ["DBT_STARROCKS_PASSWORD"] == "v-token-pass"  # noqa: S105  # pragma: allowlist secret
    assert os.environ["DBT_STARROCKS_HOST"] == "127.0.0.1"
    _, kwargs = mock_dbt_run.call_args
    assert kwargs["target"] == _ENVS["qa"]["dbt_target"]


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_ci_env_skips_port_forward_by_default(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    mock_fetch.return_value = ("user", "pass")
    run(env="ci")

    mock_port_forward.assert_not_called()
    assert os.environ["DBT_STARROCKS_HOST"] == _ENVS["ci"]["host"]
    _, kwargs = mock_dbt_run.call_args
    assert kwargs["target"] == _ENVS["ci"]["dbt_target"]


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_dev_env_reads_production_lake_from_qa_cluster(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    """`--env dev` must connect to QA but export the production lake."""
    mock_fetch.return_value = ("user", "pass")
    run(env="dev")

    mock_port_forward.assert_called_once()
    assert os.environ["DBT_DATA_LAKE_ENV"] == "production"
    _, kwargs = mock_dbt_run.call_args
    assert kwargs["target"] == "starrocks_qa_vault"


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_explicit_target_keeps_the_env_s_data_lake(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    """--target moves the cluster; it must not move the lake with it.

    The whole point of the split is that "which cluster" and "which lake" are
    answered separately, so overriding one must leave the other alone.
    """
    mock_fetch.return_value = ("user", "pass")
    run(env="dev", target="starrocks_production")

    assert os.environ["DBT_DATA_LAKE_ENV"] == "production"
    _, kwargs = mock_dbt_run.call_args
    assert kwargs["target"] == "starrocks_production"

    run(env="qa", target="starrocks_production")
    assert os.environ["DBT_DATA_LAKE_ENV"] == "qa"


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_explicit_target_overrides_env_default(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    mock_fetch.return_value = ("user", "pass")
    run(env="qa", target="starrocks_production")

    _, kwargs = mock_dbt_run.call_args
    assert kwargs["target"] == "starrocks_production"


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_explicit_port_forward_overrides_env_default(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    mock_fetch.return_value = ("user", "pass")
    run(env="ci", port_forward=True)

    mock_port_forward.assert_called_once()
    assert os.environ["DBT_STARROCKS_HOST"] == "127.0.0.1"


def test_unknown_env_exits() -> None:
    with pytest.raises(SystemExit):
        run(env="nonexistent")
