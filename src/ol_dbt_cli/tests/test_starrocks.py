"""Tests for commands/starrocks.py — StarRocks target/env resolution and credential injection."""

from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from ol_dbt_cli.commands.starrocks import (
    _ENVS,
    _LAKE_TARGETS,
    _local_raw_row_counts,
    _port_is_free,
    _profile_schema,
    _report_local_branches,
    run,
)

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
    assert _ENVS[env_name]["data_lake_env"] in {"local", "qa", "production"}


def test_dev_is_the_local_lake() -> None:
    """`dev` is the k3d lake: its own cluster and catalog, and no Vault."""
    assert _ENVS["dev"]["dbt_target"] == "starrocks_local"
    assert _ENVS["dev"]["host"] == "127.0.0.1"
    assert _ENVS["dev"]["data_lake_env"] == "local"
    assert _ENVS["dev"]["vault_addr"] is None
    assert _ENVS["dev"]["port_forward"] is False


def test_ci_connects_directly_like_production() -> None:
    """Ci has no port-forward and connects directly, like production.

    Both connect directly to their FE service, unlike qa's port-forwarded
    dev workflow.
    """
    assert _ENVS["ci"]["port_forward"] is False
    assert _ENVS["ci"]["dbt_target"] == _ENVS["production"]["dbt_target"]


def test_ci_logs_in_through_ci_vault() -> None:
    """CI StarRocks's database secrets engine is in vault-ci (mitodl/ol-data-platform#3185 for bin/starrocks-auth)."""
    assert _ENVS["ci"]["vault_addr"] == "https://vault-ci.odl.mit.edu"


# ---------------------------------------------------------------------------
# _LAKE_TARGETS against profiles.yml and dbt_project.yml
# ---------------------------------------------------------------------------

_DBT_DIR = Path(__file__).parents[2] / "ol_dbt"
_PROFILE_OUTPUTS = yaml.safe_load((_DBT_DIR / "profiles.yml").read_text())["open_learning"]["outputs"]
_LOCATION_ROOTS = yaml.safe_load((_DBT_DIR / "dbt_project.yml").read_text())["vars"]["iceberg_schema_location_roots"]


def test_every_lake_profile_is_declared() -> None:
    """A lake profile missing here would reach dbt with no profile schema made for it."""
    lake_outputs = {
        name
        for name, output in _PROFILE_OUTPUTS.items()
        if output["type"] == "starrocks" and output.get("catalog", "default_catalog") != "default_catalog"
    }
    # The local-dev stack creates ol_warehouse_local itself.
    assert lake_outputs - {"starrocks_local"} == set(_LAKE_TARGETS)


@pytest.mark.parametrize("target", list(_LAKE_TARGETS))
def test_lake_target_matches_its_profile(target: str) -> None:
    lake = _LAKE_TARGETS[target]
    output = _PROFILE_OUTPUTS[target]
    assert output["catalog"] == lake.catalog
    assert output["schema"].startswith(lake.schema)
    assert ("schema_suffix" in output["schema"]) == (lake.suffix != "none")
    assert lake.env in _ENVS
    assert lake.catalog in _LOCATION_ROOTS


_RENDER_CASES = [
    ("ci", {}, "ol_warehouse_ci"),
    ("ci", {"schema_suffix": ""}, "ol_warehouse_ci"),
    ("ci", {"schema_suffix": "pr_123"}, "ol_warehouse_ci_pr_123"),
    ("starrocks_dev_qa", {"schema_suffix": "alice"}, "ol_warehouse_qa_alice"),
    ("starrocks_qa_lake", {"schema_suffix": "alice"}, "ol_warehouse_qa"),
    ("starrocks_production_lake", {}, "ol_warehouse_production"),
]


@pytest.mark.parametrize(("target", "cli_vars", "schema"), _RENDER_CASES)
def test_profile_schema_is_what_dbt_renders(target: str, cli_vars: dict[str, str], schema: str) -> None:
    """_profile_schema has to name the database dbt will connect to, or dbt makes its own without a location."""
    profile = pytest.importorskip("dbt.config.profile")
    renderer = pytest.importorskip("dbt.config.renderer")
    from dbt_common.context import set_invocation_context  # noqa: PLC0415

    set_invocation_context({"DBT_STARROCKS_USERNAME": "u", "DBT_STARROCKS_PASSWORD": "p"})
    raw = yaml.safe_load((_DBT_DIR / "profiles.yml").read_text())["open_learning"]
    _, rendered = profile.Profile.render_profile(raw, "open_learning", target, renderer.ProfileRenderer(cli_vars))

    assert rendered["schema"] == schema
    assert rendered["catalog"] == _LAKE_TARGETS[target].catalog
    assert _profile_schema(target, _LAKE_TARGETS[target], json.dumps(cli_vars) if cli_vars else None) == schema


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_lake_target_refuses_another_env_s_cluster(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    """The CI cluster reads ol_data_lake_qa but cannot write it, and production has no CI lake."""
    with pytest.raises(SystemExit):
        run(env="ci", target="starrocks_qa_lake")
    with pytest.raises(SystemExit):
        run(env="production", target="ci")

    mock_fetch.assert_not_called()
    mock_dbt_run.assert_not_called()


@pytest.mark.parametrize("cli_vars", [None, "{schema_suffix: ''}"])
@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_developer_lake_target_needs_a_suffix(
    mock_fetch, mock_port_forward, mock_dbt_run, cli_vars: str | None
) -> None:
    with pytest.raises(SystemExit):
        run(env="qa", target="starrocks_dev_qa", vars=cli_vars)

    mock_fetch.assert_not_called()


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_lake_target_refuses_a_schema_glue_cannot_load(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    with pytest.raises(SystemExit):
        run(env="qa", target="starrocks_dev_qa", vars="{schema_suffix: my-branch}")

    mock_fetch.assert_not_called()


@pytest.mark.parametrize(
    ("env", "target", "cli_vars", "schema", "host"),
    [
        ("ci", "ci", None, "ol_warehouse_ci", _ENVS["ci"]["host"]),
        ("ci", "ci", "{schema_suffix: pr_123}", "ol_warehouse_ci_pr_123", _ENVS["ci"]["host"]),
        # qa reaches the FE through the port-forward.
        ("qa", "starrocks_dev_qa", "{schema_suffix: alice}", "ol_warehouse_qa_alice", "127.0.0.1"),
        # The profile ignores schema_suffix, so the schema made must too.
        ("qa", "starrocks_qa_lake", "{schema_suffix: alice}", "ol_warehouse_qa", "127.0.0.1"),
    ],
)
@patch("mysql.connector.connect")
@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_lake_target_gets_its_profile_schema_with_a_location_first(  # noqa: PLR0913
    mock_fetch,
    mock_port_forward,
    mock_dbt_run,
    mock_connect,
    env: str,
    target: str,
    cli_vars: str | None,
    schema: str,
    host: str,
) -> None:
    mock_fetch.return_value = ("user", "pass")
    calls: list[str] = []
    mock_connect.return_value.cursor.return_value.execute.side_effect = lambda sql: calls.append(sql)
    mock_dbt_run.side_effect = lambda *_, **__: calls.append("dbt")

    run(env=env, target=target, vars=cli_vars)

    catalog = _LAKE_TARGETS[target].catalog
    _, kwargs = mock_connect.call_args
    assert (kwargs["user"], kwargs["password"]) == ("user", "pass")  # pragma: allowlist secret
    assert kwargs["host"] == host
    # app cannot create databases in a lake catalog.
    assert mock_fetch.call_args.args[3] == "admin"
    assert calls == [
        f"CREATE DATABASE IF NOT EXISTS `{catalog}`.`{schema}` "
        f'PROPERTIES ("location" = "{_LOCATION_ROOTS[catalog]}/{schema}")',
        "dbt",
    ]
    mock_connect.return_value.close.assert_called_once()


@patch("mysql.connector.connect")
@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_default_catalog_target_makes_no_schema(mock_fetch, mock_port_forward, mock_dbt_run, mock_connect) -> None:
    """The b2b targets' databases are provisioned in ol-infrastructure."""
    mock_fetch.return_value = ("user", "pass")
    run(env="ci")

    mock_connect.assert_not_called()
    mock_dbt_run.assert_called_once()
    assert mock_fetch.call_args.args[3] == "app"


@patch("mysql.connector.connect")
@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_dbt_does_not_start_when_the_profile_schema_cannot_be_made(
    mock_fetch, mock_port_forward, mock_dbt_run, mock_connect
) -> None:
    """Starting dbt anyway would create the database itself, with no location."""
    from mysql.connector import errors  # noqa: PLC0415

    mock_fetch.return_value = ("user", "pass")
    mock_connect.return_value.cursor.return_value.execute.side_effect = errors.ProgrammingError("Access denied")

    with pytest.raises(SystemExit):
        run(env="ci", target="ci", vault_role="app")

    mock_dbt_run.assert_not_called()


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
def test_dev_env_reads_the_local_lake_without_vault(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    """`--env dev` reaches the local StarRocks with no Vault login and no tunnel of its own."""
    run(env="dev")

    mock_fetch.assert_not_called()
    mock_port_forward.assert_not_called()
    assert os.environ["DBT_STARROCKS_HOST"] == "127.0.0.1"
    assert os.environ["DBT_DATA_LAKE_ENV"] == "local"
    assert "DBT_STARROCKS_USERNAME" not in os.environ
    _, kwargs = mock_dbt_run.call_args
    assert kwargs["target"] == "starrocks_local"
    assert kwargs["indirect_selection"] == "buildable"


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_indirect_selection_is_dbt_s_default_off_the_local_lake(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    """Only the local lake changes the default: the other environments get dbt's own."""
    mock_fetch.return_value = ("user", "password")
    run(env="qa")

    _, kwargs = mock_dbt_run.call_args
    assert kwargs["indirect_selection"] is None


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_explicit_indirect_selection_overrides_env_default(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    run(env="dev", indirect_selection="eager")

    _, kwargs = mock_dbt_run.call_args
    assert kwargs["indirect_selection"] == "eager"


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_dev_env_accepts_the_local_b2b_target(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    """The profile Dagster's `dev` builds the b2b views on is reachable from the CLI too."""
    run(env="dev", target="starrocks_local_b2b")

    mock_fetch.assert_not_called()
    _, kwargs = mock_dbt_run.call_args
    assert kwargs["target"] == "starrocks_local_b2b"


@pytest.mark.parametrize(
    ("env", "target"),
    [
        ("dev", "starrocks_production"),
        ("dev", "starrocks_qa_vault"),
        ("qa", "starrocks_local"),
        ("qa", "starrocks_local_b2b"),
    ],
)
@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_target_cannot_cross_between_local_and_vault(
    mock_fetch, mock_port_forward, mock_dbt_run, env: str, target: str
) -> None:
    """A Vault-backed target under `dev` has no credentials; the local one elsewhere has the wrong ones."""
    with pytest.raises(SystemExit):
        run(env=env, target=target)

    mock_fetch.assert_not_called()
    mock_port_forward.assert_not_called()
    mock_dbt_run.assert_not_called()


@patch("ol_dbt_cli.commands.starrocks._dbt_run")
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_explicit_target_keeps_the_env_s_data_lake(mock_fetch, mock_port_forward, mock_dbt_run) -> None:
    """--target moves the cluster; it must not move the lake with it.

    The whole point of the split is that "which cluster" and "which lake" are
    answered separately, so overriding one must leave the other alone.
    """
    mock_fetch.return_value = ("user", "pass")
    run(env="production", target="starrocks_qa_vault")

    assert os.environ["DBT_DATA_LAKE_ENV"] == "production"
    _, kwargs = mock_dbt_run.call_args
    assert kwargs["target"] == "starrocks_qa_vault"

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


# ---------------------------------------------------------------------------
# The branch report after a build on the local lake
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("env", "target", "reported"),
    [
        ("dev", None, True),
        # Reads default_catalog, not the raw schema the report counts.
        ("dev", "starrocks_local_b2b", False),
        ("qa", None, False),
    ],
)
@patch("ol_dbt_cli.commands.starrocks._report_local_branches")
@patch("ol_dbt_cli.commands.starrocks._dbt_run", side_effect=SystemExit(0))
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
@patch("ol_dbt_cli.commands.starrocks.fetch_vault_db_credentials")
def test_only_a_build_on_the_local_lake_reports_branches(  # noqa: PLR0913
    mock_fetch, mock_port_forward, mock_dbt_run, mock_report, env: str, target: str | None, reported: bool
) -> None:
    mock_fetch.return_value = ("user", "password")
    with pytest.raises(SystemExit):
        run(env=env, target=target)

    assert mock_report.called is reported


@patch("ol_dbt_cli.commands.starrocks._report_local_branches")
@patch("ol_dbt_cli.commands.starrocks._dbt_run", side_effect=SystemExit(1))
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
def test_branches_are_reported_on_a_failed_build_and_dbt_s_exit_code_survives(
    mock_port_forward, mock_dbt_run, mock_report
) -> None:
    """`ol-dbt run` leaves by sys.exit, and a failed build still built the models before the failure."""
    with pytest.raises(SystemExit) as exit_info:
        run(env="dev")

    assert exit_info.value.code == 1
    mock_report.assert_called_once()


@patch("ol_dbt_cli.commands.starrocks._report_local_branches", side_effect=RuntimeError("lake is gone"))
@patch("ol_dbt_cli.commands.starrocks._dbt_run", side_effect=SystemExit(2))
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
def test_a_report_that_fails_does_not_replace_dbt_s_exit_code(
    mock_port_forward, mock_dbt_run, mock_report, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        run(env="dev")

    assert exit_info.value.code == 2  # noqa: PLR2004
    assert "Branch report skipped" in capsys.readouterr().err


@patch("ol_dbt_cli.commands.starrocks._report_local_branches")
@patch("ol_dbt_cli.commands.starrocks._dbt_run", side_effect=KeyboardInterrupt)
@patch("ol_dbt_cli.commands.starrocks._start_port_forward")
def test_an_interrupted_build_reports_nothing(mock_port_forward, mock_dbt_run, mock_report) -> None:
    with pytest.raises(KeyboardInterrupt):
        run(env="dev")

    mock_report.assert_not_called()


def test_row_counts_cover_only_the_tables_the_raw_schema_holds() -> None:
    """A count on a table that is not there would fail the whole report."""
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[("raw__a",), ("raw__other",)], [(7,)]]
    with patch("mysql.connector.connect") as mock_connect:
        mock_connect.return_value.cursor.return_value = cursor
        counts = _local_raw_row_counts({"raw__a", "raw__b"})

    assert counts == {"raw__a": 7}
    assert mock_connect.call_args.kwargs["host"] == "127.0.0.1"
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements == [
        "SHOW TABLES FROM `ol_data_lake_local`.`ol_warehouse_local_raw`",
        "SELECT count(*) FROM `ol_data_lake_local`.`ol_warehouse_local_raw`.`raw__a`",
    ]


def _source(table: str) -> dict[str, object]:
    return {
        "unique_id": f"source.pkg.raw.{table}",
        "name": table,
        "identifier": table,
        "source_name": "raw",
        "resource_type": "source",
        "original_file_path": "models/staging/_sources.yml",
    }


def _model(name: str, path: str, parents: list[str]) -> dict[str, object]:
    return {
        "unique_id": f"model.pkg.{name}",
        "name": name,
        "resource_type": "model",
        "original_file_path": path,
        "depends_on": {"nodes": parents},
    }


@pytest.fixture
def local_build(tmp_path: Path) -> Path:
    """Lay out a repo with two units, a fixture for one, and the artifacts of a build of a union of both."""
    inventory = tmp_path / "ingestion" / "inventory"
    for deployment in ("mitxonline", "xpro"):
        unit = {
            "deployment": deployment,
            "layer": "app_postgres",
            "strategies": {"qa": "ingest", "local": "fixture"},
            "tables": [{"raw_table": f"raw__{deployment}__users"}],
        }
        path = inventory / "units" / f"{deployment}__app_postgres.yml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(unit))
    fixture = inventory / "fixtures" / "mitxonline__app_postgres.yml"
    fixture.parent.mkdir()
    fixture.write_text("---\n")

    nodes = [
        _model("stg_mitxonline", "models/staging/a.sql", ["source.pkg.raw.raw__mitxonline__users"]),
        _model("stg_xpro", "models/staging/b.sql", ["source.pkg.raw.raw__xpro__users"]),
        _model("dim_user", "models/dimensional/dim_user.sql", ["model.pkg.stg_mitxonline", "model.pkg.stg_xpro"]),
    ]
    sources = [_source("raw__mitxonline__users"), _source("raw__xpro__users")]
    target = tmp_path / "src" / "ol_dbt" / "target"
    target.mkdir(parents=True)
    (target / "manifest.json").write_text(
        json.dumps(
            {
                "nodes": {str(node["unique_id"]): node for node in nodes},
                "sources": {str(source["unique_id"]): source for source in sources},
            }
        )
    )
    results = [
        {"unique_id": "model.pkg.stg_mitxonline", "status": "success"},
        {"unique_id": "model.pkg.stg_xpro", "status": "success"},
        {"unique_id": "model.pkg.dim_user", "status": "success"},
        {"unique_id": "test.pkg.not_null_dim_user_id", "status": "pass"},
    ]
    (target / "run_results.json").write_text(json.dumps({"results": results}))
    return tmp_path


def test_report_names_the_branch_a_built_union_lacks(local_build: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with (
        patch("ol_dbt_cli.commands.starrocks.get_repo_root", return_value=local_build),
        patch(
            "ol_dbt_cli.commands.starrocks._local_raw_row_counts",
            return_value={"raw__mitxonline__users": 4},
        ) as mock_counts,
    ):
        _report_local_branches(local_build / "src" / "ol_dbt", time.time() - 60)

    mock_counts.assert_called_once_with({"raw__mitxonline__users", "raw__xpro__users"})
    out = capsys.readouterr().out
    assert "dim_user: 1 of 2 branches present" in out
    assert "xpro/app_postgres: absent (1 table(s)); no fixture committed for this unit" in out
    assert "mitxonline/app_postgres: present" in out


def test_report_skips_results_left_by_an_earlier_run(local_build: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A dbt run that stops before its first node leaves the old run_results.json, which describes another build."""
    with patch("ol_dbt_cli.commands.starrocks._local_raw_row_counts") as mock_counts:
        _report_local_branches(local_build / "src" / "ol_dbt", time.time() + 60)

    mock_counts.assert_not_called()
    assert capsys.readouterr().out == ""


def test_report_opens_no_connection_when_the_build_made_no_union(
    local_build: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    results = local_build / "src" / "ol_dbt" / "target" / "run_results.json"
    results.write_text(json.dumps({"results": [{"unique_id": "model.pkg.stg_xpro", "status": "success"}]}))
    with (
        patch("ol_dbt_cli.commands.starrocks.get_repo_root", return_value=local_build),
        patch("ol_dbt_cli.commands.starrocks._local_raw_row_counts") as mock_counts,
    ):
        _report_local_branches(local_build / "src" / "ol_dbt", time.time() - 60)

    mock_counts.assert_not_called()
    assert capsys.readouterr().out == ""


def test_report_flags_a_union_rebuilt_without_its_parents(
    local_build: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    results = local_build / "src" / "ol_dbt" / "target" / "run_results.json"
    results.write_text(json.dumps({"results": [{"unique_id": "model.pkg.dim_user", "status": "success"}]}))
    with (
        patch("ol_dbt_cli.commands.starrocks.get_repo_root", return_value=local_build),
        patch(
            "ol_dbt_cli.commands.starrocks._local_raw_row_counts",
            return_value={"raw__mitxonline__users": 4, "raw__xpro__users": 2},
        ),
    ):
        _report_local_branches(local_build / "src" / "ol_dbt", time.time() - 60)

    out = capsys.readouterr().out
    assert "1 of 1 union model(s) built have rows in every branch" in out
    assert "built on parents this run did not rebuild" in out


def test_report_counts_a_snapshot_the_run_built(local_build: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """`dbt build` snapshots too, and its result must not read as a parent left alone."""
    target = local_build / "src" / "ol_dbt" / "target"
    manifest = json.loads((target / "manifest.json").read_text())
    manifest["nodes"]["snapshot.pkg.snapshot_xpro"] = {
        "unique_id": "snapshot.pkg.snapshot_xpro",
        "name": "snapshot_xpro",
        "resource_type": "snapshot",
        "original_file_path": "snapshots/xpro.sql",
        "depends_on": {"nodes": ["model.pkg.stg_xpro"]},
    }
    manifest["nodes"]["model.pkg.dim_user"]["depends_on"]["nodes"] = [
        "model.pkg.stg_mitxonline",
        "snapshot.pkg.snapshot_xpro",
    ]
    (target / "manifest.json").write_text(json.dumps(manifest))
    models = ["model.pkg.stg_mitxonline", "model.pkg.stg_xpro", "model.pkg.dim_user"]

    def report(unique_ids: list[str]) -> str:
        results = [{"unique_id": unique_id, "status": "success"} for unique_id in unique_ids]
        (target / "run_results.json").write_text(json.dumps({"results": results}))
        with (
            patch("ol_dbt_cli.commands.starrocks.get_repo_root", return_value=local_build),
            patch(
                "ol_dbt_cli.commands.starrocks._local_raw_row_counts",
                return_value={"raw__mitxonline__users": 4, "raw__xpro__users": 2},
            ),
        ):
            _report_local_branches(target.parent, time.time() - 60)
        return capsys.readouterr().out

    assert "built on parents this run did not rebuild" in report(models)
    assert "built on parents this run did not rebuild" not in report([*models, "snapshot.pkg.snapshot_xpro"])
