"""StarRocks-targeted dbt commands with Vault credential management.

Wraps ``ol-dbt run`` with automatic Vault dynamic-credential fetching and
kubectl port-forward setup, so local developers can run dbt against the
StarRocks lakehouse without manually managing credentials or tunnels.

StarRocks uses native-password auth for service accounts (dbt, Vault dynamic
users). Credentials are issued by Vault's database secrets engine
(vault-plugin-database-starrocks) and are short-lived (default TTL: 3 months,
max: 6 months). The full OIDC flow documented in ``bin/starrocks-auth`` is
for human interactive sessions; dbt uses the Vault path exclusively.

Workflow::

    # One-shot: port-forward lives for the dbt run duration
    ol-dbt starrocks run

    # Explicit env / role
    ol-dbt starrocks run --env production --vault-role readonly

    # Pass dbt flags through
    ol-dbt starrocks run --full-refresh --select my_model+

dbt ``profiles.yml`` must reference the env vars this command injects::

    starrocks:
      outputs:
        dev:
          type: starrocks
          host: "{{ env_var('DBT_STARROCKS_HOST') }}"
          port: 9030
          username: "{{ env_var('DBT_STARROCKS_USERNAME') }}"
          password: "{{ env_var('DBT_STARROCKS_PASSWORD') }}"
"""

from __future__ import annotations

import atexit
import os
import socket
import subprocess
import sys
import time
from typing import Annotated, Literal

import cyclopts
from cyclopts import Parameter
from rich.console import Console

from ol_dbt_cli.commands._vault_auth import fetch_vault_db_credentials
from ol_dbt_cli.commands.run import run as _dbt_run

console = Console()
err_console = Console(stderr=True)

_STARROCKS_PORT = 9030
_PORT_FORWARD_TIMEOUT = 15

# Mirrors ENVS in bin/starrocks-auth; keep in sync when adding environments.
_ENVS: dict[str, dict[str, str]] = {
    "qa": {
        "host": "lakehouse.qa.starrocks.ol.mit.edu",
        "eks_context": "arn:aws:eks:us-east-1:610119931565:cluster/data-qa",
        "k8s_namespace": "starrocks",
        "fe_service": "lakehouse-starrocks-fe-service",
        "vault_addr": "https://vault-qa.odl.mit.edu",
        "vault_mount": "database-starrocks-qa",
        "dbt_target": "starrocks_qa_vault",
    },
    "production": {
        "host": "lakehouse.starrocks.ol.mit.edu",
        "eks_context": "arn:aws:eks:us-east-1:610119931565:cluster/data-production",
        "k8s_namespace": "starrocks",
        "fe_service": "lakehouse-starrocks-fe-service",
        "vault_addr": "https://vault-production.odl.mit.edu",
        "vault_mount": "database-starrocks-production",
        "dbt_target": "starrocks_production",
    },
    "ci": {
        "host": "lakehouse.ci.starrocks.ol.mit.edu",
        "eks_context": "arn:aws:eks:us-east-1:610119931565:cluster/data-ci",
        "k8s_namespace": "starrocks",
        "fe_service": "lakehouse-starrocks-fe-service",
        "vault_addr": "https://vault-qa.odl.mit.edu",
        "vault_mount": "database-starrocks-ci",
        "dbt_target": "starrocks_production",
    },
}


def _start_port_forward(env_cfg: dict[str, str]) -> None:
    proc = subprocess.Popen(
        [
            "kubectl",
            "--context",
            env_cfg["eks_context"],
            "-n",
            env_cfg["k8s_namespace"],
            "port-forward",
            f"svc/{env_cfg['fe_service']}",
            f"{_STARROCKS_PORT}:{_STARROCKS_PORT}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + _PORT_FORWARD_TIMEOUT
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            err = proc.stderr.read().decode().strip() if proc.stderr else ""
            err_console.print(f"[red]kubectl port-forward exited early:[/] {err}")
            sys.exit(1)
        try:
            with socket.create_connection(("127.0.0.1", _STARROCKS_PORT), timeout=0.5):
                break
        except OSError:
            time.sleep(0.2)
    else:
        proc.terminate()
        err_console.print(
            f"[red]Port-forward to {env_cfg['fe_service']}:{_STARROCKS_PORT} "
            f"did not become ready within {_PORT_FORWARD_TIMEOUT}s[/]"
        )
        sys.exit(1)

    err_console.print(
        f"[dim]Port-forward active: localhost:{_STARROCKS_PORT} -> {env_cfg['fe_service']}:{_STARROCKS_PORT}[/]"
    )
    atexit.register(proc.terminate)


starrocks_app = cyclopts.App(
    name="starrocks",
    help="Run dbt against the StarRocks lakehouse using Vault dynamic credentials.",
)


@starrocks_app.default
def run(  # noqa: PLR0913
    subcommand: Annotated[
        Literal["build", "run", "test"],
        Parameter(show_default=True, help="dbt subcommand to execute."),
    ] = "build",
    *,
    env: Annotated[
        str,
        Parameter(name=["--env", "-e"], help="Target StarRocks environment (qa, production, ci)."),
    ] = "qa",
    vault_role: Annotated[
        str,
        Parameter(name="--vault-role", help="Vault database role: readonly, app (default), admin."),
    ] = "app",
    vault_oidc_role: Annotated[
        str,
        Parameter(
            name="--vault-oidc-role",
            help="Vault OIDC auth role used to obtain a Vault token (default: developer). "
            "Use 'admin' when the developer role lacks policy for the target database mount.",
        ),
    ] = "developer",
    port_forward: Annotated[
        bool,
        Parameter(
            name="--port-forward",
            help="Tunnel StarRocks MySQL port via kubectl port-forward (default: true).",
        ),
    ] = True,
    target: Annotated[
        str | None,
        Parameter(name=["--target", "-t"], help="dbt target profile to use."),
    ] = None,
    select: Annotated[
        str | None,
        Parameter(name=["--select", "-s"], help="Explicit dbt node selection string."),
    ] = None,
    full_refresh: Annotated[
        bool,
        Parameter(name="--full-refresh", help="Force a complete rebuild of all models, ignoring state."),
    ] = False,
    defer: Annotated[
        bool,
        Parameter(name="--defer", help="Defer upstream refs to state manifest. Default: enabled."),
    ] = True,
    save_state: Annotated[
        bool,
        Parameter(name="--save-state", help="Save state artifacts after run. Default: enabled."),
    ] = True,
    state_dir: Annotated[
        str | None,
        Parameter(name="--state-dir", help="Directory for state artifacts."),
    ] = None,
    vars: Annotated[
        str | None,
        Parameter(name="--vars", help="dbt variables as a YAML/JSON string."),
    ] = None,
    project_dir: Annotated[
        str | None,
        Parameter(name="--project-dir", help="Path to the dbt project root."),
    ] = None,
) -> None:
    """Run dbt against StarRocks with Vault-issued credentials.

    Fetches a short-lived native-password credential from Vault's dynamic
    database secrets engine, optionally starts a kubectl port-forward to the
    StarRocks FE service, injects DBT_STARROCKS_USERNAME / DBT_STARROCKS_PASSWORD /
    DBT_STARROCKS_HOST into the environment, then delegates to ``ol-dbt run``
    with full state-based incremental selection.
    """
    if env not in _ENVS:
        err_console.print(f"[red]Unknown environment:[/] {env!r}. Choose from: {', '.join(_ENVS)}")
        sys.exit(1)

    env_cfg = _ENVS[env]
    console.print(
        f"[bold]ol-dbt starrocks[/] — env: [cyan]{env}[/], "
        f"role: [cyan]{vault_role}[/], "
        f"port-forward: [cyan]{port_forward}[/]"
    )

    console.print(f"[dim]Fetching Vault credentials ({env_cfg['vault_mount']}/creds/{vault_role})...[/]")
    username, password = fetch_vault_db_credentials(
        env_cfg["vault_addr"], env_cfg["vault_mount"], env, vault_role, vault_oidc_role
    )
    console.print(f"[dim]Vault user:[/] {username}")

    host = "127.0.0.1" if port_forward else env_cfg["host"]
    if port_forward:
        _start_port_forward(env_cfg)

    # Inject credentials into the current process environment so that the dbt
    # subprocess launched by _dbt_run() inherits them automatically.
    # Use the DBT_STARROCKS_* names that profiles.yml already references.
    os.environ["DBT_STARROCKS_USERNAME"] = username
    os.environ["DBT_STARROCKS_PASSWORD"] = password
    os.environ["DBT_STARROCKS_HOST"] = host

    # Default to the env-appropriate dbt target if the caller didn't specify one.
    effective_target = target or env_cfg["dbt_target"]

    _dbt_run(
        subcommand,
        target=effective_target,
        select=select,
        full_refresh=full_refresh,
        defer=defer,
        save_state=save_state,
        state_dir=state_dir,
        vars=vars,
        project_dir=project_dir,
    )
