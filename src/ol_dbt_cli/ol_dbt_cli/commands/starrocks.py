"""StarRocks-targeted dbt commands with Vault credential management.

Wraps ``ol-dbt run`` with automatic Vault dynamic-credential fetching and
kubectl port-forward setup, so local developers can run dbt against the
StarRocks lakehouse without manually managing credentials or tunnels.

StarRocks uses native-password auth for service accounts (dbt, Vault dynamic
users). Credentials are issued by Vault's database secrets engine
(vault-plugin-database-starrocks) and are short-lived (default TTL: 3 months,
max: 6 months). The full OIDC flow documented in ``bin/starrocks-auth`` is
for human interactive sessions; dbt uses the Vault path exclusively.

The exception is ``--env dev``, the lake in the ol-infrastructure local-dev
cluster. It has no Vault: the local StarRocks takes a passwordless root, and
``--vault-role`` / ``--vault-oidc-role`` are ignored.

Workflow::

    # One-shot: port-forward lives for the dbt run duration
    ol-dbt starrocks run

    # Explicit env / role
    ol-dbt starrocks run --env production --vault-role readonly

    # Pass dbt flags through
    ol-dbt starrocks run --full-refresh --select my_model+

    # The local lake in the ol-infrastructure local-dev cluster: no Vault
    ol-dbt starrocks run --env dev --select +integrations__learn__oll_courses

    # Your own copy of the QA warehouse in ol_data_lake_qa
    ol-dbt starrocks run --target starrocks_dev_qa --vars '{schema_suffix: alice}' --select +my_model

    # A PR build in the CI lake
    ol-dbt starrocks run --env ci --target ci --vars '{schema_suffix: pr_123}' --select my_model+

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
import json
import os
import re
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal, cast

import cyclopts
import yaml
from cyclopts import Parameter
from rich.console import Console

from ol_dbt_cli.commands._vault_auth import fetch_vault_db_credentials
from ol_dbt_cli.commands.fixtures import LOCAL_CATALOG, LOCAL_RAW_SCHEMA, connect_local_lake
from ol_dbt_cli.commands.run import IndirectSelection, _find_dbt_dir
from ol_dbt_cli.commands.run import run as _dbt_run
from ol_dbt_cli.lib.git_utils import get_repo_root
from ol_dbt_cli.lib.inventory import DEFAULT_INVENTORY_DIR, load_units
from ol_dbt_cli.lib.local_branches import (
    BUILT_RESOURCE_TYPES,
    local_branches,
    parents_not_built,
    render_local_branches,
    union_tables,
)
from ol_dbt_cli.lib.manifest import load_manifest
from ol_dbt_cli.lib.raw_fixtures import fixture_path, qualified_name

console = Console()
err_console = Console(stderr=True)

_STARROCKS_PORT = 9030
_LOCAL_DBT_TARGET = "starrocks_local"
# The second local profile writes StarRocks' own tables (the b2b materialized
# views) into default_catalog on the same cluster. Dagster's `dev` builds on it.
_LOCAL_DBT_TARGETS = frozenset({_LOCAL_DBT_TARGET, "starrocks_local_b2b"})
_PORT_FORWARD_TIMEOUT = 15

# Mirrors ENVS in bin/starrocks-auth; keep in sync when adding environments.
#
# Vault's "database-starrocks" mount is not environment-specific -- each
# environment runs its own, entirely separate Vault deployment, so the Vault
# server (vault_addr below), not the mount path, is what scopes the environment.
_VAULT_MOUNT = "database-starrocks"

# `dbt_target` names a CLUSTER and its auth; `data_lake_env` names the external
# catalog the b2b models READ. They are separate axes, and `ci` shows all three
# coming apart at once: the target is *called* starrocks_production, it connects
# to the CI cluster's own FE, and it reads the QA lake. Inferring any one of
# those from another -- which `'qa' in target.name` did -- is what RFC 12711
# step 1 exists to undo. Both are stated per environment rather than derived,
# so adding an environment forces both answers.
#
# Mirrors STARROCKS_DBT_TARGET_MAP / DATA_LAKE_ENV_MAP in
# lakehouse.lib.dbt_environment; keep the two in step. The `dev` targets differ
# on purpose (see its entry).
_ENVS: dict[str, dict[str, Any]] = {
    # `dev` is the local lake from ol-infrastructure's local-dev stack
    # (`data-platform` in enabled_apps): StarRocks in k3d, with the
    # ol_data_lake_local catalog on Gravitino and RustFS. It has no Vault
    # (`vault_addr: None`): the local StarRocks takes a passwordless root, which
    # the starrocks_local profile names itself. Tilt keeps 127.0.0.1:9030
    # forwarded, so no port-forward is started here unless --port-forward asks
    # for one.
    # Dagster's `dev` is the same cluster and lake, on the starrocks_local_b2b
    # profile: its StarRocks project builds only the b2b materialized views,
    # which belong in default_catalog. `--target starrocks_local_b2b` does the
    # same from here.
    "dev": {
        "host": "127.0.0.1",
        "eks_context": "k3d-local-dev",
        "k8s_namespace": "local-infra",
        "fe_service": "starrocks",
        "vault_addr": None,
        "vault_mount": None,
        "dbt_target": _LOCAL_DBT_TARGET,
        "data_lake_env": "local",
        "port_forward": False,
        # The local lake holds only what the developer built. dbt's default runs
        # a relationships test when either model it joins is selected, so
        # `--select +one_model` would fail on the tables of every model that
        # points at one of its parents.
        "indirect_selection": "buildable",
    },
    "qa": {
        "host": "lakehouse.qa.starrocks.ol.mit.edu",
        "eks_context": "data-qa",
        "k8s_namespace": "starrocks",
        "fe_service": "lakehouse-starrocks-fe-service",
        "vault_addr": "https://vault-qa.odl.mit.edu",
        "vault_mount": _VAULT_MOUNT,
        "dbt_target": "starrocks_qa_vault",
        "data_lake_env": "qa",
    },
    "production": {
        "host": "lakehouse.starrocks.ol.mit.edu",
        "eks_context": "data-production",
        "k8s_namespace": "starrocks",
        "fe_service": "lakehouse-starrocks-fe-service",
        "vault_addr": "https://vault-production.odl.mit.edu",
        "vault_mount": _VAULT_MOUNT,
        "dbt_target": "starrocks_production",
        "data_lake_env": "production",
    },
    # The b2b default stays starrocks_production, reading the QA lake. CI's own
    # lake, ol_data_lake_ci, is the `ci` target in _LAKE_TARGETS.
    "ci": {
        "host": "lakehouse.ci.starrocks.ol.mit.edu",
        "eks_context": "data-ci",
        "k8s_namespace": "starrocks",
        "fe_service": "lakehouse-starrocks-fe-service",
        "vault_addr": "https://vault-ci.odl.mit.edu",
        "vault_mount": _VAULT_MOUNT,
        "dbt_target": "starrocks_production",
        # Matches trino_catalog_map["ci"] in the lakehouse definitions.
        "data_lake_env": "qa",
        "port_forward": False,
    },
}


# Glue's database name rule as Iceberg's GlueCatalog enforces it. The same check
# generate_schema_name.sql makes on every layer schema.
_GLUE_DATABASE_NAME = re.compile(r"[a-z0-9_]{1,252}")


@dataclass(frozen=True)
class _LakeTarget:
    """A profile that writes the layered warehouse into an Iceberg catalog.

    :param env: The one ``_ENVS`` entry whose cluster can write ``catalog``.
    :param catalog: The profile's ``catalog``.
    :param schema: The profile's ``schema`` before any ``_<schema_suffix>``.
    :param suffix: Whether the profile appends the ``schema_suffix`` var:
        always, only when one is passed, or never.
    """

    env: str
    catalog: str
    schema: str
    suffix: Literal["required", "optional", "none"]


# Every lake profile in profiles.yml except starrocks_local, whose schema the
# local-dev stack creates. dbt-starrocks connects to <catalog>.<schema> and, if
# that fails for any reason, runs a bare CREATE DATABASE for it before any macro
# can give it a location. Seeds build into that schema, so it has to exist with a
# location before dbt connects. _ensure_profile_schema makes it.
_LAKE_TARGETS: dict[str, _LakeTarget] = {
    "starrocks_qa_lake": _LakeTarget("qa", "ol_data_lake_qa", "ol_warehouse_qa", "none"),
    "starrocks_dev_qa": _LakeTarget("qa", "ol_data_lake_qa", "ol_warehouse_qa", "required"),
    "starrocks_production_lake": _LakeTarget(
        "production", "ol_data_lake_production", "ol_warehouse_production", "none"
    ),
    "ci": _LakeTarget("ci", "ol_data_lake_ci", "ol_warehouse_ci", "optional"),
}


def _profile_schema(target: str, lake: _LakeTarget, cli_vars: str | None) -> str:
    """Return the schema the *target* profile renders for these ``--vars``.

    :raises SystemExit: when the profile needs a ``schema_suffix`` and none was
        passed, or the result is not a name Iceberg can load from Glue.
    """
    suffix = (yaml.safe_load(cli_vars) or {}).get("schema_suffix") if cli_vars else None
    if lake.suffix == "required" and not suffix:
        err_console.print(f"[red]--target {target} needs a schema suffix:[/] pass --vars '{{schema_suffix: <name>}}'.")
        sys.exit(1)
    schema = f"{lake.schema}_{suffix}" if suffix and lake.suffix != "none" else lake.schema
    if not _GLUE_DATABASE_NAME.fullmatch(schema):
        err_console.print(
            f"[red]Schema {schema!r} is not one Iceberg can load from Glue:[/] "
            "use 1-252 lowercase letters, digits and underscores in schema_suffix."
        )
        sys.exit(1)
    return schema


def _ensure_profile_schema(lake: _LakeTarget, schema: str, host: str, dbt_dir: Path) -> None:
    """Create the profile's schema in *lake*'s catalog, with a location, if it is missing.

    The location follows starrocks__create_schema: ``<root>/<schema>``, with the
    root from the ``iceberg_schema_location_roots`` var in dbt_project.yml.
    ``IF NOT EXISTS`` leaves an existing database as it is, location or not.
    """
    import mysql.connector  # noqa: PLC0415
    from mysql.connector import errors  # noqa: PLC0415

    project = yaml.safe_load((dbt_dir / "dbt_project.yml").read_text())
    location = f"{project['vars']['iceberg_schema_location_roots'][lake.catalog]}/{schema}"
    try:
        connection = mysql.connector.connect(
            host=host,
            port=_STARROCKS_PORT,
            user=os.environ["DBT_STARROCKS_USERNAME"],
            password=os.environ["DBT_STARROCKS_PASSWORD"],
            autocommit=True,
        )
        try:
            connection.cursor().execute(
                f"CREATE DATABASE IF NOT EXISTS {qualified_name(lake.catalog, schema)} "
                f'PROPERTIES ("location" = "{location}")'
            )
        finally:
            connection.close()
    except errors.Error as exc:
        # Starting dbt anyway would have it create the database with no location.
        err_console.print(f"[red]Could not create {lake.catalog}.{schema}:[/] {exc}")
        sys.exit(1)
    console.print(f"[dim]Profile schema:[/] {lake.catalog}.{schema} ({location})")


def _port_is_free(port: int) -> bool:
    """Return True if nothing is currently listening on 127.0.0.1:port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            return False
        return True


def _start_port_forward(env_cfg: dict[str, Any]) -> None:
    # Check before spawning kubectl, not after: the readiness poll below only
    # tests "can I connect", which a stale/unrelated listener from a prior
    # crashed run would satisfy instantly, before this run's kubectl even
    # attempts to bind -- silently tunneling dbt to the wrong process/environment.
    if not _port_is_free(_STARROCKS_PORT):
        err_console.print(
            f"[red]Port {_STARROCKS_PORT} is already in use locally[/] -- refusing "
            "to start a new port-forward. Check for a leftover `kubectl port-forward` "
            "from a previous run (e.g. `lsof -i :9030`) and stop it first."
        )
        sys.exit(1)

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
    help="Run dbt against the StarRocks lakehouse using Vault dynamic credentials (none needed for --env dev).",
)


def _local_raw_row_counts(tables: set[str]) -> dict[str, int]:
    """Count the rows of each of ``tables`` that the local raw schema holds.

    StarRocks answers from the snapshot it has cached, which can trail an
    external write by up to 30 seconds. A staging model built in the same run
    read that snapshot too.
    """
    connection = connect_local_lake()
    cursor = connection.cursor()
    cursor.execute(f"SHOW TABLES FROM {qualified_name(LOCAL_CATALOG, LOCAL_RAW_SCHEMA)}")
    held = {str(name) for (name,) in cast("list[tuple[str]]", cursor.fetchall())}
    counts: dict[str, int] = {}
    for table in sorted(tables & held):
        cursor.execute(f"SELECT count(*) FROM {qualified_name(LOCAL_CATALOG, LOCAL_RAW_SCHEMA, table)}")  # noqa: S608
        ((counts[table],),) = cast("list[tuple[int]]", cursor.fetchall())
    connection.close()
    return counts


def _report_local_branches(dbt_dir: Path, started: float) -> None:
    """Say which ingestion branches the unions this build produced had rows for.

    The local raw schema holds what the developer loaded, so a union built here
    is usually missing branches, and dbt reports it as a success either way.
    """
    results_path = dbt_dir / "target" / "run_results.json"
    # dbt leaves the previous run's file in place when it stops before running a node.
    if not results_path.exists() or results_path.stat().st_mtime < started:
        return
    manifest = load_manifest(dbt_dir / "target" / "manifest.json")
    built = [
        manifest.nodes[result["unique_id"]]
        for result in json.loads(results_path.read_text())["results"]
        if result["status"] == "success" and result["unique_id"].split(".", 1)[0] in BUILT_RESOURCE_TYPES
    ]
    models = {node.name for node in built if node.is_model}
    inventory_dir = get_repo_root(dbt_dir) / DEFAULT_INVENTORY_DIR
    units = load_units(inventory_dir)
    unions = union_tables(manifest, units, models)
    if not unions:
        return
    rows = _local_raw_row_counts(
        {table for by_unit in unions.values() for tables in by_unit.values() for table in tables}
    )
    fixture_units = {unit.key for unit in units if fixture_path(inventory_dir, unit).exists()}
    lines = render_local_branches(
        local_branches(unions, units, rows, fixture_units),
        LOCAL_RAW_SCHEMA,
        parents_not_built(manifest, unions, {node.name for node in built}),
    )
    console.print()
    for line in lines:
        console.print(line, markup=False, highlight=False, soft_wrap=True)


@starrocks_app.default
def run(  # noqa: PLR0913
    subcommand: Annotated[
        Literal["build", "run", "test"],
        Parameter(show_default=True, help="dbt subcommand to execute."),
    ] = "build",
    *,
    env: Annotated[
        str,
        Parameter(
            name=["--env", "-e"],
            help="Target StarRocks environment (dev, qa, production, ci). `dev` is the local "
            "lake in the ol-infrastructure local-dev cluster and needs no Vault login.",
        ),
    ] = "qa",
    vault_role: Annotated[
        str | None,
        Parameter(
            name="--vault-role",
            help="Vault database role: readonly, app, admin. Default: admin for a lake target "
            "(app cannot create databases in a lake catalog), app otherwise.",
            show_default=False,
        ),
    ] = None,
    vault_oidc_role: Annotated[
        str,
        Parameter(
            name="--vault-oidc-role",
            help="Vault OIDC auth role used to obtain a Vault token (default: developer). "
            "Use 'admin' when the developer role lacks policy for the target database mount.",
        ),
    ] = "developer",
    port_forward: Annotated[
        bool | None,
        Parameter(
            name="--port-forward",
            help="Tunnel StarRocks MySQL port via kubectl port-forward (default: true; false for ci, "
            "and for dev, where Tilt already forwards it).",
        ),
    ] = None,
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
    indirect_selection: Annotated[
        IndirectSelection | None,
        Parameter(
            name="--indirect-selection",
            help="dbt's --indirect-selection. Default: buildable for dev, dbt's own (eager) elsewhere.",
            show_default=False,
        ),
    ] = None,
) -> None:
    """Run dbt against StarRocks with Vault-issued credentials.

    ``--env dev`` is the local lake and skips the Vault step. Otherwise this
    fetches a short-lived native-password credential from Vault's dynamic
    database secrets engine, optionally starts a kubectl port-forward to the
    StarRocks FE service, injects DBT_STARROCKS_USERNAME / DBT_STARROCKS_PASSWORD /
    DBT_STARROCKS_HOST into the environment, then delegates to ``ol-dbt run``
    with full state-based incremental selection.
    """
    if env not in _ENVS:
        err_console.print(f"[red]Unknown environment:[/] {env!r}. Choose from: {', '.join(_ENVS)}")
        sys.exit(1)

    env_cfg = _ENVS[env]
    if port_forward is None:
        port_forward = env_cfg.get("port_forward", True)
    # Print the lake alongside the cluster. Which data you are reading is not
    # inferable from the env name (`ci` is the CI cluster on the QA lake),
    # and "you cannot tell which mode you are in" is the specific failure this
    # separation exists to fix -- so say it, every run.
    uses_vault = env_cfg["vault_addr"] is not None
    # The local profiles carry their own credentials and the others read the
    # ones fetched from Vault below, so a --target on the other side of that
    # line would run with credentials meant for a different cluster, or none.
    if target is not None and (target in _LOCAL_DBT_TARGETS) == uses_vault:
        err_console.print(
            f"[red]--target {target} cannot be used with --env {env}:[/] "
            f"{', '.join(sorted(_LOCAL_DBT_TARGETS))} are the targets for the local lake, "
            "and they only work there."
        )
        sys.exit(1)
    # Default to the env-appropriate dbt target if the caller didn't specify one.
    effective_target = target or env_cfg["dbt_target"]
    lake = _LAKE_TARGETS.get(effective_target)
    if lake is not None and lake.env != env:
        err_console.print(
            f"[red]--target {effective_target} cannot be used with --env {env}:[/] it writes "
            f"{lake.catalog}, which only the {lake.env} cluster can write. Use --env {lake.env}."
        )
        sys.exit(1)
    lake_schema = _profile_schema(effective_target, lake, vars) if lake is not None else None
    # substructure/starrocks grants `app` only USAGE on a lake catalog and DML on
    # its tables. Building one takes CREATE DATABASE and CREATE TABLE.
    if vault_role is None:
        vault_role = "admin" if lake is not None else "app"
    console.print(
        f"[bold]ol-dbt starrocks[/] — env: [cyan]{env}[/], "
        f"cluster: [cyan]{env_cfg['host']}[/], "
        f"reading: [cyan]ol_data_lake_{env_cfg['data_lake_env']}[/], "
        + (f"writing: [cyan]{lake.catalog}.{lake_schema}_<layer>[/], " if lake is not None else "")
        + f"target: [cyan]{effective_target}[/], "
        f"role: [cyan]{vault_role if uses_vault else 'root (no Vault)'}[/], "
        f"port-forward: [cyan]{port_forward}[/]"
    )

    if uses_vault:
        console.print(f"[dim]Fetching Vault credentials ({env_cfg['vault_mount']}/creds/{vault_role})...[/]")
        username, password = fetch_vault_db_credentials(
            env_cfg["vault_addr"], env_cfg["vault_mount"], env, vault_role, vault_oidc_role
        )
        console.print(f"[dim]Vault user:[/] {username}")
        # Inject credentials into the current process environment so that the dbt
        # subprocess launched by _dbt_run() inherits them automatically.
        # Use the DBT_STARROCKS_* names that profiles.yml already references.
        os.environ["DBT_STARROCKS_USERNAME"] = username
        os.environ["DBT_STARROCKS_PASSWORD"] = password

    host = "127.0.0.1" if port_forward else env_cfg["host"]
    if port_forward:
        _start_port_forward(env_cfg)

    os.environ["DBT_STARROCKS_HOST"] = host
    # Read by _b2b_analytics__sources.yml's env_var() to pick the external
    # catalog. Set from --env even when --target overrides the cluster, so
    # "which lake" stays the environment's answer rather than something
    # inferred from a target name.
    os.environ["DBT_DATA_LAKE_ENV"] = env_cfg["data_lake_env"]

    if lake is not None and lake_schema is not None:
        _ensure_profile_schema(lake, lake_schema, host, _find_dbt_dir(project_dir))

    started = time.time()
    try:
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
            indirect_selection=indirect_selection or env_cfg.get("indirect_selection"),
        )
    except SystemExit:
        # _dbt_run always leaves this way, with dbt's return code. The b2b target
        # reads default_catalog, not the raw schema counted here.
        if effective_target == _LOCAL_DBT_TARGET:
            try:
                _report_local_branches(_find_dbt_dir(project_dir), started)
            except Exception as exc:  # noqa: BLE001
                # The report describes a build that is already over. Raising here
                # would replace dbt's exit code with a traceback's.
                err_console.print(f"[yellow]Branch report skipped:[/] {exc!r}")
        raise
