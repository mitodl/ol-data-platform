"""`ol-dbt ingest` — raw tables for the local lake, from the local apps.

A unit whose inventory entry says ``strategies.local: ingest`` names a dlt
source that can read the app's database in the ol-infrastructure local-dev
cluster. ``local`` runs those sources under the ``dev`` dlt profile, which
writes ``ol_data_lake_local.ol_warehouse_local_raw``, the schema
``ol-dbt starrocks build --env dev`` reads. No Vault, no VPN, no AWS
credentials. The port-forwards a source needs are in "Local lake" in
``src/ol_dlt/README.md``.
"""

from __future__ import annotations

import ipaddress
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from cyclopts import App
from rich.console import Console
from rich.markup import escape

from ol_dbt_cli.commands.fixtures import InventoryDir, UnitKeys
from ol_dbt_cli.lib.git_utils import get_repo_root
from ol_dbt_cli.lib.inventory import DEFAULT_INVENTORY_DIR, local_ingest_sources, validate_inventory
from ol_dbt_cli.lib.validation import ValidationReport

if TYPE_CHECKING:
    from collections.abc import Mapping

console = Console()
err_console = Console(stderr=True)

ingest_app = App(
    name="ingest",
    help="Load the local lake's raw tables from the local-dev app databases.",
)

# ol_dlt is its own uv project, outside the root workspace, and it finds
# `.dlt/` relative to the working directory.
OL_DLT_DIR = Path("src/ol_dlt")
# Set on every run and never read from the caller's shell: one that exports
# DLT_PROFILE=qa or production would send this command's load to that lake.
LOCAL_DLT_PROFILE = "dev"
# ol_dlt.database reads this and refuses a `<NAME>_DB_HOST` override that is not
# a loopback address. The check lives there because the host is resolved there,
# through dlt's providers: the environment and .dlt/secrets.toml.
LOCAL_SOURCES_ONLY_VARIABLE = "OL_DLT_LOCAL_SOURCES_ONLY"
# The local catalog and object store (.dlt/.pyiceberg.yaml) can be moved from
# the environment, for a port-forward on another port. Each has to stay on this
# machine.
_CATALOG_URL_VARIABLES = (
    "PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__URI",
    "PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__S3__ENDPOINT",
)


def _is_loopback(host: str | None) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host or "").is_loopback
    except ValueError:
        return False


def catalog_overrides_off_this_machine(environment: Mapping[str, str]) -> list[str]:
    """Return the local-catalog overrides whose host is not a loopback address.

    :param environment: The environment a source would run in.
    :returns: The names of the offending variables.
    :rtype: list[str]
    """
    return [
        name
        for name in _CATALOG_URL_VARIABLES
        if name in environment and not _is_loopback(urlsplit(environment[name]).hostname)
    ]


def source_environment() -> dict[str, str]:
    """Return the caller's environment, pinned to the local dlt profile and the local-dev databases."""
    # VIRTUAL_ENV names ol-dbt's own environment, which uv would warn about and
    # pass over: the source runs in ol_dlt's. Without PYICEBERG_HOME ol_dlt
    # reads the catalog file committed beside it.
    environment = {key: value for key, value in os.environ.items() if key not in {"VIRTUAL_ENV", "PYICEBERG_HOME"}}
    environment["DLT_PROFILE"] = LOCAL_DLT_PROFILE
    environment[LOCAL_SOURCES_ONLY_VARIABLE] = "1"
    return environment


@ingest_app.command
def local(
    *,
    inventory_dir: InventoryDir = DEFAULT_INVENTORY_DIR,
    unit: UnitKeys = None,
) -> None:
    """Run the dlt source of every unit with `strategies.local: ingest`.

    Each source replaces its tables in the local lake's raw schema with the
    rows of the app database it reads, so run it after `ol-dbt fixtures load`
    when both cover a unit, and build with `ol-dbt starrocks build --env dev`
    afterwards. A source that fails does not stop the others, and the command
    then exits non-zero.
    """
    report = ValidationReport()
    units = validate_inventory(inventory_dir, report)
    if report.errors:
        for issue in report.errors:
            err_console.print(f"[bold red]{escape(issue.model)}: {escape(issue.message)}")
        err_console.print("Nothing was run. See `ol-dbt inventory validate`.")
        sys.exit(1)

    sources = local_ingest_sources(units)
    if unknown := sorted(set(unit or []) - sources.keys()):
        err_console.print(
            f"[bold red]Not a unit with strategies.local: ingest: {escape(', '.join(unknown))}.[/] "
            f"Those are: {escape(', '.join(sorted(sources)) or 'none')}."
        )
        sys.exit(1)
    selected = {key: sources[key] for key in sorted(unit or sources)}
    if not selected:
        err_console.print(f"[bold red]No unit under {escape(str(inventory_dir))} has strategies.local: ingest.")
        sys.exit(1)

    environment = source_environment()
    if elsewhere := catalog_overrides_off_this_machine(environment):
        err_console.print(
            f"[bold red]{escape(', '.join(elsewhere))} would take the load off this machine.[/] "
            "This command loads the local lake from the local-dev cluster. Unset what is "
            "named, or run the source yourself if that is what you mean."
        )
        sys.exit(1)

    ol_dlt_dir = get_repo_root(inventory_dir) / OL_DLT_DIR
    failed = []
    for key, module in selected.items():
        console.print(f"[bold]{escape(key)}[/]: {escape(module)} (DLT_PROFILE={LOCAL_DLT_PROFILE})")
        command = ["uv", "run", "python", "-m", module]
        try:
            returncode = subprocess.run(command, cwd=ol_dlt_dir, env=environment, check=False).returncode  # noqa: S603
        except OSError as error:
            # `uv` is not on PATH, or src/ol_dlt is not under the repository the inventory is in.
            err_console.print(f"[bold red]{escape(key)} did not start:[/] {escape(str(error))}")
            returncode = 1
        if returncode:
            failed.append(key)

    if failed:
        err_console.print(f"[bold red]Failed: {escape(', '.join(failed))}")
        sys.exit(1)
