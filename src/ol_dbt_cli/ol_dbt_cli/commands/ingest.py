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

import os
import subprocess
import sys
from pathlib import Path

from cyclopts import App
from rich.console import Console
from rich.markup import escape

from ol_dbt_cli.commands.fixtures import InventoryDir, UnitKeys
from ol_dbt_cli.lib.git_utils import get_repo_root
from ol_dbt_cli.lib.inventory import DEFAULT_INVENTORY_DIR, load_units, local_ingest_sources

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


def source_environment() -> dict[str, str]:
    """Return the caller's environment, pinned to the local dlt profile."""
    # VIRTUAL_ENV names ol-dbt's own environment, which uv would warn about and
    # pass over: the source runs in ol_dlt's.
    environment = {key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"}
    environment["DLT_PROFILE"] = LOCAL_DLT_PROFILE
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
    sources = local_ingest_sources(load_units(inventory_dir))
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

    ol_dlt_dir = get_repo_root(inventory_dir) / OL_DLT_DIR
    environment = source_environment()
    failed = []
    for key, module in selected.items():
        console.print(f"[bold]{escape(key)}[/]: {escape(module)} (DLT_PROFILE={LOCAL_DLT_PROFILE})")
        command = ["uv", "run", "python", "-m", module]
        if subprocess.run(command, cwd=ol_dlt_dir, env=environment, check=False).returncode:  # noqa: S603
            failed.append(key)

    if failed:
        err_console.print(f"[bold red]Failed: {escape(', '.join(failed))}")
        sys.exit(1)
