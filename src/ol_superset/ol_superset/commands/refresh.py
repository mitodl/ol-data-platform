"""Refresh command - refresh physical dataset schemas in Superset."""

import sys
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter
from rich.console import Console
from rich.table import Table

from ol_superset.lib.superset_api import (
    create_authenticated_session,
    get_dataset_id_by_uuid,
    get_dataset_uuids_from_directory,
    get_instance_config,
    list_all_datasets_from_api,
    refresh_dataset,
)
from ol_superset.lib.utils import get_assets_dir

console = Console()


def refresh(
    instance: Annotated[
        str, Parameter(help="Target instance name (e.g., superset-production)")
    ],
    all_datasets: Annotated[
        bool,
        Parameter(
            name=["--all", "-a"],
            help="Refresh all physical datasets in the instance",
        ),
    ] = False,
    uuid: Annotated[
        list[str] | None,
        Parameter(
            name=["--uuid", "-u"],
            help="Refresh specific dataset(s) by UUID (repeatable)",
        ),
    ] = None,
    local: Annotated[
        bool,
        Parameter(
            name=["--local", "-l"],
            help="Refresh datasets found in local assets directory",
        ),
    ] = False,
    assets_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--assets-dir"],
            help="Assets directory path (default: assets/)",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        Parameter(
            name=["--dry-run", "-n"],
            help="Show what would be refreshed without refreshing",
        ),
    ] = False,
) -> None:
    """
    Refresh physical dataset schemas to pick up new columns from dbt model changes.

    Calls the Superset API to sync each dataset's column list with the underlying
    database table. Run this after deploying a dbt model that adds or removes columns
    so that charts and dashboards can use the updated schema.

    Only physical (table-backed) datasets are refreshed; virtual (SQL-based) datasets
    are skipped when using --all or --local.

    Examples:
        Refresh all physical datasets in production:
            ol-superset refresh superset-production --all

        Refresh specific datasets by UUID:
            ol-superset refresh superset-qa --uuid abc123... --uuid def456...

        Refresh datasets listed in the local assets directory:
            ol-superset refresh superset-production --local

        Preview what would be refreshed:
            ol-superset refresh superset-production --all --dry-run
    """
    if not (all_datasets or uuid or local):
        console.print(
            "[red]Error: Must specify at least one of --all, --uuid, or --local[/red]"
        )
        sys.exit(1)

    console.print()
    console.rule("[bold]Refreshing Superset Dataset Schemas[/bold]")
    console.print()
    console.print(f"Instance: [cyan]{instance}[/cyan]")
    if dry_run:
        console.print("[yellow]DRY RUN MODE - No changes will be made[/yellow]")
    console.print()

    config = get_instance_config(instance)
    if not config:
        console.print("[red]❌ Could not get instance configuration[/red]")
        sys.exit(1)

    base_url = config["url"]

    console.print("🔐 Authenticating with Superset API...")
    session = create_authenticated_session(instance)
    if not session:
        console.print("[red]❌ Could not create authenticated session[/red]")
        sys.exit(1)

    console.print(f"✅ Authenticated to {base_url}")
    console.print()

    datasets_to_refresh: list[dict[str, str | int | None]] = []

    if local:
        assets_dir: Path = get_assets_dir(assets_dir_path)
        console.print("📂 Reading datasets from local assets directory...")
        entries = get_dataset_uuids_from_directory(assets_dir, physical_only=True)

        if not entries:
            console.print(
                f"  [yellow]No physical datasets found in "
                f"{assets_dir / 'datasets'}[/yellow]"
            )
        else:
            console.print(f"  Found {len(entries)} physical dataset(s) locally")
            for entry_uuid, table_name in entries:
                dataset_id = get_dataset_id_by_uuid(session, base_url, entry_uuid)
                if dataset_id is not None:
                    datasets_to_refresh.append(
                        {
                            "id": dataset_id,
                            "uuid": entry_uuid,
                            "table_name": table_name or entry_uuid[:8] + "...",
                        }
                    )
                else:
                    console.print(
                        f"  [yellow]⚠️  Could not find dataset with UUID "
                        f"{entry_uuid}[/yellow]"
                    )

    elif uuid:
        console.print(f"🎯 Looking up {len(uuid)} specific dataset(s)...")
        for entry_uuid in uuid:
            dataset_id = get_dataset_id_by_uuid(session, base_url, entry_uuid)
            if dataset_id is not None:
                datasets_to_refresh.append(
                    {
                        "id": dataset_id,
                        "uuid": entry_uuid,
                        "table_name": entry_uuid[:8] + "...",
                    }
                )
            else:
                console.print(
                    f"  [yellow]⚠️  Could not find dataset with UUID "
                    f"{entry_uuid}[/yellow]"
                )

    elif all_datasets:
        console.print(f"📋 Fetching all physical datasets from {instance}...")
        api_datasets = list_all_datasets_from_api(session, base_url, physical_only=True)
        if not api_datasets:
            console.print("  [yellow]No physical datasets found[/yellow]")
        else:
            console.print(f"  Found {len(api_datasets)} physical dataset(s)")
            datasets_to_refresh = api_datasets

    console.print()

    if not datasets_to_refresh:
        console.print("[yellow]⚠️  No datasets to refresh[/yellow]")
        sys.exit(0)

    console.print(f"[bold]Datasets to refresh:[/bold] {len(datasets_to_refresh)}")
    console.print()

    if dry_run:
        table = Table(title="Datasets that would be refreshed")
        table.add_column("ID", style="cyan")
        table.add_column("UUID", style="magenta")
        table.add_column("Table / Schema", style="white")

        for item in datasets_to_refresh[:30]:
            schema = item.get("schema") or ""
            table_name = item.get("table_name") or ""
            label = f"{schema}.{table_name}" if schema else str(table_name)
            item_uuid = item.get("uuid") or ""
            uuid_display = str(item_uuid)[:16] + "..." if item_uuid else "N/A"
            table.add_row(str(item["id"]), uuid_display, label[:60])

        if len(datasets_to_refresh) > 30:
            table.add_row(
                "...", "...", f"({len(datasets_to_refresh) - 30} more)", style="dim"
            )

        console.print(table)
        console.print()
        console.print(
            f"[green]✅ Dry run complete. "
            f"{len(datasets_to_refresh)} dataset(s) would be refreshed.[/green]"
        )
        console.print("[dim]Remove --dry-run flag to apply changes.[/dim]")
        return

    console.print(f"🔧 Refreshing {len(datasets_to_refresh)} dataset(s)...")
    console.print()

    success_count = 0
    failed_count = 0

    for item in datasets_to_refresh:
        item_id = item["id"]
        if not isinstance(item_id, int):
            console.print(f"  [red]❌ Invalid ID for dataset: {item_id}[/red]")
            failed_count += 1
            continue

        ok = refresh_dataset(session, base_url, item_id)
        if ok:
            success_count += 1
        else:
            failed_count += 1
            item_uuid = item.get("uuid") or "unknown"
            console.print(
                f"  [red]❌ Failed to refresh dataset {str(item_uuid)[:16]}...[/red]"
            )

    console.print()
    console.rule("[bold green]Complete[/bold green]")
    console.print()
    console.print(
        f"[green]✅ Successfully refreshed {success_count} dataset(s)[/green]"
    )
    if failed_count > 0:
        console.print(
            f"[yellow]⚠️  Failed to refresh {failed_count} dataset(s)[/yellow]"
        )
    console.print()
