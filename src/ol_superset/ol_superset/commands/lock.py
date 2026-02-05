"""Lock/unlock command - manage is_managed_externally flag for Superset assets."""

import sys
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from rich.console import Console
from rich.table import Table

from ol_superset.lib.superset_api import (
    create_authenticated_session,
    get_asset_id_by_uuid,
    get_asset_uuids_from_directory,
    get_instance_config,
    update_asset_external_management_flag,
)
from ol_superset.lib.utils import get_assets_dir

console = Console()

lock_app = App(
    name="lock",
    help="""
    Manage external management flag for Superset assets.

    Lock assets to prevent manual editing in Superset UI
    (sets is_managed_externally=true).
    Unlock assets to allow manual editing (sets is_managed_externally=false).

    Use this to enforce "managed as code" workflows by locking production
    dashboards, or to enable manual editing in QA environments by unlocking.
    """,
    help_format="markdown",
)


def list_all_assets_from_api(
    session, base_url: str, asset_type: str
) -> list[dict[str, str | int]]:
    """
    List all assets of a given type from Superset API.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        asset_type: Type of asset ('dashboard' or 'chart')

    Returns:
        List of dicts with 'id', 'uuid', 'title' keys
    """
    if asset_type == "dashboard":
        endpoint = f"{base_url}/api/v1/dashboard/"
    elif asset_type == "chart":
        endpoint = f"{base_url}/api/v1/chart/"
    else:
        console.print(
            f"[red]❌ Unknown asset type '{asset_type}'. "
            "Must be 'dashboard' or 'chart'[/red]"
        )
        return []

    assets = []
    page = 0
    page_size = 100

    try:
        while True:
            # Superset API uses page/page_size for pagination
            params = {
                "q": (
                    f'{{"page": {page}, "page_size": {page_size}, '
                    '"order_column": "changed_on_delta_humanized", '
                    '"order_direction": "desc"}}'
                )
            }

            response = session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            results = data.get("result", [])
            if not results:
                break

            for item in results:
                assets.append(
                    {
                        "id": item.get("id"),
                        "uuid": item.get("uuid"),
                        "title": item.get(
                            "dashboard_title"
                            if asset_type == "dashboard"
                            else "slice_name"
                        ),
                        "is_managed_externally": item.get("is_managed_externally"),
                    }
                )

            page += 1

            # Check if we've reached the end
            if len(results) < page_size:
                break

        return assets

    except Exception as e:
        console.print(f"[red]❌ Error listing {asset_type}s: {e}[/red]")
        return []


def update_assets(
    instance_name: str,
    lock: bool,
    asset_type: str | None = None,
    uuids: list[str] | None = None,
    *,
    all_assets: bool = False,
    use_local_assets: bool = False,
    assets_dir: Path | None = None,
    dry_run: bool = False,
) -> None:
    """
    Update is_managed_externally flag for assets.

    Args:
        instance_name: Name of the instance (e.g., 'superset-qa')
        lock: True to lock (set true), False to unlock (set false)
        asset_type: Type of assets to update ('dashboard', 'chart', or None for both)
        uuids: Specific asset UUIDs to update
        all_assets: Update all assets in the instance
        use_local_assets: Use assets from local directory
        assets_dir: Path to assets directory (for use_local_assets)
        dry_run: Show what would be updated without updating
    """
    action_verb = "lock" if lock else "unlock"
    flag_value = lock

    console.print()
    console.rule(f"[bold]{'Locking' if lock else 'Unlocking'} Superset Assets[/bold]")
    console.print()
    console.print(f"Instance: [cyan]{instance_name}[/cyan]")
    console.print(f"Action: Set is_managed_externally=[yellow]{flag_value}[/yellow]")
    if dry_run:
        console.print("[yellow]DRY RUN MODE - No changes will be made[/yellow]")
    console.print()

    # Get configuration
    config = get_instance_config(instance_name)
    if not config:
        console.print("[red]❌ Could not get instance configuration[/red]")
        sys.exit(1)

    base_url = config["url"]

    # Create authenticated session
    console.print("🔐 Authenticating with Superset API...")
    session = create_authenticated_session(instance_name)
    if not session:
        console.print("[red]❌ Could not create authenticated session[/red]")
        sys.exit(1)

    console.print(f"✅ Authenticated to {base_url}")
    console.print()

    # Determine which asset types to process
    asset_types = []
    if asset_type:
        asset_types = [asset_type]
    else:
        asset_types = ["dashboard", "chart"]

    # Build list of assets to update
    assets_to_update: dict[str, list[dict[str, str | int]]] = {}

    for atype in asset_types:
        assets_to_update[atype] = []

        if use_local_assets:
            # Get UUIDs from local YAML files
            if not assets_dir:
                console.print(
                    "[red]❌ Assets directory required for --local flag[/red]"
                )
                sys.exit(1)

            console.print(f"📂 Reading {atype}s from local directory...")
            local_uuids = get_asset_uuids_from_directory(assets_dir, atype)

            if not local_uuids:
                console.print(f"  No {atype}s found in {assets_dir / atype}s/")
                continue

            console.print(f"  Found {len(local_uuids)} {atype}(s) in local directory")

            # Look up asset IDs
            for uuid in local_uuids:
                asset_id = get_asset_id_by_uuid(session, base_url, atype, uuid)
                if asset_id:
                    assets_to_update[atype].append(
                        {
                            "id": asset_id,
                            "uuid": uuid,
                            "title": f"{atype}_{uuid[:8]}...",
                        }
                    )
                else:
                    console.print(
                        f"  [yellow]⚠️  Could not find {atype} with UUID {uuid}[/yellow]"
                    )

        elif uuids:
            # Specific UUIDs provided
            console.print(f"🎯 Looking up {len(uuids)} specific {atype}(s)...")

            for uuid in uuids:
                asset_id = get_asset_id_by_uuid(session, base_url, atype, uuid)
                if asset_id:
                    assets_to_update[atype].append(
                        {
                            "id": asset_id,
                            "uuid": uuid,
                            "title": f"{atype}_{uuid[:8]}...",
                        }
                    )
                else:
                    console.print(
                        f"  [yellow]⚠️  Could not find {atype} with UUID {uuid}[/yellow]"
                    )

        elif all_assets:
            # Get all assets from API
            console.print(f"📋 Fetching all {atype}s from {instance_name}...")
            all_items = list_all_assets_from_api(session, base_url, atype)

            if not all_items:
                console.print(f"  No {atype}s found")
                continue

            console.print(f"  Found {len(all_items)} {atype}(s)")
            assets_to_update[atype] = all_items

    # Show summary
    total_assets = sum(len(items) for items in assets_to_update.values())

    if total_assets == 0:
        console.print("[yellow]⚠️  No assets to update[/yellow]")
        sys.exit(0)

    console.print()
    console.print(f"[bold]Assets to {action_verb}:[/bold]")
    for atype, items in assets_to_update.items():
        if items:
            console.print(f"  • {len(items)} {atype}(s)")

    console.print()

    if dry_run:
        console.print("[yellow]DRY RUN - Showing what would be updated:[/yellow]")
        console.print()

        for atype, items in assets_to_update.items():
            if not items:
                continue

            table = Table(title=f"{atype.capitalize()}s to {action_verb}")
            table.add_column("ID", style="cyan")
            table.add_column("UUID", style="magenta")
            table.add_column("Title", style="white")
            table.add_column("Current State", style="yellow")

            for item in items[:20]:  # Show first 20 for brevity
                current_state = (
                    "Locked"
                    if item.get("is_managed_externally")
                    else "Unlocked"
                    if item.get("is_managed_externally") is not None
                    else "Unknown"
                )
                item_title = item.get("title")
                title_str = str(item_title)[:40] if item_title is not None else "N/A"
                item_uuid = item.get("uuid")
                uuid_str = (
                    str(item_uuid)[:16] + "..." if item_uuid is not None else "N/A"
                )
                table.add_row(
                    str(item["id"]),
                    uuid_str,
                    title_str,
                    current_state,
                )

            if len(items) > 20:
                table.add_row(
                    "...",
                    "...",
                    f"({len(items) - 20} more)",
                    "...",
                    style="dim",
                )

            console.print(table)
            console.print()

        console.print(
            f"[green]✅ Dry run complete. "
            f"{total_assets} assets would be {action_verb}ed.[/green]"
        )
        console.print("[dim]Remove --dry-run flag to apply changes.[/dim]")
        return

    # Perform updates
    console.print(f"🔧 Updating {total_assets} asset(s)...")
    console.print()

    overall_success = 0
    overall_failed = 0

    for atype, items in assets_to_update.items():
        if not items:
            continue

        console.print(f"Processing {len(items)} {atype}(s)...")

        success_count = 0
        failed_count = 0

        for item in items:
            item_id = item["id"]
            if not isinstance(item_id, int):
                console.print(f"  [red]❌ Invalid ID type for {atype}: {item_id}[/red]")
                failed_count += 1
                continue

            success = update_asset_external_management_flag(
                session, base_url, atype, item_id, is_managed_externally=flag_value
            )

            if success:
                success_count += 1
            else:
                failed_count += 1
                item_uuid = item.get("uuid")
                uuid_str = str(item_uuid)[:16] if item_uuid else "unknown"
                console.print(f"  [red]❌ Failed to update {atype} {uuid_str}...[/red]")

        console.print(f"  [green]✅ Updated {success_count} {atype}(s)[/green]")
        if failed_count > 0:
            console.print(
                f"  [yellow]⚠️  Failed to update {failed_count} {atype}(s)[/yellow]"
            )

        overall_success += success_count
        overall_failed += failed_count

    console.print()
    console.rule("[bold green]Complete[/bold green]")
    console.print()
    console.print(
        f"[green]✅ Successfully {action_verb}ed {overall_success} asset(s)[/green]"
    )
    if overall_failed > 0:
        console.print(f"[yellow]⚠️  Failed to update {overall_failed} asset(s)[/yellow]")
    console.print()


@lock_app.command
def lock(
    instance: Annotated[
        str, Parameter(help="Target instance name (e.g., superset-qa)")
    ],
    dashboards_only: Annotated[
        bool,
        Parameter(
            name=["--dashboards-only", "-d"],
            help="Lock only dashboards (not charts)",
        ),
    ] = False,
    charts_only: Annotated[
        bool,
        Parameter(
            name=["--charts-only", "-c"], help="Lock only charts (not dashboards)"
        ),
    ] = False,
    all_assets: Annotated[
        bool,
        Parameter(name=["--all", "-a"], help="Lock all assets in the instance"),
    ] = False,
    uuid: Annotated[
        list[str] | None,
        Parameter(
            name=["--uuid", "-u"], help="Lock specific asset(s) by UUID (repeatable)"
        ),
    ] = None,
    local: Annotated[
        bool,
        Parameter(
            name=["--local", "-l"],
            help="Lock assets found in local assets directory",
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
            help="Show what would be locked without locking",
        ),
    ] = False,
) -> None:
    """
    Lock Superset assets to prevent manual editing (is_managed_externally=true).

    Examples:
        Lock all dashboards and charts in production:
            ol-superset lock superset-production --all

        Lock only dashboards in QA:
            ol-superset lock superset-qa --all --dashboards-only

        Lock specific dashboard by UUID:
            ol-superset lock superset-qa --uuid abc123-def456-...

        Lock all assets in local directory:
            ol-superset lock superset-production --local

        Dry run to preview changes:
            ol-superset lock superset-production --all --dry-run
    """
    if dashboards_only and charts_only:
        console.print(
            "[red]Error: Cannot specify both --dashboards-only and --charts-only[/red]"
        )
        sys.exit(1)

    if not (all_assets or uuid or local):
        console.print("[red]Error: Must specify --all, --uuid, or --local[/red]")
        sys.exit(1)

    asset_type = None
    if dashboards_only:
        asset_type = "dashboard"
    elif charts_only:
        asset_type = "chart"

    assets_dir = get_assets_dir(assets_dir_path) if local else None

    update_assets(
        instance_name=instance,
        lock=True,
        asset_type=asset_type,
        uuids=uuid,
        all_assets=all_assets,
        use_local_assets=local,
        assets_dir=assets_dir,
        dry_run=dry_run,
    )


@lock_app.command
def unlock(
    instance: Annotated[
        str, Parameter(help="Target instance name (e.g., superset-qa)")
    ],
    dashboards_only: Annotated[
        bool,
        Parameter(
            name=["--dashboards-only", "-d"],
            help="Unlock only dashboards (not charts)",
        ),
    ] = False,
    charts_only: Annotated[
        bool,
        Parameter(
            name=["--charts-only", "-c"], help="Unlock only charts (not dashboards)"
        ),
    ] = False,
    all_assets: Annotated[
        bool,
        Parameter(name=["--all", "-a"], help="Unlock all assets in the instance"),
    ] = False,
    uuid: Annotated[
        list[str] | None,
        Parameter(
            name=["--uuid", "-u"], help="Unlock specific asset(s) by UUID (repeatable)"
        ),
    ] = None,
    local: Annotated[
        bool,
        Parameter(
            name=["--local", "-l"],
            help="Unlock assets found in local assets directory",
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
            help="Show what would be unlocked without unlocking",
        ),
    ] = False,
) -> None:
    """
    Unlock Superset assets to allow manual editing (is_managed_externally=false).

    Examples:
        Unlock all dashboards and charts in QA:
            ol-superset unlock superset-qa --all

        Unlock only dashboards in QA:
            ol-superset unlock superset-qa --all --dashboards-only

        Unlock specific dashboard by UUID:
            ol-superset unlock superset-qa --uuid abc123-def456-...

        Unlock all assets in local directory:
            ol-superset unlock superset-qa --local

        Dry run to preview changes:
            ol-superset unlock superset-qa --all --dry-run
    """
    if dashboards_only and charts_only:
        console.print(
            "[red]Error: Cannot specify both --dashboards-only and --charts-only[/red]"
        )
        sys.exit(1)

    if not (all_assets or uuid or local):
        console.print("[red]Error: Must specify --all, --uuid, or --local[/red]")
        sys.exit(1)

    asset_type = None
    if dashboards_only:
        asset_type = "dashboard"
    elif charts_only:
        asset_type = "chart"

    assets_dir = get_assets_dir(assets_dir_path) if local else None

    update_assets(
        instance_name=instance,
        lock=False,
        asset_type=asset_type,
        uuids=uuid,
        all_assets=all_assets,
        use_local_assets=local,
        assets_dir=assets_dir,
        dry_run=dry_run,
    )
