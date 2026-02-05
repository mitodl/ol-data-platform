"""Deduplicate and rename Superset assets to use UUID-based naming.

This module provides functionality to solve the problem of duplicate files
created when exporting from different environments (QA vs Production) that
have different database IDs but share the same UUID for assets.
"""

import re
from pathlib import Path

import cyclopts
import yaml
from rich.console import Console
from rich.table import Table

console = Console()


def extract_uuid_from_yaml(yaml_file: Path) -> str | None:
    """Extract UUID from YAML file content.

    Args:
        yaml_file: Path to YAML file

    Returns:
        UUID string or None if not found
    """
    try:
        with yaml_file.open("r") as f:
            data = yaml.safe_load(f)
            return data.get("uuid")
    except Exception as e:
        console.print(f"[yellow]⚠️  Error reading {yaml_file}: {e}[/yellow]")
        return None


def extract_base_name_without_id(filename: str) -> str:
    """Remove database ID suffix from filename.

    Args:
        filename: Original filename (e.g., 'marts__combined__users_35.yaml')

    Returns:
        Base name without ID (e.g., 'marts__combined__users')
    """
    # Match pattern: <name>_<digits>.yaml
    match = re.match(r"^(.+)_(\d+)\.yaml$", filename)
    if match:
        return match.group(1)
    # If no ID suffix, return as-is (without .yaml extension)
    return filename.replace(".yaml", "")


def deduplicate_assets(directory: Path, dry_run: bool = False) -> tuple[int, int]:
    """Remove duplicate files that have the same UUID but different database IDs.

    Args:
        directory: Directory containing YAML asset files
        dry_run: If True, show what would be done without making changes

    Returns:
        Tuple of (kept_count, removed_count)
    """
    if not directory.exists():
        console.print(f"[red]❌ Directory not found: {directory}[/red]")
        return (0, 0)

    # Group files by base name
    files_by_base: dict[str, list[Path]] = {}

    for yaml_file in directory.rglob("*.yaml"):
        if yaml_file.name == "metadata.yaml":
            continue

        base_name = extract_base_name_without_id(yaml_file.name)
        if base_name not in files_by_base:
            files_by_base[base_name] = []
        files_by_base[base_name].append(yaml_file)

    # Find duplicates
    removed_count = 0
    kept_count = 0

    for base_name, files in files_by_base.items():
        if len(files) <= 1:
            continue

        # Group by UUID
        uuids: dict[str, list[Path]] = {}
        for file_path in files:
            uuid = extract_uuid_from_yaml(file_path)
            if uuid:
                if uuid not in uuids:
                    uuids[uuid] = []
                uuids[uuid].append(file_path)

        # For each UUID, keep the first file and remove others
        for uuid, duplicate_files in uuids.items():
            if len(duplicate_files) > 1:
                console.print(
                    f"\n[cyan]🔍 Found {len(duplicate_files)} duplicates for {base_name}[/cyan]"  # noqa: E501
                )
                console.print(f"   UUID: [dim]{uuid}[/dim]")

                # Keep the first one
                keep_file = duplicate_files[0]
                console.print(f"   [green]✅ Keeping: {keep_file.name}[/green]")
                kept_count += 1

                # Remove the rest
                for dup_file in duplicate_files[1:]:
                    if dry_run:
                        console.print(
                            f"   [yellow]Would remove: {dup_file.name}[/yellow]"
                        )
                    else:
                        dup_file.unlink()
                        console.print(f"   [red]🗑️  Removed: {dup_file.name}[/red]")
                    removed_count += 1

    return (kept_count, removed_count)


def rename_assets_to_uuid(
    directory: Path, dry_run: bool = False
) -> tuple[int, int, int]:
    """Rename all YAML files in directory to use UUID-based naming.

    Args:
        directory: Directory containing YAML asset files
        dry_run: If True, show what would be done without making changes

    Returns:
        Tuple of (renamed_count, skipped_count, error_count)
    """
    if not directory.exists():
        console.print(f"[red]❌ Directory not found: {directory}[/red]")
        return (0, 0, 0)

    yaml_files = list(directory.rglob("*.yaml"))
    if not yaml_files:
        console.print(f"[yellow]ℹ️  No YAML files found in {directory}[/yellow]")
        return (0, 0, 0)

    renamed_count = 0
    skipped_count = 0
    error_count = 0

    for yaml_file in yaml_files:
        # Skip metadata files
        if yaml_file.name == "metadata.yaml":
            continue

        uuid = extract_uuid_from_yaml(yaml_file)
        if not uuid:
            console.print(
                f"[yellow]⚠️  No UUID found in {yaml_file.relative_to(directory.parent)}[/yellow]"  # noqa: E501
            )
            error_count += 1
            continue

        # Get base name without ID suffix
        base_name = extract_base_name_without_id(yaml_file.name)

        # Create new filename with UUID
        new_name = f"{base_name}_{uuid}.yaml"
        new_path = yaml_file.parent / new_name

        # Check if already using UUID naming
        if yaml_file.name == new_name:
            skipped_count += 1
            continue

        # Check if target already exists
        if new_path.exists() and new_path != yaml_file:
            console.print(
                f"[yellow]⚠️  Target already exists: {new_path.relative_to(directory.parent)}[/yellow]"  # noqa: E501
            )
            error_count += 1
            continue

        # Perform rename
        if dry_run:
            console.print(
                f"[yellow]Would rename: {yaml_file.name} -> {new_name}[/yellow]"
            )
        else:
            yaml_file.rename(new_path)
            console.print(f"[green]✅ Renamed: {yaml_file.name} -> {new_name}[/green]")

        renamed_count += 1

    return (renamed_count, skipped_count, error_count)


dedupe_app = cyclopts.App(
    name="dedupe",
    help="""
    Deduplicate and rename Superset assets to UUID-based naming.

    This command solves the problem of duplicate files created when exporting
    from different environments (QA vs Production) that have different database
    IDs but share the same UUID for assets.

    By default, processes all asset types (datasets, charts, dashboards).
    Use --datasets, --charts, or --dashboards to process specific types.

    Examples:

      # Preview what would be done (recommended first step)
      $ ol-superset dedupe --dry-run

      # Deduplicate and rename all assets
      $ ol-superset dedupe

      # Only process datasets
      $ ol-superset dedupe --datasets

      # Process charts and dashboards but not datasets
      $ ol-superset dedupe --charts --dashboards
    """,
)


@dedupe_app.default
def dedupe(
    *,
    assets_dir: Path = Path("assets"),
    datasets: bool = False,
    charts: bool = False,
    dashboards: bool = False,
    dry_run: bool = False,
) -> None:
    """Deduplicate and rename Superset assets to UUID-based naming.

    Args:
        assets_dir: Root assets directory (default: assets/)
        datasets: Only process datasets
        charts: Only process charts
        dashboards: Only process dashboards
        dry_run: Show what would be done without making changes
    """
    # If no specific types specified, process all
    process_all = not (datasets or charts or dashboards)

    if dry_run:
        console.print("[yellow]🔍 DRY RUN MODE - No changes will be made[/yellow]\n")

    console.print("[bold]Deduplicating and Renaming Superset Assets[/bold]\n")

    total_kept = 0
    total_removed = 0
    total_renamed = 0
    total_skipped = 0
    total_errors = 0

    # Process datasets
    if process_all or datasets:
        datasets_dir = assets_dir / "datasets"
        if datasets_dir.exists():
            console.print("[bold cyan]📊 Processing datasets...[/bold cyan]")
            console.print("[dim]Deduplicating...[/dim]")
            kept, removed = deduplicate_assets(datasets_dir, dry_run=dry_run)
            total_kept += kept
            total_removed += removed

            console.print("\n[dim]Renaming to UUID-based...[/dim]")
            renamed, skipped, errors = rename_assets_to_uuid(
                datasets_dir, dry_run=dry_run
            )
            total_renamed += renamed
            total_skipped += skipped
            total_errors += errors
            console.print()

    # Process charts
    if process_all or charts:
        charts_dir = assets_dir / "charts"
        if charts_dir.exists():
            console.print("[bold cyan]📈 Processing charts...[/bold cyan]")
            console.print("[dim]Deduplicating...[/dim]")
            kept, removed = deduplicate_assets(charts_dir, dry_run=dry_run)
            total_kept += kept
            total_removed += removed

            console.print("\n[dim]Renaming to UUID-based...[/dim]")
            renamed, skipped, errors = rename_assets_to_uuid(
                charts_dir, dry_run=dry_run
            )
            total_renamed += renamed
            total_skipped += skipped
            total_errors += errors
            console.print()

    # Process dashboards
    if process_all or dashboards:
        dashboards_dir = assets_dir / "dashboards"
        if dashboards_dir.exists():
            console.print("[bold cyan]📋 Processing dashboards...[/bold cyan]")
            console.print("[dim]Deduplicating...[/dim]")
            kept, removed = deduplicate_assets(dashboards_dir, dry_run=dry_run)
            total_kept += kept
            total_removed += removed

            console.print("\n[dim]Renaming to UUID-based...[/dim]")
            renamed, skipped, errors = rename_assets_to_uuid(
                dashboards_dir, dry_run=dry_run
            )
            total_renamed += renamed
            total_skipped += skipped
            total_errors += errors
            console.print()

    # Display summary table
    table = Table(title="Summary", show_header=True, header_style="bold")
    table.add_column("Operation", style="cyan")
    table.add_column("Count", justify="right", style="green")

    table.add_row("Duplicates kept", str(total_kept))
    table.add_row("Duplicates removed", str(total_removed))
    table.add_row("Files renamed", str(total_renamed))
    table.add_row("Files skipped (already UUID-based)", str(total_skipped))
    table.add_row(
        "Errors", str(total_errors), style="red" if total_errors > 0 else "green"
    )

    console.print(table)

    if not dry_run and (total_removed > 0 or total_renamed > 0):
        console.print("\n[bold green]✅ Complete![/bold green]")
        console.print("\n[bold]Next steps:[/bold]")
        console.print("1. Review changes: [cyan]git diff assets/[/cyan]")
        console.print(
            "2. Commit changes: [cyan]git add assets/ && git commit -m 'chore: Deduplicate and rename assets to UUID-based naming'[/cyan]"  # noqa: E501
        )
    elif dry_run:
        console.print("\n[yellow]Run without --dry-run to apply these changes[/yellow]")
