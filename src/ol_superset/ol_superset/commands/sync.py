"""Sync command - sync assets between Superset instances."""

import sys
from typing import Annotated

from cyclopts import Parameter

from ol_superset.lib.database_mapping import map_database_uuids
from ol_superset.lib.superset_api import update_pushed_assets_external_flag
from ol_superset.lib.utils import (
    confirm_action,
    count_assets,
    get_assets_dir,
    run_sup_command,
)


def sync(
    source: Annotated[
        str, Parameter(help="Source instance name (e.g., superset-production)")
    ],
    target: Annotated[str, Parameter(help="Target instance name (e.g., superset-qa)")],
    assets_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--assets-dir", "-d"], help="Assets directory (default: assets/)"
        ),
    ] = None,
    skip_confirmation: Annotated[
        bool,
        Parameter(name=["--yes", "-y"], help="Skip confirmation prompt"),
    ] = False,
    dry_run: Annotated[
        bool,
        Parameter(
            name=["--dry-run", "-n"], help="Show what would be synced without syncing"
        ),
    ] = False,
    map_only: Annotated[
        bool,
        Parameter(
            name=["--map-only", "-m"],
            help="Only map database UUIDs, don't push assets to target",
        ),
    ] = False,
) -> None:
    """
    Sync Superset assets from source to target instance.

    Automatically maps database UUIDs between environments and pushes
    datasets, charts, and dashboards to the target instance.

    Examples:
        Sync production to QA:
            ol-superset sync superset-production superset-qa

        Sync QA to production (with confirmation):
            ol-superset sync superset-qa superset-production

        Dry run to preview changes:
            ol-superset sync superset-production superset-qa --dry-run

        Only map database UUIDs without pushing:
            ol-superset sync superset-production superset-qa --map-only
    """
    assets_dir = get_assets_dir(assets_dir_path)

    print("=" * 50)
    print("Syncing Superset Assets")
    print("=" * 50)
    print()
    print(f"Source: {source}")
    print(f"Target: {target}")
    print()

    if not assets_dir.exists():
        print(f"Error: Assets directory not found: {assets_dir}", file=sys.stderr)
        print(f"Run 'ol-superset export --from {source}' first", file=sys.stderr)
        sys.exit(1)

    # Count and display assets
    counts = count_assets(assets_dir)
    published = counts["published_dashboards"]

    print("Assets to sync:")
    print(f"  Dashboards: {published} (published only)")
    print(f"  Charts:     {counts['charts']}")
    print(f"  Datasets:   {counts['datasets']}")
    print()

    # Production safety check
    is_production_target = "production" in target.lower()
    if is_production_target:
        print("⚠️  WARNING: Target is PRODUCTION environment")
        print()

    if dry_run:
        print("🔍 DRY RUN MODE - No changes will be made")
        print()
        print("Would perform:")
        print("  1. Map database UUIDs")
        if not map_only:
            print(f"  2. Push {counts['datasets']} datasets")
            print(f"  3. Push {counts['charts']} charts")
            print(f"  4. Push {published} published dashboards")
        return

    if map_only:
        print("🔧 MAP ONLY MODE - Will only update database UUIDs in asset files")
        print()
        print("This will rewrite database UUIDs in your local asset files to match")
        print(f"the target instance ({target}).")
        print()

        if not skip_confirmation:
            confirmed = confirm_action("Proceed with database UUID mapping?")
            if not confirmed:
                print("Mapping cancelled.")
                sys.exit(0)

        # Step 1: Map database UUIDs
        print()
        print("Mapping database UUIDs...")
        map_database_uuids(target, assets_dir)

        print()
        print("=" * 50)
        print("Database UUID Mapping Complete!")
        print("=" * 50)
        print()
        print("Next steps:")
        print("  1. Review changes: git diff")
        print("  2. Push assets manually:")
        print(f"     sup dataset push assets/ --instance {target} --overwrite")
        print(f"     sup chart push assets/ --instance {target} --overwrite")
        print(f"     sup dashboard push assets/ --instance {target} --overwrite")
        print()
        return

    print("This will overwrite existing assets in the target if they share UUIDs.")
    print()

    # Confirmation
    if not skip_confirmation:
        if is_production_target:
            confirmed = confirm_action(
                "⚠️  Syncing to PRODUCTION. Are you sure?",
                require_exact="SYNC TO PRODUCTION",
            )
        else:
            confirmed = confirm_action("Proceed with sync?")

        if not confirmed:
            print("Sync cancelled.")
            sys.exit(0)

    # Step 1: Map database UUIDs
    print()
    print("Step 1: Mapping database UUIDs...")
    map_database_uuids(target, assets_dir)

    # Step 2: Push datasets
    print()
    print("Step 2: Syncing datasets...")
    if counts["datasets"] > 0:
        run_sup_command(
            [
                "dataset",
                "push",
                str(assets_dir),
                "--instance",
                target,
                "--overwrite",
                "--force",
                "--continue-on-error",
            ],
            check=False,
        )
        print("  ✅ Datasets synced")
    else:
        print("  No datasets to sync")

    # Step 3: Push charts
    print()
    print("Step 3: Syncing charts...")
    if counts["charts"] > 0:
        run_sup_command(
            [
                "chart",
                "push",
                str(assets_dir),
                "--instance",
                target,
                "--overwrite",
                "--force",
                "--continue-on-error",
            ],
            check=False,
        )
        print("  ✅ Charts synced")
    else:
        print("  No charts to sync")

    # Step 4: Push dashboards
    print()
    print("Step 4: Syncing dashboards...")
    if published > 0:
        run_sup_command(
            [
                "dashboard",
                "push",
                str(assets_dir),
                "--instance",
                target,
                "--overwrite",
                "--force",
                "--continue-on-error",
            ],
            check=False,
        )
        print("  ✅ Dashboards synced")
    else:
        print("  No published dashboards to sync")

    # Show completion message
    print()
    print("=" * 50)
    print("Sync Complete!")
    print("=" * 50)
    print()

    # Step 5: Update is_managed_externally flag for QA targets
    if "qa" in target.lower():
        print("Step 5: Updating asset management flags for QA...")
        try:
            update_pushed_assets_external_flag(
                instance_name=target,
                assets_dir=assets_dir,
                skip_confirmation=skip_confirmation,
            )
        except Exception as e:
            print(
                f"  ⚠️  Warning: Could not update management flags: {e}",
                file=sys.stderr,
            )
            print(
                "      Assets are synced but may not be editable in UI",
                file=sys.stderr,
            )

    print("Verify at:")
    if "qa" in target.lower():
        print("  • Dashboards: https://bi-qa.ol.mit.edu/dashboard/list/")
        print("  • Charts:     https://bi-qa.ol.mit.edu/chart/list/")
        print("  • Datasets:   https://bi-qa.ol.mit.edu/tablemodelview/list/")
    elif "production" in target.lower():
        print("  • Dashboards: https://bi.ol.mit.edu/dashboard/list/")
        print("  • Charts:     https://bi.ol.mit.edu/chart/list/")
        print("  • Datasets:   https://bi.ol.mit.edu/tablemodelview/list/")
    else:
        print("  • Check your Superset instance for imported assets")
    print()
