"""Promote command - promote assets from QA to production with safety checks."""

import subprocess
import sys
from typing import Annotated

from cyclopts import Parameter

from ol_superset.lib.database_mapping import map_database_uuids
from ol_superset.lib.utils import (
    check_git_status,
    confirm_action,
    count_assets,
    get_assets_dir,
    run_sup_command,
)


def promote(
    assets_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--assets-dir", "-d"], help="Assets directory (default: assets/)"
        ),
    ] = None,
    skip_validation: Annotated[
        bool,
        Parameter(
            name=["--skip-validation"],
            help="Skip pre-deployment validation (not recommended)",
        ),
    ] = False,
    force: Annotated[
        bool,
        Parameter(
            name=["--force", "-f"], help="Skip confirmation and git checks (DANGEROUS)"
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        Parameter(
            name=["--dry-run", "-n"],
            help="Show what would be promoted without promoting",
        ),
    ] = False,
) -> None:
    """
    Promote Superset assets from QA to production.

    This command is hardcoded to only promote from QA to production
    with extensive safety checks to prevent accidental deployments.

    Safety features:
    - Validates assets before promoting
    - Checks for uncommitted git changes
    - Requires typing "PROMOTE" to confirm
    - Shows detailed summary of what will be deployed

    Examples:
        Standard promotion workflow:
            ol-superset promote

        Preview what would be promoted:
            ol-superset promote --dry-run

        Emergency deployment (skip safety checks):
            ol-superset promote --force
    """
    assets_dir = get_assets_dir(assets_dir_path)

    print("=" * 50)
    print("Promote Superset Assets: QA → Production")
    print("=" * 50)
    print()
    print("⚠️  WARNING: This will deploy assets to PRODUCTION")
    print()

    # Check assets exist
    if not assets_dir.exists():
        print(f"Error: Assets directory not found: {assets_dir}", file=sys.stderr)
        print("Run 'ol-superset export --from superset-qa' first", file=sys.stderr)
        sys.exit(1)

    # Count assets
    counts = count_assets(assets_dir)
    published = counts["published_dashboards"]
    total_dashboards = counts["dashboards"]

    # Check for uncommitted changes (unless forced)
    if not force:
        has_changes, changed_files = check_git_status(assets_dir)
        if has_changes:
            print("⚠️  Warning: You have uncommitted changes in assets/")
            print()
            for file in changed_files[:10]:  # Show first 10
                print(f"  {file}")
            if len(changed_files) > 10:
                print(f"  ... and {len(changed_files) - 10} more")
            print()
            print("It's recommended to commit changes before promoting to production.")
            if not confirm_action("Continue anyway?"):
                print("Promotion cancelled. Please commit your changes first.")
                sys.exit(0)

    # Validate assets (unless skipped)
    if not skip_validation and not dry_run:
        print("Step 1: Validating assets...")
        print()
        validation_result = subprocess.run(
            ["ol-superset", "validate", "--assets-dir", str(assets_dir)],
            check=False,
        )
        if validation_result.returncode != 0:
            print()
            print("❌ Validation failed. Fix errors before promoting to production.")
            sys.exit(1)

    # Show what will be promoted
    print()
    print("Assets to promote:")
    print(f"  Published Dashboards: {published} (of {total_dashboards} total)")
    print(f"  Charts: {counts['charts']}")
    print(f"  Datasets: {counts['datasets']}")
    print()

    # Show recent git log for context
    try:
        git_result: subprocess.CompletedProcess[str] = subprocess.run(
            [
                "git",
                "log",
                "-1",
                "--pretty=format:  %h - %s (%cr by %an)",
                str(assets_dir),
            ],
            capture_output=True,
            text=True,
            check=False,
            cwd=assets_dir.parent,
        )
        if git_result.returncode == 0 and git_result.stdout:
            print("Last commit affecting assets:")
            print(git_result.stdout)
            print()
            print()
    except Exception:  # noqa: S110
        pass  # Git log is optional, continue if it fails

    if dry_run:
        print("🔍 DRY RUN MODE - No changes will be made")
        print()
        print("Would perform:")
        print("  1. Map database UUIDs from QA to production")
        print("  2. Set instance to superset-production")
        print(f"  3. Push {counts['charts']} charts with dependencies")
        print(f"  4. Push {published} published dashboards")
        print("  5. Create promotion manifest")
        return

    # Final confirmation (unless forced)
    if not force:
        print("⚠️  FINAL CONFIRMATION: Deploy these assets to PRODUCTION?")
        print()
        confirmed = confirm_action(
            "This will overwrite production assets.",
            require_exact="PROMOTE",
        )
        if not confirmed:
            print("Promotion cancelled.")
            sys.exit(0)

    # Map database UUIDs
    print()
    print("Step 3: Mapping database UUIDs for production...")
    try:
        map_database_uuids("superset-production", assets_dir)
        print("  ✅ Database UUID mapping complete")
    except Exception as e:
        print(f"  ⚠️  Database mapping failed: {e}")
        if not force and not confirm_action("Continue without database mapping?"):
            print("Promotion cancelled")
            sys.exit(1)

    # Set instance to production
    print()
    print("Step 4: Setting instance to superset-production...")
    run_sup_command(["instance", "use", "superset-production"])

    print()
    print("Step 5: Promoting assets to production...")
    print()

    # Push charts with dependencies
    print("Step 5a: Pushing charts (includes datasets and databases)...")
    if counts["charts"] > 0:
        result = run_sup_command(
            [
                "chart",
                "push",
                str(assets_dir),
                "--overwrite",
                "--continue-on-error",
                "--force",
            ],
            check=False,
        )
        if result.returncode == 0:
            print("  ✅ Charts promoted successfully")
        else:
            print("  ⚠️  Some charts may have failed - check logs above")
    else:
        print("  No charts to promote")

    # Push dashboards
    print()
    print("Step 5b: Pushing published dashboards...")
    if published > 0:
        result = run_sup_command(
            [
                "dashboard",
                "push",
                str(assets_dir),
                "--overwrite",
                "--continue-on-error",
                "--force",
            ],
            check=False,
        )
        if result.returncode == 0:
            print("  ✅ Dashboards promoted successfully")
        else:
            print("  ❌ Dashboard promotion failed")
            if not force and not confirm_action("Continue anyway?"):
                print("Promotion aborted")
                run_sup_command(["instance", "use", "superset-qa"])
                sys.exit(1)
    else:
        print("  No published dashboards to promote")

    # Create promotion manifest
    manifest_file = assets_dir.parent / "promotion_manifest.txt"
    print()
    print(f"Creating promotion manifest: {manifest_file}")
    with manifest_file.open("w") as f:
        f.write("# Superset Assets Promotion Manifest\n")
        f.write("# Source: QA (bi-qa.ol.mit.edu)\n")
        f.write("# Target: Production (bi.ol.mit.edu)\n")
        f.write("\n")
        f.write("## Published Dashboards Promoted\n")
        f.write("\n")

        # List promoted dashboards
        dashboard_dir = assets_dir / "dashboards"
        if dashboard_dir.exists():
            for dashboard_file in sorted(dashboard_dir.glob("*.yaml")):
                if not dashboard_file.name.startswith("untitled_"):
                    import yaml

                    with dashboard_file.open() as df:
                        try:
                            config = yaml.safe_load(df)
                            title = config.get("dashboard_title", "Unknown")
                            f.write(f"- {title} (file: {dashboard_file.name})\n")
                        except Exception:
                            f.write(f"- (file: {dashboard_file.name})\n")

    print()
    print("=" * 50)
    print("Promotion Complete!")
    print("=" * 50)
    print()
    print("Next steps:")
    print("  1. Verify dashboards at: https://bi.ol.mit.edu/dashboard/list/")
    print("  2. Test dashboard functionality with production data")
    print("  3. Monitor for any errors or issues")
    print("  4. Update team on deployed changes")
    print()
    print(f"Promotion manifest saved to: {manifest_file}")
    print()

    # Restore to QA instance
    print("Restoring instance to superset-qa...")
    run_sup_command(["instance", "use", "superset-qa"])

    print()
    print("✅ Promotion complete! All assets deployed to production.")
    print()
