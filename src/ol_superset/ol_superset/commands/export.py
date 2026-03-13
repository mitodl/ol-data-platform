"""Export command - export Superset assets from an instance."""

from typing import Annotated

from cyclopts import Parameter

from ol_superset.commands.dedupe import dedupe
from ol_superset.commands.normalize import normalize
from ol_superset.lib.utils import count_assets, get_assets_dir, run_sup_command


def export(
    from_instance: Annotated[
        str,
        Parameter(name=["--from", "-f"], help="Instance to export from"),
    ] = "superset-production",
    output_dir: Annotated[
        str | None,
        Parameter(
            name=["--output-dir", "-o"], help="Output directory (default: assets/)"
        ),
    ] = None,
    skip_dedupe: Annotated[
        bool,
        Parameter(
            name=["--skip-dedupe"],
            help="Skip automatic deduplication after export",
        ),
    ] = False,
    skip_normalize: Annotated[
        bool,
        Parameter(
            name=["--skip-normalize"],
            help="Skip automatic YAML normalization after export",
        ),
    ] = False,
) -> None:
    """
    Export all Superset assets from specified instance.

    Exports datasets, charts, dashboards, and database configurations
    using automatic pagination to fetch all assets.

    By default, automatically deduplicates, renames assets to UUID-based
    naming, and normalizes YAML for clean diffs.

    Examples:
        Export from production (default, with auto-dedupe and normalize):
            ol-superset export

        Export from QA:
            ol-superset export --from superset-qa

        Export without deduplication:
            ol-superset export --skip-dedupe

        Export without YAML normalization:
            ol-superset export --skip-normalize

        Export to custom directory:
            ol-superset export -f superset-qa -o /tmp/qa-backup
    """
    assets_dir = get_assets_dir(output_dir)

    print("=" * 50)
    print(f"Exporting Superset Assets from {from_instance}")
    print("=" * 50)
    print()

    # Set the instance
    print(f"Setting instance to {from_instance}...")
    run_sup_command(["instance", "use", from_instance])

    # Export datasets
    print()
    print("Step 1: Exporting all datasets (via pagination)...")
    run_sup_command(
        [
            "dataset",
            "pull",
            str(assets_dir),
            "--instance",
            from_instance,
            "--overwrite",
        ]
    )

    # Export charts
    print()
    print("Step 2: Exporting all charts (via pagination)...")
    run_sup_command(
        ["chart", "pull", str(assets_dir), "--instance", from_instance, "--overwrite"]
    )

    # Export dashboards
    print()
    print("Step 3: Exporting all dashboards (via pagination)...")
    run_sup_command(
        [
            "dashboard",
            "pull",
            str(assets_dir),
            "--instance",
            from_instance,
            "--overwrite",
        ]
    )

    # Show summary
    print()
    print("=" * 50)
    print("Export Complete!")
    print("=" * 50)
    print()
    print(f"Assets exported to: {assets_dir}")
    print()

    counts = count_assets(assets_dir)
    print("Summary:")
    print(f"  Datasets:   {counts['datasets']}")
    print(f"  Charts:     {counts['charts']}")
    print(f"  Dashboards: {counts['dashboards']}")
    print(f"  Databases:  {counts['databases']}")
    print()

    # Auto-deduplicate unless skipped
    if not skip_dedupe:
        print()
        print("=" * 50)
        print("Step 4: Auto-deduplicating assets...")
        print("=" * 50)
        print()
        print(
            "Running dedupe to consolidate duplicates and rename to UUID-based naming"
        )
        print()

        try:
            dedupe(assets_dir=assets_dir, dry_run=False)
            print()
            print("✅ Deduplication complete!")
        except Exception as e:
            print()
            print(f"⚠️  Deduplication failed: {e}")
            print("Assets were exported but not deduplicated.")
            print("You can manually run: ol-superset dedupe")
    else:
        print()
        print("ℹ️  Skipped deduplication (--skip-dedupe flag used)")
        print("   Note: You may have duplicate files if this instance imported")
        print("   from another environment. Run 'ol-superset dedupe' to clean up.")

    # Auto-normalize unless skipped
    if not skip_normalize:
        print()
        print("=" * 50)
        print("Step 5: Normalizing YAML assets...")
        print("=" * 50)
        print()
        print(
            "Stripping query_context, numeric datasource IDs, sorting keys, "
            "and annotating UUID fields..."
        )
        print()

        try:
            normalize(assets_dir=assets_dir, dry_run=False)
            print()
            print("✅ Normalization complete!")
        except Exception as e:
            print()
            print(f"⚠️  Normalization failed: {e}")
            print("Assets were exported but not normalized.")
            print("You can manually run: ol-superset normalize")
    else:
        print()
        print("ℹ️  Skipped normalization (--skip-normalize flag used)")
        print("   Run 'ol-superset normalize' to apply later.")

    print()
