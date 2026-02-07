#!/usr/bin/env python3
"""
Register all Iceberg tables from AWS Glue catalog as DuckDB views.

This script:
1. Queries AWS Glue to discover all Iceberg tables across multiple database layers
2. Creates DuckDB views that reference the Iceberg metadata locations
3. Enables dbt models to query Iceberg tables directly without copying data locally

Usage:
    # Register ALL layers (raw, staging, intermediate, dimensional, mart, reporting, external)
    python bin/register-glue-sources.py register --all-layers

    # Register just one database
    python bin/register-glue-sources.py register --database ol_warehouse_production_raw

    # Register specific layer
    python bin/register-glue-sources.py register --database ol_warehouse_production_staging

    # Dry run to see what would be registered
    python bin/register-glue-sources.py register --all-layers --dry-run

    # Quiet mode (less verbose, good for automation)
    python bin/register-glue-sources.py register --all-layers --quiet

    # List currently registered sources
    python bin/register-glue-sources.py list

    # Custom DuckDB database path
    python bin/register-glue-sources.py register --all-layers --duckdb-path ~/.ol-dbt/custom.duckdb
"""

import argparse
from pathlib import Path
from typing import Any

import boto3
import duckdb

DEFAULT_DUCKDB_PATH = Path.home() / ".ol-dbt" / "local.duckdb"
DEFAULT_GLUE_DATABASE = "ol_warehouse_production_raw"

# Standard dbt layer databases (in dependency order)
LAYER_DATABASES = [
    "ol_warehouse_production_raw",  # Source/raw data
    "ol_warehouse_production_staging",  # Staging layer
    "ol_warehouse_production_intermediate",  # Intermediate layer
    "ol_warehouse_production_dimensional",  # Dimensional layer
    "ol_warehouse_production_mart",  # Mart layer
    "ol_warehouse_production_reporting",  # Reporting layer
    "ol_warehouse_production_external",  # External layer
]


def get_glue_tables(database: str) -> list[dict[str, Any]]:
    """
    Get all tables from AWS Glue catalog

    Args:
        database: Glue database name

    Returns:
        List of dicts with table metadata
    """
    glue = boto3.client("glue")
    tables = []

    print(f"Fetching tables from Glue database: {database}")

    paginator = glue.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database):
        for table in page["TableList"]:
            table_name = table["Name"]
            storage_desc = table.get("StorageDescriptor", {})
            table_location = storage_desc.get("Location", "")
            parameters = table.get("Parameters", {})
            metadata_location = parameters.get("metadata_location", "")
            table_type = parameters.get("table_type", "")

            # Only process Iceberg tables
            if table_type.upper() == "ICEBERG" and metadata_location:
                tables.append(
                    {
                        "name": table_name,
                        "location": table_location,
                        "metadata_location": metadata_location,
                        "table_type": table_type,
                    }
                )
                print(f"  ✓ {table_name}")
            else:
                print(f"  ⊘ {table_name} (not Iceberg or no metadata)")

    return tables


def register_tables_in_duckdb(
    tables: list[dict[str, Any]],
    database_name: str,
    duckdb_path: Path,
    dry_run: bool = False,
    verbose: bool = True,
) -> dict[str, int]:
    """
    Register Glue tables as DuckDB views

    Args:
        tables: List of table metadata from Glue
        database_name: Glue database name (for view naming)
        duckdb_path: Path to DuckDB database file
        dry_run: If True, print SQL but don't execute
        verbose: If True, print progress for each table

    Returns:
        Dict with success/error counts
    """
    results = {"success": 0, "error": 0, "skipped": 0}

    if not dry_run:
        if verbose:
            print(f"\nConnecting to DuckDB: {duckdb_path}")
        duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(duckdb_path))

        # Load extensions (only on first connection)
        if verbose:
            print("Loading DuckDB extensions...")
        for ext in ["httpfs", "aws", "iceberg"]:
            conn.execute(f"INSTALL {ext}")
            conn.execute(f"LOAD {ext}")
        if verbose:
            print("  ✓ Extensions loaded")

        # Load AWS credentials
        conn.execute("CALL load_aws_credentials()")
        if verbose:
            print("  ✓ AWS credentials loaded")
    else:
        conn = None
        if verbose:
            print("\n🔍 DRY RUN MODE - No actual changes will be made\n")

    if verbose:
        print(f"\nRegistering {len(tables)} Iceberg tables from {database_name}...")
        print("=" * 70)

    for idx, table in enumerate(tables):
        table_name = table["name"]
        metadata_location = table["metadata_location"]

        # Create view name: glue__{database}__{table}
        view_name = f"glue__{database_name}__{table_name}"

        # Create view SQL
        create_view_sql = f"""
CREATE OR REPLACE VIEW {view_name} AS
SELECT * FROM iceberg_scan('{metadata_location}')
"""

        if dry_run:
            if verbose:
                print(f"\n-- {table_name}")
                print(create_view_sql.strip())
            results["success"] += 1
        else:
            try:
                conn.execute(create_view_sql)
                if verbose:
                    print(f"  ✓ {view_name}")
                results["success"] += 1
            except Exception as e:
                if verbose:
                    print(f"  ✗ {view_name}: {e}")
                results["error"] += 1

        # Progress indicator for large batches
        if not verbose and (idx + 1) % 100 == 0:
            print(f"  Processed {idx + 1}/{len(tables)} tables...")

    if not dry_run:
        # Create a metadata table to track registered sources
        if verbose:
            print("\nUpdating metadata registry...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _glue_source_registry (
                view_name VARCHAR PRIMARY KEY,
                glue_database VARCHAR,
                glue_table VARCHAR,
                metadata_location VARCHAR,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert/update registry
        for table in tables:
            view_name = f"glue__{database_name}__{table['name']}"
            conn.execute(
                """
                INSERT OR REPLACE INTO _glue_source_registry
                (view_name, glue_database, glue_table, metadata_location, registered_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
                (view_name, database_name, table["name"], table["metadata_location"]),
            )

        if verbose:
            print("  ✓ Registry updated")
        conn.close()

    return results


def show_registry(duckdb_path: Path):
    """Show currently registered Glue sources"""
    if not duckdb_path.exists():
        print(f"No DuckDB database found at: {duckdb_path}")
        return

    conn = duckdb.connect(str(duckdb_path))

    try:
        result = conn.execute("""
            SELECT
                view_name,
                glue_database,
                glue_table,
                registered_at
            FROM _glue_source_registry
            ORDER BY registered_at DESC
        """).fetchall()

        if not result:
            print("No Glue sources registered yet.")
            return

        print(f"\nRegistered Glue Sources ({len(result)} total):")
        print("=" * 100)
        print(f"{'View Name':<60} {'Glue Database':<30} {'Registered'}")
        print("-" * 100)

        for row in result:
            view_name, glue_db, glue_table, registered = row
            print(f"{view_name:<60} {glue_db:<30} {registered!s}")

        print("=" * 100)

    except Exception as e:
        print(f"Error reading registry: {e}")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Register AWS Glue Iceberg tables as DuckDB views"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # register command
    register_parser = subparsers.add_parser("register", help="Register Glue tables")
    register_parser.add_argument(
        "--database", help=f"Glue database name (default: {DEFAULT_GLUE_DATABASE})"
    )
    register_parser.add_argument(
        "--all-layers",
        action="store_true",
        help="Register all standard dbt layer databases",
    )
    register_parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=DEFAULT_DUCKDB_PATH,
        help=f"DuckDB database path (default: {DEFAULT_DUCKDB_PATH})",
    )
    register_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    register_parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress verbose output (show summary only)",
    )

    # list command
    list_parser = subparsers.add_parser("list", help="List registered sources")
    list_parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=DEFAULT_DUCKDB_PATH,
        help=f"DuckDB database path (default: {DEFAULT_DUCKDB_PATH})",
    )

    args = parser.parse_args()

    # Default to register if no command specified
    if not args.command:
        args.command = "register"
        args.database = None
        args.all_layers = True
        args.duckdb_path = DEFAULT_DUCKDB_PATH
        args.dry_run = False
        args.quiet = False

    if args.command == "register":
        # Determine which databases to register
        if args.all_layers:
            databases = LAYER_DATABASES
            print(f"\n🔄 Registering ALL layers ({len(databases)} databases)")
        elif args.database:
            databases = [args.database]
            print(f"\n🔄 Registering single database: {args.database}")
        else:
            databases = [DEFAULT_GLUE_DATABASE]
            print(f"\n🔄 Registering default database: {DEFAULT_GLUE_DATABASE}")

        total_results = {"success": 0, "error": 0, "skipped": 0}
        verbose = not args.quiet

        for db_idx, database in enumerate(databases):
            print(f"\n{'=' * 70}")
            print(f"[{db_idx + 1}/{len(databases)}] Processing: {database}")
            print(f"{'=' * 70}")

            # Get tables from Glue
            try:
                tables = get_glue_tables(database)
            except Exception as e:
                print(f"  ✗ Error accessing database: {e}")
                continue

            if not tables:
                print(f"  ⚠ No Iceberg tables found in {database}")
                continue

            # Register in DuckDB
            results = register_tables_in_duckdb(
                tables, database, args.duckdb_path, args.dry_run, verbose=verbose
            )

            # Accumulate totals
            for key in total_results:
                total_results[key] += results[key]

            if not verbose:
                print(f"  ✓ {results['success']} tables registered")
                if results["error"] > 0:
                    print(f"  ✗ {results['error']} errors")

        # Final summary
        print("\n" + "=" * 70)
        print(f"{'SIMULATION' if args.dry_run else 'REGISTRATION'} COMPLETE")
        print("=" * 70)
        print(f"  Databases processed: {len(databases)}")
        print(f"  ✓ Total success: {total_results['success']}")
        print(f"  ✗ Total errors: {total_results['error']}")
        print(f"  ⊘ Total skipped: {total_results['skipped']}")
        print("=" * 70)

        if not args.dry_run and total_results["success"] > 0:
            print(
                f"\n✨ {total_results['success']} Iceberg tables registered across all layers!"
            )
            print(f"   DuckDB location: {args.duckdb_path}")
            print("\n   You can now run dbt with: --target dev_local")
            print("   Raw data will be read from Iceberg (zero local storage)")
            print("   Only transformed models will be written locally")

    elif args.command == "list":
        show_registry(args.duckdb_path)


if __name__ == "__main__":
    main()
