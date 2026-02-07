#!/usr/bin/env python3
# ruff: noqa: T201, FBT001, FBT002, PLR0912, PLR0915, PLR2004, C901, BLE001, D401, S608
"""
Clean up tables and views in namespaced development schemas on Trino.

This script safely drops tables/views in developer schemas (with suffixes)
while preventing accidental deletion of production schemas.

NOTE: For most use cases, prefer the dbt run-operation commands from trino_utils:
    # Drop all schemas with prefix (inside src/ol_dbt):
    uv run dbt run-operation trino__drop_schemas_by_prefixes \\
        --args "{prefixes: ['ol_warehouse_production_myname']}"

    # Drop old/orphaned models no longer in project:
    uv run dbt run-operation trino__drop_old_relations

    # Dry run to preview:
    uv run dbt run-operation trino__drop_old_relations --args "{dry_run: true}"

This Python script provides additional features:
- Cross-target cleanup (dev_production and dev_qa)
- Detailed object listing before deletion
- Additional safety checks and confirmations
- Connection validation

Safety Features:
- Only operates on schemas with suffixes (e.g., ol_warehouse_production_tmacey)
- Requires explicit confirmation before deletion
- Dry-run mode by default
- Lists all objects before deletion
- Blocks deletion of production/qa base schemas

Usage:
    # Dry run (default) - see what would be deleted
    python bin/cleanup-dev-schemas.py

    # Clean dev_production schemas
    python bin/cleanup-dev-schemas.py --target dev_production

    # Clean dev_qa schemas
    python bin/cleanup-dev-schemas.py --target dev_qa

    # Actually delete (requires confirmation)
    python bin/cleanup-dev-schemas.py --target dev_production --execute

    # Skip confirmation (USE WITH CAUTION)
    python bin/cleanup-dev-schemas.py --target dev_production --execute --yes

    # Clean specific schema suffix
    python bin/cleanup-dev-schemas.py --target dev_production --suffix tmacey --execute

Environment Variables Required:
    DBT_TRINO_USERNAME - Trino username
    DBT_TRINO_PASSWORD - Trino password
    DBT_SCHEMA_SUFFIX - Your schema suffix (default, can override with --suffix)

See also:
    - docs/LOCAL_DEVELOPMENT.md for cleanup workflows
    - https://github.com/starburstdata/dbt-trino-utils for trino_utils macros
"""

import argparse
import os
import sys

import trino

# Production schemas that must NEVER be deleted
PROTECTED_SCHEMAS = {
    "ol_warehouse_production",
    "ol_warehouse_production_raw",
    "ol_warehouse_production_staging",
    "ol_warehouse_production_intermediate",
    "ol_warehouse_production_dimensional",
    "ol_warehouse_production_mart",
    "ol_warehouse_production_reporting",
    "ol_warehouse_production_external",
    "ol_warehouse_qa",
    "ol_warehouse_qa_raw",
    "ol_warehouse_qa_staging",
    "ol_warehouse_qa_intermediate",
    "ol_warehouse_qa_dimensional",
    "ol_warehouse_qa_mart",
    "ol_warehouse_qa_reporting",
    "ol_warehouse_qa_external",
}

# Target configurations
TARGET_CONFIGS = {
    "dev_production": {
        "host": "mitol-ol-data-lake-production.trino.galaxy.starburst.io",
        "catalog": "ol_data_lake_production",
        "base_schema": "ol_warehouse_production",
        "description": "Production cluster (development schemas)",
    },
    "dev_qa": {
        "host": "mitol-ol-data-lake-qa-0.trino.galaxy.starburst.io",
        "catalog": "ol_data_lake_qa",
        "base_schema": "ol_warehouse_qa",
        "description": "QA cluster (development schemas)",
    },
}


def get_trino_connection(
    host: str, catalog: str, schema: str
) -> trino.dbapi.Connection:
    """
    Create a Trino connection using OAuth authentication.

    Args:
        host: Trino host
        catalog: Trino catalog
        schema: Trino schema

    Returns:
        Trino connection
    """
    username = os.getenv("DBT_TRINO_USERNAME")
    password = os.getenv("DBT_TRINO_PASSWORD")

    if not username or not password:
        print("ERROR: DBT_TRINO_USERNAME and DBT_TRINO_PASSWORD must be set")
        sys.exit(1)

    return trino.dbapi.connect(
        host=host,
        port=443,
        user=username,
        catalog=catalog,
        schema=schema,
        http_scheme="https",
        auth=trino.auth.BasicAuthentication(username, password),
    )


def validate_schema_safety(schema_name: str, suffix: str) -> bool:
    """
    Validate that schema is safe to clean (has suffix, not in protected list).

    Args:
        schema_name: Schema name to validate
        suffix: Expected suffix

    Returns:
        True if safe to clean, False otherwise
    """
    if schema_name in PROTECTED_SCHEMAS:
        return False

    if not suffix or suffix == "":
        return False

    return schema_name.endswith(f"_{suffix}")


def get_schemas_to_clean(
    conn: trino.dbapi.Connection, catalog: str, base_schema: str, suffix: str
) -> list[str]:
    """
    Get list of schemas matching the suffix pattern.

    Args:
        conn: Trino connection
        catalog: Catalog name
        base_schema: Base schema name
        suffix: Schema suffix

    Returns:
        List of schema names to clean
    """
    cursor = conn.cursor()
    cursor.execute(f"SHOW SCHEMAS FROM {catalog}")
    all_schemas = [row[0] for row in cursor.fetchall()]

    # Find schemas that match the pattern
    suffixed_schema = f"{base_schema}_{suffix}"
    schemas_to_clean = []

    for schema in all_schemas:
        if schema == suffixed_schema or schema.startswith(f"{suffixed_schema}_"):
            if validate_schema_safety(schema, suffix):
                schemas_to_clean.append(schema)
            else:
                print(f"⚠️  Skipping protected schema: {schema}")

    return sorted(schemas_to_clean)


def get_tables_and_views(
    conn: trino.dbapi.Connection, catalog: str, schema: str
) -> dict[str, list[str]]:
    """
    Get all tables and views in a schema.

    Args:
        conn: Trino connection
        catalog: Catalog name
        schema: Schema name

    Returns:
        Dict with 'tables' and 'views' lists
    """
    cursor = conn.cursor()

    # Get tables
    cursor.execute(f"SHOW TABLES FROM {catalog}.{schema}")
    tables = [row[0] for row in cursor.fetchall()]

    # Get views
    try:
        cursor.execute(
            f"SELECT table_name FROM {catalog}.information_schema.views "
            f"WHERE table_schema = '{schema}'"
        )
        views = [row[0] for row in cursor.fetchall()]
    except Exception:
        views = []

    return {"tables": tables, "views": views}


def drop_objects(
    conn: trino.dbapi.Connection,
    catalog: str,
    schema: str,
    objects: dict[str, list[str]],
    dry_run: bool = True,
) -> dict[str, int]:
    """
    Drop tables and views from a schema.

    Args:
        conn: Trino connection
        catalog: Catalog name
        schema: Schema name
        objects: Dict with 'tables' and 'views' lists
        dry_run: If True, only print what would be deleted

    Returns:
        Dict with counts of dropped tables and views
    """
    cursor = conn.cursor()
    counts = {"tables": 0, "views": 0}

    # Drop views first (may depend on tables)
    for view in objects["views"]:
        sql = f"DROP VIEW IF EXISTS {catalog}.{schema}.{view}"
        if dry_run:
            print(f"  [DRY RUN] Would execute: {sql}")
        else:
            try:
                cursor.execute(sql)
                counts["views"] += 1
                print(f"  ✓ Dropped view: {view}")
            except Exception as e:
                print(f"  ✗ Failed to drop view {view}: {e}")

    # Drop tables
    for table in objects["tables"]:
        sql = f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}"
        if dry_run:
            print(f"  [DRY RUN] Would execute: {sql}")
        else:
            try:
                cursor.execute(sql)
                counts["tables"] += 1
                print(f"  ✓ Dropped table: {table}")
            except Exception as e:
                print(f"  ✗ Failed to drop table {table}: {e}")

    return counts


def cleanup_schemas(
    target: str, suffix: str, dry_run: bool = True, skip_confirm: bool = False
) -> None:
    """
    Clean up development schemas on Trino.

    Args:
        target: Target environment (dev_production or dev_qa)
        suffix: Schema suffix
        dry_run: If True, only show what would be deleted
        skip_confirm: If True, skip confirmation prompt
    """
    if target not in TARGET_CONFIGS:
        valid_targets = list(TARGET_CONFIGS.keys())
        print(f"ERROR: Invalid target '{target}'. Must be one of: {valid_targets}")
        sys.exit(1)

    config = TARGET_CONFIGS[target]

    print("=" * 70)
    print(f"Schema Cleanup Tool - {config['description']}")
    print("=" * 70)
    print(f"Target: {target}")
    print(f"Cluster: {config['host']}")
    print(f"Catalog: {config['catalog']}")
    print(f"Base Schema: {config['base_schema']}")
    print(f"Suffix: {suffix}")
    print(f"Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")
    print("=" * 70)

    # Safety check
    if not suffix or len(suffix) < 2:
        print("ERROR: Schema suffix must be at least 2 characters")
        sys.exit(1)

    # Connect
    print("\n📡 Connecting to Trino...")
    conn = get_trino_connection(
        config["host"], config["catalog"], config["base_schema"]
    )
    print("✓ Connected")

    # Get schemas to clean
    print(f"\n🔍 Finding schemas with suffix '_{suffix}'...")
    schemas = get_schemas_to_clean(
        conn, config["catalog"], config["base_schema"], suffix
    )

    if not schemas:
        print(f"✓ No schemas found with suffix '_{suffix}'")
        return

    print(f"\n📋 Found {len(schemas)} schema(s) to clean:")
    for schema in schemas:
        print(f"  - {schema}")

    # Get objects in each schema
    print("\n📊 Scanning for tables and views...")
    total_tables = 0
    total_views = 0
    schema_objects = {}

    for schema in schemas:
        objects = get_tables_and_views(conn, config["catalog"], schema)
        schema_objects[schema] = objects
        total_tables += len(objects["tables"])
        total_views += len(objects["views"])

        if objects["tables"] or objects["views"]:
            print(f"\n  {schema}:")
            print(f"    Tables: {len(objects['tables'])}")
            print(f"    Views: {len(objects['views'])}")

            if objects["tables"]:
                print(f"      Tables: {', '.join(objects['tables'][:5])}")
                if len(objects["tables"]) > 5:
                    print(f"        ... and {len(objects['tables']) - 5} more")

            if objects["views"]:
                print(f"      Views: {', '.join(objects['views'][:5])}")
                if len(objects["views"]) > 5:
                    print(f"        ... and {len(objects['views']) - 5} more")

    print("\n" + "=" * 70)
    print("Total objects to delete:")
    print(f"  Tables: {total_tables}")
    print(f"  Views: {total_views}")
    print("=" * 70)

    if total_tables == 0 and total_views == 0:
        print("\n✓ No objects to delete")
        return

    # Confirmation
    if not dry_run and not skip_confirm:
        print("\n⚠️  WARNING: This will permanently delete all objects!")
        print(f"⚠️  Schemas: {', '.join(schemas)}")
        print(f"⚠️  Objects: {total_tables} tables, {total_views} views")
        response = input("\nType 'DELETE' to confirm: ")
        if response != "DELETE":
            print("Cancelled.")
            return

    # Execute
    print("\n🗑️  Cleaning schemas...")
    grand_total = {"tables": 0, "views": 0}

    for schema in schemas:
        objects = schema_objects[schema]
        if not objects["tables"] and not objects["views"]:
            continue

        print(f"\n{schema}:")
        counts = drop_objects(conn, config["catalog"], schema, objects, dry_run=dry_run)
        grand_total["tables"] += counts["tables"]
        grand_total["views"] += counts["views"]

    # Summary
    print("\n" + "=" * 70)
    if dry_run:
        print("DRY RUN COMPLETE - No changes made")
        print(f"Would delete: {total_tables} tables, {total_views} views")
        print("\nTo execute, run with --execute flag")
    else:
        print("CLEANUP COMPLETE")
        print(f"Deleted: {grand_total['tables']} tables, {grand_total['views']} views")
    print("=" * 70)


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Clean up development schemas on Trino",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--target",
        choices=["dev_production", "dev_qa"],
        default="dev_production",
        help="Target environment (default: dev_production)",
    )
    parser.add_argument(
        "--suffix",
        help="Schema suffix (default: from DBT_SCHEMA_SUFFIX env var)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete objects (default is dry-run)",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt (USE WITH CAUTION)",
    )

    args = parser.parse_args()

    # Get suffix
    suffix = args.suffix or os.getenv("DBT_SCHEMA_SUFFIX", "")
    if not suffix:
        print("ERROR: Schema suffix required. Set DBT_SCHEMA_SUFFIX or use --suffix")
        print("Example: export DBT_SCHEMA_SUFFIX=tmacey")
        sys.exit(1)

    # Run cleanup
    cleanup_schemas(
        target=args.target,
        suffix=suffix,
        dry_run=not args.execute,
        skip_confirm=args.yes,
    )


if __name__ == "__main__":
    main()
