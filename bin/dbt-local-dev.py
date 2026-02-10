#!/usr/bin/env python3
# ruff: noqa: T201, FBT001, FBT002, S608, BLE001, C901, PLR0912, PLR0913, PLR0915, E501, RUF059, PT028, PLC0415, PLR2004, S607, RUF001
"""
CLI tool for local dbt development with DuckDB + Iceberg.

This unified CLI provides commands for:
- Registering AWS Glue Iceberg tables as DuckDB views
- Testing Glue/Iceberg connectivity
- Cleaning up Trino development schemas

Usage:
    # Register all Iceberg tables from AWS Glue
    python bin/dbt-local-dev.py register --all-layers

    # Test Glue/Iceberg connectivity
    python bin/dbt-local-dev.py test

    # Clean up Trino dev schemas
    python bin/dbt-local-dev.py cleanup --target dev_production --execute

    # Show help
    python bin/dbt-local-dev.py --help
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Annotated, Any, Literal

import boto3
import cyclopts
import duckdb
import trino
from trino.auth import BasicAuthentication

# ============================================================================
# Constants and Configuration
# ============================================================================

DEFAULT_DUCKDB_PATH = Path.home() / ".ol-dbt" / "local.duckdb"
DEFAULT_GLUE_DATABASE = "ol_warehouse_production_raw"

# Standard dbt layer databases (in dependency order)
LAYER_DATABASES = [
    "ol_warehouse_production_raw",
    "ol_warehouse_production_staging",
    "ol_warehouse_production_intermediate",
    "ol_warehouse_production_dimensional",
    "ol_warehouse_production_mart",
    "ol_warehouse_production_reporting",
    "ol_warehouse_production_external",
]

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

# Target configurations for Trino cleanup
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

# ============================================================================
# Glue/Iceberg Registration Functions
# ============================================================================


def get_glue_tables(database: str) -> list[dict[str, Any]]:
    """
    Get all Iceberg tables from AWS Glue catalog.

    Parameters
    ----------
    database
        Glue database name

    Returns
    -------
    list[dict[str, Any]]
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


def _register_single_table(
    table: dict[str, Any],
    database_name: str,
    duckdb_path: Path,
    existing_registrations: dict[str, str],
    force: bool,
    db_lock: Lock | None,  # noqa: ARG001
    verbose: bool,  # noqa: ARG001
) -> tuple[str, str, str | None]:
    """
    Register a single table. Returns (status, view_name, error_message).

    Status can be: 'success', 'skipped', 'error'
    """
    table_name = table["name"]
    metadata_location = table["metadata_location"]
    view_name = f"glue__{database_name}__{table_name}"

    # Check if we should skip this table
    if not force and view_name in existing_registrations:
        existing_metadata = existing_registrations[view_name]
        if existing_metadata == metadata_location:
            return ("skipped", view_name, None)

    # Determine if new or updated
    is_updated = view_name in existing_registrations

    # Create view SQL
    create_view_sql = f"""
CREATE OR REPLACE VIEW {view_name} AS
SELECT * FROM iceberg_scan('{metadata_location}')
"""

    try:
        # Each thread needs its own connection
        conn = duckdb.connect(str(duckdb_path))

        # Execute the CREATE VIEW
        conn.execute(create_view_sql)

        # Update registry
        conn.execute(
            """
            INSERT OR REPLACE INTO _glue_source_registry
            (view_name, glue_database, glue_table, metadata_location, registered_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
            (view_name, database_name, table_name, metadata_location),
        )

        conn.close()
    except Exception as e:
        return ("error", view_name, str(e))

    status_type = "updated" if is_updated else "new"
    return ("success", view_name, status_type)


def register_tables_in_duckdb(
    tables: list[dict[str, Any]],
    database_name: str,
    duckdb_path: Path,
    dry_run: bool = False,
    verbose: bool = True,
    force: bool = False,
    workers: int = 10,
) -> dict[str, int]:
    """
    Register Glue tables as DuckDB views.

    Parameters
    ----------
    tables
        List of table metadata from Glue
    database_name
        Glue database name (for view naming)
    duckdb_path
        Path to DuckDB database file
    dry_run
        If True, print SQL but don't execute
    verbose
        If True, print progress for each table
    workers
        Number of parallel workers for table registration (default: 10)

    Returns
    -------
    dict[str, int]
        Dict with success/error counts (success, error, skipped, updated, new)
    """
    results = {"success": 0, "error": 0, "skipped": 0, "updated": 0, "new": 0}

    if not dry_run:
        if verbose:
            print(f"\nConnecting to DuckDB: {duckdb_path}")
        duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(duckdb_path))

        # Load extensions
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

        # Create registry table first
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _glue_source_registry (
                view_name VARCHAR PRIMARY KEY,
                glue_database VARCHAR,
                glue_table VARCHAR,
                metadata_location VARCHAR,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Get existing registrations if not forcing re-registration
        existing_registrations = {}
        if not force:
            try:
                result = conn.execute(
                    """
                    SELECT view_name, metadata_location
                    FROM _glue_source_registry
                    WHERE glue_database = ?
                """,
                    (database_name,),
                ).fetchall()
                existing_registrations = {row[0]: row[1] for row in result}
            except Exception:
                # Table might not exist yet or be empty
                existing_registrations = {}
    else:
        conn = None
        existing_registrations = {}
        if verbose:
            print("\n🔍 DRY RUN MODE - No actual changes will be made\n")

    if verbose:
        mode = "all (forced)" if force else "new/changed only"
        workers_msg = f" using {workers} workers" if not dry_run else ""
        print(
            f"\nRegistering {len(tables)} Iceberg tables from {database_name} ({mode}){workers_msg}..."
        )
        print("=" * 70)

    if dry_run:
        # Dry run mode: sequential processing for readable output
        for table in tables:
            table_name = table["name"]
            metadata_location = table["metadata_location"]
            view_name = f"glue__{database_name}__{table_name}"

            # Check if we should skip this table
            if not force and view_name in existing_registrations:
                existing_metadata = existing_registrations[view_name]
                if existing_metadata == metadata_location:
                    results["skipped"] += 1
                    if verbose:
                        print(f"  ⊘ {view_name} (unchanged)")
                    continue

            is_updated = view_name in existing_registrations
            create_view_sql = f"""
CREATE OR REPLACE VIEW {view_name} AS
SELECT * FROM iceberg_scan('{metadata_location}')
"""
            if verbose:
                status = "UPDATE" if is_updated else "NEW"
                print(f"\n-- [{status}] {table_name}")
                print(create_view_sql.strip())
            results["success"] += 1
    else:
        # Parallel processing for actual registration
        db_lock = Lock()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(
                    _register_single_table,
                    table,
                    database_name,
                    duckdb_path,
                    existing_registrations,
                    force,
                    db_lock,
                    verbose,
                ): table
                for table in tables
            }

            # Process results as they complete
            for future in as_completed(futures):
                status, view_name, extra_info = future.result()

                if status == "success":
                    results["success"] += 1
                    # extra_info is 'new' or 'updated'
                    if extra_info == "new":
                        results["new"] += 1
                        if verbose:
                            print(f"  + {view_name} (new)")
                    elif extra_info == "updated":
                        results["updated"] += 1
                        if verbose:
                            print(f"  ↻ {view_name} (updated)")
                    elif verbose:
                        print(f"  ✓ {view_name}")
                elif status == "skipped":
                    results["skipped"] += 1
                    if verbose:
                        print(f"  ⊘ {view_name} (unchanged)")
                elif status == "error":
                    results["error"] += 1
                    if verbose:
                        print(f"  ✗ {view_name}: {extra_info}")

            # Progress indicator for large batches (when not verbose)
            processed = results["success"] + results["error"] + results["skipped"]
            if not verbose and processed > 0 and processed % 50 == 0:
                print(f"  Processed {processed}/{len(tables)} tables...")

        if conn:
            conn.close()

    return results


def show_registry(duckdb_path: Path) -> None:
    """
    Show currently registered Glue sources.

    Parameters
    ----------
    duckdb_path
        Path to DuckDB database file
    """
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


# ============================================================================
# Trino Cleanup Functions
# ============================================================================


def get_trino_connection(
    host: str, catalog: str, schema: str
) -> trino.dbapi.Connection:
    """
    Create a Trino connection using OAuth authentication.

    Parameters
    ----------
    host
        Trino host
    catalog
        Trino catalog
    schema
        Trino schema

    Returns
    -------
    trino.dbapi.Connection
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
        auth=BasicAuthentication(username, password),
    )


def validate_schema_safety(schema_name: str, suffix: str) -> bool:
    """
    Validate that schema is safe to clean (has suffix, not in protected list).

    Parameters
    ----------
    schema_name
        Schema name to validate
    suffix
        Expected suffix

    Returns
    -------
    bool
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

    Parameters
    ----------
    conn
        Trino connection
    catalog
        Catalog name
    base_schema
        Base schema name
    suffix
        Schema suffix

    Returns
    -------
    list[str]
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

    Parameters
    ----------
    conn
        Trino connection
    catalog
        Catalog name
    schema
        Schema name

    Returns
    -------
    dict[str, list[str]]
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

    Parameters
    ----------
    conn
        Trino connection
    catalog
        Catalog name
    schema
        Schema name
    objects
        Dict with 'tables' and 'views' lists
    dry_run
        If True, only print what would be deleted

    Returns
    -------
    dict[str, int]
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


# ============================================================================
# Cyclopts CLI Application
# ============================================================================

app = cyclopts.App(
    name="dbt-local-dev",
    help="Unified CLI for local dbt development with DuckDB + Iceberg.",
    version="1.0.0",
)


@app.command
def register(
    database: Annotated[
        str | None,
        cyclopts.Parameter(
            help=f"Glue database name (default: {DEFAULT_GLUE_DATABASE})"
        ),
    ] = None,
    all_layers: Annotated[
        bool, cyclopts.Parameter(help="Register all standard dbt layer databases")
    ] = False,
    duckdb_path: Annotated[
        Path,
        cyclopts.Parameter(
            help=f"DuckDB database path (default: {DEFAULT_DUCKDB_PATH})"
        ),
    ] = DEFAULT_DUCKDB_PATH,
    dry_run: Annotated[
        bool, cyclopts.Parameter(help="Show what would be done without making changes")
    ] = False,
    quiet: Annotated[
        bool, cyclopts.Parameter(help="Suppress verbose output (show summary only)")
    ] = False,
    force: Annotated[
        bool,
        cyclopts.Parameter(
            help="Force re-registration of all tables (default: only register new/changed)"
        ),
    ] = False,
    workers: Annotated[
        int,
        cyclopts.Parameter(
            help="Number of parallel workers for registration (default: 10, max: 20)"
        ),
    ] = 10,
) -> None:
    """
    Register AWS Glue Iceberg tables as DuckDB views.

    This command queries AWS Glue to discover Iceberg tables and creates
    DuckDB views that reference their metadata locations. This enables dbt
    models to query Iceberg tables directly without copying data locally.

    By default, this command is incremental - it only registers new tables
    or tables whose metadata has changed. Use --force to re-register all tables.

    Registration uses parallel processing with configurable workers for faster
    initial setup (5-10x speedup with default 10 workers).

    Parameters
    ----------
    database
        Glue database name
    all_layers
        Register all standard dbt layer databases
    duckdb_path
        DuckDB database path
    dry_run
        Show what would be done without making changes
    quiet
        Suppress verbose output (show summary only)
    force
        Force re-registration of all tables (skips incremental check)
    workers
        Number of parallel workers (1-20)
    """
    # Validate workers
    if workers < 1:
        print("❌ Error: --workers must be at least 1")
        sys.exit(1)
    if workers > 20:
        print("⚠️  Warning: --workers capped at 20 (you specified {workers})")
        workers = 20

    # Determine which databases to register
    if all_layers:
        databases = LAYER_DATABASES
        print(f"\n🔄 Registering ALL layers ({len(databases)} databases)")
    elif database:
        databases = [database]
        print(f"\n🔄 Registering single database: {database}")
    else:
        databases = [DEFAULT_GLUE_DATABASE]
        print(f"\n🔄 Registering default database: {DEFAULT_GLUE_DATABASE}")

    total_results = {"success": 0, "error": 0, "skipped": 0, "new": 0, "updated": 0}
    verbose = not quiet

    for db_idx, db_name in enumerate(databases):
        print(f"\n{'=' * 70}")
        print(f"[{db_idx + 1}/{len(databases)}] Processing: {db_name}")
        print(f"{'=' * 70}")

        # Get tables from Glue
        try:
            tables = get_glue_tables(db_name)
        except Exception as e:
            print(f"  ✗ Error accessing database: {e}")
            continue

        if not tables:
            print(f"  ⚠ No Iceberg tables found in {db_name}")
            continue

        # Register in DuckDB
        results = register_tables_in_duckdb(
            tables,
            db_name,
            duckdb_path,
            dry_run,
            verbose=verbose,
            force=force,
            workers=workers,
        )

        # Accumulate totals
        for key in total_results:
            total_results[key] += results[key]

        if not verbose:
            print(f"  + {results['new']} new")
            print(f"  ↻ {results['updated']} updated")
            print(f"  ⊘ {results['skipped']} skipped")
            if results["error"] > 0:
                print(f"  ✗ {results['error']} errors")

    # Final summary
    print("\n" + "=" * 70)
    print(f"{'SIMULATION' if dry_run else 'REGISTRATION'} COMPLETE")
    print("=" * 70)
    print(f"  Databases processed: {len(databases)}")
    print(f"  + New tables: {total_results['new']}")
    print(f"  ↻ Updated tables: {total_results['updated']}")
    print(f"  ⊘ Skipped (unchanged): {total_results['skipped']}")
    print(f"  ✗ Errors: {total_results['error']}")
    print("=" * 70)

    if not dry_run and (total_results["new"] > 0 or total_results["updated"] > 0):
        changed = total_results["new"] + total_results["updated"]
        print(f"\n✨ {changed} Iceberg tables registered/updated across all layers!")
        print(f"   DuckDB location: {duckdb_path}")
        if total_results["skipped"] > 0:
            print(
                f"   ⊘ {total_results['skipped']} tables skipped (unchanged - use --force to re-register)"
            )
        print("\n   You can now run dbt with: --target dev_local")
        print("   Raw data will be read from Iceberg (zero local storage)")
        print("   Only transformed models will be written locally")
    elif not dry_run and total_results["skipped"] > 0:
        print(
            f"\n✓ All {total_results['skipped']} tables already registered and up-to-date!"
        )
        print("   Use --force to re-register all tables")


@app.command
def list_sources(
    duckdb_path: Annotated[
        Path,
        cyclopts.Parameter(
            help=f"DuckDB database path (default: {DEFAULT_DUCKDB_PATH})"
        ),
    ] = DEFAULT_DUCKDB_PATH,
) -> None:
    """
    List currently registered Glue sources in DuckDB.

    Shows all Iceberg tables that have been registered as DuckDB views,
    including their Glue database, table name, and registration timestamp.

    Parameters
    ----------
    duckdb_path
        DuckDB database path
    """
    show_registry(duckdb_path)


@app.command
def test(
    duckdb_path: Annotated[
        Path,
        cyclopts.Parameter(
            help=f"DuckDB database path (default: {DEFAULT_DUCKDB_PATH})"
        ),
    ] = DEFAULT_DUCKDB_PATH,
) -> None:
    """
    Test reading Iceberg tables from AWS Glue catalog using DuckDB.

    This validates that DuckDB can:
    1. Connect to AWS Glue catalog
    2. Read Iceberg table metadata
    3. Query data from Iceberg tables in S3

    Parameters
    ----------
    duckdb_path
        DuckDB database path
    """
    print("=" * 70)
    print("Testing DuckDB + AWS Glue + Iceberg Integration")
    print("=" * 70)

    # Create/connect to test database
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nConnecting to DuckDB at: {duckdb_path}")
    conn = duckdb.connect(str(duckdb_path))

    try:
        # Install and load extensions
        print("\n1. Loading extensions...")
        for ext in ["httpfs", "aws", "iceberg"]:
            conn.execute(f"INSTALL {ext}")
            print(f"   ✓ Installed {ext}")
            conn.execute(f"LOAD {ext}")
            print(f"   ✓ Loaded {ext}")

        # Configure AWS
        print("\n2. Configuring AWS...")
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        conn.execute(f"SET s3_region='{region}'")
        print(f"   ✓ Region: {region}")

        # Try to use AWS credential chain
        conn.execute("CALL load_aws_credentials()")
        print("   ✓ Loaded AWS credentials from chain")

        # Test reading Iceberg table from Glue
        print("\n3. Testing Iceberg table read from Glue catalog...")

        # Get metadata location from Glue
        glue = boto3.client("glue")

        table_name = "raw__mitlearn__app__postgres__users_user"
        database = "ol_warehouse_production_raw"

        print(f"   Getting metadata from Glue: {database}.{table_name}")
        response = glue.get_table(DatabaseName=database, Name=table_name)

        table_location = response["Table"]["StorageDescriptor"]["Location"]
        metadata_location = response["Table"]["Parameters"].get("metadata_location")

        print(f"   Table location: {table_location}")
        print(f"   Metadata location: {metadata_location}")

        # Use metadata location for iceberg_scan
        iceberg_path = metadata_location if metadata_location else table_location
        print(f"   Reading from: {iceberg_path}")

        query = f"SELECT * FROM iceberg_scan('{iceberg_path}') LIMIT 5"
        print(f"   Query: {query}")
        result = conn.execute(query).fetchall()

        if result:
            print(f"   ✓ Successfully read {len(result)} rows from Iceberg table!")
            print("\n   Sample data (first row):")
            if len(result) > 0:
                columns = conn.execute(query).description
                col_names = [col[0] for col in columns]
                print(f"   Columns: {col_names[:5]}...")
                print(f"   First row: {str(result[0])[:100]}...")

            # Get table schema
            schema_query = f"DESCRIBE SELECT * FROM iceberg_scan('{iceberg_path}')"
            schema = conn.execute(schema_query).fetchall()
            print(f"\n   Table has {len(schema)} columns")

            print("\n" + "=" * 70)
            print("✓ SUCCESS: DuckDB can read Iceberg tables from S3!")
            print("\nNext steps:")
            print("1. Register tables: dbt-local-dev.py register --all-layers")
            print("2. Run dbt: cd src/ol_dbt && dbt run --target dev_local")
            print("=" * 70)
        else:
            print("   ⚠ Query succeeded but no rows returned")
            print("\n" + "=" * 70)
            print("✓ Connection successful (but table appears empty)")
            print("=" * 70)

    except Exception as e:
        print(f"   ✗ Error: {e}")
        import traceback

        traceback.print_exc()
        print("\n" + "=" * 70)
        print("✗ FAILED: Could not read Iceberg tables")
        print("Check error messages above")
        print("=" * 70)
        sys.exit(1)
    finally:
        conn.close()


@app.command
def cleanup(
    target: Annotated[
        Literal["dev_production", "dev_qa"],
        cyclopts.Parameter(help="Target environment"),
    ] = "dev_production",
    suffix: Annotated[
        str | None,
        cyclopts.Parameter(
            help="Schema suffix (default: from DBT_SCHEMA_SUFFIX env var)"
        ),
    ] = None,
    execute: Annotated[
        bool, cyclopts.Parameter(help="Actually delete objects (default is dry-run)")
    ] = False,
    yes: Annotated[
        bool, cyclopts.Parameter(help="Skip confirmation prompt (USE WITH CAUTION)")
    ] = False,
) -> None:
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

    Safety Features:
    - Only operates on schemas with suffixes (e.g., ol_warehouse_production_tmacey)
    - Requires explicit confirmation before deletion
    - Dry-run mode by default
    - Lists all objects before deletion
    - Blocks deletion of production/qa base schemas

    Environment Variables Required:
        DBT_TRINO_USERNAME - Trino username
        DBT_TRINO_PASSWORD - Trino password
        DBT_SCHEMA_SUFFIX - Your schema suffix (default, can override with --suffix)

    Parameters
    ----------
    target
        Target environment (dev_production or dev_qa)
    suffix
        Schema suffix (overrides DBT_SCHEMA_SUFFIX)
    execute
        Actually delete objects (default is dry-run)
    yes
        Skip confirmation prompt (USE WITH CAUTION)
    """
    # Get suffix
    suffix_val = suffix or os.getenv("DBT_SCHEMA_SUFFIX", "")
    if not suffix_val:
        print("ERROR: Schema suffix required. Set DBT_SCHEMA_SUFFIX or use --suffix")
        print("Example: export DBT_SCHEMA_SUFFIX=tmacey")
        sys.exit(1)

    # Safety check
    if not suffix_val or len(suffix_val) < 2:
        print("ERROR: Schema suffix must be at least 2 characters")
        sys.exit(1)

    config = TARGET_CONFIGS[target]

    print("=" * 70)
    print(f"Schema Cleanup Tool - {config['description']}")
    print("=" * 70)
    print(f"Target: {target}")
    print(f"Cluster: {config['host']}")
    print(f"Catalog: {config['catalog']}")
    print(f"Base Schema: {config['base_schema']}")
    print(f"Suffix: {suffix_val}")
    print(f"Mode: {'DRY RUN' if not execute else 'EXECUTE'}")
    print("=" * 70)

    # Connect
    print("\n📡 Connecting to Trino...")
    conn = get_trino_connection(
        config["host"], config["catalog"], config["base_schema"]
    )
    print("✓ Connected")

    # Get schemas to clean
    print(f"\n🔍 Finding schemas with suffix '_{suffix_val}'...")
    schemas = get_schemas_to_clean(
        conn, config["catalog"], config["base_schema"], suffix_val
    )

    if not schemas:
        print(f"✓ No schemas found with suffix '_{suffix_val}'")
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
    if execute and not yes:
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
        counts = drop_objects(
            conn, config["catalog"], schema, objects, dry_run=not execute
        )
        grand_total["tables"] += counts["tables"]
        grand_total["views"] += counts["views"]

    # Summary
    print("\n" + "=" * 70)
    if not execute:
        print("DRY RUN COMPLETE - No changes made")
        print(f"Would delete: {total_tables} tables, {total_views} views")
        print("\nTo execute, run with --execute flag")
    else:
        print("CLEANUP COMPLETE")
        print(f"Deleted: {grand_total['tables']} tables, {grand_total['views']} views")
    print("=" * 70)


@app.command
def setup(
    duckdb_path: Annotated[
        Path,
        cyclopts.Parameter(
            help=f"DuckDB database path (default: {DEFAULT_DUCKDB_PATH})"
        ),
    ] = DEFAULT_DUCKDB_PATH,
    layers: Annotated[
        Literal["all", "raw", "staging"],
        cyclopts.Parameter(help="Which layers to register"),
    ] = "raw",
    recreate: Annotated[
        bool, cyclopts.Parameter(help="Recreate DuckDB database if it exists")
    ] = False,
    skip_tests: Annotated[
        bool, cyclopts.Parameter(help="Skip testing after setup")
    ] = False,
) -> None:
    """
    Set up local dbt development environment with DuckDB + Iceberg.

    This command performs a complete setup:
    1. Checks prerequisites (AWS credentials, uv, permissions)
    2. Creates DuckDB directory and database
    3. Installs and loads DuckDB extensions
    4. Installs dbt dependencies
    5. Registers Iceberg tables from AWS Glue
    6. Tests the setup

    Prerequisites:
    - AWS credentials configured (~/.aws/credentials or environment)
    - uv package manager installed
    - Network access to S3 and AWS Glue
    - IAM permissions: glue:GetDatabase, glue:GetTable, s3:GetObject

    Parameters
    ----------
    duckdb_path
        DuckDB database path
    layers
        Which layers to register (all/raw/staging)
    recreate
        Recreate DuckDB database if it exists
    skip_tests
        Skip testing after setup
    """
    import shutil
    import subprocess

    print("\n" + "=" * 70)
    print("Local dbt Development Environment Setup")
    print("DuckDB + Iceberg Direct Read from S3")
    print("=" * 70 + "\n")

    # Step 1: Check prerequisites
    print("=" * 70)
    print("Step 1: Checking Prerequisites")
    print("=" * 70)

    all_good = True

    # Check uv
    if shutil.which("uv"):
        try:
            result = subprocess.run(
                ["uv", "--version"], capture_output=True, text=True, check=False
            )
            version = result.stdout.strip()
            print(f"✓ uv is installed ({version})")
        except Exception:
            print("✓ uv is installed")
    else:
        print("✗ uv is not installed. Install from: https://github.com/astral-sh/uv")
        all_good = False

    # Check AWS credentials
    try:
        result = subprocess.run(
            ["aws", "sts", "get-caller-identity"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            import json

            identity = json.loads(result.stdout)
            account = identity.get("Account", "unknown")
            arn = identity.get("Arn", "")
            user = arn.split("/")[-1] if "/" in arn else "unknown"
            print(f"✓ AWS credentials configured (Account: {account}, User: {user})")
        else:
            print("✗ AWS credentials not configured. Run 'aws configure'")
            all_good = False
    except Exception:
        print("✗ AWS CLI not found or credentials not configured")
        all_good = False

    # Check AWS Glue access
    try:
        result = subprocess.run(
            ["aws", "glue", "get-databases", "--max-results", "1"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            print("✓ AWS Glue access verified")
        else:
            print("⚠ Cannot access AWS Glue. Check IAM permissions")
            print("  Required: glue:GetDatabase, glue:GetTable, glue:GetTables")
    except Exception:
        print("⚠ Cannot verify AWS Glue access")

    # Check S3 access
    try:
        result = subprocess.run(
            ["aws", "s3", "ls", "s3://ol-data-lake-raw-production/"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            print("✓ S3 production bucket access verified")
        else:
            print("⚠ Cannot access s3://ol-data-lake-raw-production/")
            print("  Iceberg reads will fail without this access")
    except Exception:
        print("⚠ Cannot verify S3 access")

    if not all_good:
        print("\n✗ Prerequisites check failed. Fix issues above and try again.")
        sys.exit(1)

    # Step 2: Setup DuckDB directory
    print("\n" + "=" * 70)
    print("Step 2: Setting Up DuckDB Directory")
    print("=" * 70)

    duckdb_dir = duckdb_path.parent
    if duckdb_dir.exists():
        print(f"ℹ Directory already exists: {duckdb_dir}")
    else:
        print(f"ℹ Creating directory: {duckdb_dir}")
        duckdb_dir.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created {duckdb_dir}")

    # Create temp directory
    temp_dir = duckdb_dir / "temp"
    if not temp_dir.exists():
        temp_dir.mkdir(parents=True, exist_ok=True)
        print("✓ Created temp directory")

    # Step 3: Initialize DuckDB
    print("\n" + "=" * 70)
    print("Step 3: Initializing DuckDB Database")
    print("=" * 70)

    if duckdb_path.exists():
        size_bytes = duckdb_path.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        print(f"ℹ DuckDB database already exists (size: {size_mb:.1f} MB)")

        if recreate:
            print("ℹ Recreating database...")
            duckdb_path.unlink()
        else:
            response = input(
                "Do you want to recreate it? This will delete all local data [y/N]: "
            )
            if response.lower() in ("y", "yes"):
                print("ℹ Removing existing database...")
                duckdb_path.unlink()
            else:
                print("ℹ Keeping existing database")

    if not duckdb_path.exists():
        print("ℹ Creating new DuckDB database with extensions...")

        conn = duckdb.connect(str(duckdb_path))

        print("  Installing httpfs extension...")
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")

        print("  Installing aws extension...")
        conn.execute("INSTALL aws")
        conn.execute("LOAD aws")

        print("  Installing iceberg extension...")
        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")

        print("  Loading AWS credentials...")
        conn.execute("CALL load_aws_credentials()")

        print("  Creating registry table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _glue_source_registry (
                view_name VARCHAR PRIMARY KEY,
                glue_database VARCHAR,
                glue_table VARCHAR,
                metadata_location VARCHAR,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.close()
        print("✓ DuckDB initialized successfully")
        print(f"✓ DuckDB database created at {duckdb_path}")

    # Step 4: Install dbt dependencies
    print("\n" + "=" * 70)
    print("Step 4: Installing dbt Dependencies")
    print("=" * 70)

    # Find project root (go up from bin/)
    project_root = Path(__file__).parent.parent
    dbt_dir = project_root / "src" / "ol_dbt"

    if dbt_dir.exists():
        print(f"ℹ Running dbt deps in {dbt_dir}...")
        result = subprocess.run(
            ["uv", "run", "dbt", "deps", "--quiet"],
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            print("✓ dbt dependencies installed")
        else:
            print(f"⚠ dbt deps failed: {result.stderr}")
    else:
        print(f"⚠ dbt directory not found at {dbt_dir}")

    # Step 5: Register Iceberg sources
    print("\n" + "=" * 70)
    print("Step 5: Registering Iceberg Sources from AWS Glue")
    print("=" * 70)

    print("ℹ This will register Iceberg tables as DuckDB views")
    print("ℹ Note: This only registers metadata - no data is copied locally")
    print(f"ℹ Layers to register: {layers}")
    print()

    if layers == "all":
        print("ℹ Registering all layers (estimated time: 10-20 minutes)...")
        databases = LAYER_DATABASES
    elif layers == "raw":
        print("ℹ Registering raw layer only...")
        databases = [DEFAULT_GLUE_DATABASE]
    elif layers == "staging":
        print("ℹ Registering staging layer only...")
        databases = ["ol_warehouse_production_staging"]
    else:
        databases = [DEFAULT_GLUE_DATABASE]

    total_results = {"success": 0, "error": 0, "skipped": 0, "new": 0, "updated": 0}

    for db_idx, db_name in enumerate(databases):
        print(f"\n[{db_idx + 1}/{len(databases)}] Processing: {db_name}")

        try:
            tables = get_glue_tables(db_name)
        except Exception as e:
            print(f"  ✗ Error accessing database: {e}")
            continue

        if not tables:
            print(f"  ⚠ No Iceberg tables found in {db_name}")
            continue

        results = register_tables_in_duckdb(
            tables, db_name, duckdb_path, dry_run=False, verbose=False, force=True
        )

        for key in total_results:
            total_results[key] += results[key]

        print(f"  + {results['new']} new tables registered")
        if results["error"] > 0:
            print(f"  ✗ {results['error']} errors")

    print(f"\n✓ Total: {total_results['success']} Iceberg views registered")

    # Show statistics
    if total_results["success"] > 0:
        conn = duckdb.connect(str(duckdb_path))
        result = conn.execute("""
            SELECT
                glue_database,
                COUNT(*) as table_count
            FROM _glue_source_registry
            GROUP BY glue_database
            ORDER BY glue_database
        """).fetchall()

        print("\n  Registered tables by layer:")
        for db, count in result:
            layer = db.replace("ol_warehouse_production_", "")
            print(f"    • {layer}: {count:,} tables")

        conn.close()

    # Step 6: Test setup
    if not skip_tests:
        print("\n" + "=" * 70)
        print("Step 6: Testing Setup")
        print("=" * 70)

        print("ℹ Testing dbt compilation...")
        result = subprocess.run(
            [
                "uv",
                "run",
                "dbt",
                "compile",
                "--select",
                "stg__mitlearn__app__postgres__users_user",
                "--target",
                "dev_local",
            ],
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode == 0:
            print("✓ dbt compilation test passed")
        else:
            print("⚠ dbt compilation test failed (may need to register more layers)")

    # Show completion
    print("\n" + "=" * 70)
    print("Setup Complete! 🎉")
    print("=" * 70)
    print()
    print("✓ Local dbt development environment is ready")
    print()
    print(f"📁 DuckDB database: {duckdb_path}")

    # Calculate total size
    total_size = sum(f.stat().st_size for f in duckdb_dir.rglob("*") if f.is_file())
    total_size_mb = total_size / (1024 * 1024)
    print(f"📊 Local storage: {total_size_mb:.1f} MB")
    print()
    print("Next steps:")
    print()
    print("  1. Navigate to dbt project:")
    print(f"     cd {dbt_dir}")
    print()
    print("  2. Run a model locally:")
    print(
        "     uv run dbt run --select stg__mitlearn__app__postgres__users_user --target dev_local"
    )
    print()
    print("  3. Build multiple models:")
    print("     uv run dbt run --select tag:mitlearn --target dev_local")
    print()
    print("  4. Test your changes:")
    print(
        "     uv run dbt test --select stg__mitlearn__app__postgres__users_user --target dev_local"
    )
    print()
    print("Important notes:")
    print("  • Raw data is read directly from S3 (zero local duplication)")
    print("  • Only transformed models are stored locally")
    print("  • Use --target dev_local for local, production/qa for Trino")
    print()


if __name__ == "__main__":
    app()
