"""Local development commands for working with DuckDB + Iceberg.

Provides commands for:
- Registering AWS Glue Iceberg tables as DuckDB views
- Testing Glue/Iceberg connectivity
- Cleaning up Trino development schemas
"""

from __future__ import annotations

import datetime
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal

import boto3
import cyclopts
import duckdb
import trino
from trino.auth import OAuth2Authentication

from ol_dbt_cli.lib.git_utils import get_repo_root

if TYPE_CHECKING:
    pass

# ============================================================================
# Constants and Configuration
# ============================================================================

DEFAULT_DUCKDB_PATH = Path.home() / ".ol-dbt" / "local.duckdb"
DEFAULT_GLUE_DATABASE = "ol_warehouse_production_raw"

# How old the newest successful Glue-scan entry may get before `register` and
# `list-sources`
# warn about it. A registered view pins one Iceberg metadata.json; when production
# rebuilds that table the old metadata file is eventually cleaned up, and the
# pinned view starts failing with an S3 404 that says nothing about staleness
# being the cause. Re-running `register` re-reads Glue and repoints the view, so
# the warning exists to make "your registry is old" visible BEFORE a build fails
# on it rather than after.
#
# One day, from measured churn rather than taste: against a registry 1.1 days old,
# a dry run reported 142 of 158 intermediate-layer pointers moved (~90%) and 28 of
# 29 mart pointers. At that rate a registry is materially stale within a day, and
# a longer window would stay quiet through exactly the multi-day drift this is
# meant to catch. Hours-old registries — the same session — still do not warn.
REGISTRY_STALE_AFTER_DAYS = 1

# Every layer a model can live in. This must stay in step with the Glue databases
# macros/override_ref.sql derives from a model's schema: a layer that is derivable but not
# registered turns an unbuilt ref into a missing-view error instead of a working fallback.
LAYER_DATABASES = [
    "ol_warehouse_production_raw",
    "ol_warehouse_production_staging",
    "ol_warehouse_production_intermediate",
    "ol_warehouse_production_dimensional",
    "ol_warehouse_production_mart",
    "ol_warehouse_production_reporting",
    "ol_warehouse_production_external",
    "ol_warehouse_production_integrations",
    "ol_warehouse_production_migration",
]

PROTECTED_SCHEMAS = {
    "ol_warehouse_production",
    "ol_warehouse_production_raw",
    "ol_warehouse_production_staging",
    "ol_warehouse_production_intermediate",
    "ol_warehouse_production_dimensional",
    "ol_warehouse_production_mart",
    "ol_warehouse_production_reporting",
    "ol_warehouse_production_external",
    "ol_warehouse_production_integrations",
    "ol_warehouse_production_migration",
    "ol_warehouse_qa",
    "ol_warehouse_qa_raw",
    "ol_warehouse_qa_staging",
    "ol_warehouse_qa_intermediate",
    "ol_warehouse_qa_dimensional",
    "ol_warehouse_qa_mart",
    "ol_warehouse_qa_reporting",
    "ol_warehouse_qa_external",
    "ol_warehouse_qa_integrations",
    "ol_warehouse_qa_migration",
}

TARGET_CONFIGS: dict[str, dict[str, str]] = {
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
# Glue/Iceberg helpers
# ============================================================================


def _get_glue_tables(database: str) -> list[dict[str, Any]]:
    """Fetch all Iceberg tables from an AWS Glue catalog database."""
    glue = boto3.client("glue")
    tables: list[dict[str, Any]] = []

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


def _classify_registration(previous_location: str | None, metadata_location: str) -> str:
    """Classify what registering a table would do, from its recorded location.

    Three outcomes, deliberately distinguished so the summary can answer "did
    anything upstream actually move?":

    - ``new``       — never registered before.
    - ``updated``   — registered, and Glue now reports a DIFFERENT metadata
                      location, i.e. production genuinely rebuilt the table.
    - ``refreshed`` — registered, and Glue reports the SAME location. Re-issuing
                      the view is a no-op, so this is only reached under
                      ``--force``; without it the table is skipped instead.

    Collapsing ``refreshed`` into ``updated`` (or, as before, reporting every
    forced re-registration as ``new`` because the recorded locations were never
    loaded under ``--force``) makes a forced run claim thousands of changes and
    hides the only number that matters: how many pointers actually changed.
    """
    if previous_location is None:
        return "new"
    if previous_location == metadata_location:
        return "refreshed"
    return "updated"


def _ensure_registry_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create local registry tables if they do not exist yet."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _glue_source_registry (
            view_name VARCHAR PRIMARY KEY,
            glue_database VARCHAR,
            glue_table VARCHAR,
            metadata_location VARCHAR,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _glue_registry_scans (
            glue_database VARCHAR PRIMARY KEY,
            scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def _record_registry_scan(duckdb_path: Path, database_name: str) -> None:
    """Record that a Glue database was successfully scanned in this run."""
    with duckdb.connect(str(duckdb_path)) as conn:
        _ensure_registry_tables(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO _glue_registry_scans (glue_database, scanned_at)
            VALUES (?, CURRENT_TIMESTAMP)
            """,
            (database_name,),
        )


def _registry_last_refreshed(duckdb_path: Path, databases: list[str]) -> datetime.datetime | None:
    """Return the newest Glue scan timestamp across *databases*, or None if unknown.

    None covers every "cannot tell" case — no DuckDB file, no scan table, no rows
    for these databases — because the caller treats them identically: it has no
    age to report and says so, rather than implying a fresh registry.
    """
    if not duckdb_path.exists() or not databases:
        return None
    placeholders = ", ".join("?" for _ in databases)
    try:
        with duckdb.connect(str(duckdb_path), read_only=True) as conn:
            row = conn.execute(
                f"SELECT max(scanned_at) FROM _glue_registry_scans WHERE glue_database IN ({placeholders})",  # noqa: S608
                databases,
            ).fetchone()
    except Exception:  # noqa: BLE001
        return None
    return row[0] if row else None


def _stale_threshold_phrase() -> str:
    """Render the staleness threshold for humans, without "1 days"."""
    days = REGISTRY_STALE_AFTER_DAYS
    return "a day" if days == 1 else f"{days} days"


def _describe_registry_age(last_refreshed: datetime.datetime | None) -> tuple[str, bool]:
    """Render *last_refreshed* as a human phrase plus whether it counts as stale."""
    if last_refreshed is None:
        # "unknown", not "never": a locked or unreadable DuckDB file reaches this
        # too, and asserting there was no prior registration would be a stronger
        # claim than the caller can actually support.
        return ("unknown (no readable prior registration)", False)
    # Match the stored value's awareness rather than assuming UTC. `registered_at`
    # is a naive TIMESTAMP and DuckDB's CURRENT_TIMESTAMP writes LOCAL time into
    # it (verified: naive-local skew ~0s, naive-UTC skew = the zone offset), so
    # tz=None here yields a naive-local `now` that lines up. Hardcoding UTC would
    # overstate every age by the local offset.
    now = datetime.datetime.now(tz=last_refreshed.tzinfo)
    age = now - last_refreshed
    age_days = age.total_seconds() / 86_400
    if age_days >= 1:
        phrase = f"{age_days:.1f} days ago"
    else:
        phrase = f"{age.total_seconds() / 3600:.1f} hours ago"
    return (f"{phrase} ({last_refreshed:%Y-%m-%d %H:%M})", age_days >= REGISTRY_STALE_AFTER_DAYS)


def _register_single_table(
    table: dict[str, Any],
    database_name: str,
    duckdb_path: Path,
    existing_registrations: dict[str, str],
    force: bool,
) -> tuple[str, str, str | None]:
    """Register one Glue table as a DuckDB view. Returns (status, view_name, extra).

    Opens its own connection to the shared DuckDB file. All callers run in the same
    process (ThreadPoolExecutor), so DuckDB's internal lock manager serialises concurrent
    writes — no cross-process WAL conflicts.
    """
    table_name = table["name"]
    metadata_location = table["metadata_location"]
    view_name = f"glue__{database_name}__{table_name}"

    outcome = _classify_registration(existing_registrations.get(view_name), metadata_location)
    if not force and outcome == "refreshed":
        return ("skipped", view_name, None)

    # Escape single quotes in the S3 path so the SQL string literal is safe.
    escaped_location = metadata_location.replace("'", "''")
    # Double-quote and escape the view identifier so Glue names with hyphens
    # or other non-identifier characters don't break the CREATE VIEW statement.
    quoted_view_name = '"' + view_name.replace('"', '""') + '"'
    create_view_sql = (
        f"CREATE OR REPLACE VIEW {quoted_view_name} AS\nSELECT * FROM iceberg_scan('{escaped_location}')\n"
    )

    try:
        with duckdb.connect(str(duckdb_path)) as conn:
            for ext in ["httpfs", "aws", "iceberg"]:
                conn.execute(f"LOAD {ext}")
            conn.execute("CALL load_aws_credentials()")
            conn.execute(create_view_sql)
            conn.execute(
                """
                INSERT OR REPLACE INTO _glue_source_registry
                (view_name, glue_database, glue_table, metadata_location, registered_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (view_name, database_name, table_name, metadata_location),
            )
    except Exception as e:  # noqa: BLE001
        return ("error", view_name, str(e))

    return ("success", view_name, outcome)


def _register_tables_in_duckdb(
    tables: list[dict[str, Any]],
    database_name: str,
    duckdb_path: Path,
    dry_run: bool = False,
    verbose: bool = True,
    force: bool = False,
    workers: int = 10,
) -> dict[str, int]:
    """Register Glue tables as DuckDB views, returning counts by status."""
    results: dict[str, int] = {"success": 0, "error": 0, "skipped": 0, "updated": 0, "new": 0, "refreshed": 0}

    if not dry_run:
        if verbose:
            print(f"\nConnecting to DuckDB: {duckdb_path}")
        duckdb_path.parent.mkdir(parents=True, exist_ok=True)

        existing_registrations: dict[str, str] = {}
        with duckdb.connect(str(duckdb_path)) as setup_conn:
            if verbose:
                print("Loading DuckDB extensions...")
            for ext in ["httpfs", "aws", "iceberg"]:
                setup_conn.execute(f"INSTALL {ext}")
                setup_conn.execute(f"LOAD {ext}")
            if verbose:
                print("  ✓ Extensions loaded")

            setup_conn.execute("CALL load_aws_credentials()")
            if verbose:
                print("  ✓ AWS credentials loaded")

            _ensure_registry_tables(setup_conn)

            # Loaded even under --force. force governs only whether an unchanged
            # table is SKIPPED; the recorded locations are still what tells new
            # from updated from merely-refreshed. Skipping this read under force
            # (as this used to) left every table looking unregistered, so a forced
            # run reported its whole catalog as "new" and could not show how many
            # Glue pointers had really moved.
            try:
                rows = setup_conn.execute(
                    "SELECT view_name, metadata_location FROM _glue_source_registry WHERE glue_database = ?",
                    (database_name,),
                ).fetchall()
                existing_registrations = {row[0]: row[1] for row in rows}
            except Exception:  # noqa: BLE001
                existing_registrations = {}
        # setup_conn is now closed — worker threads open their own per-connection.
        # DuckDB persists extensions and the registry table to the file, so workers
        # only need to LOAD (not INSTALL) extensions in their own connections.
    else:
        # Dry-run: read existing registrations from the DB (read-only) if it exists,
        # so skip/update reporting accurately reflects what would happen in a real run.
        existing_registrations = {}
        if duckdb_path.exists():
            try:
                with duckdb.connect(str(duckdb_path), read_only=True) as ro_conn:
                    rows = ro_conn.execute(
                        "SELECT view_name, metadata_location FROM _glue_source_registry WHERE glue_database = ?",
                        (database_name,),
                    ).fetchall()
                    existing_registrations = {row[0]: row[1] for row in rows}
            except Exception:  # noqa: BLE001
                existing_registrations = {}
        if verbose:
            print("\n🔍 DRY RUN MODE - No actual changes will be made\n")

    if verbose:
        mode = "all (forced)" if force else "new/changed only"
        print(f"\nRegistering {len(tables)} Iceberg tables from {database_name} ({mode}) using {workers} workers...")
        print("=" * 70)

    if dry_run:
        for table in tables:
            table_name = table["name"]
            metadata_location = table["metadata_location"]
            view_name = f"glue__{database_name}__{table_name}"

            outcome = _classify_registration(existing_registrations.get(view_name), metadata_location)
            if not force and outcome == "refreshed":
                results["skipped"] += 1
                if verbose:
                    print(f"  ⊘ {view_name} (unchanged)")
                continue

            escaped_location = metadata_location.replace("'", "''")
            quoted_view_name = '"' + view_name.replace('"', '""') + '"'
            create_view_sql = (
                f"CREATE OR REPLACE VIEW {quoted_view_name} AS\nSELECT * FROM iceberg_scan('{escaped_location}')"
            )
            if verbose:
                print(f"\n-- [{outcome.upper()}] {table_name}")
                print(create_view_sql)
            results["success"] += 1
            results[outcome] += 1
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _register_single_table,
                    table,
                    database_name,
                    duckdb_path,
                    existing_registrations,
                    force,
                ): table
                for table in tables
            }

            for future in as_completed(futures):
                status, view_name, extra_info = future.result()

                if status == "success":
                    results["success"] += 1
                    if extra_info == "new":
                        results["new"] += 1
                        if verbose:
                            print(f"  + {view_name} (new)")
                    elif extra_info == "updated":
                        results["updated"] += 1
                        if verbose:
                            print(f"  ↻ {view_name} (updated — Glue pointer moved)")
                    elif extra_info == "refreshed":
                        results["refreshed"] += 1
                        if verbose:
                            print(f"  ✓ {view_name} (re-registered, unchanged)")
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

                processed = results["success"] + results["error"] + results["skipped"]
                if not verbose and processed > 0 and processed % 50 == 0:
                    print(f"  Processed {processed}/{len(tables)} tables...")

    return results


def _show_registry(duckdb_path: Path) -> None:
    """Print currently registered Glue sources from DuckDB."""
    if not duckdb_path.exists():
        print(f"No DuckDB database found at: {duckdb_path}")
        return

    conn = duckdb.connect(str(duckdb_path))
    try:
        result = conn.execute("""
            SELECT view_name, glue_database, glue_table, registered_at
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
            view_name, glue_db, _glue_table, registered = row
            print(f"{view_name:<60} {glue_db:<30} {registered!s}")
        print("=" * 100)

        databases = sorted({row[1] for row in result})
        newest_phrase, is_stale = _describe_registry_age(_registry_last_refreshed(duckdb_path, databases))
        print(f"  Last refreshed: {newest_phrase}")
        if is_stale:
            print(
                f"  ⚠ More than {_stale_threshold_phrase()} old. Run `ol-dbt local register --all-layers` "
                "before trusting a dev_local build — stale views fail as S3 404s on the pinned metadata."
            )
    except Exception as e:  # noqa: BLE001
        print(f"Error reading registry: {e}")
    finally:
        conn.close()


def _cleanup_local_tables(duckdb_path: Path, dry_run: bool = False, yes: bool = False) -> dict[str, int]:
    """Drop locally built dbt tables from DuckDB, preserving Glue views."""
    results: dict[str, int] = {"tables_dropped": 0, "views_kept": 0}

    if not duckdb_path.exists():
        print(f"❌ Database not found: {duckdb_path}")
        return results

    conn = duckdb.connect(str(duckdb_path), read_only=dry_run)
    try:
        all_objects = conn.execute("""
            SELECT table_schema, table_name,
                CASE WHEN table_type = 'BASE TABLE' THEN 'table' ELSE 'view' END
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """).fetchall()

        local_tables = []
        glue_views = []
        for schema, name, obj_type in all_objects:
            if name.startswith("glue__") or name == "_glue_source_registry":
                glue_views.append((schema, name))
            elif obj_type == "table":
                local_tables.append((schema, name))

        results["views_kept"] = len(glue_views)

        if not local_tables:
            print("\n✓ No local tables found. All Glue views are preserved.")
            print(f"  {len(glue_views)} Glue views registered")
            return results

        print(f"\n{'📊 Local Tables to Drop' if not dry_run else '📊 Local Tables (DRY RUN)'}:")
        print("=" * 70)

        by_schema: dict[str, list[str]] = {}
        for schema, name in local_tables:
            by_schema.setdefault(schema, []).append(name)

        for schema in sorted(by_schema):
            print(f"\n{schema} ({len(by_schema[schema])} tables):")
            for name in sorted(by_schema[schema]):
                print(f"  • {name}")

        print("\n" + "=" * 70)
        print(f"Total: {len(local_tables)} tables to drop")
        print(f"Kept:  {len(glue_views)} Glue views (preserved)")
        print("=" * 70)

        if dry_run:
            print("\n🔍 DRY RUN - No changes made")
            return results

        if not yes:
            response = input(f"\n⚠️  Drop {len(local_tables)} local tables? (y/N): ").lower()
            if response != "y":
                print("Cancelled.")
                return results

        print("\nDropping local tables...")
        for schema, name in local_tables:
            try:
                conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{name}"')
                results["tables_dropped"] += 1
                print(f"  ✓ {schema}.{name}")
            except Exception as e:  # noqa: BLE001
                print(f"  ✗ {schema}.{name}: {e}")

        conn.execute("VACUUM")
        conn.execute("CHECKPOINT")

        print("\n✅ Cleanup complete!")
        print(f"  Dropped: {results['tables_dropped']} tables")
        print(f"  Kept:    {results['views_kept']} Glue views")
    except Exception as e:  # noqa: BLE001
        print(f"❌ Error during cleanup: {e}")
    finally:
        conn.close()

    return results


# ============================================================================
# Trino helpers
# ============================================================================


def _get_trino_connection(host: str, catalog: str, schema: str) -> trino.dbapi.Connection:
    """Create a Trino connection using OAuth2 browser authentication."""
    return trino.dbapi.connect(
        host=host,
        port=443,
        catalog=catalog,
        schema=schema,
        http_scheme="https",
        auth=OAuth2Authentication(),
    )


def _validate_schema_safety(schema_name: str, suffix: str, base_schema: str) -> bool:
    """Return True only if it is safe to drop this schema."""
    if schema_name in PROTECTED_SCHEMAS:
        return False
    if not suffix:
        return False
    suffixed_base = f"{base_schema}_{suffix}"
    return schema_name == suffixed_base or schema_name.startswith(f"{suffixed_base}_")


def _get_schemas_to_clean(conn: trino.dbapi.Connection, catalog: str, base_schema: str, suffix: str) -> list[str]:
    """Return schemas that match the given suffix pattern and are safe to clean."""
    cursor = conn.cursor()
    cursor.execute(f"SHOW SCHEMAS FROM {catalog}")
    all_schemas = [row[0] for row in cursor.fetchall()]

    suffixed_schema = f"{base_schema}_{suffix}"
    schemas_to_clean = []
    for schema in all_schemas:
        if schema == suffixed_schema or schema.startswith(f"{suffixed_schema}_"):
            if _validate_schema_safety(schema, suffix, base_schema):
                schemas_to_clean.append(schema)
            else:
                print(f"⚠️  Skipping protected schema: {schema}")
    return sorted(schemas_to_clean)


def _get_all_eligible_schemas(conn: trino.dbapi.Connection, catalog: str, base_schema: str) -> dict[str, list[str]]:
    """Return all non-protected schemas grouped by inferred suffix."""
    cursor = conn.cursor()
    cursor.execute(f"SHOW SCHEMAS FROM {catalog}")
    all_schemas = [row[0] for row in cursor.fetchall()]

    suffix_groups: dict[str, list[str]] = {}
    prefix = f"{base_schema}_"
    for schema in all_schemas:
        if schema in PROTECTED_SCHEMAS or not schema.startswith(prefix):
            continue
        remainder = schema[len(prefix) :]
        if not remainder:
            continue
        suffix = remainder.split("_", 1)[0]
        suffix_groups.setdefault(suffix, []).append(schema)

    for key in suffix_groups:
        suffix_groups[key] = sorted(suffix_groups[key])

    return suffix_groups


def _get_tables_and_views(conn: trino.dbapi.Connection, catalog: str, schema: str) -> dict[str, list[str]]:
    """Return tables and views present in a Trino schema."""
    cursor = conn.cursor()
    cursor.execute(f'SHOW TABLES FROM "{catalog}"."{schema}"')
    tables = [row[0] for row in cursor.fetchall()]

    try:
        safe_schema = schema.replace("'", "''")
        cursor.execute(
            f"SELECT table_name FROM {catalog}.information_schema.views WHERE table_schema = '{safe_schema}'"  # noqa: S608
        )
        views = [row[0] for row in cursor.fetchall()]
    except Exception:  # noqa: BLE001
        views = []

    return {"tables": tables, "views": views}


def _drop_objects(
    conn: trino.dbapi.Connection,
    catalog: str,
    schema: str,
    objects: dict[str, list[str]],
    dry_run: bool = True,
) -> dict[str, int]:
    """Drop tables and views from a Trino schema."""
    cursor = conn.cursor()
    counts: dict[str, int] = {"tables": 0, "views": 0}

    for view in objects["views"]:
        sql = f'DROP VIEW IF EXISTS "{catalog}"."{schema}"."{view}"'
        if dry_run:
            print(f"  [DRY RUN] Would execute: {sql}")
        else:
            try:
                cursor.execute(sql)
                counts["views"] += 1
                print(f"  ✓ Dropped view: {view}")
            except Exception as e:  # noqa: BLE001
                print(f"  ✗ Failed to drop view {view}: {e}")

    for table in objects["tables"]:
        sql = f'DROP TABLE IF EXISTS "{catalog}"."{schema}"."{table}"'
        if dry_run:
            print(f"  [DRY RUN] Would execute: {sql}")
        else:
            try:
                cursor.execute(sql)
                counts["tables"] += 1
                print(f"  ✓ Dropped table: {table}")
            except Exception as e:  # noqa: BLE001
                print(f"  ✗ Failed to drop table {table}: {e}")

    return counts


# ============================================================================
# CLI Sub-App
# ============================================================================

local_app = cyclopts.App(
    name="local",
    help="Local dbt development with DuckDB + Iceberg.",
)


@local_app.command
def register(
    database: Annotated[
        str | None,
        cyclopts.Parameter(help=f"Glue database name (default: {DEFAULT_GLUE_DATABASE})"),
    ] = None,
    all_layers: Annotated[
        bool,
        cyclopts.Parameter(help="Register all standard dbt layer databases"),
    ] = False,
    duckdb_path: Annotated[
        Path,
        cyclopts.Parameter(help=f"DuckDB database path (default: {DEFAULT_DUCKDB_PATH})"),
    ] = DEFAULT_DUCKDB_PATH,
    dry_run: Annotated[
        bool,
        cyclopts.Parameter(help="Show what would be done without making changes"),
    ] = False,
    quiet: Annotated[
        bool,
        cyclopts.Parameter(help="Suppress verbose output (show summary only)"),
    ] = False,
    force: Annotated[
        bool,
        cyclopts.Parameter(help="Re-issue every view, including unchanged ones (repairs local view state)"),
    ] = False,
    workers: Annotated[
        int,
        cyclopts.Parameter(help="Parallel worker threads for registration (default: 10, max: 20)"),
    ] = 10,
) -> None:
    """Register AWS Glue Iceberg tables as DuckDB views.

    Queries AWS Glue to discover Iceberg tables and creates DuckDB views referencing
    their S3 metadata.

    Every run re-reads Glue, so a plain run already picks up upstream changes: a
    table whose metadata location has moved is re-registered and reported as
    `updated`. `--force` does NOT find more changes than that — it only re-issues
    the CREATE VIEW for tables Glue reports as unchanged, which is a no-op unless
    the local view itself is damaged. Reach for it to repair local state, not to
    check for drift.

    What a plain run cannot do is tell you it is overdue: the views it created
    keep pointing at whatever metadata file was current when it last ran, and
    nothing prompts you to run it again. So both this command and `list-sources`
    report how long ago the registry was last refreshed, and warn once that is
    more than a day — measured pointer churn is roughly 90% of a layer per day.
    """
    workers = max(1, min(workers, 20))
    verbose = not quiet

    databases: list[str]
    if all_layers:
        databases = LAYER_DATABASES
        print(f"\n🔄 Registering ALL layers ({len(databases)} databases)")
    elif database:
        databases = [database]
        print(f"\n🔄 Registering single database: {database}")
    else:
        databases = [DEFAULT_GLUE_DATABASE]
        print(f"\n🔄 Registering default database: {DEFAULT_GLUE_DATABASE}")

    # Read before any registration writes, or it would report this run's own
    # timestamp back as the previous refresh.
    age_phrase, was_stale = _describe_registry_age(_registry_last_refreshed(duckdb_path, databases))
    print(f"   Registry for these databases last refreshed: {age_phrase}")
    if was_stale:
        # A dry run changes nothing, so it must not claim to have fixed the staleness.
        remedy = "Re-run without --dry-run to refresh them." if dry_run else "This run refreshes them."
        print(
            f"   ⚠ More than {_stale_threshold_phrase()} old — views may point at metadata files "
            f"production has since replaced (Iceberg 404s on build). {remedy}"
        )

    total: dict[str, int] = {"success": 0, "error": 0, "skipped": 0, "new": 0, "updated": 0, "refreshed": 0}

    for idx, db_name in enumerate(databases):
        print(f"\n{'=' * 70}")
        print(f"[{idx + 1}/{len(databases)}] Processing: {db_name}")
        print(f"{'=' * 70}")

        try:
            tables = _get_glue_tables(db_name)
        except Exception as e:  # noqa: BLE001
            print(f"  ✗ Error accessing database: {e}")
            continue
        if not dry_run:
            _record_registry_scan(duckdb_path, db_name)

        if not tables:
            print(f"  ⚠ No Iceberg tables found in {db_name}")
            continue

        results = _register_tables_in_duckdb(
            tables, db_name, duckdb_path, dry_run, verbose=verbose, force=force, workers=workers
        )
        for key in total:
            total[key] += results[key]

        if not verbose:
            print(f"  + {results['new']} new")
            print(f"  ↻ {results['updated']} updated")
            if results["refreshed"] > 0:
                print(f"  ✓ {results['refreshed']} re-registered (unchanged)")
            print(f"  ⊘ {results['skipped']} skipped")
            if results["error"] > 0:
                print(f"  ✗ {results['error']} errors")

    print("\n" + "=" * 70)
    print(f"{'SIMULATION' if dry_run else 'REGISTRATION'} COMPLETE")
    print("=" * 70)
    print(f"  Databases processed: {len(databases)}")
    print(f"  + New tables: {total['new']}")
    print(f"  ↻ Updated tables (Glue pointer moved): {total['updated']}")
    if force:
        print(f"  ✓ Re-registered unchanged: {total['refreshed']}")
    print(f"  ⊘ Skipped (unchanged): {total['skipped']}")
    print(f"  ✗ Errors: {total['error']}")
    print("=" * 70)

    # `refreshed` is deliberately excluded from "changed": under --force it counts
    # every unchanged table, and folding it in is what made a forced run announce
    # its whole catalog as freshly registered.
    changed = total["new"] + total["updated"]
    if not dry_run and changed > 0:
        print(f"\n✨ {changed} Iceberg tables registered/updated!")
        print(f"   DuckDB location: {duckdb_path}")
        if total["skipped"] > 0:
            print(f"   ⊘ {total['skipped']} unchanged in Glue, left as-is")
        print("\n   You can now run dbt with: --target dev_local")
    elif not dry_run and (total["skipped"] > 0 or total["refreshed"] > 0):
        seen = total["skipped"] + total["refreshed"]
        print(f"\n✓ All {seen} tables already match Glue — nothing upstream has moved.")
        print("   Your registry is up to date; --force would not surface anything further.")


@local_app.command
def list_sources(
    duckdb_path: Annotated[
        Path,
        cyclopts.Parameter(help=f"DuckDB database path (default: {DEFAULT_DUCKDB_PATH})"),
    ] = DEFAULT_DUCKDB_PATH,
) -> None:
    """List currently registered Glue sources in DuckDB."""
    _show_registry(duckdb_path)


@local_app.command
def snapshot(
    model: Annotated[str, cyclopts.Parameter(help="Model name to snapshot (its current build, via ref()).")],
    as_name: Annotated[
        str,
        cyclopts.Parameter(name=["--as"], help="Name for the snapshot table, created in the target's own schema."),
    ],
    target: Annotated[
        str,
        cyclopts.Parameter(name=["--target", "-t"], help="dbt target to read/write on (default: dev_local)."),
    ] = "dev_local",
    dbt_dir_path: Annotated[
        str | None,
        cyclopts.Parameter(
            name=["--dbt-dir", "-d"],
            help="Path to dbt project root (contains dbt_project.yml). Defaults to src/ol_dbt relative to repo root.",
        ),
    ] = None,
) -> None:
    """Copy a model's current build into a plain table under a new name.

    For before/after comparisons of the SAME model (a code change, not a
    migration to a differently-named model): build it, snapshot the build with
    this command, make your change and rebuild, then compare with
    `ol-dbt diff --old <as-name> --old-raw --new <model> --primary-key ...`.
    Neither this command nor that diff touches Glue/production, so any
    difference it finds reflects the code change alone, not upstream data
    drift from asynchronous production rebuilds.
    """
    from ol_dbt_cli.commands.diff import InvalidIdentifierError, _validate_identifiers  # noqa: PLC0415
    from ol_dbt_cli.commands.run import _find_dbt_dir  # noqa: PLC0415

    try:
        _validate_identifiers("model/snapshot name", [model, as_name])
    except InvalidIdentifierError as exc:
        print(f"Error: {exc}")
        sys.exit(1)

    try:
        dbt_dir = _find_dbt_dir(dbt_dir_path)
    except RuntimeError as exc:
        print(f"Error: {exc}")
        sys.exit(1)

    # _find_dbt_dir returns an explicit --dbt-dir as-is, without checking it's a
    # real dbt project -- a typo'd path would otherwise only surface later as a
    # confusing dbt-level error instead of a clear, early failure here.
    if not (dbt_dir / "dbt_project.yml").exists():
        print(
            f"Error: dbt project not found at {dbt_dir}. "
            "Pass --dbt-dir pointing at a directory containing dbt_project.yml."
        )
        sys.exit(1)

    # Rendered into a Jinja template compiled by dbt (not executed as a raw query
    # here), so the S608 heuristic is a false positive -- model/as_name were
    # already validated as plain identifiers above.
    # `}}` in an f-string is an escape for a single literal `}` (same as `{{` for
    # `{`) -- each Jinja `{{ ... }}` block needs `{{{{`/`}}}}` from an f-string.
    dst_expr = f"api.Relation.create(database=target.database, schema=target.schema, identifier='{as_name}')"
    src_expr = f"ref('{model}')"
    ctas_sql = f"create or replace table {{{{ {dst_expr} }}}} as select * from {{{{ {src_expr} }}}}"  # noqa: S608
    cmd = ["dbt", "show", "--inline", ctas_sql, "--target", target, "--output", "json", "--limit", "-1"]
    print(f"Running: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, cwd=str(dbt_dir), capture_output=True, text=True, check=True)  # noqa: S603
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or str(exc)).strip()
        print(f"Error: dbt show failed: {detail[-500:]}")
        sys.exit(1)
    except FileNotFoundError:
        print("Error: 'dbt' command not found; install dbt and ensure it is on PATH.")
        sys.exit(1)

    print(f"✅ Snapshotted '{model}' as '{as_name}' (target={target}).")
    print(f"   Compare with: ol-dbt diff --target {target} --old {as_name} --old-raw --new {model} --primary-key <key>")


@local_app.command
def cleanup_local(
    duckdb_path: Annotated[
        Path,
        cyclopts.Parameter(help=f"DuckDB database path (default: {DEFAULT_DUCKDB_PATH})"),
    ] = DEFAULT_DUCKDB_PATH,
    dry_run: Annotated[
        bool,
        cyclopts.Parameter(help="Show what would be dropped without dropping"),
    ] = False,
    yes: Annotated[
        bool,
        cyclopts.Parameter(help="Skip confirmation prompt"),
    ] = False,
) -> None:
    """Drop all locally built dbt tables from DuckDB to free disk space.

    Preserves all registered Glue views (prefixed with glue__) so you can
    immediately resume building models without re-registering sources.
    """
    print("\n🧹 Local DuckDB Cleanup")
    print("=" * 70)
    print(f"Database: {duckdb_path}")
    _cleanup_local_tables(duckdb_path, dry_run=dry_run, yes=yes)


@local_app.command
def test(
    duckdb_path: Annotated[
        Path,
        cyclopts.Parameter(help=f"DuckDB database path (default: {DEFAULT_DUCKDB_PATH})"),
    ] = DEFAULT_DUCKDB_PATH,
) -> None:
    """Test DuckDB + AWS Glue + Iceberg connectivity.

    Validates that DuckDB can connect to AWS Glue, read Iceberg table metadata,
    and query data directly from S3.
    """
    print("=" * 70)
    print("Testing DuckDB + AWS Glue + Iceberg Integration")
    print("=" * 70)

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nConnecting to DuckDB at: {duckdb_path}")
    conn = duckdb.connect(str(duckdb_path))

    try:
        print("\n1. Loading extensions...")
        for ext in ["httpfs", "aws", "iceberg"]:
            conn.execute(f"INSTALL {ext}")
            print(f"   ✓ Installed {ext}")
            conn.execute(f"LOAD {ext}")
            print(f"   ✓ Loaded {ext}")

        print("\n2. Configuring AWS...")
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        conn.execute(f"SET s3_region='{region}'")
        print(f"   ✓ Region: {region}")
        conn.execute("CALL load_aws_credentials()")
        print("   ✓ Loaded AWS credentials from chain")

        print("\n3. Testing Iceberg table read from Glue catalog...")
        glue = boto3.client("glue")

        table_name = "raw__mitlearn__app__postgres__users_user"
        database = "ol_warehouse_production_raw"

        print(f"   Getting metadata from Glue: {database}.{table_name}")
        response = glue.get_table(DatabaseName=database, Name=table_name)

        table_location = response["Table"]["StorageDescriptor"]["Location"]
        metadata_location = response["Table"]["Parameters"].get("metadata_location")

        print(f"   Table location: {table_location}")
        print(f"   Metadata location: {metadata_location}")

        iceberg_path = metadata_location if metadata_location else table_location
        query = f"SELECT * FROM iceberg_scan('{iceberg_path}') LIMIT 5"  # noqa: S608
        print(f"   Query: {query}")
        result = conn.execute(query).fetchall()

        if result:
            print(f"   ✓ Successfully read {len(result)} rows from Iceberg table!")
            columns = conn.execute(query).description
            col_names = [col[0] for col in columns]
            print(f"\n   Columns (first 5): {col_names[:5]}")
            schema_result = conn.execute(
                f"DESCRIBE SELECT * FROM iceberg_scan('{iceberg_path}')"  # noqa: S608
            ).fetchall()
            print(f"   Table has {len(schema_result)} columns")
        else:
            print("   ⚠ Query succeeded but no rows returned")

        print("\n" + "=" * 70)
        print("✓ SUCCESS: DuckDB can read Iceberg tables from S3!")
        print("\nNext steps:")
        print("  ol-dbt local register --all-layers")
        print("  cd src/ol_dbt && dbt run --target dev_local")
        print("=" * 70)

    except Exception as e:  # noqa: BLE001
        import traceback

        print(f"   ✗ Error: {e}")
        traceback.print_exc()
        print("\n" + "=" * 70)
        print("✗ FAILED: Could not read Iceberg tables")
        print("=" * 70)
        sys.exit(1)
    finally:
        conn.close()


@local_app.command
def cleanup(
    target: Annotated[
        Literal["dev_production", "dev_qa"],
        cyclopts.Parameter(help="Target environment"),
    ] = "dev_production",
    suffix: Annotated[
        str | None,
        cyclopts.Parameter(help="Schema suffix (default: DBT_SCHEMA_SUFFIX env var)"),
    ] = None,
    list_only: Annotated[
        bool,
        cyclopts.Parameter(help="List eligible schemas without scanning for objects"),
    ] = False,
    execute: Annotated[
        bool,
        cyclopts.Parameter(help="Actually delete objects (default: dry-run)"),
    ] = False,
    yes: Annotated[
        bool,
        cyclopts.Parameter(help="Skip confirmation prompt (USE WITH CAUTION)"),
    ] = False,
) -> None:
    r"""Clean up tables and views in namespaced Trino development schemas.

    Safely drops tables/views in developer schemas (identified by a suffix) while
    blocking deletion of production and QA base schemas.

    For most cases, prefer the dbt run-operation approach inside src/ol_dbt:

        uv run dbt run-operation trino__drop_schemas_by_prefixes \\
            --args "{prefixes: ['ol_warehouse_production_myname']}"

    Safety features:
    - Only operates on schemas containing your suffix
    - Dry-run by default; requires --execute to delete
    - Requires typed confirmation before deletion
    - Blocked by a hard-coded list of protected schemas
    """
    config = TARGET_CONFIGS[target]

    if list_only:
        print("=" * 70)
        print(f"Schema Enumeration — {config['description']}")
        print("=" * 70)
        print(f"Target:      {target}")
        print(f"Cluster:     {config['host']}")
        print(f"Catalog:     {config['catalog']}")
        print(f"Base Schema: {config['base_schema']}")
        print("=" * 70)

        print("\n📡 Connecting to Trino...")
        conn = _get_trino_connection(config["host"], config["catalog"], config["base_schema"])
        print("✓ Connected")

        print("\n🔍 Enumerating all schemas with suffixes...")
        suffix_groups = _get_all_eligible_schemas(conn, config["catalog"], config["base_schema"])

        if not suffix_groups:
            print("✓ No schemas found with suffixes")
            return

        print(f"\n📋 Found {len(suffix_groups)} suffix(es) with eligible schemas:")
        print("=" * 70)
        total_schemas = 0
        for user_suffix in sorted(suffix_groups):
            schemas = suffix_groups[user_suffix]
            total_schemas += len(schemas)
            print(f"\nSuffix: {user_suffix} ({len(schemas)} schema(s))")
            for s in schemas:
                print(f"  - {s}")

        print("\n" + "=" * 70)
        print(f"Total eligible schemas: {total_schemas}")
        print("=" * 70)
        print(f"\nTo clean a specific suffix:\n  ol-dbt local cleanup --suffix <suffix> --target {target} --execute")
        return

    suffix_val = suffix or os.getenv("DBT_SCHEMA_SUFFIX", "")
    if not suffix_val or len(suffix_val) < 2:
        print("ERROR: Schema suffix required (≥2 characters). Set DBT_SCHEMA_SUFFIX or use --suffix.")
        sys.exit(1)

    print("=" * 70)
    print(f"Schema Cleanup Tool — {config['description']}")
    print("=" * 70)
    print(f"Target: {target}  |  Suffix: {suffix_val}  |  Mode: {'EXECUTE' if execute else 'DRY RUN'}")
    print(f"Cluster: {config['host']}")
    print(f"Catalog: {config['catalog']}")
    print("=" * 70)

    print("\n📡 Connecting to Trino...")
    conn = _get_trino_connection(config["host"], config["catalog"], config["base_schema"])
    print("✓ Connected")

    print(f"\n🔍 Finding schemas with suffix '_{suffix_val}'...")
    schemas = _get_schemas_to_clean(conn, config["catalog"], config["base_schema"], suffix_val)

    if not schemas:
        print(f"✓ No schemas found with suffix '_{suffix_val}'")
        return

    print(f"\n📋 Found {len(schemas)} schema(s) to clean:")
    for s in schemas:
        print(f"  - {s}")

    print("\n📊 Scanning for tables and views...")
    total_tables = 0
    total_views = 0
    schema_objects: dict[str, dict[str, list[str]]] = {}

    for s in schemas:
        objects = _get_tables_and_views(conn, config["catalog"], s)
        schema_objects[s] = objects
        total_tables += len(objects["tables"])
        total_views += len(objects["views"])

        if objects["tables"] or objects["views"]:
            print(f"\n  {s}: {len(objects['tables'])} tables, {len(objects['views'])} views")
            if objects["tables"]:
                sample = ", ".join(objects["tables"][:5])
                extra = f" … +{len(objects['tables']) - 5}" if len(objects["tables"]) > 5 else ""
                print(f"      Tables: {sample}{extra}")

    print("\n" + "=" * 70)
    print(f"Total: {total_tables} tables, {total_views} views")
    print("=" * 70)

    if total_tables == 0 and total_views == 0:
        print("\n✓ No objects to delete")
        return

    if execute and not yes:
        print("\n⚠️  WARNING: This will permanently delete all objects!")
        response = input("\nType 'DELETE' to confirm: ")
        if response != "DELETE":
            print("Cancelled.")
            return

    print("\n🗑️  Cleaning schemas...")
    grand_total: dict[str, int] = {"tables": 0, "views": 0}

    for s in schemas:
        objects = schema_objects[s]
        if not objects["tables"] and not objects["views"]:
            continue
        print(f"\n{s}:")
        counts = _drop_objects(conn, config["catalog"], s, objects, dry_run=not execute)
        grand_total["tables"] += counts["tables"]
        grand_total["views"] += counts["views"]

    print("\n" + "=" * 70)
    if not execute:
        print("DRY RUN COMPLETE — No changes made")
        print(f"Would delete: {total_tables} tables, {total_views} views")
        print("\nTo execute, add --execute flag")
    else:
        print("CLEANUP COMPLETE")
        print(f"Deleted: {grand_total['tables']} tables, {grand_total['views']} views")
    print("=" * 70)


@local_app.command
def setup(
    duckdb_path: Annotated[
        Path,
        cyclopts.Parameter(help=f"DuckDB database path (default: {DEFAULT_DUCKDB_PATH})"),
    ] = DEFAULT_DUCKDB_PATH,
    layers: Annotated[
        Literal["all", "raw", "staging"],
        cyclopts.Parameter(help="Which Glue layers to register"),
    ] = "raw",
    recreate: Annotated[
        bool,
        cyclopts.Parameter(help="Recreate DuckDB database if it already exists"),
    ] = False,
    skip_tests: Annotated[
        bool,
        cyclopts.Parameter(help="Skip dbt compilation test after setup"),
    ] = False,
) -> None:
    """Set up the local dbt development environment with DuckDB + Iceberg.

    Performs a complete one-time setup:
      1. Checks prerequisites (AWS, uv, Glue access, S3 access)
      2. Creates the DuckDB directory and database file
      3. Installs DuckDB extensions (httpfs, aws, iceberg)
      4. Installs dbt dependencies (dbt deps)
      5. Registers Iceberg tables from AWS Glue
      6. Tests dbt compilation against the local target

    Prerequisites:
      - AWS credentials configured (~/.aws/credentials or environment variables)
      - uv package manager installed
      - IAM permissions: glue:GetDatabase, glue:GetTables, s3:GetObject
    """
    import json
    import shutil
    import subprocess

    print("\n" + "=" * 70)
    print("Local dbt Development Environment Setup")
    print("DuckDB + Iceberg Direct Read from S3")
    print("=" * 70 + "\n")

    # Step 1 — Prerequisites
    print("=" * 70)
    print("Step 1: Checking Prerequisites")
    print("=" * 70)

    all_good = True

    if shutil.which("uv"):
        try:
            ver = subprocess.run(["uv", "--version"], capture_output=True, text=True, check=False).stdout.strip()
            print(f"✓ uv is installed ({ver})")
        except Exception:  # noqa: BLE001
            print("✓ uv is installed")
    else:
        print("✗ uv is not installed. Install from: https://github.com/astral-sh/uv")
        all_good = False

    if shutil.which("aws"):
        result = subprocess.run(["aws", "sts", "get-caller-identity"], capture_output=True, text=True, check=False)  # noqa: S603
        if result.returncode == 0:
            identity = json.loads(result.stdout)
            account = identity.get("Account", "unknown")
            user = identity.get("Arn", "").split("/")[-1]
            print(f"✓ AWS credentials configured (Account: {account}, User: {user})")
        else:
            print("✗ AWS credentials not configured. Run 'aws configure'")
            all_good = False

        glue_check = subprocess.run(
            ["aws", "glue", "get-databases", "--max-results", "1"],  # noqa: S603
            capture_output=True,
            text=True,
            check=False,
        )
        if glue_check.returncode == 0:
            print("✓ AWS Glue access verified")
        else:
            print("⚠ Cannot access AWS Glue. Check IAM permissions (glue:GetDatabase, glue:GetTables)")

        s3_check = subprocess.run(
            ["aws", "s3", "ls", "s3://ol-data-lake-raw-production/"],  # noqa: S603
            capture_output=True,
            text=True,
            check=False,
        )
        if s3_check.returncode == 0:
            print("✓ S3 production bucket access verified")
        else:
            print("⚠ Cannot access s3://ol-data-lake-raw-production/ — Iceberg reads may fail")
    else:
        print("✗ AWS CLI is not installed. Install from: https://aws.amazon.com/cli/")
        all_good = False

    if not all_good:
        print("\n✗ Prerequisites check failed. Fix issues above and try again.")
        sys.exit(1)

    # Step 2 — DuckDB directory
    print("\n" + "=" * 70)
    print("Step 2: Setting Up DuckDB Directory")
    print("=" * 70)

    duckdb_dir = duckdb_path.parent
    duckdb_dir.mkdir(parents=True, exist_ok=True)
    (duckdb_dir / "temp").mkdir(parents=True, exist_ok=True)
    print(f"✓ Directory ready: {duckdb_dir}")

    # Step 3 — DuckDB init
    print("\n" + "=" * 70)
    print("Step 3: Initializing DuckDB Database")
    print("=" * 70)

    if duckdb_path.exists():
        size_mb = duckdb_path.stat().st_size / (1024 * 1024)
        print(f"ℹ DuckDB database already exists ({size_mb:.1f} MB)")

        if recreate:
            duckdb_path.unlink()
            print("ℹ Removed existing database (--recreate)")
        else:
            response = input("Recreate it? This deletes all local data [y/N]: ")
            if response.lower() in ("y", "yes"):
                duckdb_path.unlink()
                print("ℹ Removed existing database")
            else:
                print("ℹ Keeping existing database")

    if not duckdb_path.exists():
        conn = duckdb.connect(str(duckdb_path))
        for ext in ["httpfs", "aws", "iceberg"]:
            conn.execute(f"INSTALL {ext}")
            conn.execute(f"LOAD {ext}")
        conn.execute("CALL load_aws_credentials()")
        _ensure_registry_tables(conn)
        conn.close()
        print(f"✓ DuckDB database initialized: {duckdb_path}")

    # Step 4 — dbt deps
    print("\n" + "=" * 70)
    print("Step 4: Installing dbt Dependencies")
    print("=" * 70)

    project_root = get_repo_root()
    dbt_dir = project_root / "src" / "ol_dbt"

    if dbt_dir.exists():
        deps_result = subprocess.run(
            ["uv", "run", "dbt", "deps", "--quiet"],
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        if deps_result.returncode == 0:
            print("✓ dbt dependencies installed")
        else:
            print(f"⚠ dbt deps failed: {deps_result.stderr}")
    else:
        print(f"⚠ dbt directory not found at {dbt_dir}")

    # Step 5 — register sources
    print("\n" + "=" * 70)
    print("Step 5: Registering Iceberg Sources from AWS Glue")
    print("=" * 70)

    layer_map: dict[str, list[str]] = {
        "all": LAYER_DATABASES,
        "raw": [DEFAULT_GLUE_DATABASE],
        "staging": ["ol_warehouse_production_staging"],
    }
    databases = layer_map[layers]
    print(f"ℹ Layers: {layers} ({len(databases)} database(s))")

    total_reg: dict[str, int] = {"success": 0, "error": 0, "skipped": 0, "new": 0, "updated": 0, "refreshed": 0}
    for idx, db_name in enumerate(databases):
        print(f"\n[{idx + 1}/{len(databases)}] Processing: {db_name}")
        try:
            tables = _get_glue_tables(db_name)
        except Exception as e:  # noqa: BLE001
            print(f"  ✗ Error: {e}")
            continue
        _record_registry_scan(duckdb_path, db_name)

        if not tables:
            print(f"  ⚠ No Iceberg tables found in {db_name}")
            continue

        reg = _register_tables_in_duckdb(tables, db_name, duckdb_path, dry_run=False, verbose=False, force=True)
        for key in total_reg:
            total_reg[key] += reg[key]
        # setup() always forces, so most tables on a re-run land in `refreshed`,
        # not `new`. Reporting only `new` made a correct re-setup read as "0
        # tables registered".
        print(f"  ✓ {reg['success']} tables registered ({reg['new']} new, {reg['updated']} updated)")
        if reg["error"] > 0:
            print(f"  ✗ {reg['error']} errors")

    print(f"\n✓ Total: {total_reg['success']} Iceberg views registered")

    if total_reg["success"] > 0:
        conn = duckdb.connect(str(duckdb_path))
        by_layer = conn.execute("""
            SELECT glue_database, COUNT(*) as cnt
            FROM _glue_source_registry GROUP BY glue_database ORDER BY glue_database
        """).fetchall()
        conn.close()
        print("\n  Registered tables by layer:")
        for db, cnt in by_layer:
            layer_label = db.replace("ol_warehouse_production_", "")
            print(f"    • {layer_label}: {cnt:,} tables")

    # Step 6 — test
    if not skip_tests and dbt_dir.exists():
        print("\n" + "=" * 70)
        print("Step 6: Testing Setup")
        print("=" * 70)

        compile_result = subprocess.run(
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
        if compile_result.returncode == 0:
            print("✓ dbt compilation test passed")
        else:
            print("⚠ dbt compilation test failed (may need to register more layers)")

    # Summary
    total_size = sum(f.stat().st_size for f in duckdb_dir.rglob("*") if f.is_file())
    print("\n" + "=" * 70)
    print("Setup Complete! 🎉")
    print("=" * 70)
    print(f"\n📁 DuckDB database: {duckdb_path}")
    print(f"📊 Local storage: {total_size / (1024 * 1024):.1f} MB")
    print("\nNext steps:")
    print(f"  cd {dbt_dir}")
    print("  uv run dbt run --select stg__mitlearn__app__postgres__users_user --target dev_local")
    print("\nNote: Raw data is read directly from S3 — only transformed models are stored locally.")
