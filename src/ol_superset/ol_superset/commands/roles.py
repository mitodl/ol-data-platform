"""Roles command - manage dataset access permissions for governance roles."""

import sys
from pathlib import Path
from typing import Annotated

import cyclopts
from cyclopts import Parameter

from ol_superset.lib.role_management import (
    add_role_permissions,
    compute_desired_dataset_ids,
    delete_role_permissions,
    find_governance_roles_json,
    get_all_datasource_permissions,
    get_all_roles,
    get_local_datasets,
    get_role_dataset_permissions,
    get_superset_datasets,
    load_governance_roles,
)
from ol_superset.lib.superset_api import (
    create_authenticated_session,
    get_instance_config,
)
from ol_superset.lib.utils import confirm_action, get_assets_dir

roles_app = cyclopts.App(
    name="roles",
    help="Manage dataset access permissions for governance roles.",
)


@roles_app.command(name="list")
def roles_list(
    assets_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--assets-dir", "-d"],
            help="Assets directory (default: assets/)",
        ),
    ] = None,
    governance_json_path: Annotated[
        str | None,
        Parameter(
            name=["--governance-json", "-g"],
            help="Path to ol_governance_roles.json (auto-detected if omitted)",
        ),
    ] = None,
) -> None:
    """
    List datasets accessible per role based on local assets and governance policy.

    Reads ol_governance_roles.json for allowed_schemas and scans local
    dataset YAMLs to show which datasets each role would have access to.
    No Superset API call is made.

    Examples:
        List dataset access from local assets:
            ol-superset roles list

        Use a specific governance JSON:
            ol-superset roles list --governance-json /path/to/ol_governance_roles.json
    """
    assets_dir = get_assets_dir(assets_dir_path)
    gov_json = _resolve_governance_json(governance_json_path, assets_dir)
    if gov_json is None:
        sys.exit(1)

    governance_roles = load_governance_roles(gov_json)
    if not governance_roles:
        sys.exit(1)

    local_datasets = get_local_datasets(assets_dir)

    print("=" * 60)
    print("Governance Role Dataset Access (local assets)")
    print("=" * 60)
    print(f"Governance policy: {gov_json}")
    print(f"Assets directory:  {assets_dir}")
    print(f"Total datasets:    {len(local_datasets)}")
    print()

    # Group local datasets by schema for quick lookup
    schema_to_datasets: dict[str, list[dict]] = {}
    for ds in local_datasets:
        schema = ds.get("schema") or ""
        schema_to_datasets.setdefault(schema, []).append(ds)

    all_schemas = sorted(schema_to_datasets.keys())
    print(f"Known schemas ({len(all_schemas)}):")
    for schema in all_schemas:
        print(f"  {schema}: {len(schema_to_datasets[schema])} dataset(s)")
    print()

    for role in governance_roles:
        name = role.get("name", "(unknown)")
        allowed_schemas = role.get("allowed_schemas", [])

        matching: list[dict] = []
        for schema in allowed_schemas:
            matching.extend(schema_to_datasets.get(schema, []))

        uncovered_schemas = [
            s for s in allowed_schemas if s not in schema_to_datasets
        ]

        print(f"Role: {name}")
        print(f"  Allowed schemas: {allowed_schemas or '(none defined)'}")
        print(f"  Accessible datasets: {len(matching)}")

        if uncovered_schemas:
            print(
                f"  ⚠️  Schemas with no local datasets: {uncovered_schemas}"
            )

        if matching:
            for ds in sorted(matching, key=lambda d: d.get("table_name") or ""):
                print(f"    • {ds['table_name']} [{ds['schema']}]")
        print()


@roles_app.command(name="check")
def roles_check(
    assets_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--assets-dir", "-d"],
            help="Assets directory (default: assets/)",
        ),
    ] = None,
    governance_json_path: Annotated[
        str | None,
        Parameter(
            name=["--governance-json", "-g"],
            help="Path to ol_governance_roles.json (auto-detected if omitted)",
        ),
    ] = None,
    database: Annotated[
        str,
        Parameter(
            name=["--database", "-db"],
            help="Only check datasets from this database subdirectory (default: Trino)",
        ),
    ] = "Trino",
) -> None:
    """
    Check that all local dataset schemas are covered by at least one role.

    Validates consistency between local dataset assets and the governance
    policy. By default, only checks datasets in the Trino database directory
    since those are subject to warehouse schema governance.

    Exits with a non-zero status if uncovered schemas exist.

    Examples:
        Check dataset schema coverage (Trino only by default):
            ol-superset roles check

        Check all databases:
            ol-superset roles check --database ""
    """
    assets_dir = get_assets_dir(assets_dir_path)
    gov_json = _resolve_governance_json(governance_json_path, assets_dir)
    if gov_json is None:
        sys.exit(1)

    governance_roles = load_governance_roles(gov_json)
    if not governance_roles:
        sys.exit(1)

    local_datasets = get_local_datasets(assets_dir)

    # Filter to the target database directory (default: Trino)
    if database:
        local_datasets = [
            ds for ds in local_datasets if ds.get("database") == database
        ]

    # Collect all schemas covered by at least one role
    covered_schemas: set[str] = set()
    for role in governance_roles:
        covered_schemas.update(role.get("allowed_schemas", []))

    # Find schemas present in local datasets but not covered by any role
    dataset_schemas: set[str] = {
        ds["schema"] for ds in local_datasets if ds.get("schema")
    }
    uncovered = dataset_schemas - covered_schemas
    extra = covered_schemas - dataset_schemas

    print("=" * 60)
    print("Governance Role Schema Coverage Check")
    print("=" * 60)
    print(f"Governance policy: {gov_json}")
    if database:
        print(f"Database filter:   {database}")
    print(f"Dataset schemas found:    {sorted(dataset_schemas)}")
    print(f"Schemas covered by roles: {sorted(covered_schemas)}")
    print()

    if extra:
        print(
            f"ℹ️  Schemas in governance policy but not in local assets "
            f"(may be expected): {sorted(extra)}"
        )
        print()

    if uncovered:
        print("❌ FAIL: Dataset schemas not covered by any governance role:")
        for schema in sorted(uncovered):
            count = sum(
                1 for ds in local_datasets if ds.get("schema") == schema
            )
            print(f"  • {schema} ({count} dataset(s))")
        print()
        print(
            "Add the missing schema(s) to the appropriate role's "
            "'allowed_schemas' in ol_governance_roles.json"
        )
        sys.exit(1)

    print(f"✅ All {len(dataset_schemas)} dataset schema(s) are covered by governance roles.")
    print()


@roles_app.command(name="sync")
def roles_sync(
    instance: Annotated[
        str,
        Parameter(help="Superset instance name (e.g., superset-production)"),
    ],
    assets_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--assets-dir", "-d"],
            help="Assets directory (default: assets/)",
        ),
    ] = None,
    governance_json_path: Annotated[
        str | None,
        Parameter(
            name=["--governance-json", "-g"],
            help="Path to ol_governance_roles.json (auto-detected if omitted)",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        Parameter(
            name=["--dry-run", "-n"],
            help="Show what would change without making API calls",
        ),
    ] = False,
    skip_confirmation: Annotated[
        bool,
        Parameter(name=["--yes", "-y"], help="Skip confirmation prompt"),
    ] = False,
    skip_revoke: Annotated[
        bool,
        Parameter(
            name=["--skip-revoke"],
            help="Only add missing permissions; do not revoke extra ones",
        ),
    ] = False,
) -> None:
    """
    Sync dataset access permissions for governance roles to a Superset instance.

    Reads ol_governance_roles.json for allowed_schemas per role, fetches all
    datasets from the target instance, then adds or removes datasource-level
    permissions so each role matches its governance policy.

    Roles with 'all_datasource_access' (e.g. ol_data_engineer) are skipped
    since they already have unrestricted access.

    Examples:
        Dry-run to preview changes:
            ol-superset roles sync superset-qa --dry-run

        Sync to QA (with confirmation):
            ol-superset roles sync superset-qa

        Sync to production (requires explicit confirmation):
            ol-superset roles sync superset-production

        Only add missing access (never revoke):
            ol-superset roles sync superset-qa --skip-revoke
    """
    assets_dir = get_assets_dir(assets_dir_path)
    gov_json = _resolve_governance_json(governance_json_path, assets_dir)
    if gov_json is None:
        sys.exit(1)

    governance_roles = load_governance_roles(gov_json)
    if not governance_roles:
        sys.exit(1)

    is_production = "production" in instance.lower()

    print("=" * 60)
    print("Syncing Governance Role Dataset Permissions")
    print("=" * 60)
    print(f"Instance:          {instance}")
    print(f"Governance policy: {gov_json}")
    if dry_run:
        print("Mode:              DRY RUN (no changes will be made)")
    elif skip_revoke:
        print("Mode:              ADD ONLY (existing extra permissions kept)")
    print()

    if is_production:
        print("⚠️  WARNING: Target is PRODUCTION environment")
        print()

    if dry_run:
        print("🔍 DRY RUN - fetching live data to compute diff...")
        print()

    # Authenticate
    config = get_instance_config(instance)
    if not config:
        sys.exit(1)

    base_url = config["url"]

    print("  🔐 Authenticating with Superset API...")
    session = create_authenticated_session(instance)
    if not session:
        print("  ❌ Authentication failed", file=sys.stderr)
        sys.exit(1)
    print(f"  ✅ Authenticated to {base_url}")
    print()

    # Fetch live data
    print("  Fetching datasets from API...")
    api_datasets = get_superset_datasets(session, base_url)
    print(f"  Found {len(api_datasets)} dataset(s) in {instance}")
    print()

    print("  Fetching roles from API...")
    api_roles = get_all_roles(session, base_url)
    role_name_to_id = {r["name"]: r["id"] for r in api_roles}
    print(f"  Found {len(api_roles)} role(s) in {instance}")
    print()

    print("  Fetching all datasource permissions from API...")
    all_ds_perms = get_all_datasource_permissions(session, base_url)
    print(f"  Found {len(all_ds_perms)} datasource permission(s)")
    print()

    # Build a lookup: dataset_id -> permission_id
    # Superset datasource permissions have view_menu_name like "[table_name](id)"
    dataset_id_to_perm_id: dict[int, int] = {}
    for perm in all_ds_perms:
        vm = perm.get("view_menu_name") or ""
        # Parse "(id)" from the end of view_menu_name
        if vm.endswith(")") and "(" in vm:
            try:
                ds_id = int(vm.rsplit("(", 1)[-1].rstrip(")"))
                dataset_id_to_perm_id[ds_id] = perm["id"]
            except ValueError:
                pass

    # Process each governance role
    total_added = 0
    total_revoked = 0
    total_skipped = 0

    for gov_role in governance_roles:
        role_name = gov_role.get("name", "")
        allowed_schemas = gov_role.get("allowed_schemas", [])

        print(f"  Role: {role_name}")

        # Check if role has all_datasource_access (skip - already has everything)
        has_all_access = any(
            p.get("view_menu", {}).get("name") == "all_datasource_access"
            or p.get("view_menu", {}).get("name") == "all_database_access"
            for p in gov_role.get("permissions", [])
        )
        if has_all_access:
            print("    ⏭️  Skipping: role has all_datasource_access")
            total_skipped += 1
            print()
            continue

        if not allowed_schemas:
            print("    ⏭️  Skipping: no allowed_schemas defined")
            total_skipped += 1
            print()
            continue

        role_id = role_name_to_id.get(role_name)
        if role_id is None:
            print(f"    ⚠️  Role not found in {instance}, skipping")
            total_skipped += 1
            print()
            continue

        # Compute desired dataset IDs based on governance schemas
        desired_ds_ids = compute_desired_dataset_ids(allowed_schemas, api_datasets)

        # Get current dataset permissions for this role
        current_perms = get_role_dataset_permissions(session, base_url, role_id)
        current_perm_ids: set[int] = {p["id"] for p in current_perms if p.get("id")}

        # Map current perm IDs back to dataset IDs
        perm_id_to_ds_id = {v: k for k, v in dataset_id_to_perm_id.items()}
        current_ds_ids: set[int] = {
            perm_id_to_ds_id[pid]
            for pid in current_perm_ids
            if pid in perm_id_to_ds_id
        }

        to_add_ds_ids = desired_ds_ids - current_ds_ids
        to_revoke_ds_ids = current_ds_ids - desired_ds_ids

        if skip_revoke:
            to_revoke_ds_ids = set()

        print(f"    Allowed schemas: {allowed_schemas}")
        print(f"    Desired datasets: {len(desired_ds_ids)}")
        print(f"    Current datasets: {len(current_ds_ids)}")
        print(f"    To add:   {len(to_add_ds_ids)}")
        print(f"    To revoke: {len(to_revoke_ds_ids)}")

        if not to_add_ds_ids and not to_revoke_ds_ids:
            print("    ✅ Already in sync")
            print()
            continue

        if dry_run:
            if to_add_ds_ids:
                names = [
                    ds.get("table_name")
                    for ds in api_datasets
                    if ds.get("id") in to_add_ds_ids
                ]
                print(f"    Would ADD access to: {sorted(names)}")
            if to_revoke_ds_ids:
                names = [
                    ds.get("table_name")
                    for ds in api_datasets
                    if ds.get("id") in to_revoke_ds_ids
                ]
                print(f"    Would REVOKE access to: {sorted(names)}")
            print()
            continue

        # Add permissions
        if to_add_ds_ids:
            perm_ids_to_add = [
                dataset_id_to_perm_id[ds_id]
                for ds_id in to_add_ds_ids
                if ds_id in dataset_id_to_perm_id
            ]
            missing_perms = to_add_ds_ids - set(dataset_id_to_perm_id.keys())
            if missing_perms:
                print(
                    f"    ⚠️  {len(missing_perms)} dataset(s) have no "
                    "datasource permission registered in Superset yet "
                    "(import them first)"
                )

            if perm_ids_to_add:
                ok = add_role_permissions(session, base_url, role_id, perm_ids_to_add)
                if ok:
                    print(f"    ✅ Added {len(perm_ids_to_add)} permission(s)")
                    total_added += len(perm_ids_to_add)

        # Revoke permissions
        if to_revoke_ds_ids:
            perm_ids_to_revoke = [
                dataset_id_to_perm_id[ds_id]
                for ds_id in to_revoke_ds_ids
                if ds_id in dataset_id_to_perm_id
            ]
            if perm_ids_to_revoke:
                ok = delete_role_permissions(
                    session, base_url, role_id, perm_ids_to_revoke
                )
                if ok:
                    print(f"    ✅ Revoked {len(perm_ids_to_revoke)} permission(s)")
                    total_revoked += len(perm_ids_to_revoke)

        print()

    print("=" * 60)
    if dry_run:
        print("DRY RUN complete — no changes were made.")
    else:
        print(
            f"Sync complete: {total_added} permission(s) added, "
            f"{total_revoked} revoked, {total_skipped} role(s) skipped."
        )
    print("=" * 60)
    print()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_governance_json(
    explicit_path: str | None, assets_dir: Path
) -> Path | None:
    """Resolve the governance roles JSON path from explicit arg or auto-detect."""
    if explicit_path:
        p = Path(explicit_path)
        if not p.exists():
            print(
                f"Error: governance JSON not found: {p}", file=sys.stderr
            )
            return None
        return p

    detected = find_governance_roles_json(assets_dir)
    if detected is None:
        print(
            "Error: Could not auto-detect ol_governance_roles.json. "
            "Pass --governance-json explicitly.",
            file=sys.stderr,
        )
        return None

    return detected
