"""Roles command - manage dataset access permissions for governance roles."""

import sys
from pathlib import Path
from typing import Annotated, cast

import cyclopts
from cyclopts import Parameter

from ol_superset.lib.asset_index import WAREHOUSE_DATABASE_DIALECTS
from ol_superset.lib.role_management import (
    GLOBAL_ACCESS_PERMISSIONS,
    PermissionView,
    access_permission_changes,
    allowed_role_datasets,
    compute_desired_dataset_ids,
    find_governance_roles_json,
    get_all_roles,
    get_local_datasets,
    get_permission_views,
    get_role_dataset_permissions,
    get_superset_databases,
    get_superset_datasets,
    load_governance_roles,
    role_dataset_access_is_managed,
    set_role_datasource_permissions,
    uncovered_dataset_schemas,
    unmatched_allowed_databases,
    unmatched_allowed_datasets,
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
    schema_to_datasets: dict[str, list[dict[str, object]]] = {}
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
        allowed_datasets = role.get("allowed_datasets", [])

        matching = allowed_role_datasets(
            allowed_schemas, local_datasets, allowed_datasets
        )

        uncovered_schemas = [s for s in allowed_schemas if s not in schema_to_datasets]
        unmatched = unmatched_allowed_datasets(allowed_datasets, local_datasets)

        print(f"Role: {name}")
        print(f"  Allowed schemas: {allowed_schemas or '(none defined)'}")
        if allowed_datasets:
            print(f"  Allowed datasets: {allowed_datasets}")
        if "allowed_databases" in role:
            print(f"  Allowed databases (SQL Lab): {role['allowed_databases']}")
        print(f"  Accessible datasets: {len(matching)}")

        if uncovered_schemas:
            print(f"  ⚠️  Schemas with no local datasets: {uncovered_schemas}")
        if unmatched:
            print(f"  ⚠️  allowed_datasets with no local dataset: {unmatched}")

        if matching:
            for ds in sorted(matching, key=lambda d: str(d.get("table_name") or "")):
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
        str | None,
        Parameter(
            name=["--database", "-db"],
            help=(
                "Only check datasets from this database subdirectory "
                "(default: every warehouse database)"
            ),
        ),
    ] = None,
) -> None:
    """
    Check that all local dataset schemas are covered by at least one role.

    Validates consistency between local dataset assets and the governance
    policy. By default, only checks datasets in the warehouse database
    directories (Trino and StarRocks) since those are subject to warehouse
    schema governance.

    Exits with a non-zero status if uncovered schemas exist.

    Examples:
        Check dataset schema coverage (warehouse databases by default):
            ol-superset roles check

        Check one database:
            ol-superset roles check --database Trino

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

    if database is None:
        local_datasets = [
            ds
            for ds in local_datasets
            if ds.get("database") in WAREHOUSE_DATABASE_DIALECTS
        ]
    elif database:
        local_datasets = [ds for ds in local_datasets if ds.get("database") == database]

    # Schemas granted whole by at least one role
    covered_schemas: set[str] = set()
    for role in governance_roles:
        covered_schemas.update(role.get("allowed_schemas", []))

    # Schemas holding a local dataset that no role may read, whole or singly
    dataset_schemas: set[str] = {
        ds["schema"] for ds in local_datasets if ds.get("schema")
    }
    uncovered = uncovered_dataset_schemas(governance_roles, local_datasets)
    extra = covered_schemas - dataset_schemas

    print("=" * 60)
    print("Governance Role Schema Coverage Check")
    print("=" * 60)
    print(f"Governance policy: {gov_json}")
    if database is None:
        print(f"Database filter:   {sorted(WAREHOUSE_DATABASE_DIALECTS)}")
    elif database:
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
        for schema, count in sorted(uncovered.items()):
            print(f"  • {schema} ({count} dataset(s))")
        print()
        print(
            "Add the missing schema(s) to the appropriate role's "
            "'allowed_schemas', or the tables to its 'allowed_datasets', in "
            "ol_governance_roles.json"
        )
        sys.exit(1)

    print(
        f"✅ All {len(dataset_schemas)} dataset schema(s) are covered"
        " by governance roles."
    )
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

    Roles with 'all_datasource_access' (e.g. ol_data_engineer) still have
    their allowed_schemas processed to add individual datasource_access
    permissions as belt-and-suspenders coverage — permissions are only added,
    never revoked, for those roles.

    A role with 'allowed_databases' gets database_access (SQL Lab) on exactly
    those databases, by name. A global access permission (all_database_access,
    all_datasource_access, all_query_access) that a role holds but its
    definition no longer lists is revoked, since `flask fab import-roles` only
    adds permissions.

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

    if not dry_run and not skip_confirmation:
        prompt = (
            "⚠️  Sync governance role permissions to PRODUCTION?"
            if is_production
            else "Sync governance role permissions?"
        )
        require = "SYNC TO PRODUCTION" if is_production else None
        confirmed = confirm_action(prompt, require_exact=require)
        if not confirmed:
            print("Cancelled.")
            sys.exit(0)
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

    print("  Fetching databases from API...")
    api_databases = get_superset_databases(session, base_url)
    print(f"  Found {len(api_databases)} database(s) in {instance}")
    print()

    print("  Fetching data-access permissions from API...")
    access_perms = get_permission_views(
        session,
        base_url,
        frozenset({"datasource_access", "database_access"}) | GLOBAL_ACCESS_PERMISSIONS,
    )
    print(f"  Found {len(access_perms)} data-access permission(s)")
    print()

    # (permission, view menu) -> PermissionView id, for database and global
    # access grants.
    pv_to_perm_id: dict[PermissionView, int] = {
        (str(perm["permission_name"]), str(perm["view_menu_name"])): cast(
            int, perm["id"]
        )
        for perm in access_perms
    }

    # Build a lookup: dataset_id -> permission_id
    # Superset datasource permissions have view_menu_name like:
    #   "[database].[table_name](id:42)"
    dataset_id_to_perm_id: dict[int, int] = {}
    for perm in access_perms:
        if perm.get("permission_name") != "datasource_access":
            continue
        vm = perm.get("view_menu_name") or ""
        # Extract the trailing "(id:NNN)" suffix
        if vm.endswith(")") and "(" in vm:
            try:
                suffix = vm.rsplit("(", 1)[-1].rstrip(")")  # e.g. "id:42"
                if suffix.startswith("id:"):
                    suffix = suffix[3:]
                ds_id = int(suffix)
                dataset_id_to_perm_id[ds_id] = perm["id"]
            except ValueError:
                pass
    perm_id_to_ds_id = {v: k for k, v in dataset_id_to_perm_id.items()}

    # Process each governance role
    total_added = 0
    total_revoked = 0
    total_skipped = 0

    for gov_role in governance_roles:
        role_name = gov_role.get("name", "")
        allowed_schemas = gov_role.get("allowed_schemas", [])
        allowed_datasets = gov_role.get("allowed_datasets", [])

        print(f"  Role: {role_name}")

        role_id = role_name_to_id.get(role_name)
        if role_id is None:
            print(f"    ⚠️  Role not found in {instance}, skipping")
            total_skipped += 1
            print()
            continue

        # Check if role has all_datasource_access.
        # Even if it does, still apply individual datasource_access permissions
        # for its allowed_schemas as belt-and-suspenders — all_datasource_access
        # alone can be insufficient if the FAB PermissionView isn't properly
        # seeded or the Superset version doesn't honour it for all code paths.
        # For such roles, only add missing permissions; never revoke.
        has_all_access = any(
            p.get("view_menu", {}).get("name") == "all_datasource_access"
            or p.get("view_menu", {}).get("name") == "all_database_access"
            for p in gov_role.get("permissions", [])
        )

        current_perms = get_role_dataset_permissions(session, base_url, role_id)
        current_perm_ids: set[int] = {p["id"] for p in current_perms if p.get("id")}

        to_add_ds_ids: set[int] = set()
        to_revoke_ds_ids: set[int] = set()
        if role_dataset_access_is_managed(gov_role):
            desired_ds_ids = compute_desired_dataset_ids(
                allowed_schemas, api_datasets, allowed_datasets
            )
            unmatched = unmatched_allowed_datasets(allowed_datasets, api_datasets)
            if unmatched:
                print(
                    f"    ⚠️  allowed_datasets with no dataset in {instance}: "
                    f"{unmatched}"
                )

            # Map current perm IDs back to dataset IDs
            current_ds_ids: set[int] = {
                perm_id_to_ds_id[pid]
                for pid in current_perm_ids
                if pid in perm_id_to_ds_id
            }
            to_add_ds_ids = desired_ds_ids - current_ds_ids
            # Roles with all_datasource_access keep any extra permissions they
            # already hold — only add what's missing, never revoke.
            if not (skip_revoke or has_all_access):
                to_revoke_ds_ids = current_ds_ids - desired_ds_ids

            if has_all_access:
                print(
                    "    ℹ️  Role has all_datasource_access"
                    " — adding missing per-dataset permissions (no revoke)"
                )
            print(f"    Allowed schemas: {allowed_schemas}")
            print(f"    Allowed datasets: {allowed_datasets}")
            print(f"    Desired datasets: {len(desired_ds_ids)}")
            print(f"    Current datasets: {len(current_ds_ids)}")
        else:
            print("    ⏭️  Datasets unmanaged: no allowed_schemas or allowed_datasets")

        held: set[PermissionView] = {
            (str(p["permission_name"]), str(p["view_menu_name"])) for p in current_perms
        }
        add_access, revoke_access = access_permission_changes(
            gov_role, api_databases, held
        )
        if "allowed_databases" in gov_role:
            print(f"    Allowed databases: {gov_role['allowed_databases']}")
            unmatched_dbs = unmatched_allowed_databases(
                cast(list[str], gov_role["allowed_databases"]), api_databases
            )
            if unmatched_dbs:
                print(
                    f"    ⚠️  allowed_databases with no database in {instance}: "
                    f"{unmatched_dbs}"
                )
        if skip_revoke:
            revoke_access = set()

        print(f"    To add:   {len(to_add_ds_ids)} dataset(s), {sorted(add_access)}")
        print(
            f"    To revoke: {len(to_revoke_ds_ids)} dataset(s), "
            f"{sorted(revoke_access)}"
        )

        if not (to_add_ds_ids or to_revoke_ds_ids or add_access or revoke_access):
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

        # Apply permission changes via a single replace call.
        # The FAB API replaces all permissions atomically, so we compute the
        # full new set by fetching all current IDs (including non-datasource
        # ones) and applying the add/revoke delta.
        pvm_ids_to_add: set[int] = {
            dataset_id_to_perm_id[ds_id]
            for ds_id in to_add_ds_ids
            if ds_id in dataset_id_to_perm_id
        }
        pvm_ids_to_revoke: set[int] = {
            dataset_id_to_perm_id[ds_id]
            for ds_id in to_revoke_ds_ids
            if ds_id in dataset_id_to_perm_id
        }
        # A held permission is in the role's own listing, so its id is known.
        pvm_ids_to_revoke |= {
            cast(int, p["id"])
            for p in current_perms
            if (p["permission_name"], p["view_menu_name"]) in revoke_access
        }

        missing_perms = to_add_ds_ids - set(dataset_id_to_perm_id.keys())
        if missing_perms:
            print(
                f"    ⚠️  {len(missing_perms)} dataset(s) have no "
                "datasource permission registered in Superset yet "
                "(import them first)"
            )
        missing_access = {pv for pv in add_access if pv not in pv_to_perm_id}
        if missing_access:
            print(
                "    ⚠️  No permission registered in Superset for "
                f"{sorted(missing_access)}"
            )
        pvm_ids_to_add |= {
            pv_to_perm_id[pv] for pv in add_access if pv in pv_to_perm_id
        }

        if pvm_ids_to_add or pvm_ids_to_revoke:
            ok = set_role_datasource_permissions(
                session, base_url, role_id, pvm_ids_to_add, pvm_ids_to_revoke
            )
            if ok:
                if pvm_ids_to_add:
                    print(f"    ✅ Added {len(pvm_ids_to_add)} permission(s)")
                    total_added += len(pvm_ids_to_add)
                if pvm_ids_to_revoke:
                    print(f"    ✅ Revoked {len(pvm_ids_to_revoke)} permission(s)")
                    total_revoked += len(pvm_ids_to_revoke)

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
            print(f"Error: governance JSON not found: {p}", file=sys.stderr)
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
