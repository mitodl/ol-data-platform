"""Role management for Superset - sync dataset access from governance policy."""

import json
import sys
from pathlib import Path
from typing import cast

import requests
import yaml

# Permissions that open every database or dataset at once. Superset 6.1's
# can_access_database() accepts all_datasource_access as well as
# all_database_access, so either one reaches every database in SQL Lab.
GLOBAL_ACCESS_PERMISSIONS = frozenset(
    {"all_database_access", "all_datasource_access", "all_query_access"}
)

# A (permission name, view menu name) pair, e.g.
# ("database_access", "[Trino].(id:1)").
PermissionView = tuple[str, str]


def load_governance_roles(governance_json_path: Path) -> list[dict[str, object]]:
    """
    Load role definitions from the ol_governance_roles.json file.

    Each entry is expected to have 'name', 'permissions', and optionally
    'allowed_schemas', 'allowed_datasets' and 'allowed_databases' fields. A
    malformed 'allowed_datasets' or 'allowed_databases' is reported and nothing
    is returned, so no command acts on part of a policy.

    Args:
        governance_json_path: Path to ol_governance_roles.json

    Returns:
        List of role definition dicts, empty when the file is missing or invalid
    """
    if not governance_json_path.exists():
        print(
            f"Error: governance roles file not found: {governance_json_path}",
            file=sys.stderr,
        )
        return []

    with governance_json_path.open() as f:
        roles = json.load(f)
    try:
        for role in roles:
            parse_allowed_datasets(role.get("allowed_datasets", []))
            parse_allowed_databases(role.get("allowed_databases", []))
    except ValueError as e:
        print(f"Error: {governance_json_path}: {e}", file=sys.stderr)
        return []
    return roles


def get_local_datasets(assets_dir: Path) -> list[dict[str, object]]:
    """
    Read dataset metadata from local YAML files in assets/datasets/.

    Returns a list of dicts with keys: uuid, table_name, schema, catalog,
    database (the subdirectory name, e.g. "Trino"), path.

    Args:
        assets_dir: Path to assets directory (contains datasets/ subdirectory)

    Returns:
        List of dataset metadata dicts
    """
    datasets_dir = assets_dir / "datasets"
    if not datasets_dir.exists():
        return []

    datasets: list[dict[str, object]] = []
    for yaml_file in sorted(datasets_dir.rglob("*.yaml")):
        try:
            with yaml_file.open() as f:
                data = yaml.safe_load(f)
            if not isinstance(data, dict):
                continue
            # The immediate parent directory of the YAML is the database name
            database = yaml_file.parent.name
            datasets.append(
                {
                    "uuid": data.get("uuid"),
                    "table_name": data.get("table_name"),
                    "schema": data.get("schema"),
                    "catalog": data.get("catalog"),
                    "database": database,
                    "path": str(yaml_file),
                }
            )
        except Exception as e:
            print(f"  ⚠️  Error reading {yaml_file}: {e}", file=sys.stderr)

    return datasets


def get_superset_datasets(
    session: requests.Session, base_url: str
) -> list[dict[str, object]]:
    """
    Fetch all datasets from the Superset API.

    Returns a list of dicts with keys: id, table_name, schema, catalog, uuid.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance

    Returns:
        List of dataset dicts from API
    """
    datasets: list[dict[str, object]] = []
    page = 0
    page_size = 100

    while True:
        params = {
            "q": json.dumps(
                {
                    "page": page,
                    "page_size": page_size,
                    "columns": ["id", "table_name", "schema", "database", "uuid"],
                }
            )
        }
        try:
            response = session.get(
                f"{base_url}/api/v1/dataset/", params=params, timeout=30
            )
            response.raise_for_status()
        except Exception as e:
            print(f"  ❌ Error fetching datasets from API: {e}", file=sys.stderr)
            break

        data = response.json()
        results = data.get("result", [])
        if not results:
            break

        for ds in results:
            db = ds.get("database") or {}
            datasets.append(
                {
                    "id": ds.get("id"),
                    "table_name": ds.get("table_name"),
                    "schema": ds.get("schema"),
                    "catalog": db.get("backend"),
                    "uuid": ds.get("uuid"),
                }
            )

        count = data.get("count", 0)
        page += 1
        if page * page_size >= count:
            break

    return datasets


def get_all_roles(session: requests.Session, base_url: str) -> list[dict[str, object]]:
    """
    Fetch all roles from Superset, returning list of {id, name}.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance

    Returns:
        List of role dicts with 'id' and 'name'
    """
    roles: list[dict[str, object]] = []
    page = 0
    page_size = 100

    while True:
        params = {
            "q": json.dumps(
                {"page": page, "page_size": page_size, "columns": ["id", "name"]}
            )
        }
        try:
            response = session.get(
                f"{base_url}/api/v1/security/roles/", params=params, timeout=30
            )
            response.raise_for_status()
        except Exception as e:
            print(f"  ❌ Error fetching roles: {e}", file=sys.stderr)
            break

        data = response.json()
        results = data.get("result", [])
        if not results:
            break

        roles.extend({"id": r["id"], "name": r["name"]} for r in results)

        count = data.get("count", 0)
        page += 1
        if page * page_size >= count:
            break

    return roles


def get_role_dataset_permissions(
    session: requests.Session, base_url: str, role_id: int
) -> list[dict[str, object]]:
    """
    Get current data-access permissions for a role.

    Filters to datasource, schema and database-level permissions (not UI
    permissions), global access permissions included.

    The GET /api/v1/security/roles/{id}/permissions/ endpoint returns flat dicts
    with keys "id", "permission_name", "view_menu_name".

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        role_id: Superset role ID

    Returns:
        List of permission dicts with 'id', 'permission_name', 'view_menu_name'
    """
    # Raise rather than return nothing: an empty list reads as "holds no
    # access", which roles sync would act on.
    response = session.get(
        f"{base_url}/api/v1/security/roles/{role_id}/permissions/", timeout=30
    )
    response.raise_for_status()

    data = response.json()
    results = data.get("result", [])

    # Keep only data-access permissions; the response uses flat keys.
    return [
        {
            "id": p.get("id"),
            "permission_name": p.get("permission_name"),
            "view_menu_name": p.get("view_menu_name"),
        }
        for p in results
        if "datasource" in (p.get("permission_name") or "").lower()
        or "schema" in (p.get("permission_name") or "").lower()
        or p.get("permission_name") == "database_access"
        or p.get("permission_name") in GLOBAL_ACCESS_PERMISSIONS
    ]


def get_role_all_permission_ids(
    session: requests.Session, base_url: str, role_id: int
) -> list[int]:
    """
    Get all PermissionView IDs currently assigned to a role (all permission types).

    Used to preserve non-datasource permissions (UI permissions, etc.) when
    computing the full new permission set to POST back.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        role_id: Superset role ID

    Returns:
        List of all PermissionView IDs assigned to the role
    """
    # Raise rather than return nothing: the caller POSTs the full permission
    # list back, so an empty one would strip the role of everything else.
    response = session.get(
        f"{base_url}/api/v1/security/roles/{role_id}/permissions/", timeout=30
    )
    response.raise_for_status()

    data = response.json()
    results = data.get("result", [])
    return [p["id"] for p in results if p.get("id") is not None]


def get_permission_views(
    session: requests.Session, base_url: str, permission_names: frozenset[str]
) -> list[dict[str, object]]:
    """
    Get the FAB PermissionView records for the named permissions.

    E.g. "datasource_access" records have view_menu.name
    "[table_name](id:<dataset_id>)" and "database_access" records
    "[database_name].(id:<database_id>)".

    Fetches all pages from /api/v1/security/permissions-resources/ without a
    server-side filter (the FAB rel_o_m filter requires a related-object integer
    ID, not a name string, making server-side name filtering impractical) and
    filters client-side by permission name.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        permission_names: The permission names to keep

    Returns:
        List of permission dicts with 'id', 'permission_name', 'view_menu_name'

    Raises:
        requests.HTTPError: When a page can't be fetched
    """
    permissions: list[dict[str, object]] = []
    page = 0
    page_size = 100

    while True:
        params = {"q": json.dumps({"page": page, "page_size": page_size})}
        # Raise rather than return part of the list: roles sync would revoke a
        # global grant and then find no database_access permission to add.
        response = session.get(
            f"{base_url}/api/v1/security/permissions-resources/",
            params=params,
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        results = data.get("result", [])
        if not results:
            break

        for p in results:
            perm_name = p.get("permission", {}).get("name", "")
            if perm_name in permission_names:
                permissions.append(
                    {
                        "id": p.get("id"),
                        "permission_name": perm_name,
                        "view_menu_name": p.get("view_menu", {}).get("name"),
                    }
                )

        count = data.get("count", 0)
        page += 1
        if page * page_size >= count:
            break

    return permissions


def compute_schema_to_datasets(
    api_datasets: list[dict[str, object]],
) -> dict[str, list[dict[str, object]]]:
    """
    Group API datasets by schema name.

    Args:
        api_datasets: List of dataset dicts from get_superset_datasets()

    Returns:
        Dict mapping schema name -> list of dataset dicts
    """
    schema_map: dict[str, list[dict[str, object]]] = {}
    for ds in api_datasets:
        schema = str(ds.get("schema") or "")
        schema_map.setdefault(schema, []).append(ds)
    return schema_map


def parse_allowed_datasets(allowed_datasets: list[str]) -> set[tuple[str, str]]:
    """
    Split a role's ``allowed_datasets`` entries into (schema, table_name) pairs.

    An entry names one table, e.g.
    ``ol_warehouse_production_mart.marts__combined__orders``, and matches that
    table's dataset on every database (Trino and StarRocks each register their
    own).

    Args:
        allowed_datasets: The role's ``allowed_datasets`` entries

    Returns:
        The (schema, table_name) pairs

    Raises:
        ValueError: When the field is not a list of strings, or an entry is not
            ``<schema>.<table_name>``
    """
    # A JSON object would iterate as its keys and grant them.
    if not isinstance(allowed_datasets, list) or not all(
        isinstance(entry, str) for entry in allowed_datasets
    ):
        msg = "allowed_datasets must be a list of '<schema>.<table_name>' strings"
        raise ValueError(msg)
    pairs: set[tuple[str, str]] = set()
    for entry in allowed_datasets:
        schema, _, table_name = entry.partition(".")
        if not schema or not table_name or "." in table_name:
            msg = f"allowed_datasets entry {entry!r} is not '<schema>.<table_name>'"
            raise ValueError(msg)
        pairs.add((schema, table_name))
    return pairs


def parse_allowed_databases(allowed_databases: list[str]) -> set[str]:
    """
    Validate a role's ``allowed_databases`` entries, the Superset database names
    (e.g. ``"Trino"``) the role may query in SQL Lab.

    Names, not ids, because the ids differ between QA and production.

    Args:
        allowed_databases: The role's ``allowed_databases`` entries

    Returns:
        The database names

    Raises:
        ValueError: When the field is not a list of non-empty strings
    """
    if not isinstance(allowed_databases, list) or not all(
        isinstance(entry, str) and entry for entry in allowed_databases
    ):
        msg = "allowed_databases must be a list of Superset database names"
        raise ValueError(msg)
    return set(allowed_databases)


def database_access_view_menu(database: dict[str, object]) -> str:
    """
    Return the view menu name Superset gives a database's ``database_access``.

    Superset 6.1 names it ``Database.perm``: ``[<database_name>].(id:<id>)``.

    Args:
        database: A database from ``get_superset_databases``

    Returns:
        The view menu name, e.g. ``[Trino].(id:1)``
    """
    return f"[{database['database_name']}].(id:{database['id']})"


def unmatched_allowed_databases(
    allowed_databases: list[str], api_databases: list[dict[str, object]]
) -> list[str]:
    """
    Return the ``allowed_databases`` entries that name no database.

    Args:
        allowed_databases: The role's ``allowed_databases`` entries
        api_databases: Databases from ``get_superset_databases``

    Returns:
        The unmatched names, sorted
    """
    present = {db["database_name"] for db in api_databases}
    return sorted(parse_allowed_databases(allowed_databases) - present)


def access_permission_changes(
    gov_role: dict[str, object],
    api_databases: list[dict[str, object]],
    held: set[PermissionView],
) -> tuple[set[PermissionView], set[PermissionView]]:
    """
    Compute the database-level permissions to add to and revoke from a role.

    ``flask fab import-roles`` only ever adds permissions, so taking a global
    access permission out of ol_governance_roles.json leaves it on the live
    role. Any global access permission the role holds but its definition no
    longer lists is revoked here.

    A role with an ``allowed_databases`` key gets ``database_access`` on exactly
    those databases; its other ``database_access`` grants are revoked, so an
    empty list revokes them all.

    Args:
        gov_role: A role definition from ``load_governance_roles``
        api_databases: Databases from ``get_superset_databases``
        held: The (permission, view menu) pairs the live role holds

    Returns:
        The pairs to add and the pairs to revoke
    """
    listed: set[PermissionView] = {
        (p["permission"]["name"], p["view_menu"]["name"])
        for p in cast(list[dict[str, dict[str, str]]], gov_role.get("permissions", []))
    }
    to_revoke = {
        pv for pv in held if pv[0] in GLOBAL_ACCESS_PERMISSIONS and pv not in listed
    }
    to_add: set[PermissionView] = set()
    if "allowed_databases" in gov_role:
        allowed = parse_allowed_databases(
            cast(list[str], gov_role["allowed_databases"])
        )
        desired = {
            ("database_access", database_access_view_menu(db))
            for db in api_databases
            if db["database_name"] in allowed
        }
        current = {pv for pv in held if pv[0] == "database_access"}
        to_add = desired - current
        to_revoke |= current - desired
    return to_add, to_revoke


def unmatched_allowed_datasets(
    allowed_datasets: list[str],
    datasets: list[dict[str, object]],
) -> list[str]:
    """
    Return the ``allowed_datasets`` entries that match none of ``datasets``.

    Args:
        allowed_datasets: The role's ``allowed_datasets`` entries
        datasets: Datasets from the Superset API or the local assets

    Returns:
        The unmatched entries, sorted
    """
    present = {(ds.get("schema"), ds.get("table_name")) for ds in datasets}
    return sorted(
        f"{schema}.{table_name}"
        for schema, table_name in parse_allowed_datasets(allowed_datasets)
        if (schema, table_name) not in present
    )


def allowed_role_datasets(
    allowed_schemas: list[str],
    datasets: list[dict[str, object]],
    allowed_datasets: list[str],
) -> list[dict[str, object]]:
    """
    Return the datasets a role may read: its whole schemas and its single tables.

    Args:
        allowed_schemas: List of schema names the role is allowed to access
        datasets: Datasets from the Superset API or the local assets
        allowed_datasets: ``<schema>.<table_name>`` entries the role may read
            outside its allowed schemas

    Returns:
        The matching datasets, in their original order
    """
    allowed_set = set(allowed_schemas)
    allowed_tables = parse_allowed_datasets(allowed_datasets)
    return [
        ds
        for ds in datasets
        if ds.get("schema") in allowed_set
        or (ds.get("schema"), ds.get("table_name")) in allowed_tables
    ]


def role_dataset_access_is_managed(role: dict[str, object]) -> bool:
    """
    Say whether ``roles sync`` reconciles a role's dataset permissions.

    A role with allowed schemas is managed, and so is one with an
    ``allowed_datasets`` key, even an empty one: emptying the list must revoke
    what it granted rather than leave the role unmanaged with its old grants.

    Args:
        role: A role definition from ``load_governance_roles``

    Returns:
        True when the role's dataset permissions should be synced
    """
    return bool(role.get("allowed_schemas")) or "allowed_datasets" in role


def uncovered_dataset_schemas(
    governance_roles: list[dict[str, object]],
    datasets: list[dict[str, object]],
) -> dict[str, int]:
    """
    Count, per schema, the datasets no governance role may read.

    A dataset is covered by a role's ``allowed_schemas`` or by an
    ``allowed_datasets`` entry naming it.

    Args:
        governance_roles: Role definitions from ``load_governance_roles``
        datasets: Datasets from the Superset API or the local assets

    Returns:
        Schema name -> number of uncovered datasets in it
    """
    covered: set[int] = set()
    for role in governance_roles:
        covered.update(
            id(ds)
            for ds in allowed_role_datasets(
                cast(list[str], role.get("allowed_schemas", [])),
                datasets,
                cast(list[str], role.get("allowed_datasets", [])),
            )
        )
    uncovered: dict[str, int] = {}
    for ds in datasets:
        schema = ds.get("schema")
        if schema and id(ds) not in covered:
            uncovered[str(schema)] = uncovered.get(str(schema), 0) + 1
    return uncovered


def compute_desired_dataset_ids(
    allowed_schemas: list[str],
    api_datasets: list[dict[str, object]],
    allowed_datasets: list[str],
) -> set[int]:
    """
    Compute the set of Superset dataset IDs that a role should have access to.

    Args:
        allowed_schemas: List of schema names the role is allowed to access
        api_datasets: All datasets from the Superset API
        allowed_datasets: ``<schema>.<table_name>`` entries the role may read
            outside its allowed schemas

    Returns:
        Set of dataset IDs the role should have access to
    """
    return {
        cast(int, ds["id"])
        for ds in allowed_role_datasets(allowed_schemas, api_datasets, allowed_datasets)
        if ds.get("id") is not None
    }


def set_role_datasource_permissions(
    session: requests.Session,
    base_url: str,
    role_id: int,
    pvm_ids_to_add: set[int],
    pvm_ids_to_revoke: set[int],
) -> bool:
    """
    Update datasource permissions for a Superset role.

    The FAB RoleApi POST /roles/{id}/permissions endpoint **replaces** the
    entire permission list, so we must:
      1. Fetch all current PermissionView IDs for the role (including
         non-datasource permissions such as UI permissions).
      2. Compute the new set: (all_current - to_revoke) | to_add
      3. POST the full new set in one request.

    The endpoint is at /permissions (no trailing slash); the trailing-slash
    variant is GET-only.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        role_id: Role ID to update
        pvm_ids_to_add: PermissionView IDs to add to the role
        pvm_ids_to_revoke: PermissionView IDs to remove from the role

    Returns:
        True if successful
    """
    all_current_ids = get_role_all_permission_ids(session, base_url, role_id)
    new_ids = (set(all_current_ids) - pvm_ids_to_revoke) | pvm_ids_to_add

    try:
        response = session.post(
            f"{base_url}/api/v1/security/roles/{role_id}/permissions",
            json={"permission_view_menu_ids": sorted(new_ids)},
            timeout=30,
        )
        response.raise_for_status()
        return True
    except requests.exceptions.HTTPError as e:
        print(
            f"  ❌ HTTP error updating permissions for role {role_id}: {e}",
            file=sys.stderr,
        )
        if e.response is not None:
            try:
                print(f"      Details: {e.response.json()}", file=sys.stderr)
            except Exception:  # noqa: S110
                pass
        return False
    except Exception as e:
        print(
            f"  ❌ Error updating permissions for role {role_id}: {e}",
            file=sys.stderr,
        )
        return False


def find_governance_roles_json(start_dir: Path) -> Path | None:
    """
    Search upward from start_dir for ol_governance_roles.json.

    Falls back to the well-known path relative to the data_infra repo root.

    Args:
        start_dir: Directory to start searching from

    Returns:
        Path to the JSON file, or None if not found
    """
    # Walk up looking for ol-infrastructure relative to data_infra repo root
    candidate = start_dir
    for _ in range(10):
        json_path = (
            candidate
            / "ol-infrastructure"
            / "src"
            / "ol_infrastructure"
            / "applications"
            / "superset"
            / "ol_governance_roles.json"
        )
        if json_path.exists():
            return json_path
        if candidate.parent == candidate:
            break
        candidate = candidate.parent

    return None
