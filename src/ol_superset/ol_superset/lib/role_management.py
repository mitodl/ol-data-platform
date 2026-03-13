"""Role management for Superset - sync dataset access from governance policy."""

import json
import sys
from pathlib import Path
from typing import cast

import requests
import yaml


def load_governance_roles(governance_json_path: Path) -> list[dict[str, object]]:
    """
    Load role definitions from the ol_governance_roles.json file.

    Each entry is expected to have 'name', 'permissions', and optionally
    'allowed_schemas' fields.

    Args:
        governance_json_path: Path to ol_governance_roles.json

    Returns:
        List of role definition dicts
    """
    if not governance_json_path.exists():
        print(
            f"Error: governance roles file not found: {governance_json_path}",
            file=sys.stderr,
        )
        return []

    with governance_json_path.open() as f:
        return json.load(f)


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
    Get current dataset-access permissions for a role.

    Filters to only datasource/schema-level permissions (not UI permissions).

    The GET /api/v1/security/roles/{id}/permissions/ endpoint returns flat dicts
    with keys "id", "permission_name", "view_menu_name".

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        role_id: Superset role ID

    Returns:
        List of permission dicts with 'id', 'permission_name', 'view_menu_name'
    """
    try:
        response = session.get(
            f"{base_url}/api/v1/security/roles/{role_id}/permissions/", timeout=30
        )
        response.raise_for_status()
    except Exception as e:
        print(
            f"  ❌ Error fetching permissions for role {role_id}: {e}",
            file=sys.stderr,
        )
        return []

    data = response.json()
    results = data.get("result", [])

    # Keep only datasource/schema-level permissions; the response uses flat keys.
    return [
        {
            "id": p.get("id"),
            "permission_name": p.get("permission_name"),
            "view_menu_name": p.get("view_menu_name"),
        }
        for p in results
        if "datasource" in (p.get("permission_name") or "").lower()
        or "schema" in (p.get("permission_name") or "").lower()
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
    try:
        response = session.get(
            f"{base_url}/api/v1/security/roles/{role_id}/permissions/", timeout=30
        )
        response.raise_for_status()
    except Exception as e:
        print(
            f"  ❌ Error fetching all permissions for role {role_id}: {e}",
            file=sys.stderr,
        )
        return []

    data = response.json()
    results = data.get("result", [])
    return [p["id"] for p in results if p.get("id") is not None]


def get_all_datasource_permissions(
    session: requests.Session, base_url: str
) -> list[dict[str, object]]:
    """
    Get all available datasource-level permissions from Superset.

    These are FAB PermissionView records where permission.name == "datasource_access"
    and view_menu.name has the form "[table_name](dataset_id)".

    Fetches all pages from /api/v1/security/permissions-resources/ without a
    server-side filter (the FAB rel_o_m filter requires a related-object integer
    ID, not a name string, making server-side name filtering impractical) and
    filters client-side for "datasource_access" permissions.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance

    Returns:
        List of permission dicts with 'id', 'permission_name', 'view_menu_name'
    """
    permissions: list[dict[str, object]] = []
    page = 0
    page_size = 100

    while True:
        params = {"q": json.dumps({"page": page, "page_size": page_size})}
        try:
            response = session.get(
                f"{base_url}/api/v1/security/permissions-resources/",
                params=params,
                timeout=30,
            )
            response.raise_for_status()
        except Exception as e:
            print(f"  ❌ Error fetching datasource permissions: {e}", file=sys.stderr)
            break

        data = response.json()
        results = data.get("result", [])
        if not results:
            break

        for p in results:
            # FAB stores this permission as "datasource_access" (underscore)
            perm_name = p.get("permission", {}).get("name", "")
            if perm_name == "datasource_access":
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


def compute_desired_dataset_ids(
    allowed_schemas: list[str],
    api_datasets: list[dict[str, object]],
) -> set[int]:
    """
    Compute the set of Superset dataset IDs that a role should have access to.

    Args:
        allowed_schemas: List of schema names the role is allowed to access
        api_datasets: All datasets from the Superset API

    Returns:
        Set of dataset IDs the role should have access to
    """
    allowed_set = set(allowed_schemas)
    return {
        cast(int, ds["id"])
        for ds in api_datasets
        if ds.get("schema") in allowed_set and ds.get("id") is not None
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
