"""Superset API client for direct API interactions."""

import json
import sys
from pathlib import Path

import requests
import yaml
from preset_cli.auth.oauth_interactive import InteractiveOAuthAuth
from yarl import URL


class SupersetTokenAuth(requests.auth.AuthBase):
    """
    requests.AuthBase that integrates InteractiveOAuthAuth with automatic
    token refresh on 401 "Token has expired" responses.

    Tokens are fetched via InteractiveOAuthAuth.get_token() on every request,
    which transparently handles local-cache expiry and refresh-token rotation.
    When the Superset server rejects a token with 401 (e.g. because the server
    clock differs from the local cache expiry), the auth handler forces an
    immediate refresh-token exchange and retries the original request once —
    avoiding interactive browser prompts as long as the refresh token is valid.
    """

    def __init__(self, oauth_auth: InteractiveOAuthAuth) -> None:
        self._oauth_auth = oauth_auth

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        r.headers["Authorization"] = f"Bearer {self._oauth_auth.get_token()}"
        r.register_hook("response", self._handle_401)
        return r

    def _force_refresh(self) -> str:
        """Force a token refresh via the refresh-token grant, bypassing local cache."""
        try:
            # Directly invoke the refresh-token grant without re-checking local expiry.
            # _refresh_access_token() updates _access_token, _token_expires, and
            # re-writes the cache file, keeping subsequent calls efficient.
            return self._oauth_auth._refresh_access_token()  # noqa: SLF001
        except Exception:
            # Refresh token itself has expired — fall back to interactive browser flow.
            self._oauth_auth.clear_cached_tokens()
            return self._oauth_auth.get_token()

    def _handle_401(
        self, response: requests.Response, **kwargs: object
    ) -> requests.Response:
        if response.status_code != 401:
            return response

        # Only retry when Superset explicitly signals token expiry.
        try:
            msg = response.json().get("msg", "").lower()
        except Exception:
            msg = ""

        if "expired" not in msg:
            return response

        # Consume response body so the connection can be reused.
        _ = response.content
        response.raw.release_conn()

        new_token = self._force_refresh()

        # Retry the original request with the refreshed token.
        prep = response.request.copy()
        prep.headers["Authorization"] = f"Bearer {new_token}"
        # Remove this hook from the retry to prevent infinite recursion.
        prep.deregister_hook("response", self._handle_401)

        new_response = response.connection.send(prep, **kwargs)
        new_response.history.append(response)
        new_response.request = prep
        return new_response


def get_instance_config(instance_name: str) -> dict[str, str]:
    """
    Get Superset instance configuration from sup config.

    Args:
        instance_name: Name of the instance (e.g., 'superset-qa')

    Returns:
        Dict with config values
    """
    try:
        config_path = Path.home() / ".sup" / "config.yml"
        if not config_path.exists():
            print(f"Error: sup config not found at {config_path}", file=sys.stderr)
            return {}

        with config_path.open() as f:
            config = yaml.safe_load(f)

        instances = config.get("superset_instances", {})
        instance_config = instances.get(instance_name, {})

        if not instance_config:
            print(
                f"Error: Instance '{instance_name}' not found in sup config",
                file=sys.stderr,
            )
            return {}

        return instance_config
    except Exception as e:
        print(f"Error reading sup config: {e}", file=sys.stderr)
        return {}


def _create_oauth_auth(instance_name: str) -> InteractiveOAuthAuth | None:
    """
    Build an InteractiveOAuthAuth instance for the given Superset instance.

    Args:
        instance_name: Name of the instance (e.g., 'superset-qa')

    Returns:
        Configured InteractiveOAuthAuth object or None if configuration is missing
    """
    config = get_instance_config(instance_name)
    if not config:
        return None

    base_url = config.get("url")
    oauth_authorization_url = config.get("oauth_authorization_url")
    oauth_token_url = config.get("oauth_token_url")
    client_id = config.get("oauth_client_id", "superset-cli")
    scope = config.get("oauth_scope", "openid profile email")

    if not base_url or not oauth_authorization_url or not oauth_token_url:
        print(
            f"Error: Missing OAuth configuration for instance '{instance_name}'. "
            "Ensure url, oauth_authorization_url, and oauth_token_url are set "
            "in ~/.sup/config.yml.",
            file=sys.stderr,
        )
        return None

    try:
        return InteractiveOAuthAuth(
            base_url=URL(base_url),
            authorization_url=URL(oauth_authorization_url),
            token_url=URL(oauth_token_url),
            client_id=client_id,
            scope=scope,
        )
    except Exception as e:
        print(f"  ❌ Error initialising OAuth auth: {e}", file=sys.stderr)
        return None


def get_oauth_token_with_pkce(instance_name: str) -> str | None:
    """
    Get OAuth access token for a Superset instance, using cached tokens when available.

    Tokens are cached in ~/.sup/tokens/<hostname>.json and reused across
    invocations. The interactive browser flow (PKCE) is only triggered when no
    valid cached token exists and the refresh token has also expired, matching
    the behavior of the sup CLI.

    Args:
        instance_name: Name of the instance (e.g., 'superset-qa')

    Returns:
        Access token string or None if failed
    """
    auth = _create_oauth_auth(instance_name)
    if not auth:
        return None
    try:
        return auth.get_token()
    except Exception as e:
        print(f"  ❌ Error obtaining OAuth token: {e}", file=sys.stderr)
        return None


def create_authenticated_session(instance_name: str) -> requests.Session | None:
    """
    Create an authenticated requests session for Superset API.

    The session uses SupersetTokenAuth which calls InteractiveOAuthAuth.get_token()
    on every request. This means token refresh (via the OAuth refresh-token grant)
    happens automatically when the local cache detects expiry, and a forced refresh
    is triggered on any 401 "Token has expired" response from the server — without
    requiring browser interaction as long as the refresh token remains valid.

    Args:
        instance_name: Name of the instance (e.g., 'superset-qa')

    Returns:
        Authenticated requests Session or None if failed
    """
    oauth_auth = _create_oauth_auth(instance_name)
    if not oauth_auth:
        return None

    # Eagerly validate credentials so callers get an immediate failure on bad auth.
    try:
        oauth_auth.get_token()
    except Exception as e:
        print(f"  ❌ Error obtaining OAuth token: {e}", file=sys.stderr)
        return None

    session = requests.Session()
    session.auth = SupersetTokenAuth(oauth_auth)
    session.headers.update({"Content-Type": "application/json"})

    return session


def get_csrf_token(session: requests.Session, base_url: str) -> str | None:
    """
    Get CSRF token from Superset for API requests.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance

    Returns:
        CSRF token or None if failed
    """
    try:
        response = session.get(f"{base_url}/api/v1/security/csrf_token/", timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("result")
    except Exception as e:
        print(f"  ❌ Error getting CSRF token: {e}", file=sys.stderr)
        return None


def get_asset_id_by_uuid(
    session: requests.Session, base_url: str, asset_type: str, uuid: str
) -> int | None:
    """
    Get Superset asset ID (pk) from UUID.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        asset_type: Type of asset ('dashboard' or 'chart')
        uuid: UUID of the asset

    Returns:
        Integer asset ID or None if not found
    """
    try:
        if asset_type == "dashboard":
            endpoint = f"{base_url}/api/v1/dashboard/"
            # Use filters to find by UUID
            params = {
                "q": json.dumps(
                    {"filters": [{"col": "uuid", "opr": "eq", "value": uuid}]}
                )
            }
        elif asset_type == "chart":
            endpoint = f"{base_url}/api/v1/chart/"
            params = {
                "q": json.dumps(
                    {"filters": [{"col": "uuid", "opr": "eq", "value": uuid}]}
                )
            }
        else:
            return None

        response = session.get(endpoint, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        results = data.get("result", [])

        if results and len(results) > 0:
            return results[0].get("id")

        return None

    except Exception as e:
        print(
            f"  ⚠️  Error looking up {asset_type} UUID {uuid}: {e}",
            file=sys.stderr,
        )
        return None


def update_asset_external_management_flag(
    session: requests.Session,
    base_url: str,
    asset_type: str,
    asset_id: int,
    is_managed_externally: bool = False,
) -> bool:
    """
    Update the is_managed_externally flag for a Superset asset via API.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        asset_type: Type of asset ('dashboard' or 'chart')
        asset_id: ID of the asset (integer PK)
        is_managed_externally: Value to set (default: False for QA)

    Returns:
        True if successful, False otherwise
    """
    # Get CSRF token
    csrf_token = get_csrf_token(session, base_url)
    if not csrf_token:
        return False

    # Add CSRF token and Referer headers (required by Superset)
    session.headers.update(
        {
            "X-CSRFToken": csrf_token,
            "Referer": base_url,
        }
    )

    # API endpoints per Superset documentation
    if asset_type == "dashboard":
        endpoint = f"{base_url}/api/v1/dashboard/{asset_id}"
    elif asset_type == "chart":
        endpoint = f"{base_url}/api/v1/chart/{asset_id}"
    elif asset_type == "dataset":
        endpoint = f"{base_url}/api/v1/dataset/{asset_id}"
    else:
        print(
            f"  ❌ Unknown asset type '{asset_type}'. "
            "Must be 'dashboard', 'chart', or 'dataset'",
            file=sys.stderr,
        )
        return False

    # Prepare the update payload
    payload = {"is_managed_externally": is_managed_externally}

    try:
        response = session.put(endpoint, json=payload, timeout=30)
        response.raise_for_status()
        return True

    except requests.exceptions.HTTPError as e:
        print(
            f"  ⚠️  HTTP error updating {asset_type} {asset_id}: {e}",
            file=sys.stderr,
        )
        if e.response is not None:
            try:
                error_data = e.response.json()
                print(f"      Details: {error_data}", file=sys.stderr)
            except Exception:  # noqa: S110
                # Unable to parse error response
                pass
        return False
    except Exception as e:
        print(
            f"  ❌ Error updating {asset_type} {asset_id}: {e}",
            file=sys.stderr,
        )
        return False


def list_all_datasets_from_api(
    session: requests.Session,
    base_url: str,
    *,
    physical_only: bool = True,
) -> list[dict[str, str | int | None]]:
    """
    List all datasets from Superset API.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        physical_only: If True, return only physical (table) datasets, not virtual (SQL)

    Returns:
        List of dicts with 'id', 'uuid', 'table_name', 'schema', 'kind' keys
    """
    endpoint = f"{base_url}/api/v1/dataset/"
    datasets = []
    page = 0
    page_size = 100

    try:
        while True:
            params = {
                "q": json.dumps(
                    {
                        "page": page,
                        "page_size": page_size,
                    }
                )
            }

            response = session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            results = data.get("result", [])
            if not results:
                break

            for item in results:
                kind = item.get("kind")
                if physical_only and kind != "physical":
                    continue
                datasets.append(
                    {
                        "id": item.get("id"),
                        "uuid": item.get("uuid"),
                        "table_name": item.get("table_name"),
                        "schema": item.get("schema"),
                        "kind": kind,
                    }
                )

            page += 1

            if len(results) < page_size:
                break

        return datasets

    except Exception as e:
        print(f"  ❌ Error listing datasets: {e}", file=sys.stderr)
        return []


def get_dataset_id_by_uuid(
    session: requests.Session, base_url: str, uuid: str
) -> int | None:
    """
    Get Superset dataset ID (pk) from UUID.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        uuid: UUID of the dataset

    Returns:
        Integer dataset ID or None if not found
    """
    try:
        endpoint = f"{base_url}/api/v1/dataset/"
        params = {
            "q": json.dumps({"filters": [{"col": "uuid", "opr": "eq", "value": uuid}]})
        }

        response = session.get(endpoint, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        results = data.get("result", [])

        if results:
            return results[0].get("id")

        return None

    except Exception as e:
        print(
            f"  ⚠️  Error looking up dataset UUID {uuid}: {e}",
            file=sys.stderr,
        )
        return None


def refresh_dataset(session: requests.Session, base_url: str, dataset_id: int) -> bool:
    """
    Refresh a Superset physical dataset to pick up new/changed columns.

    Calls PUT /api/v1/dataset/{pk}/refresh which syncs the dataset schema
    with the underlying database table.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        dataset_id: Integer primary key of the dataset

    Returns:
        True if successful, False otherwise
    """
    csrf_token = get_csrf_token(session, base_url)
    if not csrf_token:
        return False

    session.headers.update(
        {
            "X-CSRFToken": csrf_token,
            "Referer": base_url,
        }
    )

    endpoint = f"{base_url}/api/v1/dataset/{dataset_id}/refresh"

    try:
        response = session.put(endpoint, timeout=30)
        response.raise_for_status()
        return True

    except requests.exceptions.HTTPError as e:
        print(
            f"  ⚠️  HTTP error refreshing dataset {dataset_id}: {e}",
            file=sys.stderr,
        )
        if e.response is not None:
            try:
                error_data = e.response.json()
                print(f"      Details: {error_data}", file=sys.stderr)
            except Exception:  # noqa: S110
                pass
        return False
    except Exception as e:
        print(
            f"  ❌ Error refreshing dataset {dataset_id}: {e}",
            file=sys.stderr,
        )
        return False


def get_dataset_uuids_from_directory(
    assets_dir: Path, *, physical_only: bool = True
) -> list[tuple[str, str | None]]:
    """
    Extract dataset UUIDs (and table names) from local dataset YAML files.

    Args:
        assets_dir: Path to assets directory
        physical_only: If True, skip virtual datasets (those with non-null sql field)

    Returns:
        List of (uuid, table_name) tuples
    """
    results: list[tuple[str, str | None]] = []

    dataset_dir = assets_dir / "datasets"
    if not dataset_dir.exists():
        return results

    yaml_files = list(dataset_dir.rglob("*.yaml")) + list(dataset_dir.rglob("*.yml"))

    for yaml_file in yaml_files:
        try:
            with yaml_file.open() as f:
                data = yaml.safe_load(f)

            if physical_only and data.get("sql"):
                continue

            uuid = data.get("uuid")
            if uuid:
                results.append((uuid, data.get("table_name")))

        except Exception as e:
            print(
                f"  ⚠️  Error reading {yaml_file}: {e}",
                file=sys.stderr,
            )

    return results


def update_dataset_physical_connection(
    session: requests.Session,
    base_url: str,
    dataset_id: int,
    table_name: str,
    schema: str,
) -> bool:
    """
    Update the physical table connection of a Superset dataset via the API.

    Superset's import API (/api/v1/assets/import/) does not update table_name
    or schema for existing physical datasets — it only updates metadata (columns,
    metrics, etc.). This function explicitly PATCHes the dataset's physical
    connection after an import to ensure the table reference is correct.

    Args:
        session: Authenticated requests session
        base_url: Base URL of Superset instance
        dataset_id: Integer primary key of the dataset
        table_name: Target table name to set
        schema: Target schema name to set

    Returns:
        True if successful, False otherwise
    """
    csrf_token = get_csrf_token(session, base_url)
    if not csrf_token:
        return False

    session.headers.update({"X-CSRFToken": csrf_token, "Referer": base_url})

    endpoint = f"{base_url}/api/v1/dataset/{dataset_id}"
    payload = {"table_name": table_name, "schema": schema}

    try:
        response = session.put(endpoint, json=payload, timeout=30)
        response.raise_for_status()
        return True

    except requests.exceptions.HTTPError as e:
        print(
            f"  ⚠️  HTTP error updating dataset {dataset_id} physical connection: {e}",
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
            f"  ❌ Error updating dataset {dataset_id} physical connection: {e}",
            file=sys.stderr,
        )
        return False


def sync_physical_dataset_connections(
    instance_name: str,
    assets_dir: Path,
) -> None:
    """
    Ensure physical dataset table_name and schema match the local YAML files.

    Superset's import API does not update the physical table connection of
    existing datasets. This function reads each physical dataset YAML, looks
    up the dataset in the target instance by UUID, and explicitly updates
    table_name and schema via the dataset PUT API if they differ.

    Args:
        instance_name: Target Superset instance name
        assets_dir: Path to local assets directory
    """
    session = create_authenticated_session(instance_name)
    if not session:
        print(
            f"  ❌ Could not authenticate to {instance_name}",
            file=sys.stderr,
        )
        return

    config = get_instance_config(instance_name)
    base_url = config.get("url", "").rstrip("/")

    dataset_dir = assets_dir / "datasets"
    if not dataset_dir.exists():
        return

    updated = 0
    skipped = 0
    failed = 0

    for yaml_file in sorted(dataset_dir.rglob("*.yaml")):
        try:
            with yaml_file.open() as f:
                data = yaml.safe_load(f)
        except Exception as e:
            print(f"  ⚠️  Could not read {yaml_file.name}: {e}", file=sys.stderr)
            continue

        # Skip virtual datasets (SQL-defined)
        if data.get("sql"):
            continue

        uuid = data.get("uuid")
        desired_table = data.get("table_name")
        desired_schema = data.get("schema")

        if not uuid or not desired_table:
            continue

        dataset_id = get_dataset_id_by_uuid(session, base_url, uuid)
        if dataset_id is None:
            # Dataset doesn't exist yet in target — import will create it correctly
            skipped += 1
            continue

        # Check current state in target
        try:
            resp = session.get(f"{base_url}/api/v1/dataset/{dataset_id}", timeout=10)
            resp.raise_for_status()
            current = resp.json().get("result", {})
            current_table = current.get("table_name")
            current_schema = current.get("schema")
        except Exception as e:
            print(f"  ⚠️  Could not fetch dataset {dataset_id}: {e}", file=sys.stderr)
            failed += 1
            continue

        if current_table == desired_table and current_schema == desired_schema:
            skipped += 1
            continue

        print(
            f"  Updating '{data.get('table_name', uuid)}': "
            f"{current_schema}.{current_table} → {desired_schema}.{desired_table}"
        )
        success = update_dataset_physical_connection(
            session, base_url, dataset_id, desired_table, desired_schema or ""
        )
        if success:
            updated += 1
        else:
            failed += 1

    print(
        f"  Physical connections: {updated} updated, {skipped} already correct, "
        f"{failed} failed"
    )


def get_asset_uuids_from_directory(assets_dir: Path, asset_type: str) -> list[str]:
    """
    Extract UUIDs from asset YAML files.

    Args:
        assets_dir: Path to assets directory
        asset_type: Type of assets ('dashboard' or 'chart')

    Returns:
        List of UUIDs found in YAML files
    """
    uuids: list[str] = []

    asset_dir = assets_dir / f"{asset_type}s"
    if not asset_dir.exists():
        return uuids

    # Get all YAML files
    yaml_files = list(asset_dir.rglob("*.yaml")) + list(asset_dir.rglob("*.yml"))

    for yaml_file in yaml_files:
        # Skip untitled dashboards (unpublished)
        if asset_type == "dashboard" and yaml_file.name.startswith("untitled_"):
            continue

        try:
            with yaml_file.open() as f:
                data = yaml.safe_load(f)

            uuid = data.get("uuid")
            if uuid:
                uuids.append(uuid)

        except Exception as e:
            print(
                f"  ⚠️  Error reading {yaml_file}: {e}",
                file=sys.stderr,
            )

    return uuids


def update_pushed_assets_external_flag(
    instance_name: str,
    assets_dir: Path,
    *,
    skip_confirmation: bool = False,
) -> None:
    """
    Update is_managed_externally flag for all pushed assets.

    This function should be called after sup push operations to QA
    to enable UI editing of the pushed assets.

    Args:
        instance_name: Name of the instance (e.g., 'superset-qa')
        assets_dir: Path to assets directory
        skip_confirmation: Skip user confirmation prompt
    """
    # Only run for QA instances
    if "qa" not in instance_name.lower():
        print(
            f"\n  ℹ️  Skipping external management flag update "
            f"(not a QA instance: {instance_name})"
        )
        return

    print("\n" + "=" * 50)
    print("Updating Asset Management Flags")
    print("=" * 50)
    print()
    print("Setting is_managed_externally=false to enable UI editing in QA...")
    print()

    # Get configuration
    config = get_instance_config(instance_name)
    if not config:
        print("  ❌ Could not get instance configuration", file=sys.stderr)
        return

    base_url = config["url"]

    # Create authenticated session
    print("  🔐 Authenticating with Superset API...")
    session = create_authenticated_session(instance_name)
    if not session:
        print("  ❌ Could not create authenticated session", file=sys.stderr)
        return

    print(f"  ✅ Authenticated to {base_url}")
    print()

    # Process dashboards and charts
    for asset_type in ["dashboard", "chart"]:
        print(f"  Processing {asset_type}s...")

        # Get UUIDs from YAML files
        uuids = get_asset_uuids_from_directory(assets_dir, asset_type)

        if not uuids:
            print(f"    No {asset_type}s found")
            continue

        print(f"    Found {len(uuids)} {asset_type}(s)")

        success_count = 0
        failed_count = 0

        for uuid in uuids:
            # Look up asset ID by UUID
            asset_id = get_asset_id_by_uuid(session, base_url, asset_type, uuid)

            if asset_id is None:
                print(f"    ⚠️  Could not find {asset_type} with UUID {uuid}")
                failed_count += 1
                continue

            # Update the flag
            success = update_asset_external_management_flag(
                session, base_url, asset_type, asset_id, is_managed_externally=False
            )

            if success:
                success_count += 1
            else:
                failed_count += 1

        print(f"    ✅ Updated {success_count} {asset_type}(s)")
        if failed_count > 0:
            print(f"    ⚠️  Failed to update {failed_count} {asset_type}(s)")

    # Process datasets
    print("  Processing datasets...")
    dataset_entries = get_dataset_uuids_from_directory(assets_dir, physical_only=False)

    if not dataset_entries:
        print("    No datasets found")
    else:
        print(f"    Found {len(dataset_entries)} dataset(s)")

        success_count = 0
        failed_count = 0

        for uuid, _table_name in dataset_entries:
            asset_id = get_dataset_id_by_uuid(session, base_url, uuid)

            if asset_id is None:
                print(f"    ⚠️  Could not find dataset with UUID {uuid}")
                failed_count += 1
                continue

            success = update_asset_external_management_flag(
                session, base_url, "dataset", asset_id, is_managed_externally=False
            )

            if success:
                success_count += 1
            else:
                failed_count += 1

        print(f"    ✅ Updated {success_count} dataset(s)")
        if failed_count > 0:
            print(f"    ⚠️  Failed to update {failed_count} dataset(s)")

    print()
    print("=" * 50)
    print(f"✅ Asset management flags updated for {instance_name}")
    print("=" * 50)
    print()


# ---------------------------------------------------------------------------
# Row-Level Security helpers
# ---------------------------------------------------------------------------


def list_roles(session: requests.Session, base_url: str) -> dict[str, int]:
    """Return {role_name: role_id} for all roles in the Superset instance."""
    result: dict[str, int] = {}
    page = 0
    page_size = 100
    while True:
        response = session.get(
            f"{base_url}/api/v1/security/roles/",
            params={"q": json.dumps({"page": page, "page_size": page_size})},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("result", []):
            result[item["name"]] = item["id"]
        if len(data.get("result", [])) < page_size:
            break
        page += 1
    return result


def list_datasets_name_to_id(
    session: requests.Session, base_url: str
) -> dict[str, int]:
    """Return dataset name→id mapping with bare-name collision detection.

    Each dataset is indexed under:
    - bare ``table_name``  (e.g. ``"instructor_module_report"``)
    - qualified ``schema.table_name``

    If two datasets share the same bare ``table_name`` across different schemas
    the bare key is removed and a warning is printed.  Policies must then use
    the fully-qualified ``schema.table_name`` form.
    """
    result: dict[str, int] = {}
    bare_collision: set[str] = set()
    page = 0
    page_size = 100
    while True:
        response = session.get(
            f"{base_url}/api/v1/dataset/",
            params={
                "q": json.dumps(
                    {
                        "page": page,
                        "page_size": page_size,
                        "columns": ["id", "table_name", "schema"],
                    }
                )
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("result", []):
            schema = item.get("schema") or ""
            table = item.get("table_name", "")
            dataset_id = int(item["id"])
            if table in bare_collision:
                pass  # already marked ambiguous
            elif table in result:
                bare_collision.add(table)
                del result[table]
                print(
                    f"  ⚠️  Multiple datasets share table_name '{table}'; "
                    "use schema-qualified name in RLS policies.",
                )
            else:
                result[table] = dataset_id
            if schema:
                result[f"{schema}.{table}"] = dataset_id
        if len(data.get("result", [])) < page_size:
            break
        page += 1
    return result


def list_rls_filters(session: requests.Session, base_url: str) -> dict[str, int]:
    """Return {filter_name: filter_id} for all existing RLS filters."""
    result: dict[str, int] = {}
    page = 0
    page_size = 100
    while True:
        response = session.get(
            f"{base_url}/api/v1/rowlevelsecurity/",
            params={"q": json.dumps({"page": page, "page_size": page_size})},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("result", []):
            result[item["name"]] = item["id"]
        if len(data.get("result", [])) < page_size:
            break
        page += 1
    return result


def apply_rls_filter(
    session: requests.Session,
    base_url: str,
    policy: dict[str, object],
    role_map: dict[str, int],
    dataset_map: dict[str, int],
    existing: dict[str, int],
    *,
    dry_run: bool = False,
) -> bool:
    """Create or update a single RLS filter from a policy definition.

    Returns ``True`` on success (or in dry-run mode), ``False`` on any error.
    Raises ``ValueError`` if required roles or datasets are missing so that
    partially-configured filters are never silently accepted.
    """
    name = str(policy["name"])
    roles: list[str] = list(policy["roles"])  # type: ignore[call-overload]
    tables: list[str] = list(policy["tables"])  # type: ignore[call-overload]

    missing_roles = [r for r in roles if r not in role_map]
    if missing_roles:
        msg = (
            f"Required roles not found in Superset for filter '{name}': "
            f"{', '.join(missing_roles)}"
        )
        raise ValueError(msg)

    missing_tables = [t for t in tables if t not in dataset_map]
    if missing_tables:
        msg = (
            f"Required datasets not found in Superset for filter '{name}': "
            f"{', '.join(missing_tables)}"
        )
        raise ValueError(msg)

    payload = {
        "name": name,
        "description": str(policy.get("description", "")),
        "filter_type": str(policy["filter_type"]),
        "clause": str(policy["clause"]),
        "group_key": str(policy.get("group_key", "")),
        "roles": [role_map[r] for r in roles],
        "tables": [dataset_map[t] for t in tables],
    }

    if dry_run:
        action = "update" if name in existing else "create"
        print(f"  [dry-run] Would {action} RLS filter '{name}'")
        return True

    csrf_token = get_csrf_token(session, base_url)
    if not csrf_token:
        print(f"  ❌ Could not get CSRF token for filter '{name}'")
        return False
    session.headers.update({"X-CSRFToken": csrf_token, "Referer": base_url})

    try:
        if name in existing:
            filter_id = existing[name]
            resp = session.put(
                f"{base_url}/api/v1/rowlevelsecurity/{filter_id}",
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            print(f"  ✅ Updated RLS filter '{name}' (id={filter_id})")
        else:
            resp = session.post(
                f"{base_url}/api/v1/rowlevelsecurity/",
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            new_id = resp.json().get("id", "?")
            print(f"  ✅ Created RLS filter '{name}' (id={new_id})")
        return True
    except requests.exceptions.HTTPError as exc:
        body = exc.response.text if exc.response is not None else "no response"
        print(f"  ❌ HTTP error applying filter '{name}': {exc} — {body}")
        return False
    except Exception as exc:
        print(f"  ❌ Error applying filter '{name}': {exc}")
        return False
