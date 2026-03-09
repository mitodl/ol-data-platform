"""Superset API client for direct API interactions."""

import hashlib
import json
import secrets
import sys
import webbrowser
from base64 import urlsafe_b64encode
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import parse_qs, urlencode, urlparse

import requests
import yaml


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


def get_oauth_token_with_pkce(instance_name: str) -> str | None:
    """
    Get OAuth access token using PKCE flow for Superset instance.

    Args:
        instance_name: Name of the instance (e.g., 'superset-qa')

    Returns:
        Access token string or None if failed
    """
    config = get_instance_config(instance_name)
    if not config:
        return None

    # PKCE parameters
    code_verifier = (
        urlsafe_b64encode(secrets.token_bytes(32)).decode("utf-8").rstrip("=")
    )
    code_challenge = (
        urlsafe_b64encode(hashlib.sha256(code_verifier.encode("utf-8")).digest())
        .decode("utf-8")
        .rstrip("=")
    )

    # OAuth parameters - use port 8080 to match sup CLI configuration
    redirect_uri = "http://localhost:8080/callback"
    state = secrets.token_urlsafe(16)

    auth_url = config["oauth_authorization_url"]
    token_url = config["oauth_token_url"]
    client_id = config["oauth_client_id"]
    scope = config.get("oauth_scope", "openid profile email")

    # Build authorization URL
    auth_params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "scope": scope,
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }

    authorization_url = f"{auth_url}?{urlencode(auth_params)}"

    # Store authorization code from callback
    auth_code: dict[str, str | None] = {"code": None, "error": None}

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            # Parse query parameters
            query = urlparse(self.path).query
            params = parse_qs(query)

            if "code" in params:
                auth_code["code"] = params["code"][0]
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h1>Authorization successful!</h1>"
                    b"<p>You can close this window and return to the "
                    b"terminal.</p></body></html>"
                )
            else:
                error = params.get("error", ["Unknown error"])[0]
                auth_code["error"] = error
                self.send_response(400)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h1>Authorization failed!</h1>"
                    b"<p>Error: " + error.encode() + b"</p></body></html>"
                )

        def log_message(self, format, *args):  # noqa: ARG002, A002
            # Suppress log messages
            pass

    # Start local server to receive callback - use port 8080 to match redirect_uri
    server = HTTPServer(("localhost", 8080), CallbackHandler)

    def run_server():
        server.handle_request()

    server_thread = Thread(target=run_server, daemon=True)
    server_thread.start()

    # Open browser for authorization
    print(f"\n  Opening browser for authorization to {instance_name}...")
    print(f"  If browser doesn't open, visit: {authorization_url}\n")
    webbrowser.open(authorization_url)

    # Wait for callback
    server_thread.join(timeout=120)  # 2 minute timeout

    if not auth_code["code"]:
        print(
            f"  ❌ Authorization failed: {auth_code.get('error', 'Timeout')}",
            file=sys.stderr,
        )
        return None

    # Exchange authorization code for access token
    token_data = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "code": auth_code["code"],
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
    }

    try:
        response = requests.post(token_url, data=token_data, timeout=30)
        response.raise_for_status()

        token_response = response.json()
        return token_response.get("access_token")

    except Exception as e:
        print(f"  ❌ Error exchanging code for token: {e}", file=sys.stderr)
        return None


def create_authenticated_session(instance_name: str) -> requests.Session | None:
    """
    Create an authenticated requests session for Superset API.

    Args:
        instance_name: Name of the instance (e.g., 'superset-qa')

    Returns:
        Authenticated requests Session or None if failed
    """
    config = get_instance_config(instance_name)
    if not config or not config.get("url"):
        return None

    # Get OAuth token
    access_token = get_oauth_token_with_pkce(instance_name)
    if not access_token:
        return None

    # Create session with auth header
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
    )

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
            filters = (
                [{"col": "kind", "opr": "eq", "value": "physical"}]
                if physical_only
                else []
            )
            params = {
                "q": json.dumps(
                    {
                        "page": page,
                        "page_size": page_size,
                        "filters": filters,
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
                datasets.append(
                    {
                        "id": item.get("id"),
                        "uuid": item.get("uuid"),
                        "table_name": item.get("table_name"),
                        "schema": item.get("schema"),
                        "kind": item.get("kind"),
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
