"""Database UUID mapping utilities."""

import json
import re
import sys
from pathlib import Path

import yaml


def get_target_databases(instance_name: str) -> dict[str, str]:
    """
    Fetch databases from the target Superset instance via its REST API.

    Uses the same OAuth credentials as the rest of the promote workflow
    (via ``create_authenticated_session``) rather than the ``sup`` CLI, so
    this works in non-interactive environments (e.g. Concourse CI) where
    the ``sup`` CLI session may not be pre-authenticated.

    Args:
        instance_name: Target instance name (must be in ~/.sup/config.yml)

    Returns:
        Dict mapping database_name to uuid
    """
    # Import here to avoid a circular dependency between the two lib modules.
    from ol_superset.lib.superset_api import (  # noqa: PLC0415
        create_authenticated_session,
        get_instance_config,
    )

    config = get_instance_config(instance_name)
    base_url = config.get("url", "").rstrip("/")
    if not base_url:
        print(
            f"Error: No URL configured for instance '{instance_name}'",
            file=sys.stderr,
        )
        sys.exit(1)

    session = create_authenticated_session(instance_name)
    if session is None:
        print(
            f"Error: Failed to authenticate with '{instance_name}'",
            file=sys.stderr,
        )
        sys.exit(1)

    endpoint = f"{base_url}/api/v1/database/"
    databases: dict[str, str] = {}
    page = 0
    page_size = 100

    try:
        while True:
            params = {"q": json.dumps({"page": page, "page_size": page_size})}
            response = session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            results = data.get("result", [])
            if not results:
                break
            for db in results:
                name = db.get("database_name")
                uuid = db.get("uuid")
                if name and uuid:
                    databases[name] = uuid
            page += 1
            if len(results) < page_size:
                break
    except Exception as e:  # noqa: BLE001
        print(
            f"Error: Failed to fetch databases from {instance_name}: {e}",
            file=sys.stderr,
        )
        sys.exit(1)

    return databases


def rewrite_all_assets(assets_dir: Path, target_dbs: dict[str, str]) -> None:
    """
    Rewrite database UUIDs in all asset files using name-based lookup.

    Database YAML files are updated by matching their ``database_name`` field
    against the target instance.  Dataset files are updated by matching the
    subdirectory they live in (``datasets/<DatabaseName>/``) against the target
    instance.  This handles datasets that were exported from a different
    environment or when a database connection was recreated and received a new
    UUID, both of which would cause the old UUID-to-UUID mapping approach to
    silently miss affected files.

    Args:
        assets_dir: Path to assets directory
        target_dbs: Database name -> UUID mapping fetched from the target instance
    """
    total_files = 0

    # Update database config files: force uuid to match target by name.
    db_dir = assets_dir / "databases"
    if db_dir.exists():
        for yaml_file in db_dir.glob("*.yaml"):
            with yaml_file.open() as f:
                content = f.read()
            config = yaml.safe_load(content)
            db_name = config.get("database_name")
            if db_name not in target_dbs:
                print(
                    f"  Warning: database '{db_name}' not found in target instance",
                    file=sys.stderr,
                )
                continue
            target_uuid = target_dbs[db_name]
            new_content = re.sub(
                r"^(uuid:\s*)[0-9a-f-]{36}",
                rf"\g<1>{target_uuid}",
                content,
                flags=re.MULTILINE,
            )
            if new_content != content:
                with yaml_file.open("w") as f:
                    f.write(new_content)
                total_files += 1

    # Update dataset files: the immediate subdirectory name under datasets/ is
    # always the database name (e.g. datasets/Trino/<dataset>.yaml).  Use that
    # name to look up the correct target UUID and overwrite whatever
    # database_uuid the file currently contains — this covers stale UUIDs from
    # old connections, cross-environment exports, etc.
    datasets_dir = assets_dir / "datasets"
    if datasets_dir.exists():
        for db_subdir in datasets_dir.iterdir():
            if not db_subdir.is_dir():
                continue
            db_name = db_subdir.name
            if db_name not in target_dbs:
                print(
                    f"  Warning: datasets/{db_name}/ has no matching database "
                    "in target instance",
                    file=sys.stderr,
                )
                continue
            target_uuid = target_dbs[db_name]
            for yaml_file in db_subdir.rglob("*.yaml"):
                with yaml_file.open() as f:
                    content = f.read()
                new_content = re.sub(
                    r"^(database_uuid:\s*)[0-9a-f-]{36}",
                    rf"\g<1>{target_uuid}",
                    content,
                    flags=re.MULTILINE,
                )
                if new_content != content:
                    with yaml_file.open("w") as f:
                        f.write(new_content)
                    total_files += 1

    if total_files > 0:
        print(f"  Remapped database UUIDs across {total_files} files")
    else:
        print("  All database UUIDs already match target — no changes needed")


def map_database_uuids(target_instance: str, assets_dir: Path) -> None:
    """
    Map database UUIDs from source assets to target instance.

    Fetches the authoritative UUID for each database from the target Superset
    instance by name, then rewrites every database config and dataset file so
    that all UUID references point at the correct target-instance values.

    Args:
        target_instance: Target instance name
        assets_dir: Path to assets directory
    """
    print(f"  Mapping database UUIDs for {target_instance}...")

    target_dbs = get_target_databases(target_instance)
    rewrite_all_assets(assets_dir, target_dbs)
