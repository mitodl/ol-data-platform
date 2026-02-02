"""Database UUID mapping utilities."""

import json
import re
import subprocess
import sys
from pathlib import Path

import yaml


def get_target_databases(instance_name: str) -> dict[str, str]:
    """
    Fetch databases from target Superset instance using sup CLI.

    Args:
        instance_name: Target instance name

    Returns:
        Dict mapping database_name to uuid
    """
    result = subprocess.run(
        ["sup", "database", "list", "--instance", instance_name, "--json"],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        print(f"Error: Failed to fetch databases from {instance_name}", file=sys.stderr)
        sys.exit(1)

    # Find the start of JSON array (skip spinner output)
    start_idx = result.stdout.find("[")
    if start_idx == -1:
        print("Error: Could not extract database list from sup output", file=sys.stderr)
        sys.exit(1)

    # Find the end of JSON array
    end_idx = result.stdout.rfind("]")
    if end_idx == -1:
        end_idx = len(result.stdout)
    else:
        end_idx += 1

    json_text = result.stdout[start_idx:end_idx]

    # Try to parse JSON, fall back to regex if needed
    try:
        db_list = json.loads(json_text)
        return {db["database_name"]: db["uuid"] for db in db_list}
    except json.JSONDecodeError:
        # Fall back: extract UUID/name pairs using regex
        name_pattern = r'"database_name":\s*"([^"]+)"'
        uuid_pattern = r'"uuid":\s*"([0-9a-f-]{36})"'

        names = re.findall(name_pattern, json_text)
        uuids = re.findall(uuid_pattern, json_text)

        if len(names) == len(uuids):
            return dict(zip(names, uuids, strict=False))

        print(
            "Error: Could not parse database information from sup CLI",
            file=sys.stderr,
        )
        sys.exit(1)


def load_source_databases(assets_dir: Path) -> dict[str, str]:
    """
    Load source database configurations from exported assets.

    Args:
        assets_dir: Path to assets directory

    Returns:
        Dict mapping database_name to uuid
    """
    db_dir = assets_dir / "databases"
    if not db_dir.exists():
        print(
            f"Error: No databases directory found in {assets_dir}",
            file=sys.stderr,
        )
        sys.exit(1)

    databases = {}
    for db_file in db_dir.glob("*.yaml"):
        with db_file.open() as f:
            config = yaml.safe_load(f)
            databases[config["database_name"]] = config["uuid"]

    return databases


def build_uuid_mapping(
    source_dbs: dict[str, str], target_dbs: dict[str, str]
) -> dict[str, str]:
    """
    Build mapping from source UUIDs to target UUIDs based on database names.

    Args:
        source_dbs: Source database name -> UUID mapping
        target_dbs: Target database name -> UUID mapping

    Returns:
        Dict mapping source_uuid to target_uuid
    """
    mapping = {}

    for db_name, source_uuid in source_dbs.items():
        if db_name not in target_dbs:
            print(
                f"Warning: Database '{db_name}' not found in target instance",
                file=sys.stderr,
            )
            continue

        target_uuid = target_dbs[db_name]
        if source_uuid != target_uuid:
            mapping[source_uuid] = target_uuid

    return mapping


def rewrite_uuids_in_file(file_path: Path, uuid_mapping: dict[str, str]) -> int:
    """
    Rewrite database UUIDs in a YAML file.

    Args:
        file_path: Path to YAML file
        uuid_mapping: Source UUID -> target UUID mapping

    Returns:
        Number of replacements made
    """
    with file_path.open() as f:
        content = f.read()

    replacements = 0
    for source_uuid, target_uuid in uuid_mapping.items():
        if source_uuid in content:
            content = content.replace(source_uuid, target_uuid)
            replacements += 1

    if replacements > 0:
        with file_path.open("w") as f:
            f.write(content)

    return replacements


def rewrite_all_assets(assets_dir: Path, uuid_mapping: dict[str, str]) -> None:
    """
    Rewrite database UUIDs in all asset files.

    Args:
        assets_dir: Path to assets directory
        uuid_mapping: Source UUID -> target UUID mapping
    """
    total_files = 0
    total_replacements = 0

    # Update database configs first
    db_dir = assets_dir / "databases"
    if db_dir.exists():
        for yaml_file in db_dir.glob("*.yaml"):
            replacements = rewrite_uuids_in_file(yaml_file, uuid_mapping)
            if replacements > 0:
                total_files += 1
                total_replacements += replacements

    # Update datasets, charts, dashboards
    for asset_type in ["datasets", "charts", "dashboards"]:
        asset_dir = assets_dir / asset_type
        if not asset_dir.exists():
            continue

        for yaml_file in asset_dir.rglob("*.yaml"):
            replacements = rewrite_uuids_in_file(yaml_file, uuid_mapping)
            if replacements > 0:
                total_files += 1
                total_replacements += replacements

    if total_replacements > 0:
        print(
            f"  Remapped {total_replacements} database references "
            f"across {total_files} files"
        )


def map_database_uuids(target_instance: str, assets_dir: Path) -> None:
    """
    Map database UUIDs from source assets to target instance.

    Args:
        target_instance: Target instance name
        assets_dir: Path to assets directory
    """
    print(f"  Mapping database UUIDs for {target_instance}...")

    source_dbs = load_source_databases(assets_dir)
    target_dbs = get_target_databases(target_instance)
    uuid_mapping = build_uuid_mapping(source_dbs, target_dbs)

    if not uuid_mapping:
        print("  No database UUID mapping needed (UUIDs already match)")
        return

    rewrite_all_assets(assets_dir, uuid_mapping)
