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
